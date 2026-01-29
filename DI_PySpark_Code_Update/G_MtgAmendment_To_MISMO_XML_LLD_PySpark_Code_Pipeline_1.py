_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: PySpark ETL job for migrating Mortgage Amendment to MISMO XML generation from Ab Initio to Spark/Databricks platform
## *Version*: 1 
## *Updated on*: 
_____________________________________________

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import sys
import logging
import re
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MortgageAmendmentToMISMOXML:
    def __init__(self):
        """Initialize the Mortgage Amendment to MISMO XML ETL job"""
        self.spark = self._get_spark_session()
        self.setup_schemas()
        self.setup_paths()
        
    def _get_spark_session(self):
        """Get or create Spark session compatible with Spark Connect"""
        try:
            # Try to get active session first (Spark Connect compatible)
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("MortgageAmendmentToMISMOXML") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def setup_schemas(self):
        """Define StructType schemas for all data structures"""
        # Kafka event input schema
        self.kafka_event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("json_payload", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # Event metadata intermediate schema
        self.event_meta_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("amendment_type", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("new_rate", StringType(), True),
            StructField("borrower_name", StringType(), True),
            StructField("property_address", StringType(), True)
        ])
        
        # Template record schema
        self.template_record_schema = StructType([
            StructField("template_id", StringType(), True),
            StructField("xml_template", StringType(), True)
        ])
        
        # Reject record schema
        self.reject_record_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("reject_reason", StringType(), True),
            StructField("reject_type", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # XML output record schema
        self.xml_out_record_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("xml_content", StringType(), True),
            StructField("generated_timestamp", TimestampType(), True)
        ])
    
    def setup_paths(self):
        """Configure dynamic file paths"""
        # Use current timestamp if not provided via sys.argv
        timestamp = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")
        
        self.LANDING_DIR = f"/tmp/landing/amendment_events_{timestamp}.dat"
        self.RAW_OUT = f"/tmp/raw_events/{timestamp}/"
        self.MISMO_OUT = f"/tmp/mismo_xml/{timestamp}/"
        self.ERROR_LOG_PATH = f"/tmp/rejects/{timestamp}/"
        self.TEMPLATE_PATH = "/tmp/lookup/mismo_template_record.dat"
        
        logger.info(f"Configured paths for timestamp: {timestamp}")
    
    def json_find_string_value(self, json_str, key):
        """UDF to extract scalar fields from JSON payload using regex"""
        if json_str is None:
            return None
        pattern = f'"({key})"\s*:\s*"([^"]*?)"'
        match = re.search(pattern, json_str)
        return match.group(2) if match else None
    
    def business_reject_reason(self, amendment_type, new_rate, effective_date):
        """UDF to validate business logic and return reject reason"""
        if effective_date is None or effective_date == "":
            return "Missing effective_date"
        if amendment_type == "RATE_CHANGE" and (new_rate is None or new_rate == ""):
            return "Missing new_rate for RATE_CHANGE"
        return None
    
    def render_xml(self, template, event_data):
        """UDF to perform token replacement on XML template"""
        if template is None or event_data is None:
            return None
        
        xml_content = template
        # Replace common tokens
        xml_content = xml_content.replace("{{LOAN_ID}}", str(event_data.get('loan_id', '')))
        xml_content = xml_content.replace("{{EVENT_ID}}", str(event_data.get('event_id', '')))
        xml_content = xml_content.replace("{{AMENDMENT_TYPE}}", str(event_data.get('amendment_type', '')))
        xml_content = xml_content.replace("{{EFFECTIVE_DATE}}", str(event_data.get('effective_date', '')))
        xml_content = xml_content.replace("{{NEW_RATE}}", str(event_data.get('new_rate', '')))
        xml_content = xml_content.replace("{{BORROWER_NAME}}", str(event_data.get('borrower_name', '')))
        xml_content = xml_content.replace("{{PROPERTY_ADDRESS}}", str(event_data.get('property_address', '')))
        
        return xml_content
    
    def create_sample_data(self):
        """Create sample data for testing purposes"""
        # Sample landing data
        landing_data = [
            ("evt_001", "loan_12345", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-01-15","new_rate":"3.75","borrower_name":"John Doe","property_address":"123 Main St"}',
             datetime.now()),
            ("evt_002", "loan_67890", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"TERM_CHANGE","effective_date":"2024-01-20","new_term":"360","borrower_name":"Jane Smith","property_address":"456 Oak Ave"}',
             datetime.now()),
            ("evt_003", None, "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-01-25","new_rate":"4.25"}',
             datetime.now()),  # Schema reject - missing loan_id
            ("evt_004", "loan_11111", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","new_rate":"3.50","borrower_name":"Bob Johnson"}',
             datetime.now())  # Business reject - missing effective_date
        ]
        
        # Sample template data
        template_data = [(
            "MISMO_TEMPLATE_001",
            '''<?xml version="1.0" encoding="UTF-8"?>
<MISMO_AMENDMENT>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
    <BORROWER_NAME>{{BORROWER_NAME}}</BORROWER_NAME>
    <PROPERTY_ADDRESS>{{PROPERTY_ADDRESS}}</PROPERTY_ADDRESS>
    <GENERATED_TIMESTAMP>{}</GENERATED_TIMESTAMP>
</MISMO_AMENDMENT>'''.format(datetime.now().isoformat())
        )]
        
        return landing_data, template_data
    
    def ingest_and_extract_metadata(self, landing_data):
        """Ingest landing files and extract metadata"""
        logger.info("Starting data ingestion and metadata extraction")
        
        # Create DataFrame from sample data
        landing_df = self.spark.createDataFrame(landing_data, self.kafka_event_schema)
        
        # Register UDFs
        json_find_udf = udf(self.json_find_string_value, StringType())
        
        # Extract metadata using regex-based UDFs
        events_with_meta = landing_df.withColumn(
            "amendment_type", json_find_udf(col("json_payload"), lit("amendment_type"))
        ).withColumn(
            "effective_date", json_find_udf(col("json_payload"), lit("effective_date"))
        ).withColumn(
            "new_rate", json_find_udf(col("json_payload"), lit("new_rate"))
        ).withColumn(
            "borrower_name", json_find_udf(col("json_payload"), lit("borrower_name"))
        ).withColumn(
            "property_address", json_find_udf(col("json_payload"), lit("property_address"))
        )
        
        # Persist raw events immediately after extraction
        raw_events = events_with_meta.select(
            "event_id", "loan_id", "event_type", "amendment_type", 
            "effective_date", "new_rate", "borrower_name", "property_address", "timestamp"
        )
        
        logger.info(f"Extracted metadata for {raw_events.count()} events")
        return raw_events
    
    def validate_events(self, events_df):
        """Implement validation logic for schema and business rules"""
        logger.info("Starting event validation")
        
        # Schema validation - filter records where event_type != 'MORTGAGE_AMENDMENT' or missing critical fields
        schema_rejects = events_df.filter(
            (col("event_type") != "MORTGAGE_AMENDMENT") |
            (col("event_id").isNull()) |
            (col("loan_id").isNull())
        ).withColumn("reject_reason", lit("Schema validation failed")) \
         .withColumn("reject_type", lit("SCHEMA_REJECT")) \
         .withColumn("timestamp", current_timestamp())
        
        # Valid events after schema check
        valid_events = events_df.filter(
            (col("event_type") == "MORTGAGE_AMENDMENT") &
            (col("event_id").isNotNull()) &
            (col("loan_id").isNotNull())
        )
        
        # Business validation
        business_reject_udf = udf(self.business_reject_reason, StringType())
        
        events_with_business_check = valid_events.withColumn(
            "business_reject_reason", 
            business_reject_udf(col("amendment_type"), col("new_rate"), col("effective_date"))
        )
        
        # Business rejects
        business_rejects = events_with_business_check.filter(
            col("business_reject_reason").isNotNull()
        ).withColumn("reject_reason", col("business_reject_reason")) \
         .withColumn("reject_type", lit("BUSINESS_REJECT")) \
         .withColumn("timestamp", current_timestamp()) \
         .select("event_id", "loan_id", "reject_reason", "reject_type", "timestamp")
        
        # Final valid events
        final_valid_events = events_with_business_check.filter(
            col("business_reject_reason").isNull()
        ).drop("business_reject_reason")
        
        # Combine all rejects
        all_rejects = schema_rejects.select("event_id", "loan_id", "reject_reason", "reject_type", "timestamp") \
                                   .union(business_rejects)
        
        logger.info(f"Validation complete: {final_valid_events.count()} valid, {all_rejects.count()} rejected")
        return final_valid_events, all_rejects
    
    def generate_xml(self, valid_events, template_data):
        """Generate MISMO XML using template join and token replacement"""
        logger.info("Starting XML generation")
        
        # Create template DataFrame
        template_df = self.spark.createDataFrame(template_data, self.template_record_schema)
        
        # Broadcast cross join (template is single record)
        events_with_template = valid_events.crossJoin(broadcast(template_df))
        
        # Register render XML UDF
        render_xml_udf = udf(lambda template, event_data: self.render_xml(template, event_data), StringType())
        
        # Create event data map for token replacement
        events_with_xml = events_with_template.withColumn(
            "event_data_map", 
            create_map(
                lit("loan_id"), col("loan_id"),
                lit("event_id"), col("event_id"),
                lit("amendment_type"), col("amendment_type"),
                lit("effective_date"), col("effective_date"),
                lit("new_rate"), col("new_rate"),
                lit("borrower_name"), col("borrower_name"),
                lit("property_address"), col("property_address")
            )
        )
        
        # Generate XML content using token replacement
        xml_output = events_with_xml.withColumn(
            "xml_content",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(col("xml_template"), 
                                                 "\\{\\{LOAN_ID\\}\\}", coalesce(col("loan_id"), lit(""))),
                                                "\\{\\{EVENT_ID\\}\\}", coalesce(col("event_id"), lit(""))),
                                            "\\{\\{AMENDMENT_TYPE\\}\\}", coalesce(col("amendment_type"), lit(""))),
                                        "\\{\\{EFFECTIVE_DATE\\}\\}", coalesce(col("effective_date"), lit(""))),
                                    "\\{\\{NEW_RATE\\}\\}", coalesce(col("new_rate"), lit(""))),
                                "\\{\\{BORROWER_NAME\\}\\}", coalesce(col("borrower_name"), lit(""))),
                            "\\{\\{PROPERTY_ADDRESS\\}\\}", coalesce(col("property_address"), lit("")))
        ).withColumn("generated_timestamp", current_timestamp()) \
         .select("event_id", "loan_id", "xml_content", "generated_timestamp")
        
        logger.info(f"Generated XML for {xml_output.count()} events")
        return xml_output
    
    def write_to_delta(self, df, path, table_name):
        """Write DataFrame to Delta table"""
        try:
            df.write.format("delta").mode("overwrite").option("path", path).saveAsTable(table_name)
            logger.info(f"Successfully wrote {df.count()} records to {table_name}")
        except Exception as e:
            logger.error(f"Error writing to Delta table {table_name}: {e}")
            raise
    
    def read_from_delta(self, path):
        """Read DataFrame from Delta table"""
        try:
            return self.spark.read.format("delta").load(path)
        except Exception as e:
            logger.error(f"Error reading from Delta path {path}: {e}")
            raise
    
    def run_etl(self):
        """Execute the complete ETL pipeline"""
        logger.info("Starting Mortgage Amendment to MISMO XML ETL job")
        
        try:
            # Create sample data
            landing_data, template_data = self.create_sample_data()
            
            # Step 1: Ingest and extract metadata
            raw_events = self.ingest_and_extract_metadata(landing_data)
            
            # Persist raw events
            self.write_to_delta(raw_events, self.RAW_OUT, "raw_mortgage_events")
            
            # Step 2: Validate events
            valid_events, rejects = self.validate_events(raw_events)
            
            # Persist rejects
            if rejects.count() > 0:
                self.write_to_delta(rejects, self.ERROR_LOG_PATH, "mortgage_rejects")
            
            # Step 3: Generate XML
            xml_output = self.generate_xml(valid_events, template_data)
            
            # Persist XML output
            self.write_to_delta(xml_output, self.MISMO_OUT, "mismo_xml_output")
            
            # Validation: Ensure no data loss
            total_input = raw_events.count()
            total_output = xml_output.count()
            total_rejects = rejects.count()
            
            logger.info(f"ETL Summary: Input={total_input}, Output={total_output}, Rejects={total_rejects}")
            
            if total_input != (total_output + total_rejects):
                logger.warning("Data loss detected! Input count doesn't match output + rejects")
            else:
                logger.info("Data integrity validated - no data loss")
            
            return xml_output, rejects, raw_events
            
        except Exception as e:
            logger.error(f"ETL job failed: {e}")
            raise
        finally:
            logger.info("ETL job completed")

# Main execution
if __name__ == "__main__":
    etl_job = MortgageAmendmentToMISMOXML()
    xml_output, rejects, raw_events = etl_job.run_etl()
    
    # Display results
    print("\n=== ETL Job Results ===")
    print(f"Raw Events Count: {raw_events.count()}")
    print(f"XML Output Count: {xml_output.count()}")
    print(f"Rejects Count: {rejects.count()}")
    
    print("\n=== Sample XML Output ===")
    xml_output.select("event_id", "loan_id", "xml_content").show(2, truncate=False)
    
    print("\n=== Sample Rejects ===")
    if rejects.count() > 0:
        rejects.show(truncate=False)
    else:
        print("No rejects found")
