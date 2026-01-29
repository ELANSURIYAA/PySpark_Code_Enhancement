_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Migrate Mortgage Amendment to MISMO XML Graph to PySpark
## *Version*: 1 
## *Updated on*: 
_____________________________________________

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import sys
import re
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MortgageAmendmentToMISMOXML:
    def __init__(self):
        """Initialize Spark session with Delta Lake support"""
        self.spark = SparkSession.getActiveSession()
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("G_MtgAmendment_To_MISMO_XML") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        
        # Define schemas
        self.kafka_event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("json_payload", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        self.event_meta_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("amendment_type", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("new_rate", StringType(), True),
            StructField("json_payload", StringType(), True)
        ])
        
        self.template_record_schema = StructType([
            StructField("template_id", StringType(), True),
            StructField("xml_template", StringType(), True)
        ])
        
        self.reject_record_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("reject_reason", StringType(), True),
            StructField("reject_type", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        self.xml_out_record_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("xml_content", StringType(), True),
            StructField("processed_timestamp", TimestampType(), True)
        ])
    
    def json_find_string_value(self, json_str, key):
        """UDF to extract values from JSON string using regex"""
        if json_str is None:
            return None
        pattern = f'"({key})"\s*:\s*"([^"]*?)"'
        match = re.search(pattern, json_str)
        return match.group(2) if match else None
    
    def business_reject_reason(self, amendment_type, new_rate, effective_date):
        """UDF to validate business rules"""
        if effective_date is None or effective_date == "":
            return "Missing effective_date"
        if amendment_type == "RATE_CHANGE" and (new_rate is None or new_rate == ""):
            return "Missing new_rate for RATE_CHANGE"
        return None
    
    def render_xml(self, template, event_id, loan_id, amendment_type, effective_date, new_rate):
        """UDF to render XML template with actual values"""
        if template is None:
            return None
        
        xml_content = template
        xml_content = xml_content.replace("{{LOAN_ID}}", str(loan_id) if loan_id else "")
        xml_content = xml_content.replace("{{EVENT_ID}}", str(event_id) if event_id else "")
        xml_content = xml_content.replace("{{AMENDMENT_TYPE}}", str(amendment_type) if amendment_type else "")
        xml_content = xml_content.replace("{{EFFECTIVE_DATE}}", str(effective_date) if effective_date else "")
        xml_content = xml_content.replace("{{NEW_RATE}}", str(new_rate) if new_rate else "")
        
        return xml_content
    
    def create_sample_data(self):
        """Create sample data for testing"""
        # Sample landing data
        landing_data = [
            ("evt_001", "loan_12345", "MORTGAGE_AMENDMENT", '{"amendment_type":"RATE_CHANGE","effective_date":"2024-01-15","new_rate":"4.5"}', datetime.now()),
            ("evt_002", "loan_12346", "MORTGAGE_AMENDMENT", '{"amendment_type":"TERM_CHANGE","effective_date":"2024-01-16","new_term":"30"}', datetime.now()),
            ("evt_003", None, "MORTGAGE_AMENDMENT", '{"amendment_type":"RATE_CHANGE","effective_date":"2024-01-17","new_rate":"3.8"}', datetime.now()),  # Schema reject - missing loan_id
            ("evt_004", "loan_12348", "MORTGAGE_AMENDMENT", '{"amendment_type":"RATE_CHANGE","new_rate":"5.2"}', datetime.now()),  # Business reject - missing effective_date
            ("evt_005", "loan_12349", "MORTGAGE_AMENDMENT", '{"amendment_type":"RATE_CHANGE","effective_date":"2024-01-18"}', datetime.now())  # Business reject - missing new_rate for RATE_CHANGE
        ]
        
        # Sample template data
        template_data = [
            ("MISMO_TEMPLATE_001", 
             """<?xml version="1.0" encoding="UTF-8"?>
<MISMO_DOCUMENT>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
</MISMO_DOCUMENT>""")
        ]
        
        return landing_data, template_data
    
    def read_landing_files(self, landing_data):
        """Read and process landing files"""
        logger.info("Reading landing files...")
        
        # Create DataFrame from sample data
        landing_df = self.spark.createDataFrame(landing_data, self.kafka_event_schema)
        
        # Register UDFs
        json_find_udf = udf(self.json_find_string_value, StringType())
        
        # Extract metadata from JSON payload
        event_meta_df = landing_df.select(
            col("event_id"),
            col("loan_id"),
            col("event_type"),
            json_find_udf(col("json_payload"), lit("amendment_type")).alias("amendment_type"),
            json_find_udf(col("json_payload"), lit("effective_date")).alias("effective_date"),
            json_find_udf(col("json_payload"), lit("new_rate")).alias("new_rate"),
            col("json_payload")
        )
        
        return landing_df, event_meta_df
    
    def validate_events(self, event_meta_df):
        """Validate events and separate valid from rejected records"""
        logger.info("Validating events...")
        
        # Register UDF for business validation
        business_reject_udf = udf(self.business_reject_reason, StringType())
        
        # Schema validation - filter records with missing required fields
        schema_rejects = event_meta_df.filter(
            (col("event_id").isNull()) | 
            (col("loan_id").isNull()) | 
            (col("event_type") != "MORTGAGE_AMENDMENT")
        ).select(
            col("event_id"),
            col("loan_id"),
            lit("Schema validation failed").alias("reject_reason"),
            lit("SCHEMA_REJECT").alias("reject_type"),
            current_timestamp().alias("timestamp")
        )
        
        # Valid schema records
        valid_schema_df = event_meta_df.filter(
            (col("event_id").isNotNull()) & 
            (col("loan_id").isNotNull()) & 
            (col("event_type") == "MORTGAGE_AMENDMENT")
        )
        
        # Business validation
        business_validated_df = valid_schema_df.withColumn(
            "business_reject_reason",
            business_reject_udf(col("amendment_type"), col("new_rate"), col("effective_date"))
        )
        
        # Business rejects
        business_rejects = business_validated_df.filter(
            col("business_reject_reason").isNotNull()
        ).select(
            col("event_id"),
            col("loan_id"),
            col("business_reject_reason").alias("reject_reason"),
            lit("BUSINESS_REJECT").alias("reject_type"),
            current_timestamp().alias("timestamp")
        )
        
        # Valid records
        valid_records = business_validated_df.filter(
            col("business_reject_reason").isNull()
        ).drop("business_reject_reason")
        
        # Combine all rejects
        all_rejects = schema_rejects.union(business_rejects)
        
        return valid_records, all_rejects
    
    def generate_xml(self, valid_records, template_data):
        """Generate XML using template join"""
        logger.info("Generating XML...")
        
        # Create template DataFrame
        template_df = self.spark.createDataFrame(template_data, self.template_record_schema)
        
        # Register XML rendering UDF
        render_xml_udf = udf(self.render_xml, StringType())
        
        # Broadcast cross join with template (since template is single record)
        xml_output = valid_records.crossJoin(broadcast(template_df)).select(
            col("event_id"),
            col("loan_id"),
            render_xml_udf(
                col("xml_template"),
                col("event_id"),
                col("loan_id"),
                col("amendment_type"),
                col("effective_date"),
                col("new_rate")
            ).alias("xml_content"),
            current_timestamp().alias("processed_timestamp")
        )
        
        return xml_output
    
    def write_to_delta(self, df, table_path, mode="overwrite"):
        """Write DataFrame to Delta table"""
        logger.info(f"Writing to Delta table: {table_path}")
        df.write.format("delta").mode(mode).save(table_path)
    
    def read_from_delta(self, table_path):
        """Read DataFrame from Delta table"""
        logger.info(f"Reading from Delta table: {table_path}")
        return self.spark.read.format("delta").load(table_path)
    
    def run_etl_pipeline(self):
        """Main ETL pipeline execution"""
        try:
            logger.info("Starting Mortgage Amendment to MISMO XML ETL Pipeline...")
            
            # Create sample data
            landing_data, template_data = self.create_sample_data()
            
            # Step 1: Read landing files
            raw_events_df, event_meta_df = self.read_landing_files(landing_data)
            
            # Step 2: Persist raw events (before any filtering)
            self.write_to_delta(raw_events_df, "/tmp/delta/raw_events")
            logger.info("Raw events persisted successfully")
            
            # Step 3: Validate events
            valid_records, reject_records = self.validate_events(event_meta_df)
            
            # Step 4: Persist reject records
            if reject_records.count() > 0:
                self.write_to_delta(reject_records, "/tmp/delta/reject_records")
                logger.info(f"Reject records persisted: {reject_records.count()}")
            
            # Step 5: Generate XML for valid records
            if valid_records.count() > 0:
                xml_output = self.generate_xml(valid_records, template_data)
                
                # Step 6: Persist XML output
                self.write_to_delta(xml_output, "/tmp/delta/xml_output")
                logger.info(f"XML output generated and persisted: {xml_output.count()}")
                
                # Display sample XML output
                logger.info("Sample XML Output:")
                xml_output.select("event_id", "loan_id", "xml_content").show(truncate=False)
            
            # Step 7: Data validation - ensure no data loss
            raw_count = raw_events_df.count()
            valid_count = valid_records.count() if valid_records.count() > 0 else 0
            reject_count = reject_records.count() if reject_records.count() > 0 else 0
            
            logger.info(f"Data Validation - Raw: {raw_count}, Valid: {valid_count}, Rejected: {reject_count}")
            logger.info(f"Data Loss Check: {raw_count == (valid_count + reject_count)}")
            
            logger.info("ETL Pipeline completed successfully!")
            
            return {
                "raw_events": raw_events_df,
                "valid_records": valid_records,
                "reject_records": reject_records,
                "xml_output": xml_output if valid_records.count() > 0 else None
            }
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            raise e

# Main execution
if __name__ == "__main__":
    # Initialize and run the ETL pipeline
    etl_job = MortgageAmendmentToMISMOXML()
    results = etl_job.run_etl_pipeline()
    
    print("\n=== ETL Pipeline Execution Summary ===")
    print(f"Raw Events Count: {results['raw_events'].count()}")
    print(f"Valid Records Count: {results['valid_records'].count()}")
    print(f"Reject Records Count: {results['reject_records'].count()}")
    if results['xml_output']:
        print(f"XML Output Count: {results['xml_output'].count()}")
    print("=== Pipeline Completed Successfully ===")
