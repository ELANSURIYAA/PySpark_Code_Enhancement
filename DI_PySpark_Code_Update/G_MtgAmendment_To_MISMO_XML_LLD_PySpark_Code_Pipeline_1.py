"""
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: PySpark ETL job to migrate Mortgage Amendment to MISMO XML generation from Ab Initio to Spark/Databricks platform
## *Version*: 1 
## *Updated on*: 
_____________________________________________
"""

import sys
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import broadcast
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MortgageAmendmentToMISMOXML:
    def __init__(self):
        """Initialize the PySpark ETL job for Mortgage Amendment to MISMO XML conversion"""
        self.spark = self._get_spark_session()
        self.setup_schemas()
        
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
        # Input Kafka Event Schema
        self.kafka_event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("json_payload", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # Event Metadata Schema (after JSON parsing)
        self.event_meta_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("amendment_type", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("new_rate", DoubleType(), True),
            StructField("borrower_name", StringType(), True),
            StructField("property_address", StringType(), True),
            StructField("loan_amount", DoubleType(), True)
        ])
        
        # Template Record Schema
        self.template_schema = StructType([
            StructField("template_id", StringType(), True),
            StructField("xml_template", StringType(), True)
        ])
        
        # Reject Record Schema
        self.reject_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("reject_reason", StringType(), True),
            StructField("reject_type", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # XML Output Schema
        self.xml_output_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("xml_content", StringType(), True),
            StructField("generated_timestamp", TimestampType(), True)
        ])
    
    def create_sample_data(self):
        """Create sample data for testing since we don't have actual tables"""
        # Sample Kafka events data
        kafka_events_data = [
            ("evt_001", "LOAN_12345", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-01-15","new_rate":4.5,"borrower_name":"John Doe","property_address":"123 Main St","loan_amount":250000}',
             datetime.now()),
            ("evt_002", "LOAN_67890", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"TERM_CHANGE","effective_date":"2024-01-20","borrower_name":"Jane Smith","property_address":"456 Oak Ave","loan_amount":300000}',
             datetime.now()),
            ("evt_003", None, "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","effective_date":"2024-01-25","new_rate":3.8}',
             datetime.now()),  # Missing loan_id - should be schema reject
            ("evt_004", "LOAN_11111", "MORTGAGE_AMENDMENT", 
             '{"amendment_type":"RATE_CHANGE","new_rate":5.2,"borrower_name":"Bob Johnson"}',
             datetime.now())   # Missing effective_date - should be business reject
        ]
        
        # Sample MISMO XML template
        template_data = [
            ("MISMO_TEMPLATE_001", 
             '''<?xml version="1.0" encoding="UTF-8"?>
<MISMO_LOAN_AMENDMENT>
    <EVENT_ID>{{EVENT_ID}}</EVENT_ID>
    <LOAN_ID>{{LOAN_ID}}</LOAN_ID>
    <AMENDMENT_TYPE>{{AMENDMENT_TYPE}}</AMENDMENT_TYPE>
    <EFFECTIVE_DATE>{{EFFECTIVE_DATE}}</EFFECTIVE_DATE>
    <NEW_RATE>{{NEW_RATE}}</NEW_RATE>
    <BORROWER_NAME>{{BORROWER_NAME}}</BORROWER_NAME>
    <PROPERTY_ADDRESS>{{PROPERTY_ADDRESS}}</PROPERTY_ADDRESS>
    <LOAN_AMOUNT>{{LOAN_AMOUNT}}</LOAN_AMOUNT>
    <GENERATED_TIMESTAMP>{{GENERATED_TIMESTAMP}}</GENERATED_TIMESTAMP>
</MISMO_LOAN_AMENDMENT>''')
        ]
        
        return kafka_events_data, template_data
    
    def json_find_string_value_udf(self):
        """UDF to extract values from JSON payload using regex (simulating legacy logic)"""
        def extract_json_value(json_str, field_name):
            if json_str is None:
                return None
            try:
                pattern = f'"({field_name})"\s*:\s*"([^"]*)"|"({field_name})"\s*:\s*([^,}}]*)'
                match = re.search(pattern, json_str)
                if match:
                    return match.group(2) if match.group(2) else match.group(4)
                return None
            except Exception:
                return None
        return udf(extract_json_value, StringType())
    
    def json_find_double_value_udf(self):
        """UDF to extract numeric values from JSON payload"""
        def extract_json_double(json_str, field_name):
            if json_str is None:
                return None
            try:
                pattern = f'"({field_name})"\s*:\s*([0-9.]+)'
                match = re.search(pattern, json_str)
                if match:
                    return float(match.group(2))
                return None
            except Exception:
                return None
        return udf(extract_json_double, DoubleType())
    
    def business_reject_reason_udf(self):
        """UDF to determine business rejection reasons"""
        def check_business_rules(amendment_type, effective_date, new_rate):
            if effective_date is None or effective_date == "":
                return "Missing effective_date"
            if amendment_type == "RATE_CHANGE" and (new_rate is None or new_rate == 0):
                return "Missing new_rate for RATE_CHANGE"
            return None
        return udf(check_business_rules, StringType())
    
    def render_xml_udf(self):
        """UDF to perform token replacement in XML template"""
        def replace_tokens(template, event_id, loan_id, amendment_type, effective_date, 
                          new_rate, borrower_name, property_address, loan_amount):
            if template is None:
                return None
            
            xml_content = template
            xml_content = xml_content.replace("{{EVENT_ID}}", str(event_id) if event_id else "")
            xml_content = xml_content.replace("{{LOAN_ID}}", str(loan_id) if loan_id else "")
            xml_content = xml_content.replace("{{AMENDMENT_TYPE}}", str(amendment_type) if amendment_type else "")
            xml_content = xml_content.replace("{{EFFECTIVE_DATE}}", str(effective_date) if effective_date else "")
            xml_content = xml_content.replace("{{NEW_RATE}}", str(new_rate) if new_rate else "")
            xml_content = xml_content.replace("{{BORROWER_NAME}}", str(borrower_name) if borrower_name else "")
            xml_content = xml_content.replace("{{PROPERTY_ADDRESS}}", str(property_address) if property_address else "")
            xml_content = xml_content.replace("{{LOAN_AMOUNT}}", str(loan_amount) if loan_amount else "")
            xml_content = xml_content.replace("{{GENERATED_TIMESTAMP}}", str(datetime.now()))
            
            return xml_content
        
        return udf(replace_tokens, StringType())
    
    def read_landing_files(self):
        """Read pipe-delimited landing files (simulated with sample data)"""
        logger.info("Reading landing files...")
        
        kafka_events_data, template_data = self.create_sample_data()
        
        # Create DataFrames from sample data
        raw_events_df = self.spark.createDataFrame(kafka_events_data, self.kafka_event_schema)
        template_df = self.spark.createDataFrame(template_data, self.template_schema)
        
        logger.info(f"Loaded {raw_events_df.count()} raw events")
        return raw_events_df, template_df
    
    def persist_raw_events(self, raw_events_df, output_path="/tmp/raw_events"):
        """Persist raw events before any processing"""
        logger.info(f"Persisting raw events to {output_path}")
        try:
            raw_events_df.write.mode("overwrite").option("path", output_path).saveAsTable("raw_events_table")
            logger.info("Raw events persisted successfully")
        except Exception as e:
            logger.error(f"Error persisting raw events: {e}")
    
    def extract_metadata(self, raw_events_df):
        """Extract metadata from JSON payload using regex UDFs"""
        logger.info("Extracting metadata from JSON payloads...")
        
        json_string_udf = self.json_find_string_value_udf()
        json_double_udf = self.json_find_double_value_udf()
        
        event_meta_df = raw_events_df.select(
            col("event_id"),
            col("loan_id"),
            col("event_type"),
            json_string_udf(col("json_payload"), lit("amendment_type")).alias("amendment_type"),
            json_string_udf(col("json_payload"), lit("effective_date")).alias("effective_date"),
            json_double_udf(col("json_payload"), lit("new_rate")).alias("new_rate"),
            json_string_udf(col("json_payload"), lit("borrower_name")).alias("borrower_name"),
            json_string_udf(col("json_payload"), lit("property_address")).alias("property_address"),
            json_double_udf(col("json_payload"), lit("loan_amount")).alias("loan_amount")
        )
        
        logger.info(f"Extracted metadata for {event_meta_df.count()} events")
        return event_meta_df
    
    def validate_schema(self, event_meta_df):
        """Perform schema validation and separate rejects"""
        logger.info("Performing schema validation...")
        
        # Schema validation: event_type must be 'MORTGAGE_AMENDMENT' and required fields must not be null
        valid_events = event_meta_df.filter(
            (col("event_type") == "MORTGAGE_AMENDMENT") &
            (col("event_id").isNotNull()) &
            (col("loan_id").isNotNull())
        )
        
        # Schema rejects
        schema_rejects = event_meta_df.filter(
            (col("event_type") != "MORTGAGE_AMENDMENT") |
            (col("event_id").isNull()) |
            (col("loan_id").isNull())
        ).select(
            col("event_id"),
            col("loan_id"),
            lit("Schema validation failed").alias("reject_reason"),
            lit("SCHEMA_REJECT").alias("reject_type"),
            current_timestamp().alias("timestamp")
        )
        
        logger.info(f"Schema validation: {valid_events.count()} valid, {schema_rejects.count()} rejects")
        return valid_events, schema_rejects
    
    def validate_business_rules(self, valid_events_df):
        """Perform business rule validation"""
        logger.info("Performing business rule validation...")
        
        business_reject_udf = self.business_reject_reason_udf()
        
        # Add business validation column
        events_with_validation = valid_events_df.withColumn(
            "business_reject_reason",
            business_reject_udf(col("amendment_type"), col("effective_date"), col("new_rate"))
        )
        
        # Separate business valid and reject records
        business_valid = events_with_validation.filter(col("business_reject_reason").isNull())
        
        business_rejects = events_with_validation.filter(col("business_reject_reason").isNotNull()).select(
            col("event_id"),
            col("loan_id"),
            col("business_reject_reason").alias("reject_reason"),
            lit("BUSINESS_REJECT").alias("reject_type"),
            current_timestamp().alias("timestamp")
        )
        
        logger.info(f"Business validation: {business_valid.count()} valid, {business_rejects.count()} rejects")
        return business_valid, business_rejects
    
    def generate_xml(self, valid_events_df, template_df):
        """Generate MISMO XML using template join and token replacement"""
        logger.info("Generating MISMO XML...")
        
        render_xml_udf = self.render_xml_udf()
        
        # Broadcast the template (single record)
        template_broadcast = broadcast(template_df)
        
        # Cross join with template (since template is single record)
        events_with_template = valid_events_df.crossJoin(template_broadcast)
        
        # Generate XML using token replacement
        xml_output = events_with_template.select(
            col("event_id"),
            col("loan_id"),
            render_xml_udf(
                col("xml_template"),
                col("event_id"),
                col("loan_id"),
                col("amendment_type"),
                col("effective_date"),
                col("new_rate"),
                col("borrower_name"),
                col("property_address"),
                col("loan_amount")
            ).alias("xml_content"),
            current_timestamp().alias("generated_timestamp")
        )
        
        logger.info(f"Generated XML for {xml_output.count()} events")
        return xml_output
    
    def persist_rejects(self, schema_rejects_df, business_rejects_df, output_path="/tmp/rejects"):
        """Persist all reject records for audit trail"""
        logger.info(f"Persisting reject records to {output_path}")
        
        try:
            # Union all rejects
            all_rejects = schema_rejects_df.union(business_rejects_df)
            
            all_rejects.write.mode("overwrite").option("path", output_path).saveAsTable("reject_records_table")
            logger.info(f"Persisted {all_rejects.count()} reject records")
        except Exception as e:
            logger.error(f"Error persisting rejects: {e}")
    
    def persist_xml_output(self, xml_output_df, output_path="/tmp/mismo_xml_output"):
        """Persist generated XML output"""
        logger.info(f"Persisting XML output to {output_path}")
        
        try:
            xml_output_df.write.mode("overwrite").option("path", output_path).saveAsTable("xml_output_table")
            logger.info(f"Persisted {xml_output_df.count()} XML records")
        except Exception as e:
            logger.error(f"Error persisting XML output: {e}")
    
    def run_etl_pipeline(self):
        """Execute the complete ETL pipeline"""
        logger.info("Starting Mortgage Amendment to MISMO XML ETL pipeline...")
        
        try:
            # Step 1: Read landing files
            raw_events_df, template_df = self.read_landing_files()
            
            # Step 2: Persist raw events (audit trail)
            self.persist_raw_events(raw_events_df)
            
            # Step 3: Extract metadata from JSON
            event_meta_df = self.extract_metadata(raw_events_df)
            
            # Step 4: Schema validation
            valid_events_df, schema_rejects_df = self.validate_schema(event_meta_df)
            
            # Step 5: Business rule validation
            business_valid_df, business_rejects_df = self.validate_business_rules(valid_events_df)
            
            # Step 6: Generate XML
            xml_output_df = self.generate_xml(business_valid_df, template_df)
            
            # Step 7: Persist rejects and output
            self.persist_rejects(schema_rejects_df, business_rejects_df)
            self.persist_xml_output(xml_output_df)
            
            # Step 8: Data validation - ensure no data loss
            total_input = raw_events_df.count()
            total_output = xml_output_df.count()
            total_rejects = schema_rejects_df.count() + business_rejects_df.count()
            
            logger.info(f"Pipeline completed successfully:")
            logger.info(f"  Input records: {total_input}")
            logger.info(f"  XML output: {total_output}")
            logger.info(f"  Total rejects: {total_rejects}")
            logger.info(f"  Data integrity check: {total_input == (total_output + total_rejects)}")
            
            return {
                'raw_events': raw_events_df,
                'xml_output': xml_output_df,
                'schema_rejects': schema_rejects_df,
                'business_rejects': business_rejects_df,
                'template': template_df
            }
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

def main():
    """Main execution function"""
    try:
        # Initialize and run the ETL job
        etl_job = MortgageAmendmentToMISMOXML()
        results = etl_job.run_etl_pipeline()
        
        print("\n=== ETL Pipeline Execution Summary ===")
        print(f"Raw Events Count: {results['raw_events'].count()}")
        print(f"XML Output Count: {results['xml_output'].count()}")
        print(f"Schema Rejects Count: {results['schema_rejects'].count()}")
        print(f"Business Rejects Count: {results['business_rejects'].count()}")
        
        # Show sample results
        print("\n=== Sample XML Output ===")
        results['xml_output'].select("event_id", "loan_id", "xml_content").show(2, truncate=False)
        
        print("\n=== Sample Rejects ===")
        if results['schema_rejects'].count() > 0:
            results['schema_rejects'].show(truncate=False)
        if results['business_rejects'].count() > 0:
            results['business_rejects'].show(truncate=False)
            
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
