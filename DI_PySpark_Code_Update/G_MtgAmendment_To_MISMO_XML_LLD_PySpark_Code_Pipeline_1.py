_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced PySpark ETL pipeline for Mortgage Amendment to MISMO XML conversion with improved readability and validation
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf
from delta.tables import DeltaTable
import logging

# =========================
# Configuration Section
# =========================
class Config:
    """Centralized configuration for parameterized values"""
    def __init__(self, window_ts=None):
        self.window_ts = window_ts or "2026012816"
        self.aws_bucket_url = "/data/landing/mortgage_amendment"
        self.project_dir = "/apps/mortgage-amendment-mismo"
        self.error_log_path = f"/data/out/rejects/rejects_{self.window_ts}.dat"
        self.product_miss_path = f"/data/out/raw_events/raw_events_{self.window_ts}.dat"
        self.mismo_out_path = f"/data/out/mismo_xml/mismo_amendment_{self.window_ts}.xml"
        self.template_record_file = f"{self.project_dir}/lookups/mismo_template_record.dat"
        self.landing_file = f"{self.aws_bucket_url}/amendment_events_{self.window_ts}.dat"
        
        # Delta table paths
        self.delta_raw_events = f"/delta/raw_events"
        self.delta_rejects = f"/delta/rejects"
        self.delta_mismo_output = f"/delta/mismo_output"

# =========================
# Schema Definitions
# =========================
class SchemaDefinitions:
    """Centralized schema definitions"""
    
    @staticmethod
    def get_kafka_event_schema():
        return StructType([
            StructField("kafka_key", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True)
        ])
    
    @staticmethod
    def get_event_meta_schema():
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("kafka_key", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True)
        ])
    
    @staticmethod
    def get_template_record_schema():
        return StructType([
            StructField("template_key", StringType(), True),
            StructField("template_text", StringType(), True)
        ])
    
    @staticmethod
    def get_reject_record_schema():
        return StructType([
            StructField("reject_type", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True)
        ])
    
    @staticmethod
    def get_xml_out_record_schema():
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("xml_text", StringType(), True)
        ])

# =========================
# Utility Functions
# =========================
class DataTransformationUtils:
    """Reusable transformation utilities"""
    
    @staticmethod
    def json_find_string_value(json_str, key):
        """Extract string value from JSON string"""
        import re
        if not json_str:
            return ""
        pattern = r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key))
        match = re.search(pattern, json_str)
        return match.group(1) if match else ""
    
    @staticmethod
    def json_find_scalar_value(json_str, key):
        """Extract scalar value from JSON string"""
        import re
        if not json_str:
            return ""
        pattern = r'"{}"\s*:\s*([0-9.\-]+)'.format(re.escape(key))
        match = re.search(pattern, json_str)
        return match.group(1).strip() if match else ""
    
    @staticmethod
    def get_schema_validation_condition():
        """Returns schema validation conditions"""
        return (
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .otherwise(lit(None))
        )
    
    @staticmethod
    def business_reject_reason(effective_date, amendment_type, new_rate):
        """Business validation logic"""
        if not effective_date:
            return "Missing effective_date"
        if not amendment_type:
            return "Missing amendment_type"
        if amendment_type == "RATE_CHANGE" and not new_rate:
            return "Missing new_rate for RATE_CHANGE"
        return None
    
    @staticmethod
    def render_xml(template_text, loan_id, event_id, effective_date, amendment_type, new_rate, new_term_months):
        """Render XML from template with token replacement"""
        xml = template_text or ""
        replacements = {
            "{{LOAN_ID}}": loan_id or "",
            "{{EVENT_ID}}": event_id or "",
            "{{EFFECTIVE_DATE}}": effective_date or "",
            "{{AMENDMENT_TYPE}}": amendment_type or "",
            "{{NEW_RATE}}": new_rate or "",
            "{{NEW_TERM_MONTHS}}": new_term_months or ""
        }
        
        for token, value in replacements.items():
            xml = xml.replace(token, value)
        return xml

# =========================
# Main ETL Pipeline Class
# =========================
class MortgageAmendmentETL:
    """Main ETL pipeline for Mortgage Amendment to MISMO XML conversion"""
    
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.getActiveSession() or SparkSession.builder.appName("G_MtgAmendment_To_MISMO_XML").getOrCreate()
        self.schemas = SchemaDefinitions()
        self.utils = DataTransformationUtils()
        
        # Register UDFs
        self.json_find_string_value_udf = udf(self.utils.json_find_string_value, StringType())
        self.json_find_scalar_value_udf = udf(self.utils.json_find_scalar_value, StringType())
        self.business_reject_reason_udf = udf(self.utils.business_reject_reason, StringType())
        self.render_xml_udf = udf(self.utils.render_xml, StringType())
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def log_row_count(self, df, step_name):
        """Log row count for validation"""
        count = df.count()
        self.logger.info(f"{step_name}: {count} rows")
        return count
    
    def ingest_landing_data(self):
        """Step 1: Ingest landing file data"""
        self.logger.info("Starting data ingestion...")
        
        # Create sample data for self-contained execution
        sample_data = [
            ("key1", 0, 1001, "2026-01-29T10:00:00Z", '{"event_id":"evt_001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T10:00:00Z","loan_id":"loan_001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"4.5","new_rate":"3.75","prior_term_months":"360","new_term_months":"360"}'),
            ("key2", 0, 1002, "2026-01-29T10:01:00Z", '{"event_id":"evt_002","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T10:01:00Z","loan_id":"loan_002","source_system":"CORE","amendment_type":"TERM_CHANGE","effective_date":"2026-02-01","prior_rate":"5.0","new_rate":"5.0","prior_term_months":"360","new_term_months":"240"}'),
            ("key3", 0, 1003, "2026-01-29T10:02:00Z", '{"event_id":"","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-29T10:02:00Z","loan_id":"loan_003","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"4.25","new_rate":"3.5"}'),
        ]
        
        landing_df = self.spark.createDataFrame(sample_data, self.schemas.get_kafka_event_schema())
        self.log_row_count(landing_df, "Landing Data Ingestion")
        return landing_df
    
    def extract_event_metadata(self, landing_df):
        """Step 2: Extract event metadata from JSON payload"""
        self.logger.info("Extracting event metadata...")
        
        event_meta_df = landing_df.selectExpr(
            "kafka_key",
            "kafka_partition", 
            "kafka_offset",
            "ingest_ts",
            "payload_json"
        ).withColumn("event_id", self.json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
         .withColumn("event_type", self.json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
         .withColumn("event_ts", self.json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
         .withColumn("loan_id", self.json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
         .withColumn("source_system", self.json_find_string_value_udf(col("payload_json"), lit("source_system"))) \
         .select(
            "event_id", "event_type", "event_ts", "loan_id", "source_system",
            "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
        )
        
        self.log_row_count(event_meta_df, "Event Metadata Extraction")
        return event_meta_df
    
    def persist_raw_events(self, event_meta_df):
        """Step 3: Persist raw events to Delta table"""
        self.logger.info("Persisting raw events...")
        
        # Write to Delta table
        event_meta_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.config.delta_raw_events)
        
        self.log_row_count(event_meta_df, "Raw Events Persisted")
    
    def validate_schema_and_split(self, event_meta_df):
        """Step 4: Schema validation and split valid/invalid records"""
        self.logger.info("Performing schema validation...")
        
        # Add validation condition
        validated_df = event_meta_df.withColumn(
            "reject_reason", 
            self.utils.get_schema_validation_condition()
        )
        
        # Split into rejects and valid records
        schema_reject_df = validated_df.filter(col("reject_reason").isNotNull()) \
            .withColumn("reject_type", lit("SCHEMA")) \
            .withColumn("reason", col("reject_reason")) \
            .select(
                "reject_type", "reason", "event_id", "loan_id", "event_ts", "source_system",
                "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
            )
        
        valid_event_meta_df = validated_df.filter(col("reject_reason").isNull()) \
            .drop("reject_reason")
        
        self.log_row_count(schema_reject_df, "Schema Rejects")
        self.log_row_count(valid_event_meta_df, "Schema Valid Records")
        
        return schema_reject_df, valid_event_meta_df
    
    def canonicalize_amendment_data(self, valid_event_meta_df):
        """Step 5: Canonicalize mortgage amendment data"""
        self.logger.info("Canonicalizing amendment data...")
        
        canonical_amendment_df = valid_event_meta_df.selectExpr(
            "*",
            "payload_json as temp_payload"
        ).withColumn("amendment_type", self.json_find_string_value_udf(col("temp_payload"), lit("amendment_type"))) \
         .withColumn("effective_date", self.json_find_string_value_udf(col("temp_payload"), lit("effective_date"))) \
         .withColumn("prior_rate", self.json_find_scalar_value_udf(col("temp_payload"), lit("prior_rate"))) \
         .withColumn("new_rate", self.json_find_scalar_value_udf(col("temp_payload"), lit("new_rate"))) \
         .withColumn("prior_term_months", self.json_find_scalar_value_udf(col("temp_payload"), lit("prior_term_months"))) \
         .withColumn("new_term_months", self.json_find_scalar_value_udf(col("temp_payload"), lit("new_term_months"))) \
         .drop("temp_payload")
        
        self.log_row_count(canonical_amendment_df, "Canonicalized Records")
        return canonical_amendment_df
    
    def load_template_data(self):
        """Step 6: Load template data"""
        self.logger.info("Loading template data...")
        
        # Create sample template data
        template_data = [("MISMO_TEMPLATE", "<MISMO><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths></MISMO>")]
        
        template_df = self.spark.createDataFrame(template_data, self.schemas.get_template_record_schema())
        self.log_row_count(template_df, "Template Records")
        return template_df
    
    def business_validate_and_render_xml(self, canonical_amendment_df, template_df):
        """Step 7: Business validation and XML rendering"""
        self.logger.info("Performing business validation and XML rendering...")
        
        # Join with template (broadcast for performance)
        canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
        
        # Business validation
        business_validated_df = canonical_with_template_df.withColumn(
            "reject_reason",
            self.business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        )
        
        # Split business rejects and valid records
        business_reject_df = business_validated_df.filter(col("reject_reason").isNotNull()) \
            .withColumn("reject_type", lit("BUSINESS")) \
            .withColumn("reason", col("reject_reason")) \
            .withColumn("payload_json", lit("")) \
            .select(
                "reject_type", "reason", "event_id", "loan_id", "event_ts", "source_system",
                "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
            )
        
        business_valid_df = business_validated_df.filter(col("reject_reason").isNull()) \
            .drop("reject_reason")
        
        # Render XML
        xml_out_df = business_valid_df.withColumn(
            "xml_text",
            self.render_xml_udf(
                col("template_text"),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).select("event_id", "loan_id", "xml_text")
        
        self.log_row_count(business_reject_df, "Business Rejects")
        self.log_row_count(xml_out_df, "XML Output Records")
        
        return business_reject_df, xml_out_df
    
    def write_outputs(self, schema_reject_df, business_reject_df, xml_out_df):
        """Step 8: Write all outputs to Delta tables"""
        self.logger.info("Writing outputs...")
        
        # Combine all rejects
        all_rejects_df = schema_reject_df.union(business_reject_df)
        
        # Write rejects to Delta
        if all_rejects_df.count() > 0:
            all_rejects_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(self.config.delta_rejects)
        
        # Write XML output to Delta
        if xml_out_df.count() > 0:
            xml_out_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(self.config.delta_mismo_output)
        
        self.log_row_count(all_rejects_df, "Total Rejects Written")
        self.log_row_count(xml_out_df, "XML Output Written")
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        self.logger.info("Starting Mortgage Amendment ETL Pipeline...")
        
        try:
            # Step 1: Ingest landing data
            landing_df = self.ingest_landing_data()
            
            # Step 2: Extract event metadata
            event_meta_df = self.extract_event_metadata(landing_df)
            
            # Step 3: Persist raw events
            self.persist_raw_events(event_meta_df)
            
            # Step 4: Schema validation and split
            schema_reject_df, valid_event_meta_df = self.validate_schema_and_split(event_meta_df)
            
            # Step 5: Canonicalize amendment data
            canonical_amendment_df = self.canonicalize_amendment_data(valid_event_meta_df)
            
            # Step 6: Load template data
            template_df = self.load_template_data()
            
            # Step 7: Business validation and XML rendering
            business_reject_df, xml_out_df = self.business_validate_and_render_xml(canonical_amendment_df, template_df)
            
            # Step 8: Write outputs
            self.write_outputs(schema_reject_df, business_reject_df, xml_out_df)
            
            self.logger.info("Pipeline completed successfully!")
            return xml_out_df, schema_reject_df.union(business_reject_df)
            
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {str(e)}")
            raise

# =========================
# Main Execution
# =========================
if __name__ == "__main__":
    # Initialize configuration
    window_ts = sys.argv[1] if len(sys.argv) > 1 else "2026012816"
    config = Config(window_ts)
    
    # Initialize and run ETL pipeline
    etl_pipeline = MortgageAmendmentETL(config)
    xml_output, rejects_output = etl_pipeline.run_pipeline()
    
    # Display results
    print("\n=== XML OUTPUT ===")
    xml_output.show(truncate=False)
    
    print("\n=== REJECTS ===")
    rejects_output.show(truncate=False)
    
    print("\n=== PIPELINE COMPLETED ===")