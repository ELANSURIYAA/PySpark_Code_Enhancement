'''
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Refactored PySpark job for improved readability, reusability, and data validation - Convert Mortgage Amendment events to MISMO-like XML
## *Version*: 1 
## *Updated on*: 
_____________________________________________
'''

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf

# =========================
# Configuration and Parameters
# =========================
class Config:
    """Configuration class to hold all parameters and constants"""
    def __init__(self, window_ts, project_dir="/apps/mortgage-amendment-mismo", aws_bucket_url="/data/landing/mortgage_amendment"):
        self.window_ts = window_ts
        self.project_dir = project_dir
        self.aws_bucket_url = aws_bucket_url
        self.error_log_path = f"/data/out/rejects/rejects_{window_ts}.dat"
        self.product_miss_path = f"/data/out/raw_events/raw_events_{window_ts}.dat"
        self.mismo_out_path = f"/data/out/mismo_xml/mismo_amendment_{window_ts}.xml"
        self.template_record_file = f"{project_dir}/lookups/mismo_template_record.dat"
        self.landing_file = f"{aws_bucket_url}/amendment_events_{window_ts}.dat"

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
    """Utility class for common data transformation operations"""
    
    @staticmethod
    def setup_logging():
        """Setup logging configuration"""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)
    
    @staticmethod
    def log_dataframe_count(df, step_name, logger):
        """Log dataframe row count for validation"""
        count = df.count()
        logger.info(f"Row count after {step_name}: {count}")
        return count
    
    @staticmethod
    def json_find_string_value(json_str, key):
        """Extract string value from JSON string"""
        import re
        m = re.search(r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key)), json_str or "")
        return m.group(1) if m else ""
    
    @staticmethod
    def json_find_scalar_value(json_str, key):
        """Extract scalar value from JSON string"""
        import re
        m = re.search(r'"{}"\s*:\s*([0-9.\-]+)'.format(re.escape(key)), json_str or "")
        return m.group(1).strip() if m else ""
    
    @staticmethod
    def create_json_udfs():
        """Create UDFs for JSON parsing"""
        json_find_string_value_udf = udf(DataTransformationUtils.json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(DataTransformationUtils.json_find_scalar_value, StringType())
        return json_find_string_value_udf, json_find_scalar_value_udf
    
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
        xml = xml.replace("{{LOAN_ID}}", loan_id or "")
        xml = xml.replace("{{EVENT_ID}}", event_id or "")
        xml = xml.replace("{{EFFECTIVE_DATE}}", effective_date or "")
        xml = xml.replace("{{AMENDMENT_TYPE}}", amendment_type or "")
        xml = xml.replace("{{NEW_RATE}}", new_rate or "")
        xml = xml.replace("{{NEW_TERM_MONTHS}}", new_term_months or "")
        return xml

# =========================
# Data Processing Functions
# =========================
class MortgageAmendmentProcessor:
    """Main processor class for mortgage amendment data"""
    
    def __init__(self, config, spark_session, logger):
        self.config = config
        self.spark = spark_session
        self.logger = logger
        self.utils = DataTransformationUtils()
        self.schemas = SchemaDefinitions()
        
        # Create UDFs
        self.json_find_string_value_udf, self.json_find_scalar_value_udf = self.utils.create_json_udfs()
        self.business_reject_reason_udf = udf(self.utils.business_reject_reason, StringType())
        self.render_xml_udf = udf(self.utils.render_xml, StringType())
    
    def ingest_landing_file(self):
        """Ingest landing file data"""
        self.logger.info(f"Ingesting landing file: {self.config.landing_file}")
        
        landing_df = self.spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(self.schemas.get_kafka_event_schema()) \
            .csv(self.config.landing_file)
        
        self.utils.log_dataframe_count(landing_df, "landing file ingestion", self.logger)
        return landing_df
    
    def extract_event_metadata(self, landing_df):
        """Extract event metadata from payload JSON"""
        self.logger.info("Extracting event metadata")
        
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
        
        self.utils.log_dataframe_count(event_meta_df, "event metadata extraction", self.logger)
        return event_meta_df
    
    def persist_raw_events(self, event_meta_df):
        """Persist raw events before validation"""
        self.logger.info(f"Persisting raw events to: {self.config.product_miss_path}")
        
        event_meta_df.write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .csv(self.config.product_miss_path)
    
    def validate_schema_and_split(self, event_meta_df):
        """Validate schema and split valid/invalid records"""
        self.logger.info("Performing schema validation")
        
        # Create reject reason column
        df_with_reject_reason = event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .otherwise(lit(None))
        )
        
        # Schema rejects
        schema_reject_df = df_with_reject_reason.filter(col("reject_reason").isNotNull()) \
            .selectExpr(
                "'SCHEMA' as reject_type",
                "reject_reason as reason",
                "event_id",
                "loan_id", 
                "event_ts",
                "source_system",
                "kafka_partition",
                "kafka_offset",
                "ingest_ts",
                "payload_json"
            )
        
        # Valid events
        valid_event_meta_df = df_with_reject_reason.filter(col("reject_reason").isNull()) \
            .drop("reject_reason")
        
        self.utils.log_dataframe_count(schema_reject_df, "schema validation - rejects", self.logger)
        self.utils.log_dataframe_count(valid_event_meta_df, "schema validation - valid", self.logger)
        
        return schema_reject_df, valid_event_meta_df
    
    def canonicalize_amendment_data(self, valid_event_meta_df):
        """Canonicalize mortgage amendment data"""
        self.logger.info("Canonicalizing amendment data")
        
        canonical_amendment_df = valid_event_meta_df.selectExpr(
            "event_id",
            "event_type", 
            "event_ts",
            "loan_id",
            "source_system",
            "kafka_key",
            "kafka_partition",
            "kafka_offset", 
            "ingest_ts",
            "payload_json"
        ).withColumn("amendment_type", self.json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
         .withColumn("effective_date", self.json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
         .withColumn("prior_rate", self.json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
         .withColumn("new_rate", self.json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
         .withColumn("prior_term_months", self.json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
         .withColumn("new_term_months", self.json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
        
        self.utils.log_dataframe_count(canonical_amendment_df, "canonicalization", self.logger)
        return canonical_amendment_df
    
    def load_template_data(self):
        """Load template record data"""
        self.logger.info(f"Loading template data from: {self.config.template_record_file}")
        
        template_df = self.spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(self.schemas.get_template_record_schema()) \
            .csv(self.config.template_record_file)
        
        self.utils.log_dataframe_count(template_df, "template data load", self.logger)
        return template_df
    
    def join_with_template(self, canonical_amendment_df, template_df):
        """Join canonical data with template"""
        self.logger.info("Joining canonical data with template")
        
        canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
        
        self.utils.log_dataframe_count(canonical_with_template_df, "template join", self.logger)
        return canonical_with_template_df
    
    def business_validate_and_render_xml(self, canonical_with_template_df):
        """Perform business validation and render XML"""
        self.logger.info("Performing business validation and XML rendering")
        
        # Add business validation
        df_with_business_validation = canonical_with_template_df.withColumn(
            "reject_reason",
            self.business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        )
        
        # Business rejects
        business_reject_df = df_with_business_validation.filter(col("reject_reason").isNotNull()) \
            .selectExpr(
                "'BUSINESS' as reject_type",
                "reject_reason as reason",
                "event_id",
                "loan_id",
                "event_ts", 
                "source_system",
                "kafka_partition",
                "kafka_offset",
                "ingest_ts",
                "'' as payload_json"
            )
        
        # Business valid records with XML rendering
        business_valid_df = df_with_business_validation.filter(col("reject_reason").isNull()) \
            .drop("reject_reason")
        
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
        
        self.utils.log_dataframe_count(business_reject_df, "business validation - rejects", self.logger)
        self.utils.log_dataframe_count(xml_out_df, "business validation - valid XML output", self.logger)
        
        return business_reject_df, xml_out_df
    
    def write_outputs(self, schema_reject_df, business_reject_df, xml_out_df):
        """Write all output files"""
        self.logger.info("Writing output files")
        
        # Combine rejects
        all_rejects_df = schema_reject_df.union(business_reject_df)
        
        # Write rejects
        self.logger.info(f"Writing rejects to: {self.config.error_log_path}")
        all_rejects_df.write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .csv(self.config.error_log_path)
        
        # Write XML output
        self.logger.info(f"Writing XML output to: {self.config.mismo_out_path}")
        xml_out_df.write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .csv(self.config.mismo_out_path)
        
        self.logger.info("All outputs written successfully")

# =========================
# Main Execution Function
# =========================
def main():
    """Main execution function"""
    # Setup logging
    logger = DataTransformationUtils.setup_logging()
    logger.info("Starting Mortgage Amendment to MISMO XML processing")
    
    # Get parameters
    if len(sys.argv) < 2:
        logger.error("Missing required parameter: WINDOW_TS")
        sys.exit(1)
    
    window_ts = sys.argv[1]
    logger.info(f"Processing window: {window_ts}")
    
    # Initialize configuration
    config = Config(window_ts)
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("G_MtgAmendment_To_MISMO_XML_Refactored").getOrCreate()
    
    try:
        # Initialize processor
        processor = MortgageAmendmentProcessor(config, spark, logger)
        
        # Execute processing pipeline
        logger.info("=== Step 1: Ingest Landing File ===")
        landing_df = processor.ingest_landing_file()
        
        logger.info("=== Step 2: Extract Event Metadata ===")
        event_meta_df = processor.extract_event_metadata(landing_df)
        
        logger.info("=== Step 3: Persist Raw Events ===")
        processor.persist_raw_events(event_meta_df)
        
        logger.info("=== Step 4: Schema Validation and Split ===")
        schema_reject_df, valid_event_meta_df = processor.validate_schema_and_split(event_meta_df)
        
        logger.info("=== Step 5: Canonicalize Amendment Data ===")
        canonical_amendment_df = processor.canonicalize_amendment_data(valid_event_meta_df)
        
        logger.info("=== Step 6: Load Template Data ===")
        template_df = processor.load_template_data()
        
        logger.info("=== Step 7: Join with Template ===")
        canonical_with_template_df = processor.join_with_template(canonical_amendment_df, template_df)
        
        logger.info("=== Step 8: Business Validation and XML Rendering ===")
        business_reject_df, xml_out_df = processor.business_validate_and_render_xml(canonical_with_template_df)
        
        logger.info("=== Step 9: Write Outputs ===")
        processor.write_outputs(schema_reject_df, business_reject_df, xml_out_df)
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Processing failed with error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
