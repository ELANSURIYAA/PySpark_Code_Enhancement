_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Refactored PySpark job for Mortgage Amendment XML Generation with improved readability, reusability, and data validation
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# =========================
# Configuration and Parameters
# =========================
class Config:
    def __init__(self, window_ts=None, project_dir=None, aws_bucket_url=None):
        self.window_ts = window_ts or "2026012816"
        self.project_dir = project_dir or "/apps/mortgage-amendment-mismo"
        self.aws_bucket_url = aws_bucket_url or "/data/landing/mortgage_amendment"
        
        # Derived paths
        self.error_log_path = f"/data/out/rejects/rejects_{self.window_ts}.dat"
        self.product_miss_path = f"/data/out/raw_events/raw_events_{self.window_ts}.dat"
        self.mismo_out_path = f"/data/out/mismo_xml/mismo_amendment_{self.window_ts}.xml"
        self.template_record_file = f"{self.project_dir}/lookups/mismo_template_record.dat"
        self.landing_file = f"{self.aws_bucket_url}/amendment_events_{self.window_ts}.dat"

# =========================
# Schema Definitions
# =========================
class SchemaDefinitions:
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
    @staticmethod
    def log_row_count(df: DataFrame, step_name: str, logger):
        """Log row count for validation checkpoints"""
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
    def create_json_extraction_udfs():
        """Create UDFs for JSON extraction"""
        json_find_string_value_udf = udf(DataTransformationUtils.json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(DataTransformationUtils.json_find_scalar_value, StringType())
        return json_find_string_value_udf, json_find_scalar_value_udf
    
    @staticmethod
    def extract_event_metadata(df: DataFrame, json_string_udf, logger):
        """Extract event metadata from payload JSON"""
        logger.info("Starting event metadata extraction")
        
        event_meta_df = df.selectExpr(
            "kafka_key",
            "kafka_partition", 
            "kafka_offset",
            "ingest_ts",
            "payload_json"
        ).withColumn("event_id", json_string_udf(col("payload_json"), lit("event_id"))) \
         .withColumn("event_type", json_string_udf(col("payload_json"), lit("event_type"))) \
         .withColumn("event_ts", json_string_udf(col("payload_json"), lit("event_ts"))) \
         .withColumn("loan_id", json_string_udf(col("payload_json"), lit("loan_id"))) \
         .withColumn("source_system", json_string_udf(col("payload_json"), lit("source_system"))) \
         .select(
            "event_id", "event_type", "event_ts", "loan_id", "source_system",
            "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
        )
        
        DataTransformationUtils.log_row_count(event_meta_df, "event metadata extraction", logger)
        return event_meta_df
    
    @staticmethod
    def validate_schema_and_split(df: DataFrame, logger):
        """Validate schema and split into valid/reject records"""
        logger.info("Starting schema validation")
        
        # Create reject records
        schema_reject_df = df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .otherwise(lit(None))
        ).filter(col("reject_reason").isNotNull()) \
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
        
        # Create valid records
        valid_event_meta_df = df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .otherwise(lit(None))
        ).filter(col("reject_reason").isNull()) \
         .drop("reject_reason")
        
        DataTransformationUtils.log_row_count(schema_reject_df, "schema validation - rejects", logger)
        DataTransformationUtils.log_row_count(valid_event_meta_df, "schema validation - valid records", logger)
        
        return valid_event_meta_df, schema_reject_df
    
    @staticmethod
    def canonicalize_amendment_data(df: DataFrame, json_string_udf, json_scalar_udf, logger):
        """Canonicalize mortgage amendment data"""
        logger.info("Starting canonicalization")
        
        canonical_df = df.selectExpr(
            "event_id", "event_type", "event_ts", "loan_id", "source_system",
            "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
        ).withColumn("amendment_type", json_string_udf(col("payload_json"), lit("amendment_type"))) \
         .withColumn("effective_date", json_string_udf(col("payload_json"), lit("effective_date"))) \
         .withColumn("prior_rate", json_scalar_udf(col("payload_json"), lit("prior_rate"))) \
         .withColumn("new_rate", json_scalar_udf(col("payload_json"), lit("new_rate"))) \
         .withColumn("prior_term_months", json_scalar_udf(col("payload_json"), lit("prior_term_months"))) \
         .withColumn("new_term_months", json_scalar_udf(col("payload_json"), lit("new_term_months")))
        
        DataTransformationUtils.log_row_count(canonical_df, "canonicalization", logger)
        return canonical_df
    
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
    def validate_business_rules_and_split(df: DataFrame, logger):
        """Validate business rules and split into valid/reject records"""
        logger.info("Starting business validation")
        
        business_reject_reason_udf = udf(DataTransformationUtils.business_reject_reason, StringType())
        
        # Create business reject records
        business_reject_df = df.withColumn(
            "reject_reason",
            business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        ).filter(col("reject_reason").isNotNull()) \
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
        
        # Create business valid records
        business_valid_df = df.withColumn(
            "reject_reason",
            business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
        ).filter(col("reject_reason").isNull()) \
         .drop("reject_reason")
        
        DataTransformationUtils.log_row_count(business_reject_df, "business validation - rejects", logger)
        DataTransformationUtils.log_row_count(business_valid_df, "business validation - valid records", logger)
        
        return business_valid_df, business_reject_df
    
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
    
    @staticmethod
    def generate_xml_output(df: DataFrame, logger):
        """Generate XML output from business valid records"""
        logger.info("Starting XML generation")
        
        render_xml_udf = udf(DataTransformationUtils.render_xml, StringType())
        
        xml_out_df = df.withColumn(
            "xml_text",
            render_xml_udf(
                col("template_text"),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).select("event_id", "loan_id", "xml_text")
        
        DataTransformationUtils.log_row_count(xml_out_df, "XML generation", logger)
        return xml_out_df

# =========================
# Main ETL Class
# =========================
class MortgageAmendmentETL:
    def __init__(self, config: Config):
        self.config = config
        self.spark = SparkSession.getActiveSession() or SparkSession.builder.appName("G_MtgAmendment_To_MISMO_XML_Refactored").getOrCreate()
        self.logger = self._setup_logging()
        self.schemas = SchemaDefinitions()
        self.utils = DataTransformationUtils()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)
    
    def read_landing_data(self):
        """Read landing file data"""
        self.logger.info(f"Reading landing file: {self.config.landing_file}")
        
        landing_df = self.spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(self.schemas.get_kafka_event_schema()) \
            .csv(self.config.landing_file)
        
        self.utils.log_row_count(landing_df, "landing data ingestion", self.logger)
        return landing_df
    
    def read_template_data(self):
        """Read template record data"""
        self.logger.info(f"Reading template file: {self.config.template_record_file}")
        
        template_df = self.spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(self.schemas.get_template_record_schema()) \
            .csv(self.config.template_record_file)
        
        self.utils.log_row_count(template_df, "template data ingestion", self.logger)
        return template_df
    
    def write_output_data(self, df: DataFrame, output_path: str, step_name: str):
        """Write output data to specified path"""
        self.logger.info(f"Writing {step_name} to: {output_path}")
        
        df.write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .csv(output_path)
        
        self.logger.info(f"Successfully wrote {step_name}")
    
    def run_etl_pipeline(self):
        """Execute the complete ETL pipeline"""
        self.logger.info("Starting Mortgage Amendment ETL Pipeline")
        
        try:
            # Step 1: Ingest landing data
            landing_df = self.read_landing_data()
            
            # Step 2: Extract event metadata
            json_string_udf, json_scalar_udf = self.utils.create_json_extraction_udfs()
            event_meta_df = self.utils.extract_event_metadata(landing_df, json_string_udf, self.logger)
            
            # Step 3: Persist raw events
            self.write_output_data(event_meta_df, self.config.product_miss_path, "raw events")
            
            # Step 4: Schema validation and split
            valid_event_meta_df, schema_reject_df = self.utils.validate_schema_and_split(event_meta_df, self.logger)
            
            # Step 5: Canonicalize mortgage amendment data
            canonical_amendment_df = self.utils.canonicalize_amendment_data(
                valid_event_meta_df, json_string_udf, json_scalar_udf, self.logger
            )
            
            # Step 6: Join with template data
            template_df = self.read_template_data()
            canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
            self.utils.log_row_count(canonical_with_template_df, "template join", self.logger)
            
            # Step 7: Business validation and split
            business_valid_df, business_reject_df = self.utils.validate_business_rules_and_split(
                canonical_with_template_df, self.logger
            )
            
            # Step 8: Generate XML output
            xml_out_df = self.utils.generate_xml_output(business_valid_df, self.logger)
            
            # Step 9: Write all outputs
            # Combine all rejects
            all_rejects_df = schema_reject_df.union(business_reject_df)
            self.write_output_data(all_rejects_df, self.config.error_log_path, "reject records")
            self.write_output_data(xml_out_df, self.config.mismo_out_path, "MISMO XML output")
            
            self.logger.info("ETL Pipeline completed successfully")
            return xml_out_df
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            raise

# =========================
# Main Execution
# =========================
def main():
    """Main execution function"""
    # Get window timestamp from command line arguments or use default
    window_ts = sys.argv[1] if len(sys.argv) > 1 else "2026012816"
    
    # Initialize configuration
    config = Config(window_ts=window_ts)
    
    # Create and run ETL pipeline
    etl = MortgageAmendmentETL(config)
    result_df = etl.run_etl_pipeline()
    
    # Show sample results for verification
    print("Sample XML Output:")
    result_df.show(5, truncate=False)
    
    return result_df

if __name__ == "__main__":
    main()