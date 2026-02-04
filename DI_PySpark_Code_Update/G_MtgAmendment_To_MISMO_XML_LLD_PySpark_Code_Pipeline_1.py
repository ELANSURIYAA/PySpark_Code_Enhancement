```
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Refactored PySpark job for improved readability and data validation with parameterized configuration
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# =========================
# Configuration Section
# =========================
class Config:
    def __init__(self):
        # Parameterized configuration - can be overridden by environment variables or command line args
        self.AWS_BUCKET_URL = os.getenv('AWS_BUCKET_URL', '/data/landing/mortgage_amendment')
        self.WINDOW_TS = sys.argv[1] if len(sys.argv) > 1 else os.getenv('WINDOW_TS', '2026012816')
        self.PROJECT_DIR = os.getenv('PROJECT_DIR', '/apps/mortgage-amendment-mismo')
        self.ERROR_LOG_PATH = os.getenv('ERROR_LOG_PATH', f'/data/out/rejects/rejects_{self.WINDOW_TS}.dat')
        self.PRODUCT_MISS_PATH = os.getenv('PRODUCT_MISS_PATH', f'/data/out/raw_events/raw_events_{self.WINDOW_TS}.dat')
        self.MISMO_OUT_PATH = os.getenv('MISMO_OUT_PATH', f'/data/out/mismo_xml/mismo_amendment_{self.WINDOW_TS}.xml')
        self.TEMPLATE_RECORD_FILE = os.getenv('TEMPLATE_RECORD_FILE', f'{self.PROJECT_DIR}/lookups/mismo_template_record.dat')
        self.LANDING_FILE = os.getenv('LANDING_FILE', f'{self.AWS_BUCKET_URL}/amendment_events_{self.WINDOW_TS}.dat')
        
        # Delta table paths for Databricks
        self.DELTA_LANDING_TABLE = os.getenv('DELTA_LANDING_TABLE', '/delta/landing/kafka_events')
        self.DELTA_RAW_EVENTS_TABLE = os.getenv('DELTA_RAW_EVENTS_TABLE', '/delta/raw/event_meta')
        self.DELTA_REJECTS_TABLE = os.getenv('DELTA_REJECTS_TABLE', '/delta/rejects/reject_records')
        self.DELTA_XML_OUTPUT_TABLE = os.getenv('DELTA_XML_OUTPUT_TABLE', '/delta/output/xml_records')
        self.DELTA_TEMPLATE_TABLE = os.getenv('DELTA_TEMPLATE_TABLE', '/delta/lookups/template_records')

# =========================
# Logging Configuration
# =========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_row_count(df, step_name):
    """Log row count for a given DataFrame and step"""
    count = df.count()
    logger.info(f"Step: {step_name} - Row count: {count}")
    return count

# =========================
# Schema Definitions
# =========================
# Section 3.1: kafka_event.dml
kafka_event_schema = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("ingest_ts", StringType(), True),
    StructField("payload_json", StringType(), True)
])

# Section 3.3: event_meta.dml
event_meta_schema = StructType([
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

# Section 3.2: template_record.dml
template_record_schema = StructType([
    StructField("template_key", StringType(), True),
    StructField("template_text", StringType(), True)
])

# Section 3.4: reject_record.dml
reject_record_schema = StructType([
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

# Section 3.5: xml_out_record.dml
xml_out_record_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("xml_text", StringType(), True)
])

def create_sample_data(spark, config):
    """Create sample data for testing purposes"""
    
    # Sample kafka events data
    kafka_sample_data = [
        ("key1", 0, 1001, "2026-01-28T10:00:00Z", '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.5","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}'),
        ("key2", 0, 1002, "2026-01-28T10:01:00Z", '{"event_id":"evt002","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:01:00Z","loan_id":"loan002","source_system":"CORE","amendment_type":"TERM_CHANGE","effective_date":"2026-02-01","prior_rate":"4.0","new_rate":"4.0","prior_term_months":"360","new_term_months":"300"}'),
        ("key3", 0, 1003, "2026-01-28T10:02:00Z", '{"event_id":"","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:02:00Z","loan_id":"loan003","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.75","new_rate":"3.5"}'),  # Missing event_id - should be rejected
        ("key4", 0, 1004, "2026-01-28T10:03:00Z", '{"event_id":"evt004","event_type":"OTHER_EVENT","event_ts":"2026-01-28T10:03:00Z","loan_id":"loan004","source_system":"CORE"}')  # Wrong event type - should be rejected
    ]
    
    kafka_df = spark.createDataFrame(kafka_sample_data, kafka_event_schema)
    kafka_df.write.format("delta").mode("overwrite").save(config.DELTA_LANDING_TABLE)
    
    # Sample template data
    template_sample_data = [
        ("MISMO_TEMPLATE", "<MISMO><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths></MISMO>")
    ]
    
    template_df = spark.createDataFrame(template_sample_data, template_record_schema)
    template_df.write.format("delta").mode("overwrite").save(config.DELTA_TEMPLATE_TABLE)
    
    logger.info("Sample data created successfully")

def main():
    # Initialize configuration
    config = Config()
    
    # =========================
    # Spark Session
    # =========================
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder \
            .appName("G_MtgAmendment_To_MISMO_XML_Refactored") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    logger.info("Starting Mortgage Amendment to MISMO XML processing")
    
    # Create sample data for testing
    create_sample_data(spark, config)
    
    # =========================
    # Step 1: Ingest Landing File
    # =========================
    logger.info(f"Reading landing data from: {config.DELTA_LANDING_TABLE}")
    landing_df = spark.read.format("delta").load(config.DELTA_LANDING_TABLE)
    log_row_count(landing_df, "Landing Data Ingestion")
    
    # =========================
    # Step 2: Extract Event Metadata
    # =========================
    logger.info("Extracting event metadata from JSON payload")
    
    def json_find_string_value(json_str, key):
        import re
        if not json_str:
            return ""
        # Matches "key":"value"
        pattern = r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key))
        m = re.search(pattern, json_str)
        return m.group(1) if m else ""
    
    def json_find_scalar_value(json_str, key):
        import re
        if not json_str:
            return ""
        # Matches "key":123 or "key":7.25
        pattern = r'"{}"\s*:\s*"?([0-9.\-]+)"?'.format(re.escape(key))
        m = re.search(pattern, json_str)
        return m.group(1).strip() if m else ""
    
    json_find_string_value_udf = udf(json_find_string_value, StringType())
    json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())
    
    event_meta_df = landing_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
        .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
        .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
        .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
        .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system"))) \
        .select(
            "event_id", "event_type", "event_ts", "loan_id", "source_system",
            "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
        )
    
    log_row_count(event_meta_df, "Event Metadata Extraction")
    
    # =========================
    # Step 3: Persist Raw Events
    # =========================
    logger.info(f"Persisting raw events to: {config.DELTA_RAW_EVENTS_TABLE}")
    event_meta_df.write.format("delta").mode("overwrite").save(config.DELTA_RAW_EVENTS_TABLE)
    
    # =========================
    # Step 4: Schema Validation and Split
    # =========================
    logger.info("Performing schema validation")
    
    schema_reject_df = event_meta_df.withColumn(
        "reject_reason",
        when(col("event_id") == "", lit("Missing event_id"))
        .when(col("loan_id") == "", lit("Missing loan_id"))
        .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
        .otherwise(lit(None))
    ).filter(col("reject_reason").isNotNull()) \
     .withColumn("reject_type", lit("SCHEMA")) \
     .withColumn("reason", col("reject_reason")) \
     .select(
        "reject_type", "reason", "event_id", "loan_id", "event_ts", "source_system",
        "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
    )
    
    log_row_count(schema_reject_df, "Schema Validation Rejects")
    
    valid_event_meta_df = event_meta_df.withColumn(
        "reject_reason",
        when(col("event_id") == "", lit("Missing event_id"))
        .when(col("loan_id") == "", lit("Missing loan_id"))
        .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
        .otherwise(lit(None))
    ).filter(col("reject_reason").isNull()) \
     .drop("reject_reason")
    
    log_row_count(valid_event_meta_df, "Valid Events After Schema Validation")
    
    # =========================
    # Step 5: Canonicalize Mortgage Amendment
    # =========================
    logger.info("Canonicalizing mortgage amendment data")
    
    canonical_amendment_df = valid_event_meta_df \
        .withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
        .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
        .withColumn("prior_rate", json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
        .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
        .withColumn("prior_term_months", json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
        .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
    
    log_row_count(canonical_amendment_df, "Canonical Amendment Data")
    
    # =========================
    # Step 6: Join Template Record
    # =========================
    logger.info(f"Reading template records from: {config.DELTA_TEMPLATE_TABLE}")
    
    template_df = spark.read.format("delta").load(config.DELTA_TEMPLATE_TABLE)
    log_row_count(template_df, "Template Records")
    
    # Only one record, broadcast join
    canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
    log_row_count(canonical_with_template_df, "After Template Join")
    
    # =========================
    # Step 7: Business Validation and XML Render
    # =========================
    logger.info("Performing business validation")
    
    def business_reject_reason(effective_date, amendment_type, new_rate):
        if not effective_date:
            return "Missing effective_date"
        if not amendment_type:
            return "Missing amendment_type"
        if amendment_type == "RATE_CHANGE" and not new_rate:
            return "Missing new_rate for RATE_CHANGE"
        return None
    
    business_reject_reason_udf = udf(business_reject_reason, StringType())
    
    business_reject_df = canonical_with_template_df.withColumn(
        "reject_reason",
        business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
    ).filter(col("reject_reason").isNotNull()) \
     .withColumn("reject_type", lit("BUSINESS")) \
     .withColumn("reason", col("reject_reason")) \
     .withColumn("payload_json", lit("")) \
     .select(
        "reject_type", "reason", "event_id", "loan_id", "event_ts", "source_system",
        "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
    )
    
    log_row_count(business_reject_df, "Business Validation Rejects")
    
    business_valid_df = canonical_with_template_df.withColumn(
        "reject_reason",
        business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
    ).filter(col("reject_reason").isNull()) \
     .drop("reject_reason")
    
    log_row_count(business_valid_df, "Valid Events After Business Validation")
    
    # Token replacement for XML rendering
    def render_xml(template_text, loan_id, event_id, effective_date, amendment_type, new_rate, new_term_months):
        xml = template_text or ""
        xml = xml.replace("{{LOAN_ID}}", loan_id or "")
        xml = xml.replace("{{EVENT_ID}}", event_id or "")
        xml = xml.replace("{{EFFECTIVE_DATE}}", effective_date or "")
        xml = xml.replace("{{AMENDMENT_TYPE}}", amendment_type or "")
        xml = xml.replace("{{NEW_RATE}}", new_rate or "")
        xml = xml.replace("{{NEW_TERM_MONTHS}}", new_term_months or "")
        return xml
    
    render_xml_udf = udf(render_xml, StringType())
    
    xml_out_df = business_valid_df.withColumn(
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
    
    log_row_count(xml_out_df, "Final XML Output")
    
    # =========================
    # Step 8: Write Outputs
    # =========================
    logger.info("Writing output files")
    
    # Combine all rejects
    all_rejects_df = schema_reject_df.union(business_reject_df)
    log_row_count(all_rejects_df, "Total Rejects")
    
    # Write rejects to Delta table
    logger.info(f"Writing rejects to: {config.DELTA_REJECTS_TABLE}")
    all_rejects_df.write.format("delta").mode("overwrite").save(config.DELTA_REJECTS_TABLE)
    
    # Write MISMO XML output to Delta table
    logger.info(f"Writing XML output to: {config.DELTA_XML_OUTPUT_TABLE}")
    xml_out_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
    
    logger.info("Processing completed successfully")
    
    # Return summary for validation
    return {
        "landing_count": landing_df.count(),
        "schema_rejects": schema_reject_df.count(),
        "business_rejects": business_reject_df.count(),
        "xml_output_count": xml_out_df.count()
    }

if __name__ == "__main__":
    summary = main()
    print(f"Processing Summary: {summary}")
```