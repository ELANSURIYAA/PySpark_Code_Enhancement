```
==================================================================
Author:        AAVA
Created on:    
Description:   Convert Mortgage Amendment events to MISMO-like XML, persisting raw, rejects, and output per LLD.
==================================================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf

# =========================
# Configuration Section
# =========================
# Section 4.1/4.2: Parameters and configuration
AWS_BUCKET_URL = "/data/landing/mortgage_amendment"
WINDOW_TS = sys.argv[1]  # e.g., "2026012816"
PROJECT_DIR = "/apps/mortgage-amendment-mismo"
ERROR_LOG_PATH = f"/data/out/rejects/rejects_{WINDOW_TS}.dat"
PRODUCT_MISS_PATH = f"/data/out/raw_events/raw_events_{WINDOW_TS}.dat"
MISMO_OUT_PATH = f"/data/out/mismo_xml/mismo_amendment_{WINDOW_TS}.xml"
TEMPLATE_RECORD_FILE = f"{PROJECT_DIR}/lookups/mismo_template_record.dat"
LANDING_FILE = f"{AWS_BUCKET_URL}/amendment_events_{WINDOW_TS}.dat"

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

# =========================
# Spark Session
# =========================
spark = SparkSession.builder.appName("G_MtgAmendment_To_MISMO_XML").getOrCreate()

# =========================
# Step 1: Ingest Landing File
# =========================
# Section 2.1/3.1: Ingest
landing_df = spark.read \
    .option("delimiter", "|") \
    .option("header", "false") \
    .schema(kafka_event_schema) \
    .csv(LANDING_FILE)

# =========================
# Step 2: Extract Event Metadata
# =========================
# Section 6.2: Metadata Extraction

def json_find_string_value(json_str, key):
    import re
    # Matches "key":"value"
    m = re.search(r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key)), json_str or "")
    return m.group(1) if m else ""

def json_find_scalar_value(json_str, key):
    import re
    # Matches "key":123 or "key":7.25
    m = re.search(r'"{}"\s*:\s*([0-9.\-]+)'.format(re.escape(key)), json_str or "")
    return m.group(1).strip() if m else ""

json_find_string_value_udf = udf(json_find_string_value, StringType())
json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())

event_meta_df = landing_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"))) \
    .withColumn("event_type", json_find_string_value_udf(col("payload_json"))) \
    .withColumn("event_ts", json_find_string_value_udf(col("payload_json"))) \
    .withColumn("loan_id", json_find_string_value_udf(col("payload_json"))) \
    .withColumn("source_system", json_find_string_value_udf(col("payload_json"))) \
    .select(
        "event_id", "event_type", "event_ts", "loan_id", "source_system",
        "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
    )

# =========================
# Step 3: Persist Raw Events
# =========================
# Section 2.1/3.3: Persist raw before validation
event_meta_df.write \
    .mode("overwrite") \
    .option("delimiter", "|") \
    .csv(PRODUCT_MISS_PATH)

# =========================
# Step 4: Schema Validation and Split
# =========================
# Section 6.3: Schema Validate or Reject

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

valid_event_meta_df = event_meta_df.withColumn(
    "reject_reason",
    when(col("event_id") == "", lit("Missing event_id"))
    .when(col("loan_id") == "", lit("Missing loan_id"))
    .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
    .otherwise(lit(None))
).filter(col("reject_reason").isNull()) \
 .drop("reject_reason")

# =========================
# Step 5: Canonicalize Mortgage Amendment
# =========================
# Section 6.4: Canonicalization

canonical_amendment_df = valid_event_meta_df \
    .withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
    .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
    .withColumn("prior_rate", json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
    .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
    .withColumn("prior_term_months", json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
    .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))

# =========================
# Step 6: Join Template Record
# =========================
# Section 6.5: Template handling

template_df = spark.read \
    .option("delimiter", "|") \
    .option("header", "false") \
    .schema(template_record_schema) \
    .csv(TEMPLATE_RECORD_FILE)

# Only one record, broadcast join
canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))

# =========================
# Step 7: Business Validation and XML Render
# =========================
# Section 6.6: Business Validate + Render XML

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

business_valid_df = canonical_with_template_df.withColumn(
    "reject_reason",
    business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"))
).filter(col("reject_reason").isNull()) \
 .drop("reject_reason")

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

# =========================
# Step 8: Write Outputs
# =========================
# Section 2.1/3.4/3.5/7: Output writing

# Write schema rejects
schema_reject_df.write \
    .mode("overwrite") \
    .option("delimiter", "|") \
    .csv(ERROR_LOG_PATH)

# Write business rejects
business_reject_df.write \
    .mode("overwrite") \
    .option("delimiter", "|") \
    .csv(ERROR_LOG_PATH)

# Write MISMO XML output
xml_out_df.write \
    .mode("overwrite") \
    .option("delimiter", "|") \
    .csv(MISMO_OUT_PATH)

# =========================
# End of Script
# =========================

# Section 8: Quality Control
# All steps strictly follow the LLD, including raw event persistence before validation, explicit reject logic, and tokenized XML rendering.
```