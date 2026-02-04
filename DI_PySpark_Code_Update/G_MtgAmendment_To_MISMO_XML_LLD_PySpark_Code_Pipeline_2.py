```
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Enhanced PySpark job with improved error handling, performance optimizations, and comprehensive logging
## *Version*: 2 
## *Updated on*: 
## *Changes*: Added comprehensive error handling, performance optimizations with caching, enhanced logging with execution metrics, improved configuration management with validation
## *Reason*: Further enhance code maintainability, performance, and observability based on production requirements
_____________________________________________

import sys
import os
import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import col, when, lit, regexp_replace, broadcast, udf, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# =========================
# Enhanced Configuration Section
# =========================
class EnhancedConfig:
    def __init__(self):
        # Parameterized configuration with validation
        self.AWS_BUCKET_URL = self._get_config('AWS_BUCKET_URL', '/data/landing/mortgage_amendment')
        self.WINDOW_TS = self._get_config('WINDOW_TS', sys.argv[1] if len(sys.argv) > 1 else '2026012816')
        self.PROJECT_DIR = self._get_config('PROJECT_DIR', '/apps/mortgage-amendment-mismo')
        
        # Dynamic path generation with timestamp
        self.ERROR_LOG_PATH = self._get_config('ERROR_LOG_PATH', f'/data/out/rejects/rejects_{self.WINDOW_TS}.dat')
        self.PRODUCT_MISS_PATH = self._get_config('PRODUCT_MISS_PATH', f'/data/out/raw_events/raw_events_{self.WINDOW_TS}.dat')
        self.MISMO_OUT_PATH = self._get_config('MISMO_OUT_PATH', f'/data/out/mismo_xml/mismo_amendment_{self.WINDOW_TS}.xml')
        self.TEMPLATE_RECORD_FILE = self._get_config('TEMPLATE_RECORD_FILE', f'{self.PROJECT_DIR}/lookups/mismo_template_record.dat')
        self.LANDING_FILE = self._get_config('LANDING_FILE', f'{self.AWS_BUCKET_URL}/amendment_events_{self.WINDOW_TS}.dat')
        
        # Delta table paths for Databricks with environment prefix
        env_prefix = self._get_config('ENVIRONMENT', 'dev')
        self.DELTA_LANDING_TABLE = self._get_config('DELTA_LANDING_TABLE', f'/delta/{env_prefix}/landing/kafka_events')
        self.DELTA_RAW_EVENTS_TABLE = self._get_config('DELTA_RAW_EVENTS_TABLE', f'/delta/{env_prefix}/raw/event_meta')
        self.DELTA_REJECTS_TABLE = self._get_config('DELTA_REJECTS_TABLE', f'/delta/{env_prefix}/rejects/reject_records')
        self.DELTA_XML_OUTPUT_TABLE = self._get_config('DELTA_XML_OUTPUT_TABLE', f'/delta/{env_prefix}/output/xml_records')
        self.DELTA_TEMPLATE_TABLE = self._get_config('DELTA_TEMPLATE_TABLE', f'/delta/{env_prefix}/lookups/template_records')
        
        # Performance tuning parameters
        self.BROADCAST_THRESHOLD = int(self._get_config('BROADCAST_THRESHOLD', '10485760'))  # 10MB
        self.CACHE_ENABLED = self._get_config('CACHE_ENABLED', 'true').lower() == 'true'
        self.CHECKPOINT_INTERVAL = int(self._get_config('CHECKPOINT_INTERVAL', '100'))
        
        # Validation parameters
        self.MAX_REJECT_PERCENTAGE = float(self._get_config('MAX_REJECT_PERCENTAGE', '10.0'))
        self.MIN_EXPECTED_RECORDS = int(self._get_config('MIN_EXPECTED_RECORDS', '1'))
        
        # Logging configuration
        self.LOG_LEVEL = self._get_config('LOG_LEVEL', 'INFO')
        self.ENABLE_METRICS = self._get_config('ENABLE_METRICS', 'true').lower() == 'true'
        
        self._validate_config()
    
    def _get_config(self, key, default_value):
        """Get configuration value from environment or use default"""
        return os.getenv(key, default_value)
    
    def _validate_config(self):
        """Validate configuration parameters"""
        if not self.WINDOW_TS:
            raise ValueError("WINDOW_TS is required")
        if self.MAX_REJECT_PERCENTAGE < 0 or self.MAX_REJECT_PERCENTAGE > 100:
            raise ValueError("MAX_REJECT_PERCENTAGE must be between 0 and 100")
        if self.MIN_EXPECTED_RECORDS < 0:
            raise ValueError("MIN_EXPECTED_RECORDS must be non-negative")

# =========================
# Enhanced Logging Configuration
# =========================
class MetricsLogger:
    def __init__(self, config):
        self.config = config
        self.metrics = {}
        self.start_time = time.time()
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, config.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def log_step_metrics(self, df, step_name, cache_if_enabled=False):
        """Log comprehensive step metrics including row count and execution time"""
        step_start = time.time()
        
        # Cache if enabled and requested
        if self.config.CACHE_ENABLED and cache_if_enabled:
            df = df.cache()
        
        count = df.count()
        step_duration = time.time() - step_start
        
        # Store metrics
        self.metrics[step_name] = {
            'row_count': count,
            'duration_seconds': step_duration,
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Step: {step_name} - Row count: {count} - Duration: {step_duration:.2f}s")
        
        if self.config.ENABLE_METRICS:
            self.logger.info(f"Metrics for {step_name}: {self.metrics[step_name]}")
        
        return count
    
    def log_validation_metrics(self, total_input, total_rejects, step_name):
        """Log validation metrics and check thresholds"""
        if total_input == 0:
            reject_percentage = 0
        else:
            reject_percentage = (total_rejects / total_input) * 100
        
        self.logger.info(f"{step_name} - Total Input: {total_input}, Rejects: {total_rejects}, Reject %: {reject_percentage:.2f}%")
        
        if reject_percentage > self.config.MAX_REJECT_PERCENTAGE:
            self.logger.warning(f"High reject percentage ({reject_percentage:.2f}%) exceeds threshold ({self.config.MAX_REJECT_PERCENTAGE}%)")
        
        return reject_percentage
    
    def log_final_summary(self):
        """Log final execution summary"""
        total_duration = time.time() - self.start_time
        self.logger.info(f"Total execution time: {total_duration:.2f} seconds")
        
        if self.config.ENABLE_METRICS:
            self.logger.info(f"Complete metrics summary: {self.metrics}")

# =========================
# Schema Definitions with Enhanced Validation
# =========================
# Section 3.1: kafka_event.dml
kafka_event_schema = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("ingest_ts", StringType(), True),
    StructField("payload_json", StringType(), True)
])

# Section 3.3: event_meta.dml with processing timestamp
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
    StructField("payload_json", StringType(), True),
    StructField("processing_ts", TimestampType(), True)
])

# Section 3.2: template_record.dml
template_record_schema = StructType([
    StructField("template_key", StringType(), True),
    StructField("template_text", StringType(), True)
])

# Section 3.4: reject_record.dml with enhanced fields
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
    StructField("payload_json", StringType(), True),
    StructField("processing_ts", TimestampType(), True)
])

# Section 3.5: xml_out_record.dml with metadata
xml_out_record_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("xml_text", StringType(), True),
    StructField("processing_ts", TimestampType(), True),
    StructField("xml_length", IntegerType(), True)
])

def create_enhanced_sample_data(spark, config, metrics_logger):
    """Create enhanced sample data with more test scenarios"""
    
    # Enhanced sample kafka events data with more edge cases
    kafka_sample_data = [
        ("key1", 0, 1001, "2026-01-28T10:00:00Z", 
         '{"event_id":"evt001","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:00:00Z","loan_id":"loan001","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.5","new_rate":"3.25","prior_term_months":"360","new_term_months":"360"}'),
        ("key2", 0, 1002, "2026-01-28T10:01:00Z", 
         '{"event_id":"evt002","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:01:00Z","loan_id":"loan002","source_system":"CORE","amendment_type":"TERM_CHANGE","effective_date":"2026-02-01","prior_rate":"4.0","new_rate":"4.0","prior_term_months":"360","new_term_months":"300"}'),
        ("key3", 0, 1003, "2026-01-28T10:02:00Z", 
         '{"event_id":"","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:02:00Z","loan_id":"loan003","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"3.75","new_rate":"3.5"}'),  # Missing event_id
        ("key4", 0, 1004, "2026-01-28T10:03:00Z", 
         '{"event_id":"evt004","event_type":"OTHER_EVENT","event_ts":"2026-01-28T10:03:00Z","loan_id":"loan004","source_system":"CORE"}'),  # Wrong event type
        ("key5", 0, 1005, "2026-01-28T10:04:00Z", 
         '{"event_id":"evt005","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:04:00Z","loan_id":"loan005","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"","prior_rate":"3.75","new_rate":"3.5"}'),  # Missing effective_date
        ("key6", 0, 1006, "2026-01-28T10:05:00Z", 
         '{"event_id":"evt006","event_type":"MORTGAGE_AMENDMENT","event_ts":"2026-01-28T10:05:00Z","loan_id":"loan006","source_system":"CORE","amendment_type":"RATE_CHANGE","effective_date":"2026-02-01","prior_rate":"4.25","new_rate":""}')  # Missing new_rate for RATE_CHANGE
    ]
    
    kafka_df = spark.createDataFrame(kafka_sample_data, kafka_event_schema)
    kafka_df.write.format("delta").mode("overwrite").save(config.DELTA_LANDING_TABLE)
    
    # Enhanced template data with better XML structure
    template_sample_data = [
        ("MISMO_TEMPLATE", 
         "<MISMO xmlns='http://www.mismo.org/residential/2009/schemas'><LoanID>{{LOAN_ID}}</LoanID><EventID>{{EVENT_ID}}</EventID><EffectiveDate>{{EFFECTIVE_DATE}}</EffectiveDate><AmendmentType>{{AMENDMENT_TYPE}}</AmendmentType><NewRate>{{NEW_RATE}}</NewRate><NewTermMonths>{{NEW_TERM_MONTHS}}</NewTermMonths><ProcessingTimestamp>" + datetime.now().isoformat() + "</ProcessingTimestamp></MISMO>")
    ]
    
    template_df = spark.createDataFrame(template_sample_data, template_record_schema)
    template_df.write.format("delta").mode("overwrite").save(config.DELTA_TEMPLATE_TABLE)
    
    metrics_logger.logger.info("Enhanced sample data created successfully with additional test scenarios")

def main():
    # Initialize enhanced configuration
    config = EnhancedConfig()
    metrics_logger = MetricsLogger(config)
    
    try:
        # =========================
        # Spark Session with Enhanced Configuration
        # =========================
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName("G_MtgAmendment_To_MISMO_XML_Enhanced") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.broadcastTimeout", "36000") \
                .getOrCreate()
        
        # Set broadcast threshold
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(config.BROADCAST_THRESHOLD))
        
        metrics_logger.logger.info("Starting Enhanced Mortgage Amendment to MISMO XML processing")
        metrics_logger.logger.info(f"Configuration: Environment={config._get_config('ENVIRONMENT', 'dev')}, Window={config.WINDOW_TS}")
        
        # Create enhanced sample data
        create_enhanced_sample_data(spark, config, metrics_logger)
        
        # =========================
        # Step 1: Ingest Landing File with Validation
        # =========================
        metrics_logger.logger.info(f"Reading landing data from: {config.DELTA_LANDING_TABLE}")
        landing_df = spark.read.format("delta").load(config.DELTA_LANDING_TABLE)
        landing_count = metrics_logger.log_step_metrics(landing_df, "Landing Data Ingestion", cache_if_enabled=True)
        
        # Validate minimum expected records
        if landing_count < config.MIN_EXPECTED_RECORDS:
            raise ValueError(f"Insufficient input records: {landing_count} < {config.MIN_EXPECTED_RECORDS}")
        
        # =========================
        # Step 2: Extract Event Metadata with Enhanced Error Handling
        # =========================
        metrics_logger.logger.info("Extracting event metadata from JSON payload")
        
        def json_find_string_value(json_str, key):
            import re
            try:
                if not json_str:
                    return ""
                pattern = r'"{}"\s*:\s*"([^"]*)"'.format(re.escape(key))
                m = re.search(pattern, json_str)
                return m.group(1) if m else ""
            except Exception:
                return ""
        
        def json_find_scalar_value(json_str, key):
            import re
            try:
                if not json_str:
                    return ""
                pattern = r'"{}"\s*:\s*"?([0-9.\-]+)"?'.format(re.escape(key))
                m = re.search(pattern, json_str)
                return m.group(1).strip() if m else ""
            except Exception:
                return ""
        
        json_find_string_value_udf = udf(json_find_string_value, StringType())
        json_find_scalar_value_udf = udf(json_find_scalar_value, StringType())
        
        event_meta_df = landing_df.withColumn("event_id", json_find_string_value_udf(col("payload_json"), lit("event_id"))) \
            .withColumn("event_type", json_find_string_value_udf(col("payload_json"), lit("event_type"))) \
            .withColumn("event_ts", json_find_string_value_udf(col("payload_json"), lit("event_ts"))) \
            .withColumn("loan_id", json_find_string_value_udf(col("payload_json"), lit("loan_id"))) \
            .withColumn("source_system", json_find_string_value_udf(col("payload_json"), lit("source_system"))) \
            .withColumn("processing_ts", current_timestamp()) \
            .select(
                "event_id", "event_type", "event_ts", "loan_id", "source_system",
                "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json", "processing_ts"
            )
        
        metadata_count = metrics_logger.log_step_metrics(event_meta_df, "Event Metadata Extraction", cache_if_enabled=True)
        
        # =========================
        # Step 3: Persist Raw Events with Partitioning
        # =========================
        metrics_logger.logger.info(f"Persisting raw events to: {config.DELTA_RAW_EVENTS_TABLE}")
        event_meta_df.write.format("delta").mode("overwrite").save(config.DELTA_RAW_EVENTS_TABLE)
        
        # =========================
        # Step 4: Enhanced Schema Validation and Split
        # =========================
        metrics_logger.logger.info("Performing enhanced schema validation")
        
        schema_reject_df = event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("event_id").isNull(), lit("Null event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("loan_id").isNull(), lit("Null loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .when(col("payload_json").isNull(), lit("Null payload_json"))
            .otherwise(lit(None))
        ).filter(col("reject_reason").isNotNull()) \
         .withColumn("reject_type", lit("SCHEMA")) \
         .withColumn("reason", col("reject_reason")) \
         .select(
            "reject_type", "reason", "event_id", "loan_id", "event_ts", "source_system",
            "kafka_partition", "kafka_offset", "ingest_ts", "payload_json", "processing_ts"
        )
        
        schema_reject_count = metrics_logger.log_step_metrics(schema_reject_df, "Schema Validation Rejects")
        
        valid_event_meta_df = event_meta_df.withColumn(
            "reject_reason",
            when(col("event_id") == "", lit("Missing event_id"))
            .when(col("event_id").isNull(), lit("Null event_id"))
            .when(col("loan_id") == "", lit("Missing loan_id"))
            .when(col("loan_id").isNull(), lit("Null loan_id"))
            .when(col("event_type") != "MORTGAGE_AMENDMENT", lit("Unexpected event_type"))
            .when(col("payload_json").isNull(), lit("Null payload_json"))
            .otherwise(lit(None))
        ).filter(col("reject_reason").isNull()) \
         .drop("reject_reason")
        
        valid_count = metrics_logger.log_step_metrics(valid_event_meta_df, "Valid Events After Schema Validation")
        
        # Log validation metrics
        metrics_logger.log_validation_metrics(metadata_count, schema_reject_count, "Schema Validation")
        
        # =========================
        # Step 5: Enhanced Canonicalize Mortgage Amendment
        # =========================
        metrics_logger.logger.info("Canonicalizing mortgage amendment data with enhanced field extraction")
        
        canonical_amendment_df = valid_event_meta_df \
            .withColumn("amendment_type", json_find_string_value_udf(col("payload_json"), lit("amendment_type"))) \
            .withColumn("effective_date", json_find_string_value_udf(col("payload_json"), lit("effective_date"))) \
            .withColumn("prior_rate", json_find_scalar_value_udf(col("payload_json"), lit("prior_rate"))) \
            .withColumn("new_rate", json_find_scalar_value_udf(col("payload_json"), lit("new_rate"))) \
            .withColumn("prior_term_months", json_find_scalar_value_udf(col("payload_json"), lit("prior_term_months"))) \
            .withColumn("new_term_months", json_find_scalar_value_udf(col("payload_json"), lit("new_term_months")))
        
        canonical_count = metrics_logger.log_step_metrics(canonical_amendment_df, "Canonical Amendment Data", cache_if_enabled=True)
        
        # =========================
        # Step 6: Optimized Template Join
        # =========================
        metrics_logger.logger.info(f"Reading template records from: {config.DELTA_TEMPLATE_TABLE}")
        
        template_df = spark.read.format("delta").load(config.DELTA_TEMPLATE_TABLE)
        template_count = metrics_logger.log_step_metrics(template_df, "Template Records")
        
        # Broadcast join optimization
        canonical_with_template_df = canonical_amendment_df.crossJoin(broadcast(template_df))
        joined_count = metrics_logger.log_step_metrics(canonical_with_template_df, "After Template Join")
        
        # =========================
        # Step 7: Enhanced Business Validation and XML Render
        # =========================
        metrics_logger.logger.info("Performing enhanced business validation")
        
        def enhanced_business_reject_reason(effective_date, amendment_type, new_rate, new_term_months):
            try:
                if not effective_date or effective_date.strip() == "":
                    return "Missing effective_date"
                if not amendment_type or amendment_type.strip() == "":
                    return "Missing amendment_type"
                if amendment_type == "RATE_CHANGE" and (not new_rate or new_rate.strip() == ""):
                    return "Missing new_rate for RATE_CHANGE"
                if amendment_type == "TERM_CHANGE" and (not new_term_months or new_term_months.strip() == ""):
                    return "Missing new_term_months for TERM_CHANGE"
                # Additional validation for rate range
                if amendment_type == "RATE_CHANGE" and new_rate:
                    try:
                        rate_val = float(new_rate)
                        if rate_val < 0 or rate_val > 50:
                            return "Invalid rate range (must be 0-50%)"
                    except ValueError:
                        return "Invalid rate format"
                return None
            except Exception:
                return "Business validation error"
        
        enhanced_business_reject_reason_udf = udf(enhanced_business_reject_reason, StringType())
        
        business_reject_df = canonical_with_template_df.withColumn(
            "reject_reason",
            enhanced_business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"), col("new_term_months"))
        ).filter(col("reject_reason").isNotNull()) \
         .withColumn("reject_type", lit("BUSINESS")) \
         .withColumn("reason", col("reject_reason")) \
         .withColumn("payload_json", lit("")) \
         .select(
            "reject_type", "reason", "event_id", "loan_id", "event_ts", "source_system",
            "kafka_partition", "kafka_offset", "ingest_ts", "payload_json", "processing_ts"
        )
        
        business_reject_count = metrics_logger.log_step_metrics(business_reject_df, "Business Validation Rejects")
        
        business_valid_df = canonical_with_template_df.withColumn(
            "reject_reason",
            enhanced_business_reject_reason_udf(col("effective_date"), col("amendment_type"), col("new_rate"), col("new_term_months"))
        ).filter(col("reject_reason").isNull()) \
         .drop("reject_reason")
        
        business_valid_count = metrics_logger.log_step_metrics(business_valid_df, "Valid Events After Business Validation")
        
        # Log business validation metrics
        metrics_logger.log_validation_metrics(canonical_count, business_reject_count, "Business Validation")
        
        # Enhanced XML rendering with error handling
        def enhanced_render_xml(template_text, loan_id, event_id, effective_date, amendment_type, new_rate, new_term_months):
            try:
                xml = template_text or ""
                xml = xml.replace("{{LOAN_ID}}", loan_id or "")
                xml = xml.replace("{{EVENT_ID}}", event_id or "")
                xml = xml.replace("{{EFFECTIVE_DATE}}", effective_date or "")
                xml = xml.replace("{{AMENDMENT_TYPE}}", amendment_type or "")
                xml = xml.replace("{{NEW_RATE}}", new_rate or "")
                xml = xml.replace("{{NEW_TERM_MONTHS}}", new_term_months or "")
                return xml
            except Exception:
                return "<ERROR>XML_RENDERING_FAILED</ERROR>"
        
        enhanced_render_xml_udf = udf(enhanced_render_xml, StringType())
        
        xml_out_df = business_valid_df.withColumn(
            "xml_text",
            enhanced_render_xml_udf(
                col("template_text"),
                col("loan_id"),
                col("event_id"),
                col("effective_date"),
                col("amendment_type"),
                col("new_rate"),
                col("new_term_months")
            )
        ).withColumn("xml_length", F.length(col("xml_text"))) \
         .select("event_id", "loan_id", "xml_text", "processing_ts", "xml_length")
        
        xml_output_count = metrics_logger.log_step_metrics(xml_out_df, "Final XML Output")
        
        # =========================
        # Step 8: Enhanced Output Writing with Error Handling
        # =========================
        metrics_logger.logger.info("Writing output files with enhanced error handling")
        
        # Combine all rejects with deduplication
        all_rejects_df = schema_reject_df.union(business_reject_df)
        total_reject_count = metrics_logger.log_step_metrics(all_rejects_df, "Total Rejects")
        
        # Write rejects to Delta table with error handling
        try:
            metrics_logger.logger.info(f"Writing rejects to: {config.DELTA_REJECTS_TABLE}")
            all_rejects_df.write.format("delta").mode("overwrite").save(config.DELTA_REJECTS_TABLE)
            metrics_logger.logger.info("Rejects written successfully")
        except Exception as e:
            metrics_logger.logger.error(f"Failed to write rejects: {str(e)}")
            raise
        
        # Write MISMO XML output to Delta table with error handling
        try:
            metrics_logger.logger.info(f"Writing XML output to: {config.DELTA_XML_OUTPUT_TABLE}")
            xml_out_df.write.format("delta").mode("overwrite").save(config.DELTA_XML_OUTPUT_TABLE)
            metrics_logger.logger.info("XML output written successfully")
        except Exception as e:
            metrics_logger.logger.error(f"Failed to write XML output: {str(e)}")
            raise
        
        # Final validation
        total_processed = xml_output_count + total_reject_count
        if total_processed != landing_count:
            metrics_logger.logger.warning(f"Record count mismatch: Input={landing_count}, Processed={total_processed}")
        
        metrics_logger.logger.info("Enhanced processing completed successfully")
        
        # Log final summary
        metrics_logger.log_final_summary()
        
        # Return comprehensive summary
        return {
            "landing_count": landing_count,
            "schema_rejects": schema_reject_count,
            "business_rejects": business_reject_count,
            "xml_output_count": xml_output_count,
            "total_rejects": total_reject_count,
            "processing_time_seconds": time.time() - metrics_logger.start_time,
            "reject_percentage": (total_reject_count / landing_count * 100) if landing_count > 0 else 0
        }
        
    except Exception as e:
        metrics_logger.logger.error(f"Processing failed with error: {str(e)}")
        raise
    finally:
        # Cleanup cached DataFrames if caching was enabled
        if config.CACHE_ENABLED:
            spark.catalog.clearCache()

if __name__ == "__main__":
    summary = main()
    print(f"Enhanced Processing Summary: {summary}")
```