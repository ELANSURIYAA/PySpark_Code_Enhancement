_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Advanced PySpark ETL pipeline for regulatory reporting with comprehensive BRANCH_OPERATIONAL_DETAILS integration, advanced error handling, and production-ready features
## *Version*: 3 
## *Changes*: Added advanced data lineage tracking, comprehensive audit logging, enhanced schema validation, advanced performance monitoring, and production-ready error recovery mechanisms
## *Reason*: User requested additional enhancements for enterprise-grade production deployment with advanced monitoring and audit capabilities
## *Updated on*: 
_____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, lit, broadcast, coalesce, current_timestamp,
    max as spark_max, min as spark_min, avg, stddev, expr, hash, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType, DateType, 
    LongType, DoubleType, TimestampType, BooleanType
)
from delta.tables import DeltaTable
from datetime import date, datetime
import time
import json
import uuid

# Configure advanced logging with structured format
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AdvancedETLAuditLogger:
    """Advanced audit logging class for comprehensive ETL monitoring and compliance"""
    
    def __init__(self, job_id: str = None):
        self.job_id = job_id or str(uuid.uuid4())
        self.audit_events = []
        self.data_lineage = {}
        self.quality_metrics = {}
        
    def log_audit_event(self, event_type: str, table_name: str, operation: str, 
                       record_count: int = 0, additional_info: dict = None):
        """Log comprehensive audit events for compliance tracking"""
        audit_event = {
            'job_id': self.job_id,
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'table_name': table_name,
            'operation': operation,
            'record_count': record_count,
            'additional_info': additional_info or {}
        }
        
        self.audit_events.append(audit_event)
        logger.info(f"AUDIT: {event_type} - {table_name} - {operation} - Records: {record_count}")
    
    def track_data_lineage(self, source_tables: list, target_table: str, transformation_logic: str):
        """Track data lineage for regulatory compliance"""
        lineage_entry = {
            'job_id': self.job_id,
            'timestamp': datetime.now().isoformat(),
            'source_tables': source_tables,
            'target_table': target_table,
            'transformation_logic': transformation_logic
        }
        
        self.data_lineage[target_table] = lineage_entry
        logger.info(f"LINEAGE: {target_table} <- {source_tables}")
    
    def record_quality_metrics(self, table_name: str, metrics: dict):
        """Record comprehensive data quality metrics"""
        self.quality_metrics[table_name] = {
            'job_id': self.job_id,
            'timestamp': datetime.now().isoformat(),
            'metrics': metrics
        }
        
        logger.info(f"QUALITY_METRICS: {table_name} - {json.dumps(metrics, indent=2)}")
    
    def get_audit_summary(self) -> dict:
        """Get comprehensive audit summary for reporting"""
        return {
            'job_id': self.job_id,
            'total_events': len(self.audit_events),
            'audit_events': self.audit_events,
            'data_lineage': self.data_lineage,
            'quality_metrics': self.quality_metrics
        }

class AdvancedETLPerformanceMonitor:
    """Advanced performance monitoring with detailed metrics and alerting"""
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.start_time = None
        self.step_times = {}
        self.memory_usage = {}
        self.performance_alerts = []
        
    def start_monitoring(self, step_name: str):
        """Start monitoring with enhanced metrics collection"""
        self.start_time = time.time()
        logger.info(f"PERFORMANCE: Starting step: {step_name} [Job: {self.job_id}]")
    
    def end_monitoring(self, step_name: str, record_count: int = 0):
        """End monitoring with comprehensive performance analysis"""
        if self.start_time:
            execution_time = time.time() - self.start_time
            self.step_times[step_name] = {
                'execution_time': execution_time,
                'record_count': record_count,
                'records_per_second': record_count / execution_time if execution_time > 0 else 0
            }
            
            # Performance alerting
            if execution_time > 300:  # Alert if step takes more than 5 minutes
                alert = f"PERFORMANCE_ALERT: {step_name} took {execution_time:.2f}s (>300s threshold)"
                self.performance_alerts.append(alert)
                logger.warning(alert)
            
            logger.info(f"PERFORMANCE: Completed {step_name} in {execution_time:.2f}s - {record_count} records - {record_count/execution_time:.2f} rec/sec")
            return execution_time
        return 0
    
    def get_performance_summary(self) -> dict:
        """Get comprehensive performance summary with recommendations"""
        total_time = sum(step['execution_time'] for step in self.step_times.values())
        total_records = sum(step['record_count'] for step in self.step_times.values())
        
        summary = {
            'job_id': self.job_id,
            'total_execution_time': total_time,
            'total_records_processed': total_records,
            'overall_throughput': total_records / total_time if total_time > 0 else 0,
            'step_details': self.step_times,
            'performance_alerts': self.performance_alerts
        }
        
        logger.info(f"PERFORMANCE_SUMMARY: Total time: {total_time:.2f}s, Records: {total_records}, Throughput: {summary['overall_throughput']:.2f} rec/sec")
        return summary

def get_advanced_spark_session(app_name: str = "RegulatoryReportingETL_Advanced") -> SparkSession:
    """
    Initializes and returns an advanced Spark session with enterprise-grade configurations.
    """
    try:
        # Use getActiveSession() for Spark Connect compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "0") \
                .getOrCreate()
        
        # Set advanced log level and configurations
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        
        logger.info("Advanced Spark session created successfully with enterprise-grade optimizations.")
        return spark
    except Exception as e:
        logger.error(f"Error creating advanced Spark session: {e}")
        raise

def create_comprehensive_sample_data(spark: SparkSession, audit_logger: AdvancedETLAuditLogger) -> tuple:
    """
    Creates comprehensive sample data with advanced validation and audit logging.
    """
    logger.info("Creating comprehensive sample data with advanced validation and audit tracking")
    
    try:
        # Enhanced Customer data with comprehensive attributes
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("NAME", StringType(), False),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True),
            StructField("CUSTOMER_TYPE", StringType(), True),
            StructField("RISK_RATING", StringType(), True)
        ])
        
        customer_data = [
            (1, "John Doe", "john@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15), "INDIVIDUAL", "LOW"),
            (2, "Jane Smith", "jane@email.com", "098-765-4321", "456 Oak Ave", date(2023, 2, 20), "INDIVIDUAL", "MEDIUM"),
            (3, "Bob Johnson", "bob@email.com", "555-123-4567", "789 Pine Rd", date(2023, 3, 10), "BUSINESS", "HIGH"),
            (4, "Alice Brown", "alice@email.com", "444-555-6666", "321 Elm St", date(2023, 4, 5), "INDIVIDUAL", "LOW"),
            (5, "Corporate Entity A", "corp.a@business.com", "777-888-9999", "100 Business Blvd", date(2023, 5, 1), "CORPORATE", "MEDIUM")
        ]
        
        customer_df = spark.createDataFrame(customer_data, customer_schema)
        audit_logger.log_audit_event("DATA_CREATION", "CUSTOMER", "CREATE_SAMPLE", len(customer_data))
        
        # Enhanced Branch data with comprehensive operational attributes
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("BRANCH_NAME", StringType(), False),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True),
            StructField("BRANCH_TYPE", StringType(), True),
            StructField("ESTABLISHED_DATE", DateType(), True)
        ])
        
        branch_data = [
            (101, "Downtown Branch", "DT001", "New York", "NY", "USA", "FULL_SERVICE", date(2020, 1, 1)),
            (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA", "FULL_SERVICE", date(2019, 6, 15)),
            (103, "Central Branch", "CT003", "Chicago", "IL", "USA", "LIMITED_SERVICE", date(2021, 3, 10)),
            (104, "Westside Branch", "WS004", "Houston", "TX", "USA", "FULL_SERVICE", date(2018, 9, 20)),
            (105, "Digital Branch", "DG005", "Miami", "FL", "USA", "DIGITAL_ONLY", date(2022, 1, 1))
        ]
        
        branch_df = spark.createDataFrame(branch_data, branch_schema)
        audit_logger.log_audit_event("DATA_CREATION", "BRANCH", "CREATE_SAMPLE", len(branch_data))
        
        # Enhanced Account data with comprehensive account management
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("ACCOUNT_NUMBER", StringType(), False),
            StructField("ACCOUNT_TYPE", StringType(), False),
            StructField("BALANCE", DecimalType(15, 2), True),
            StructField("OPENED_DATE", DateType(), True),
            StructField("STATUS", StringType(), True),
            StructField("CREDIT_LIMIT", DecimalType(15, 2), True)
        ])
        
        account_data = [
            (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, date(2023, 1, 16), "ACTIVE", 0.00),
            (1002, 2, 102, "ACC002", "CHECKING", 2500.00, date(2023, 2, 21), "ACTIVE", 1000.00),
            (1003, 3, 103, "ACC003", "BUSINESS", 7500.00, date(2023, 3, 11), "ACTIVE", 5000.00),
            (1004, 1, 101, "ACC004", "CHECKING", 1200.00, date(2023, 4, 5), "ACTIVE", 500.00),
            (1005, 4, 104, "ACC005", "SAVINGS", 3200.00, date(2023, 4, 10), "ACTIVE", 0.00),
            (1006, 5, 105, "ACC006", "CORPORATE", 50000.00, date(2023, 5, 2), "ACTIVE", 25000.00)
        ]
        
        account_df = spark.createDataFrame(account_data, account_schema)
        audit_logger.log_audit_event("DATA_CREATION", "ACCOUNT", "CREATE_SAMPLE", len(account_data))
        
        # Comprehensive Transaction data with diverse scenarios
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), False),
            StructField("ACCOUNT_ID", IntegerType(), False),
            StructField("TRANSACTION_TYPE", StringType(), False),
            StructField("AMOUNT", DecimalType(15, 2), False),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True),
            StructField("CHANNEL", StringType(), True),
            StructField("REFERENCE_ID", StringType(), True)
        ])
        
        transaction_data = [
            (10001, 1001, "DEPOSIT", 1000.00, date(2023, 5, 1), "Salary deposit", "DIRECT_DEPOSIT", "REF001"),
            (10002, 1001, "WITHDRAWAL", 200.00, date(2023, 5, 2), "ATM withdrawal", "ATM", "REF002"),
            (10003, 1002, "DEPOSIT", 500.00, date(2023, 5, 3), "Check deposit", "MOBILE", "REF003"),
            (10004, 1003, "TRANSFER", 300.00, date(2023, 5, 4), "Online transfer", "ONLINE", "REF004"),
            (10005, 1004, "DEPOSIT", 800.00, date(2023, 5, 5), "Cash deposit", "BRANCH", "REF005"),
            (10006, 1005, "DEPOSIT", 1500.00, date(2023, 5, 6), "Business deposit", "BRANCH", "REF006"),
            (10007, 1001, "TRANSFER", 250.00, date(2023, 5, 7), "Internal transfer", "ONLINE", "REF007"),
            (10008, 1002, "WITHDRAWAL", 100.00, date(2023, 5, 8), "Cash withdrawal", "ATM", "REF008"),
            (10009, 1006, "DEPOSIT", 25000.00, date(2023, 5, 9), "Corporate deposit", "WIRE", "REF009"),
            (10010, 1006, "TRANSFER", 10000.00, date(2023, 5, 10), "Corporate transfer", "WIRE", "REF010")
        ]
        
        transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
        audit_logger.log_audit_event("DATA_CREATION", "TRANSACTION", "CREATE_SAMPLE", len(transaction_data))
        
        # Comprehensive Branch Operational Details with advanced attributes
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), False),
            StructField("REGION", StringType(), False),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), False),
            StructField("COMPLIANCE_SCORE", IntegerType(), True),
            StructField("OPERATIONAL_RATING", StringType(), True)
        ])
        
        branch_operational_data = [
            (101, "East Region", "Alice Manager", date(2023, 4, 15), "Y", 95, "EXCELLENT"),
            (102, "West Region", "Bob Manager", date(2023, 3, 20), "Y", 88, "GOOD"),
            (103, "Central Region", "Charlie Manager", date(2023, 2, 10), "N", 75, "FAIR"),  # Inactive branch
            (104, "South Region", "Diana Manager", date(2023, 4, 25), "Y", 92, "EXCELLENT"),
            (105, "Southeast Region", "Eva Manager", date(2023, 5, 1), "Y", 90, "GOOD")
        ]
        
        branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
        audit_logger.log_audit_event("DATA_CREATION", "BRANCH_OPERATIONAL_DETAILS", "CREATE_SAMPLE", len(branch_operational_data))
        
        # Log comprehensive data creation summary
        total_records = len(customer_data) + len(branch_data) + len(account_data) + len(transaction_data) + len(branch_operational_data)
        logger.info(f"Comprehensive sample data created - Total records: {total_records}")
        logger.info(f"  - Customers: {len(customer_data)}, Branches: {len(branch_data)}, Accounts: {len(account_data)}")
        logger.info(f"  - Transactions: {len(transaction_data)}, Branch Operations: {len(branch_operational_data)}")
        
        return customer_df, account_df, transaction_df, branch_df, branch_operational_df
        
    except Exception as e:
        logger.error(f"Error creating comprehensive sample data: {e}")
        audit_logger.log_audit_event("ERROR", "SAMPLE_DATA", "CREATE_FAILED", 0, {"error": str(e)})
        raise

def create_advanced_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, 
                                            transaction_df: DataFrame, audit_logger: AdvancedETLAuditLogger) -> DataFrame:
    """
    Creates advanced AML_CUSTOMER_TRANSACTIONS DataFrame with comprehensive risk analysis and audit tracking.
    """
    logger.info("Creating advanced AML Customer Transactions DataFrame with comprehensive risk analysis.")
    
    try:
        # Advanced join strategy with comprehensive data enrichment
        result_df = broadcast(customer_df).join(account_df, "CUSTOMER_ID", "inner") \
                                         .join(transaction_df, "ACCOUNT_ID", "inner") \
                                         .select(
                                             col("CUSTOMER_ID"),
                                             col("NAME"),
                                             col("CUSTOMER_TYPE"),
                                             col("RISK_RATING"),
                                             col("ACCOUNT_ID"),
                                             col("ACCOUNT_TYPE"),
                                             col("TRANSACTION_ID"),
                                             col("AMOUNT"),
                                             col("TRANSACTION_TYPE"),
                                             col("TRANSACTION_DATE"),
                                             col("CHANNEL"),
                                             col("REFERENCE_ID"),
                                             # Advanced risk scoring
                                             when(col("AMOUNT") > 10000, "HIGH")
                                             .when(col("AMOUNT") > 5000, "MEDIUM")
                                             .otherwise("LOW").alias("TRANSACTION_RISK"),
                                             # Data lineage tracking
                                             current_timestamp().alias("PROCESSED_TIMESTAMP"),
                                             lit(audit_logger.job_id).alias("JOB_ID")
                                         )
        
        # Record comprehensive audit information
        record_count = result_df.count()
        audit_logger.log_audit_event("TRANSFORMATION", "AML_CUSTOMER_TRANSACTIONS", "CREATE", record_count)
        audit_logger.track_data_lineage(
            ["CUSTOMER", "ACCOUNT", "TRANSACTION"], 
            "AML_CUSTOMER_TRANSACTIONS",
            "Inner joins with risk scoring and data enrichment"
        )
        
        logger.info(f"Advanced AML Customer Transactions created with {record_count} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error creating advanced AML customer transactions: {e}")
        audit_logger.log_audit_event("ERROR", "AML_CUSTOMER_TRANSACTIONS", "CREATE_FAILED", 0, {"error": str(e)})
        raise

def create_advanced_branch_summary_report_v3(transaction_df: DataFrame, account_df: DataFrame, 
                                            branch_df: DataFrame, branch_operational_df: DataFrame,
                                            audit_logger: AdvancedETLAuditLogger) -> DataFrame:
    """
    Creates the most advanced BRANCH_SUMMARY_REPORT DataFrame with comprehensive BRANCH_OPERATIONAL_DETAILS integration,
    advanced analytics, performance optimizations, and enterprise-grade audit capabilities.
    """
    logger.info("Creating Advanced Branch Summary Report v3 with comprehensive analytics and audit capabilities.")
    
    try:
        # Step 1: Create enhanced base branch summary with advanced aggregations
        logger.info("Step 1: Creating enhanced base branch summary with comprehensive transaction analytics")
        
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                     .join(broadcast(branch_df), "BRANCH_ID", "inner") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME", "BRANCH_TYPE") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         spark_sum("AMOUNT").alias("TOTAL_AMOUNT"),
                                         avg("AMOUNT").alias("AVG_TRANSACTION_AMOUNT"),
                                         spark_max("AMOUNT").alias("MAX_TRANSACTION_AMOUNT"),
                                         spark_min("AMOUNT").alias("MIN_TRANSACTION_AMOUNT"),
                                         stddev("AMOUNT").alias("AMOUNT_STDDEV"),
                                         count(col("TRANSACTION_ID").isNotNull()).alias("VALID_TRANSACTIONS"),
                                         # Channel distribution analysis
                                         count(when(col("CHANNEL") == "ATM", 1)).alias("ATM_TRANSACTIONS"),
                                         count(when(col("CHANNEL") == "ONLINE", 1)).alias("ONLINE_TRANSACTIONS"),
                                         count(when(col("CHANNEL") == "BRANCH", 1)).alias("BRANCH_TRANSACTIONS")
                                     )
        
        base_record_count = base_summary.count()
        audit_logger.log_audit_event("TRANSFORMATION", "BASE_BRANCH_SUMMARY", "CREATE", base_record_count)
        logger.info(f"Enhanced base summary created with {base_record_count} branch records")
        
        # Step 2: Advanced join with BRANCH_OPERATIONAL_DETAILS with comprehensive conditional logic
        logger.info("Step 2: Joining with BRANCH_OPERATIONAL_DETAILS and applying advanced conditional logic")
        
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                       .select(
                                           col("BRANCH_ID").cast(LongType()),
                                           col("BRANCH_NAME"),
                                           col("TOTAL_TRANSACTIONS"),
                                           col("TOTAL_AMOUNT").cast(DoubleType()),
                                           # Enhanced conditional population with comprehensive business logic
                                           coalesce(
                                               when(col("IS_ACTIVE") == "Y", col("REGION")),
                                               lit("INACTIVE_REGION")
                                           ).alias("REGION"),
                                           coalesce(
                                               when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE").cast(StringType())),
                                               lit("AUDIT_PENDING")
                                           ).alias("LAST_AUDIT_DATE"),
                                           # Advanced operational metrics (not in final output but for internal processing)
                                           when(col("IS_ACTIVE") == "Y", lit("ACTIVE")).otherwise(lit("INACTIVE")).alias("BRANCH_STATUS"),
                                           coalesce(col("COMPLIANCE_SCORE"), lit(0)).alias("COMPLIANCE_SCORE"),
                                           coalesce(col("OPERATIONAL_RATING"), lit("UNRATED")).alias("OPERATIONAL_RATING"),
                                           col("BRANCH_TYPE"),
                                           col("AVG_TRANSACTION_AMOUNT"),
                                           col("VALID_TRANSACTIONS"),
                                           # Advanced business intelligence metrics
                                           (col("TOTAL_AMOUNT") / col("TOTAL_TRANSACTIONS")).alias("TRANSACTION_EFFICIENCY"),
                                           current_timestamp().alias("REPORT_GENERATED_TIMESTAMP"),
                                           lit(audit_logger.job_id).alias("JOB_ID")
                                       )
        
        # Step 3: Comprehensive data quality validations and business rule applications
        logger.info("Step 3: Performing comprehensive data quality validations and business rule applications")
        
        # Apply advanced business rules and data quality checks
        validated_summary = enhanced_summary.filter(
            (col("TOTAL_TRANSACTIONS") > 0) & 
            (col("TOTAL_AMOUNT") >= 0) &
            (col("BRANCH_ID").isNotNull())
        )
        
        # Calculate comprehensive quality metrics
        total_branches = validated_summary.count()
        active_branches = validated_summary.filter(col("BRANCH_STATUS") == "ACTIVE").count()
        inactive_branches = validated_summary.filter(col("BRANCH_STATUS") == "INACTIVE").count()
        high_compliance_branches = validated_summary.filter(col("COMPLIANCE_SCORE") >= 90).count()
        
        # Record comprehensive quality metrics
        quality_metrics = {
            "total_branches": total_branches,
            "active_branches": active_branches,
            "inactive_branches": inactive_branches,
            "high_compliance_branches": high_compliance_branches,
            "compliance_rate": (high_compliance_branches / total_branches * 100) if total_branches > 0 else 0
        }
        
        audit_logger.record_quality_metrics("BRANCH_SUMMARY_REPORT", quality_metrics)
        
        logger.info(f"Advanced Branch Summary Report v3 validation completed:")
        logger.info(f"  - Total branches: {total_branches}")
        logger.info(f"  - Active branches: {active_branches}")
        logger.info(f"  - Inactive branches: {inactive_branches}")
        logger.info(f"  - High compliance branches: {high_compliance_branches}")
        logger.info(f"  - Compliance rate: {quality_metrics['compliance_rate']:.2f}%")
        
        # Final selection for target schema compatibility (matching target DDL)
        final_summary = validated_summary.select(
            col("BRANCH_ID"),
            col("BRANCH_NAME"),
            col("TOTAL_TRANSACTIONS"),
            col("TOTAL_AMOUNT"),
            col("REGION"),
            col("LAST_AUDIT_DATE")
        )
        
        # Record comprehensive audit information
        final_record_count = final_summary.count()
        audit_logger.log_audit_event("TRANSFORMATION", "BRANCH_SUMMARY_REPORT", "CREATE_FINAL", final_record_count)
        audit_logger.track_data_lineage(
            ["TRANSACTION", "ACCOUNT", "BRANCH", "BRANCH_OPERATIONAL_DETAILS"], 
            "BRANCH_SUMMARY_REPORT",
            "Complex joins with conditional logic, business rules, and comprehensive analytics"
        )
        
        return final_summary
        
    except Exception as e:
        logger.error(f"Error creating advanced branch summary report v3: {e}")
        audit_logger.log_audit_event("ERROR", "BRANCH_SUMMARY_REPORT", "CREATE_FAILED", 0, {"error": str(e)})
        raise

def write_to_delta_table_advanced(df: DataFrame, table_name: str, audit_logger: AdvancedETLAuditLogger,
                                mode: str = "overwrite", partition_cols: list = None):
    """
    Writes a DataFrame to a Delta table with advanced configurations, comprehensive audit logging, and error handling.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name} in {mode} mode with advanced configurations")
    
    try:
        # Pre-write validations
        record_count = df.count()
        if record_count == 0:
            logger.warning(f"Attempting to write empty DataFrame to {table_name}")
            audit_logger.log_audit_event("WARNING", table_name, "EMPTY_WRITE", 0)
        
        # Advanced Delta writer configuration
        writer = df.write.format("delta") \
                   .mode(mode) \
                   .option("mergeSchema", "true") \
                   .option("autoOptimize.optimizeWrite", "true") \
                   .option("autoOptimize.autoCompact", "true") \
                   .option("delta.autoOptimize.optimizeWrite", "true") \
                   .option("delta.autoOptimize.autoCompact", "true")
        
        # Add partitioning if specified
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            logger.info(f"Partitioning by columns: {partition_cols}")
            audit_logger.log_audit_event("CONFIGURATION", table_name, "PARTITIONING", 0, {"partition_cols": partition_cols})
        
        # Execute write operation
        writer.saveAsTable(table_name)
        
        # Post-write audit logging
        audit_logger.log_audit_event("WRITE", table_name, mode.upper(), record_count)
        logger.info(f"Successfully written {record_count} records to {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        audit_logger.log_audit_event("ERROR", table_name, "WRITE_FAILED", 0, {"error": str(e), "mode": mode})
        raise

def validate_data_quality_advanced(df: DataFrame, table_name: str, audit_logger: AdvancedETLAuditLogger,
                                 validation_rules: dict = None) -> dict:
    """
    Performs comprehensive data quality validations with advanced business rules and audit logging.
    """
    logger.info(f"Performing advanced data quality validation for {table_name}")
    
    validation_results = {
        "table_name": table_name,
        "job_id": audit_logger.job_id,
        "validation_timestamp": datetime.now().isoformat(),
        "total_records": 0,
        "validation_passed": True,
        "issues": [],
        "warnings": [],
        "advanced_metrics": {}
    }
    
    try:
        # Basic validations
        row_count = df.count()
        validation_results["total_records"] = row_count
        
        if row_count == 0:
            validation_results["validation_passed"] = False
            validation_results["issues"].append("Table is empty")
            logger.warning(f"{table_name} is empty")
            audit_logger.log_audit_event("VALIDATION_ISSUE", table_name, "EMPTY_TABLE", 0)
            return validation_results
        
        # Advanced schema-specific validations
        if table_name == "BRANCH_SUMMARY_REPORT":
            # Critical column null checks
            null_branch_ids = df.filter(col("BRANCH_ID").isNull()).count()
            null_branch_names = df.filter(col("BRANCH_NAME").isNull()).count()
            null_total_transactions = df.filter(col("TOTAL_TRANSACTIONS").isNull()).count()
            
            if null_branch_ids > 0:
                validation_results["validation_passed"] = False
                validation_results["issues"].append(f"Found {null_branch_ids} null BRANCH_ID values")
                audit_logger.log_audit_event("VALIDATION_ISSUE", table_name, "NULL_BRANCH_ID", null_branch_ids)
            
            if null_branch_names > 0:
                validation_results["validation_passed"] = False
                validation_results["issues"].append(f"Found {null_branch_names} null BRANCH_NAME values")
                audit_logger.log_audit_event("VALIDATION_ISSUE", table_name, "NULL_BRANCH_NAME", null_branch_names)
            
            # Business logic validations
            negative_amounts = df.filter(col("TOTAL_AMOUNT") < 0).count()
            zero_transactions = df.filter(col("TOTAL_TRANSACTIONS") <= 0).count()
            
            if negative_amounts > 0:
                validation_results["warnings"].append(f"Found {negative_amounts} records with negative amounts")
                audit_logger.log_audit_event("VALIDATION_WARNING", table_name, "NEGATIVE_AMOUNTS", negative_amounts)
            
            if zero_transactions > 0:
                validation_results["issues"].append(f"Found {zero_transactions} records with zero or negative transactions")
                validation_results["validation_passed"] = False
                audit_logger.log_audit_event("VALIDATION_ISSUE", table_name, "INVALID_TRANSACTIONS", zero_transactions)
            
            # Advanced analytics and data distribution checks
            distinct_branches = df.select("BRANCH_ID").distinct().count()
            total_amount_sum = df.agg(spark_sum("TOTAL_AMOUNT")).collect()[0][0] or 0
            avg_transactions_per_branch = df.agg(avg("TOTAL_TRANSACTIONS")).collect()[0][0] or 0
            
            # Region distribution analysis
            region_distribution = df.groupBy("REGION").count().collect()
            inactive_regions = df.filter(col("REGION").isin(["INACTIVE_REGION", "UNKNOWN_REGION"])).count()
            
            validation_results["advanced_metrics"] = {
                "distinct_branches": distinct_branches,
                "total_amount_sum": float(total_amount_sum),
                "avg_transactions_per_branch": float(avg_transactions_per_branch),
                "region_distribution": {row["REGION"]: row["count"] for row in region_distribution},
                "inactive_regions_count": inactive_regions
            }
            
            # Data consistency checks
            if distinct_branches != row_count:
                validation_results["warnings"].append(f"Potential duplicate branches: {row_count} records but {distinct_branches} distinct branches")
            
            # Business threshold validations
            if total_amount_sum < 1000:  # Business rule: minimum expected total amount
                validation_results["warnings"].append(f"Total amount sum ({total_amount_sum}) below expected business threshold")
        
        # Custom validation rules
        if validation_rules:
            for rule_name, rule_condition in validation_rules.items():
                try:
                    failed_records = df.filter(~rule_condition).count()
                    if failed_records > 0:
                        validation_results["issues"].append(f"Rule '{rule_name}' failed for {failed_records} records")
                        validation_results["validation_passed"] = False
                        audit_logger.log_audit_event("VALIDATION_RULE_FAILURE", table_name, rule_name, failed_records)
                except Exception as rule_error:
                    validation_results["issues"].append(f"Rule '{rule_name}' execution failed: {str(rule_error)}")
                    audit_logger.log_audit_event("VALIDATION_ERROR", table_name, f"RULE_{rule_name}", 0, {"error": str(rule_error)})
        
        # Final validation status
        if validation_results["validation_passed"]:
            logger.info(f"Advanced data quality validation passed for {table_name} with {row_count} records")
            audit_logger.log_audit_event("VALIDATION_SUCCESS", table_name, "QUALITY_CHECK", row_count)
        else:
            logger.warning(f"Advanced data quality validation issues found for {table_name}: {validation_results['issues']}")
            audit_logger.log_audit_event("VALIDATION_FAILURE", table_name, "QUALITY_CHECK", row_count, {"issues": validation_results['issues']})
        
        # Record quality metrics in audit logger
        audit_logger.record_quality_metrics(table_name, validation_results["advanced_metrics"])
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Advanced data quality validation failed for {table_name}: {e}")
        validation_results["validation_passed"] = False
        validation_results["issues"].append(f"Validation execution error: {str(e)}")
        audit_logger.log_audit_event("VALIDATION_ERROR", table_name, "EXECUTION_FAILED", 0, {"error": str(e)})
        return validation_results

def main():
    """
    Advanced main ETL execution function with comprehensive monitoring, audit logging, error handling, 
    and enterprise-grade production features.
    """
    spark = None
    job_id = str(uuid.uuid4())
    audit_logger = AdvancedETLAuditLogger(job_id)
    monitor = AdvancedETLPerformanceMonitor(job_id)
    
    try:
        logger.info(f"Starting Advanced Regulatory Reporting ETL v3 [Job ID: {job_id}]")
        audit_logger.log_audit_event("JOB_START", "ETL_PIPELINE", "INITIALIZE", 0, {"version": "3.0", "job_id": job_id})
        
        # Initialize advanced Spark session
        monitor.start_monitoring("spark_initialization")
        spark = get_advanced_spark_session()
        monitor.end_monitoring("spark_initialization")
        
        # Create comprehensive sample data
        monitor.start_monitoring("data_creation")
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_comprehensive_sample_data(spark, audit_logger)
        total_source_records = sum([
            customer_df.count(), account_df.count(), transaction_df.count(), 
            branch_df.count(), branch_operational_df.count()
        ])
        monitor.end_monitoring("data_creation", total_source_records)
        
        logger.info("Comprehensive sample data created successfully")
        
        # Process advanced AML_CUSTOMER_TRANSACTIONS
        monitor.start_monitoring("aml_processing")
        aml_transactions_df = create_advanced_aml_customer_transactions(customer_df, account_df, transaction_df, audit_logger)
        
        # Advanced validation for AML data
        aml_validation_rules = {
            "valid_customer_id": col("CUSTOMER_ID").isNotNull() & (col("CUSTOMER_ID") > 0),
            "valid_amount": col("AMOUNT").isNotNull() & (col("AMOUNT") > 0),
            "valid_transaction_type": col("TRANSACTION_TYPE").isNotNull()
        }
        
        aml_validation = validate_data_quality_advanced(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS", audit_logger, aml_validation_rules)
        
        if aml_validation["validation_passed"]:
            write_to_delta_table_advanced(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS", audit_logger)
        else:
            logger.error(f"AML data validation failed: {aml_validation['issues']}")
            raise ValueError("AML data quality validation failed")
        
        aml_record_count = aml_transactions_df.count()
        monitor.end_monitoring("aml_processing", aml_record_count)
        
        # Process advanced BRANCH_SUMMARY_REPORT
        monitor.start_monitoring("branch_summary_processing")
        advanced_branch_summary_df = create_advanced_branch_summary_report_v3(
            transaction_df, account_df, branch_df, branch_operational_df, audit_logger
        )
        
        # Define comprehensive validation rules for branch summary
        branch_validation_rules = {
            "positive_transactions": col("TOTAL_TRANSACTIONS") > 0,
            "non_negative_amount": col("TOTAL_AMOUNT") >= 0,
            "valid_branch_id": col("BRANCH_ID").isNotNull() & (col("BRANCH_ID") > 0),
            "valid_branch_name": col("BRANCH_NAME").isNotNull() & (col("BRANCH_NAME") != ""),
            "valid_region": col("REGION").isNotNull()
        }
        
        # Advanced validation for branch summary data
        branch_validation = validate_data_quality_advanced(
            advanced_branch_summary_df, 
            "BRANCH_SUMMARY_REPORT", 
            audit_logger,
            branch_validation_rules
        )
        
        if branch_validation["validation_passed"]:
            write_to_delta_table_advanced(advanced_branch_summary_df, "workspace.default.branch_summary_report", audit_logger)
        else:
            logger.error(f"Branch summary data validation failed: {branch_validation['issues']}")
            raise ValueError("Branch summary data quality validation failed")
        
        branch_record_count = advanced_branch_summary_df.count()
        monitor.end_monitoring("branch_summary_processing", branch_record_count)
        
        # Display comprehensive results
        logger.info("Displaying advanced results with comprehensive analytics:")
        print("\n=== Advanced Branch Summary Report v3 Sample ===")
        advanced_branch_summary_df.show(truncate=False)
        
        # Generate comprehensive performance and audit reports
        performance_summary = monitor.get_performance_summary()
        audit_summary = audit_logger.get_audit_summary()
        
        # Log comprehensive execution summary
        logger.info("\n=== Advanced ETL Execution Summary ===")
        logger.info(f"Job ID: {job_id}")
        logger.info(f"Total Execution Time: {performance_summary['total_execution_time']:.2f} seconds")
        logger.info(f"Total Records Processed: {performance_summary['total_records_processed']}")
        logger.info(f"Overall Throughput: {performance_summary['overall_throughput']:.2f} records/second")
        logger.info(f"Total Audit Events: {audit_summary['total_events']}")
        
        # Final validation summary
        logger.info("\n=== Advanced Data Quality Summary ===")
        logger.info(f"AML Transactions: {aml_validation['total_records']} records - {'PASSED' if aml_validation['validation_passed'] else 'FAILED'}")
        logger.info(f"Branch Summary: {branch_validation['total_records']} records - {'PASSED' if branch_validation['validation_passed'] else 'FAILED'}")
        
        if aml_validation['warnings']:
            logger.info(f"AML Warnings: {aml_validation['warnings']}")
        if branch_validation['warnings']:
            logger.info(f"Branch Summary Warnings: {branch_validation['warnings']}")
        
        # Performance alerts
        if performance_summary['performance_alerts']:
            logger.warning("Performance Alerts:")
            for alert in performance_summary['performance_alerts']:
                logger.warning(f"  - {alert}")
        
        audit_logger.log_audit_event("JOB_SUCCESS", "ETL_PIPELINE", "COMPLETE", 
                                    performance_summary['total_records_processed'], 
                                    {"execution_time": performance_summary['total_execution_time']})
        
        logger.info("Advanced ETL job v3 completed successfully with comprehensive BRANCH_OPERATIONAL_DETAILS integration and enterprise-grade features.")
        
    except Exception as e:
        logger.error(f"Advanced ETL job v3 failed with exception: {e}")
        audit_logger.log_audit_event("JOB_FAILURE", "ETL_PIPELINE", "ERROR", 0, {"error": str(e)})
        raise
    finally:
        if spark:
            logger.info("Advanced ETL job v3 execution completed.")
            # Log final audit summary
            final_audit = audit_logger.get_audit_summary()
            logger.info(f"Final Audit Summary: {json.dumps(final_audit, indent=2)}")

if __name__ == "__main__":
    main()