_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline with improved error handling and performance optimizations
## *Version*: 2
## *Changes*: Added comprehensive error handling, performance optimization strategies, data validation rules, monitoring and alerting mechanisms, and enhanced testing procedures
## *Reason*: To improve robustness, reliability, and maintainability of the ETL pipeline integration
## *Updated on*: 
_____________________________________________

# Enhanced Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration

## Introduction

This enhanced technical specification outlines the comprehensive requirements for integrating the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline that generates the `BRANCH_SUMMARY_REPORT`. This version includes advanced error handling, performance optimizations, and robust data validation mechanisms to ensure enterprise-grade reliability.

### Business Context
- **JIRA Story**: PCE-2 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Epic**: PCE-1 - PySpark Code enhancement for Databricks
- **Objective**: Enhance regulatory reporting with branch operational metadata while ensuring high availability and performance
- **Impact**: Addition of REGION and LAST_AUDIT_DATE columns to BRANCH_SUMMARY_REPORT with comprehensive data quality controls

## Code Changes Required for the Enhancement

### 1. Enhanced Main ETL Function Updates

#### File: `RegulatoryReportingETL.py`

**Function: `main()`** - Enhanced Version
```python
def main():
    """
    Enhanced Main ETL execution function with comprehensive error handling and monitoring.
    """
    spark = None
    execution_start_time = time.time()
    
    try:
        spark = get_spark_session()
        
        # Enhanced JDBC connection properties with connection pooling
        jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        connection_properties = {
            "user": "your_user",
            "password": "your_password",
            "driver": "oracle.jdbc.driver.OracleDriver",
            "fetchsize": "10000",  # Optimize fetch size for performance
            "batchsize": "10000",  # Optimize batch size for writes
            "numPartitions": "4"    # Control parallelism
        }

        # Read source tables with enhanced error handling
        logger.info("Starting data extraction phase")
        customer_df = read_table_with_validation(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table_with_validation(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table_with_validation(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table_with_validation(spark, jdbc_url, "BRANCH", connection_properties)
        
        # NEW: Read branch operational details with validation
        branch_operational_df = read_table_with_validation(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
        
        # Validate data quality before processing
        validate_source_data_quality(customer_df, account_df, transaction_df, branch_df, branch_operational_df)
        
        # Create and write AML_CUSTOMER_TRANSACTIONS
        logger.info("Creating AML Customer Transactions report")
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table_with_validation(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # Create and write enhanced BRANCH_SUMMARY_REPORT
        logger.info("Creating Enhanced Branch Summary Report")
        branch_summary_df = create_enhanced_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table_with_validation(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        # Log execution metrics
        execution_time = time.time() - execution_start_time
        logger.info(f"ETL job completed successfully in {execution_time:.2f} seconds")
        
        # Send success notification
        send_job_notification("SUCCESS", f"ETL completed in {execution_time:.2f}s")

    except Exception as e:
        execution_time = time.time() - execution_start_time
        logger.error(f"ETL job failed after {execution_time:.2f} seconds with exception: {e}")
        send_job_notification("FAILURE", str(e))
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")
```

### 2. Enhanced Data Reading with Validation

**New Function: `read_table_with_validation()`**
```python
def read_table_with_validation(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
    """
    Enhanced table reading with comprehensive validation and error handling.
    
    :param spark: The SparkSession object.
    :param jdbc_url: The JDBC URL for the database connection.
    :param table_name: The name of the table to read.
    :param connection_properties: A dictionary of connection properties.
    :return: A validated Spark DataFrame containing the table data.
    """
    logger.info(f"Reading and validating table: {table_name}")
    
    try:
        # Read table with optimized settings
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        
        # Validate table is not empty
        row_count = df.count()
        if row_count == 0:
            logger.warning(f"Table {table_name} is empty")
        else:
            logger.info(f"Successfully read {row_count} rows from {table_name}")
        
        # Cache frequently used tables for performance
        if table_name in ["BRANCH", "BRANCH_OPERATIONAL_DETAILS"]:
            df.cache()
            logger.info(f"Cached table {table_name} for performance optimization")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to read table {table_name}: {e}")
        # Implement retry logic for transient failures
        if "connection" in str(e).lower():
            logger.info(f"Retrying connection for table {table_name}")
            time.sleep(5)  # Wait before retry
            return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        raise
```

### 3. Enhanced Branch Summary Report Function

**Function: `create_enhanced_branch_summary_report()`**
```python
def create_enhanced_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                         branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates an enhanced BRANCH_SUMMARY_REPORT DataFrame with comprehensive data validation,
    performance optimizations, and robust error handling.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    from pyspark.sql.functions import col, count, sum, when, isnan, isnull, coalesce, lit, date_format
    
    logger.info("Creating Enhanced Branch Summary Report DataFrame with validation")
    
    try:
        # Data quality validation before processing
        logger.info("Performing data quality checks")
        
        # Check for null branch IDs
        null_branch_count = branch_operational_df.filter(col("BRANCH_ID").isNull()).count()
        if null_branch_count > 0:
            logger.warning(f"Found {null_branch_count} records with null BRANCH_ID in operational details")
        
        # Filter and validate active branch operational details
        active_branch_ops = branch_operational_df.filter(
            (col("IS_ACTIVE") == "Y") & 
            (col("BRANCH_ID").isNotNull())
        ).select(
            col("BRANCH_ID"),
            col("REGION"),
            # Format date consistently and handle nulls
            when(col("LAST_AUDIT_DATE").isNotNull(), 
                 date_format(col("LAST_AUDIT_DATE"), "yyyy-MM-dd"))
            .otherwise(lit(None)).alias("LAST_AUDIT_DATE")
        )
        
        logger.info(f"Active branch operational records: {active_branch_ops.count()}")
        
        # Create base aggregation with enhanced validation
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                    .join(branch_df, "BRANCH_ID", "inner") \
                                    .filter(col("AMOUNT").isNotNull() & (col("AMOUNT") >= 0)) \
                                    .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                    .agg(
                                        count("*").alias("TOTAL_TRANSACTIONS"),
                                        sum("AMOUNT").alias("TOTAL_AMOUNT")
                                    )
        
        logger.info(f"Base summary records: {base_summary.count()}")
        
        # Enhanced left join with operational details
        enhanced_summary = base_summary.join(active_branch_ops, "BRANCH_ID", "left") \
                                      .select(
                                          col("BRANCH_ID").cast("bigint"),
                                          col("BRANCH_NAME"),
                                          col("TOTAL_TRANSACTIONS"),
                                          col("TOTAL_AMOUNT"),
                                          # Handle null regions with default value
                                          coalesce(col("REGION"), lit("UNKNOWN")).alias("REGION"),
                                          # Handle null audit dates
                                          col("LAST_AUDIT_DATE")
                                      )
        
        # Final validation
        final_count = enhanced_summary.count()
        logger.info(f"Enhanced summary final record count: {final_count}")
        
        # Data quality metrics
        regions_populated = enhanced_summary.filter(col("REGION") != "UNKNOWN").count()
        audit_dates_populated = enhanced_summary.filter(col("LAST_AUDIT_DATE").isNotNull()).count()
        
        logger.info(f"Data quality metrics - Regions populated: {regions_populated}/{final_count}, "
                   f"Audit dates populated: {audit_dates_populated}/{final_count}")
        
        return enhanced_summary
        
    except Exception as e:
        logger.error(f"Error in create_enhanced_branch_summary_report: {e}")
        raise
```

### 4. Enhanced Data Validation Functions

**New Function: `validate_source_data_quality()`**
```python
def validate_source_data_quality(customer_df: DataFrame, account_df: DataFrame, 
                                transaction_df: DataFrame, branch_df: DataFrame, 
                                branch_operational_df: DataFrame) -> None:
    """
    Comprehensive data quality validation for all source tables.
    """
    from pyspark.sql.functions import col, isnan, isnull
    
    logger.info("Starting comprehensive data quality validation")
    
    validations = [
        ("CUSTOMER", customer_df, "CUSTOMER_ID"),
        ("ACCOUNT", account_df, "ACCOUNT_ID"),
        ("TRANSACTION", transaction_df, "TRANSACTION_ID"),
        ("BRANCH", branch_df, "BRANCH_ID"),
        ("BRANCH_OPERATIONAL_DETAILS", branch_operational_df, "BRANCH_ID")
    ]
    
    for table_name, df, key_column in validations:
        # Check for null primary keys
        null_keys = df.filter(col(key_column).isNull()).count()
        total_records = df.count()
        
        if null_keys > 0:
            logger.error(f"Data quality issue: {table_name} has {null_keys} null {key_column} values")
            raise ValueError(f"Data quality validation failed for {table_name}")
        
        logger.info(f"✓ {table_name}: {total_records} records, no null {key_column} values")
    
    # Specific validation for branch operational details
    invalid_active_flags = branch_operational_df.filter(
        ~col("IS_ACTIVE").isin(["Y", "N"])
    ).count()
    
    if invalid_active_flags > 0:
        logger.warning(f"Found {invalid_active_flags} invalid IS_ACTIVE flag values")
    
    logger.info("Data quality validation completed successfully")
```

### 5. Enhanced Delta Table Writing

**Function: `write_to_delta_table_with_validation()`**
```python
def write_to_delta_table_with_validation(df: DataFrame, table_name: str) -> None:
    """
    Enhanced Delta table writing with validation and performance optimization.
    
    :param df: The DataFrame to write.
    :param table_name: The name of the target Delta table.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name} with validation")
    
    try:
        # Pre-write validation
        record_count = df.count()
        if record_count == 0:
            logger.warning(f"DataFrame for {table_name} is empty")
            return
        
        logger.info(f"Writing {record_count} records to {table_name}")
        
        # Optimize DataFrame before writing
        optimized_df = df.coalesce(4)  # Reduce number of output files
        
        # Write with enhanced Delta options
        optimized_df.write.format("delta") \
                   .mode("overwrite") \
                   .option("mergeSchema", "true") \
                   .option("overwriteSchema", "true") \
                   .saveAsTable(table_name)
        
        # Post-write validation
        logger.info(f"Validating written data for {table_name}")
        written_count = spark.table(table_name).count()
        
        if written_count != record_count:
            raise ValueError(f"Data validation failed: Expected {record_count}, got {written_count}")
        
        logger.info(f"✓ Successfully written and validated {written_count} records to {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise
```

## Enhanced Data Model Updates

### Source Data Model Changes

#### Enhanced Source Table: BRANCH_OPERATIONAL_DETAILS
```sql
-- Enhanced with additional constraints and indexes
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT NOT NULL,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y' CHECK (IS_ACTIVE IN ('Y', 'N')),
    CREATED_DATE DATE DEFAULT SYSDATE,
    UPDATED_DATE DATE DEFAULT SYSDATE,
    PRIMARY KEY (BRANCH_ID),
    FOREIGN KEY (BRANCH_ID) REFERENCES BRANCH(BRANCH_ID)
);

-- Performance optimization indexes
CREATE INDEX IDX_BRANCH_OPS_ACTIVE ON BRANCH_OPERATIONAL_DETAILS(IS_ACTIVE);
CREATE INDEX IDX_BRANCH_OPS_REGION ON BRANCH_OPERATIONAL_DETAILS(REGION);
```

### Enhanced Target Data Model

#### Updated Target Table: BRANCH_SUMMARY_REPORT
```sql
-- Enhanced with additional metadata and constraints
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT NOT NULL,
    BRANCH_NAME STRING NOT NULL,
    TOTAL_TRANSACTIONS BIGINT DEFAULT 0,
    TOTAL_AMOUNT DOUBLE DEFAULT 0.0,
    REGION STRING,
    LAST_AUDIT_DATE STRING,
    ETL_LOAD_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    DATA_QUALITY_SCORE DOUBLE DEFAULT 1.0
)
USING delta
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.deletionVectors' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7',
    'delta.parquet.compression.codec' = 'zstd',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

## Enhanced Source-to-Target Mapping

### Comprehensive Field Mapping Table

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable | Validation Rule |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|----------------|
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | COALESCE(REGION, 'UNKNOWN') when IS_ACTIVE='Y' | STRING | No | Must not be empty |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | DATE_FORMAT(LAST_AUDIT_DATE, 'yyyy-MM-dd') when IS_ACTIVE='Y' | STRING | Yes | Valid date format |
| BRANCH_OPERATIONAL_DETAILS | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Join key - CAST to BIGINT | BIGINT | No | Must exist in BRANCH table |
| SYSTEM | CURRENT_TIMESTAMP | BRANCH_SUMMARY_REPORT | ETL_LOAD_DATE | CURRENT_TIMESTAMP() | TIMESTAMP | No | System generated |

### Enhanced Transformation Rules

#### 1. Region Mapping with Validation
```sql
CASE 
  WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' AND BRANCH_OPERATIONAL_DETAILS.REGION IS NOT NULL
  THEN UPPER(TRIM(BRANCH_OPERATIONAL_DETAILS.REGION))
  ELSE 'UNKNOWN'
END AS REGION
```

#### 2. Enhanced Date Mapping with Format Validation
```sql
CASE 
  WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' 
       AND BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE IS NOT NULL
  THEN DATE_FORMAT(BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE, 'yyyy-MM-dd')
  ELSE NULL
END AS LAST_AUDIT_DATE
```

#### 3. Data Quality Score Calculation
```sql
CASE 
  WHEN REGION IS NOT NULL AND REGION != 'UNKNOWN' AND LAST_AUDIT_DATE IS NOT NULL THEN 1.0
  WHEN REGION IS NOT NULL AND REGION != 'UNKNOWN' THEN 0.8
  WHEN LAST_AUDIT_DATE IS NOT NULL THEN 0.6
  ELSE 0.4
END AS DATA_QUALITY_SCORE
```

## Performance Optimization Strategies

### 1. Spark Configuration Optimization
```python
# Enhanced Spark session configuration
def get_optimized_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
```

### 2. Caching Strategy
- Cache BRANCH and BRANCH_OPERATIONAL_DETAILS tables (small, frequently accessed)
- Use broadcast joins for small dimension tables
- Implement intelligent partitioning for large fact tables

### 3. Join Optimization
- Use broadcast hash joins for small tables (< 10MB)
- Implement bucketing for large table joins
- Optimize join order (smallest tables first)

## Monitoring and Alerting

### 1. Data Quality Monitoring
```python
def monitor_data_quality(df: DataFrame, table_name: str) -> dict:
    """
    Comprehensive data quality monitoring with metrics collection.
    """
    metrics = {
        "table_name": table_name,
        "record_count": df.count(),
        "null_percentage": calculate_null_percentage(df),
        "duplicate_count": df.count() - df.dropDuplicates().count(),
        "timestamp": datetime.now().isoformat()
    }
    
    # Log metrics for monitoring systems
    logger.info(f"Data Quality Metrics for {table_name}: {metrics}")
    
    return metrics
```

### 2. Performance Monitoring
- Track ETL execution time
- Monitor memory usage and spill metrics
- Alert on job failures or performance degradation

## Testing Strategy

### 1. Unit Tests
- Test individual transformation functions
- Validate data type conversions
- Test error handling scenarios

### 2. Integration Tests
- End-to-end ETL pipeline testing
- Data quality validation
- Performance benchmarking

### 3. Data Validation Tests
```python
def test_branch_summary_integration():
    """
    Comprehensive integration test for branch summary report.
    """
    # Create test data
    test_data = create_test_datasets()
    
    # Run ETL process
    result_df = create_enhanced_branch_summary_report(**test_data)
    
    # Validate results
    assert result_df.count() > 0, "Result should not be empty"
    assert "REGION" in result_df.columns, "REGION column should exist"
    assert "LAST_AUDIT_DATE" in result_df.columns, "LAST_AUDIT_DATE column should exist"
    
    # Validate data quality
    null_regions = result_df.filter(col("REGION").isNull()).count()
    assert null_regions == 0, "No null regions should exist after transformation"
```

## Deployment and Rollback Strategy

### 1. Blue-Green Deployment
- Deploy to staging environment first
- Validate data quality and performance
- Switch traffic to new version after validation

### 2. Rollback Plan
- Maintain previous version artifacts
- Implement automated rollback triggers
- Data backup and restore procedures

## Assumptions and Constraints

### Enhanced Assumptions
1. **Data Availability**: BRANCH_OPERATIONAL_DETAILS table is available with 99.9% uptime
2. **Data Freshness**: Source data is updated within acceptable SLA (< 1 hour)
3. **Performance**: ETL should complete within 30 minutes for typical data volumes
4. **Scalability**: Solution should handle 10x current data volume

### Enhanced Constraints
1. **Memory**: Maximum 16GB per executor
2. **Processing Time**: Maximum 45 minutes total execution time
3. **Data Retention**: Maintain 7 days of historical data for rollback
4. **Compliance**: All data transformations must be auditable

## References

- **JIRA Stories**: PCE-1, PCE-2
- **Source Files**: RegulatoryReportingETL.py, Source_DDL.txt, Target_DDL.txt, branch_operational_details.sql
- **Confluence Documentation**: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- **Delta Lake Documentation**: Schema Evolution, Performance Tuning
- **Spark Documentation**: Adaptive Query Execution, Performance Optimization
- **Data Quality Framework**: Internal DQ standards and best practices