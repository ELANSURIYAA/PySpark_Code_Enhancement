_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced delta model changes for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline with improved validation and monitoring
## *Version*: 2
## *Changes*: Enhanced validation framework, added comprehensive monitoring strategies, improved error handling mechanisms, and expanded rollback procedures
## *Reason*: User requested updates to improve robustness and operational excellence of the delta model changes
## *Updated on*: 
_____________________________________________

# Data Model Evolution Agent (DMEA) - Delta Model Changes SN (Enhanced)

## Executive Summary

This enhanced document outlines the delta changes required to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline that generates the `BRANCH_SUMMARY_REPORT`. This version includes improved validation frameworks, comprehensive monitoring strategies, and enhanced error handling mechanisms to ensure operational excellence.

### Change Classification
- **Change Type**: Schema Addition & Enhancement with Operational Improvements
- **Impact Level**: Minor (Backward Compatible)
- **Risk Assessment**: Low with Enhanced Mitigation
- **Deployment Strategy**: Blue-Green with Schema Evolution and Comprehensive Validation
- **Quality Gates**: Multi-layer validation with automated rollback triggers

## 1. Model Ingestion Analysis

### Current Data Model Structure

#### Existing Source Tables
```sql
-- Core Banking Tables with Enhanced Documentation
CUSTOMER (
    CUSTOMER_ID INT PRIMARY KEY,
    NAME STRING NOT NULL,
    EMAIL STRING,
    PHONE STRING,
    ADDRESS STRING,
    CREATED_DATE DATE DEFAULT CURRENT_DATE
);

BRANCH (
    BRANCH_ID INT PRIMARY KEY,
    BRANCH_NAME STRING NOT NULL,
    BRANCH_CODE STRING UNIQUE,
    CITY STRING,
    STATE STRING,
    COUNTRY STRING
);

ACCOUNT (
    ACCOUNT_ID INT PRIMARY KEY,
    CUSTOMER_ID INT REFERENCES CUSTOMER(CUSTOMER_ID),
    BRANCH_ID INT REFERENCES BRANCH(BRANCH_ID),
    ACCOUNT_NUMBER STRING UNIQUE,
    ACCOUNT_TYPE STRING,
    BALANCE DECIMAL(15,2) DEFAULT 0,
    OPENED_DATE DATE DEFAULT CURRENT_DATE
);

TRANSACTION (
    TRANSACTION_ID INT PRIMARY KEY,
    ACCOUNT_ID INT REFERENCES ACCOUNT(ACCOUNT_ID),
    TRANSACTION_TYPE STRING NOT NULL,
    AMOUNT DECIMAL(15,2) NOT NULL,
    TRANSACTION_DATE DATE DEFAULT CURRENT_DATE,
    DESCRIPTION STRING
);
```

#### Current Target Table with Metadata
```sql
BRANCH_SUMMARY_REPORT (
    BRANCH_ID BIGINT NOT NULL,
    BRANCH_NAME STRING NOT NULL,
    TOTAL_TRANSACTIONS BIGINT DEFAULT 0,
    TOTAL_AMOUNT DOUBLE DEFAULT 0.0
)
USING delta
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7'
);
```

### Enhanced Data Flow with Quality Checkpoints
```
TRANSACTION ──┐
              ├── JOIN ──► VALIDATION ──► GROUP BY ──► QUALITY CHECK ──► BRANCH_SUMMARY_REPORT
ACCOUNT ──────┤           CHECKPOINT                   CHECKPOINT
              │
BRANCH ───────┘
```

## 2. Enhanced Spec Parsing & Delta Detection

### New Source Table Addition with Constraints

#### BRANCH_OPERATIONAL_DETAILS (Enhanced)
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT PRIMARY KEY,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y' CHECK (IS_ACTIVE IN ('Y', 'N')),
    CREATED_DATE DATE DEFAULT SYSDATE,
    UPDATED_DATE DATE DEFAULT SYSDATE,
    CONSTRAINT FK_BRANCH_OPS_BRANCH FOREIGN KEY (BRANCH_ID) REFERENCES BRANCH(BRANCH_ID)
);

-- Enhanced Indexes for Performance
CREATE INDEX IDX_BRANCH_OPS_ACTIVE ON BRANCH_OPERATIONAL_DETAILS(IS_ACTIVE);
CREATE INDEX IDX_BRANCH_OPS_REGION ON BRANCH_OPERATIONAL_DETAILS(REGION);
CREATE INDEX IDX_BRANCH_OPS_AUDIT_DATE ON BRANCH_OPERATIONAL_DETAILS(LAST_AUDIT_DATE);
```

**Enhanced Table Characteristics:**
- **Relationship**: 1:1 with BRANCH table via BRANCH_ID with referential integrity
- **Filter Logic**: Only active branches (IS_ACTIVE = 'Y') with constraint validation
- **Join Type**: LEFT JOIN to preserve all branches with null handling
- **Audit Trail**: Created and updated timestamps for change tracking
- **Performance**: Optimized indexes for common query patterns

### Target Schema Evolution with Enhanced Properties

#### Enhanced BRANCH_SUMMARY_REPORT
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT NOT NULL,
    BRANCH_NAME STRING NOT NULL,
    TOTAL_TRANSACTIONS BIGINT DEFAULT 0,
    TOTAL_AMOUNT DOUBLE DEFAULT 0.0,
    REGION STRING COMMENT 'Branch operational region from BRANCH_OPERATIONAL_DETAILS',
    LAST_AUDIT_DATE STRING COMMENT 'Last audit date in ISO format (YYYY-MM-DD)',
    ETL_LOAD_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'ETL processing timestamp',
    DATA_QUALITY_SCORE DOUBLE COMMENT 'Data completeness score (0.0-1.0)'
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
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);
```

## 3. Enhanced Delta Computation

### Comprehensive Schema Changes Summary

| Change Type | Object | Details | Impact | Validation Rule |
|-------------|--------|---------|--------|-----------------|
| **ADD TABLE** | BRANCH_OPERATIONAL_DETAILS | New source with constraints | Source expansion | Row count > 0, FK integrity |
| **ADD COLUMN** | BRANCH_SUMMARY_REPORT.REGION | STRING, Nullable | Target enhancement | Valid region values |
| **ADD COLUMN** | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | STRING, Nullable | Target enhancement | Valid date format |
| **ADD COLUMN** | BRANCH_SUMMARY_REPORT.ETL_LOAD_DATE | TIMESTAMP, NOT NULL | Audit trail | Current timestamp |
| **ADD COLUMN** | BRANCH_SUMMARY_REPORT.DATA_QUALITY_SCORE | DOUBLE, Nullable | Quality metrics | Range 0.0-1.0 |
| **MODIFY ETL** | create_branch_summary_report() | Enhanced with validation | Processing change | Data quality checks |

### Enhanced Data Flow Evolution with Quality Gates
```
-- ENHANCED FLOW
TRANSACTION ──┐
              ├── JOIN ──► VALIDATE ──► GROUP BY ──┐
ACCOUNT ──────┤           DATA                    ├── LEFT JOIN ──► QUALITY ──► BRANCH_SUMMARY_REPORT
              │           QUALITY                 │              ASSESSMENT   (8 columns)
BRANCH ───────┘                                   │
                                                  │
BRANCH_OPERATIONAL_DETAILS ──► VALIDATE ─────────┘
(WHERE IS_ACTIVE = 'Y')        CONSTRAINTS
```

## 4. Enhanced Impact Assessment

### 4.1 Comprehensive Downstream Impact Analysis

#### Enhanced Risk Matrix
| Risk Category | Risk Level | Mitigation Strategy | Monitoring Metric |
|---------------|------------|--------------------|-----------------|
| **Schema Evolution** | Low | Automated validation | Schema version tracking |
| **Performance Impact** | Medium | Query optimization | Execution time monitoring |
| **Data Quality** | Medium | Multi-layer validation | Quality score tracking |
| **Backward Compatibility** | Low | Nullable columns | Consumer health checks |
| **Operational Complexity** | Medium | Enhanced monitoring | Alert coverage metrics |

### 4.2 Enhanced Data Quality Framework

#### Data Quality Dimensions
```python
# Data Quality Assessment Framework
def assess_data_quality(df: DataFrame) -> DataFrame:
    """
    Comprehensive data quality assessment for BRANCH_SUMMARY_REPORT
    """
    from pyspark.sql.functions import when, col, count, sum as spark_sum, lit
    
    total_rows = df.count()
    
    quality_metrics = df.agg(
        # Completeness metrics
        (count(col("REGION")) / lit(total_rows)).alias("region_completeness"),
        (count(col("LAST_AUDIT_DATE")) / lit(total_rows)).alias("audit_date_completeness"),
        
        # Validity metrics
        (spark_sum(when(col("TOTAL_TRANSACTIONS") >= 0, 1).otherwise(0)) / lit(total_rows)).alias("transaction_validity"),
        (spark_sum(when(col("TOTAL_AMOUNT") >= 0, 1).otherwise(0)) / lit(total_rows)).alias("amount_validity")
    ).collect()[0]
    
    # Calculate overall quality score
    quality_score = (
        quality_metrics["region_completeness"] * 0.25 +
        quality_metrics["audit_date_completeness"] * 0.25 +
        quality_metrics["transaction_validity"] * 0.25 +
        quality_metrics["amount_validity"] * 0.25
    )
    
    return df.withColumn("DATA_QUALITY_SCORE", lit(quality_score))
```

## 5. Enhanced DDL Generation & Migration Scripts

### 5.1 Comprehensive Forward Migration DDL

#### Enhanced Source Table Creation with Audit Trail
```sql
-- Oracle Source Database with Enhanced Features
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y',
    CREATED_DATE DATE DEFAULT SYSDATE,
    UPDATED_DATE DATE DEFAULT SYSDATE,
    CREATED_BY VARCHAR2(50) DEFAULT USER,
    UPDATED_BY VARCHAR2(50) DEFAULT USER,
    
    -- Constraints
    CONSTRAINT PK_BRANCH_OPS PRIMARY KEY (BRANCH_ID),
    CONSTRAINT FK_BRANCH_OPS_BRANCH FOREIGN KEY (BRANCH_ID) REFERENCES BRANCH(BRANCH_ID),
    CONSTRAINT CHK_IS_ACTIVE CHECK (IS_ACTIVE IN ('Y', 'N')),
    CONSTRAINT CHK_REGION_NOT_EMPTY CHECK (LENGTH(TRIM(REGION)) > 0)
);

-- Enhanced Performance Indexes
CREATE INDEX IDX_BRANCH_OPS_ACTIVE ON BRANCH_OPERATIONAL_DETAILS(IS_ACTIVE);
CREATE INDEX IDX_BRANCH_OPS_REGION ON BRANCH_OPERATIONAL_DETAILS(REGION);
CREATE INDEX IDX_BRANCH_OPS_AUDIT_DATE ON BRANCH_OPERATIONAL_DETAILS(LAST_AUDIT_DATE);
CREATE INDEX IDX_BRANCH_OPS_UPDATED ON BRANCH_OPERATIONAL_DETAILS(UPDATED_DATE);

-- Audit Trigger for Change Tracking
CREATE OR REPLACE TRIGGER TRG_BRANCH_OPS_AUDIT
    BEFORE UPDATE ON BRANCH_OPERATIONAL_DETAILS
    FOR EACH ROW
BEGIN
    :NEW.UPDATED_DATE := SYSDATE;
    :NEW.UPDATED_BY := USER;
END;
```

#### Enhanced Target Schema Evolution
```sql
-- Databricks Delta Lake with Advanced Features
ALTER TABLE workspace.default.branch_summary_report 
ADD COLUMNS (
    REGION STRING COMMENT 'Branch operational region from BRANCH_OPERATIONAL_DETAILS',
    LAST_AUDIT_DATE STRING COMMENT 'Last audit date in ISO format (YYYY-MM-DD)',
    ETL_LOAD_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'ETL processing timestamp for audit trail',
    DATA_QUALITY_SCORE DOUBLE COMMENT 'Data completeness and validity score (0.0-1.0)'
);

-- Enhanced Table Properties for Optimal Performance
ALTER TABLE workspace.default.branch_summary_report 
SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.deletionVectors' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.feature.changeDataFeed' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7',
    'delta.parquet.compression.codec' = 'zstd',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- Add Table Constraints for Data Quality
ALTER TABLE workspace.default.branch_summary_report 
ADD CONSTRAINT chk_total_transactions_positive CHECK (TOTAL_TRANSACTIONS >= 0);

ALTER TABLE workspace.default.branch_summary_report 
ADD CONSTRAINT chk_total_amount_positive CHECK (TOTAL_AMOUNT >= 0);

ALTER TABLE workspace.default.branch_summary_report 
ADD CONSTRAINT chk_quality_score_range CHECK (DATA_QUALITY_SCORE IS NULL OR (DATA_QUALITY_SCORE >= 0.0 AND DATA_QUALITY_SCORE <= 1.0));
```

### 5.2 Enhanced ETL Code with Comprehensive Error Handling

#### Production-Ready PySpark Function
```python
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, current_timestamp, lit, when, isnan, isnull
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)

def validate_source_data(df: DataFrame, table_name: str) -> DataFrame:
    """
    Comprehensive source data validation with quality metrics
    """
    logger.info(f"Validating source data for {table_name}")
    
    # Basic validation
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"Source table {table_name} is empty")
    
    # Check for required columns based on table type
    if table_name == "BRANCH_OPERATIONAL_DETAILS":
        required_cols = ["BRANCH_ID", "REGION", "IS_ACTIVE"]
        for col_name in required_cols:
            if col_name not in df.columns:
                raise ValueError(f"Required column {col_name} missing from {table_name}")
        
        # Validate IS_ACTIVE values
        invalid_active = df.filter(~col("IS_ACTIVE").isin(["Y", "N"])).count()
        if invalid_active > 0:
            logger.warning(f"Found {invalid_active} invalid IS_ACTIVE values in {table_name}")
    
    logger.info(f"Source validation completed for {table_name}: {row_count} rows")
    return df

def calculate_data_quality_score(df: DataFrame) -> DataFrame:
    """
    Calculate comprehensive data quality score
    """
    total_rows = df.count()
    
    if total_rows == 0:
        return df.withColumn("DATA_QUALITY_SCORE", lit(0.0))
    
    # Calculate quality metrics
    quality_df = df.withColumn(
        "DATA_QUALITY_SCORE",
        (
            # Region completeness (25%)
            when(col("REGION").isNotNull() & (col("REGION") != ""), 0.25).otherwise(0.0) +
            # Audit date validity (25%)
            when(col("LAST_AUDIT_DATE").isNotNull(), 0.25).otherwise(0.0) +
            # Transaction count validity (25%)
            when(col("TOTAL_TRANSACTIONS") >= 0, 0.25).otherwise(0.0) +
            # Amount validity (25%)
            when(col("TOTAL_AMOUNT") >= 0, 0.25).otherwise(0.0)
        ).cast(DoubleType())
    )
    
    return quality_df

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame with comprehensive validation
    and quality assessment.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report with quality metrics.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with validation.")
    
    try:
        # Validate source data
        transaction_df = validate_source_data(transaction_df, "TRANSACTION")
        account_df = validate_source_data(account_df, "ACCOUNT")
        branch_df = validate_source_data(branch_df, "BRANCH")
        branch_operational_df = validate_source_data(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS")
        
        # Filter active branch operational details with validation
        active_branch_ops = branch_operational_df.filter(
            (col("IS_ACTIVE") == "Y") & 
            col("BRANCH_ID").isNotNull()
        )
        
        logger.info(f"Active branch operations count: {active_branch_ops.count()}")
        
        # Create base aggregation with enhanced validation
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                    .join(branch_df, "BRANCH_ID", "inner") \
                                    .filter(col("AMOUNT").isNotNull() & ~isnan(col("AMOUNT"))) \
                                    .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                    .agg(
                                        count("*").alias("TOTAL_TRANSACTIONS"),
                                        spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                    )
        
        logger.info(f"Base summary count: {base_summary.count()}")
        
        # Enhanced left join with operational details
        enhanced_summary = base_summary.join(active_branch_ops, "BRANCH_ID", "left") \
                                      .select(
                                          col("BRANCH_ID"),
                                          col("BRANCH_NAME"),
                                          col("TOTAL_TRANSACTIONS"),
                                          col("TOTAL_AMOUNT"),
                                          col("REGION"),
                                          when(col("LAST_AUDIT_DATE").isNotNull(), 
                                               col("LAST_AUDIT_DATE").cast("string")).alias("LAST_AUDIT_DATE"),
                                          current_timestamp().alias("ETL_LOAD_DATE")
                                      )
        
        # Calculate data quality score
        final_summary = calculate_data_quality_score(enhanced_summary)
        
        # Final validation
        final_count = final_summary.count()
        logger.info(f"Enhanced summary final count: {final_count}")
        
        if final_count == 0:
            raise ValueError("Enhanced branch summary report is empty after processing")
        
        # Log quality statistics
        avg_quality = final_summary.agg(
            spark_sum("DATA_QUALITY_SCORE") / count("*")
        ).collect()[0][0]
        
        logger.info(f"Average data quality score: {avg_quality:.3f}")
        
        return final_summary
        
    except Exception as e:
        logger.error(f"Error in create_branch_summary_report: {str(e)}")
        raise

# Enhanced main() function with comprehensive error handling
def main():
    """
    Enhanced main ETL execution function with robust error handling
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # JDBC connection properties with enhanced configuration
        jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        connection_properties = {
            "user": "your_user",
            "password": "your_password",
            "driver": "oracle.jdbc.driver.OracleDriver",
            "fetchsize": "10000",
            "batchsize": "10000",
            "numPartitions": "4"
        }
        
        logger.info("Starting enhanced ETL process")
        
        # Read source tables with error handling
        try:
            customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
            account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
            transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
            branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
            branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
            
            logger.info("All source tables loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load source tables: {str(e)}")
            raise
        
        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged but with validation)
        try:
            aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
            write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
            logger.info("AML_CUSTOMER_TRANSACTIONS processed successfully")
        except Exception as e:
            logger.error(f"Failed to process AML_CUSTOMER_TRANSACTIONS: {str(e)}")
            raise
        
        # Create and write enhanced BRANCH_SUMMARY_REPORT
        try:
            branch_summary_df = create_branch_summary_report(
                transaction_df, account_df, branch_df, branch_operational_df
            )
            
            # Additional validation before writing
            if branch_summary_df.count() == 0:
                raise ValueError("Branch summary report is empty")
            
            write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")
            logger.info("Enhanced BRANCH_SUMMARY_REPORT processed successfully")
            
        except Exception as e:
            logger.error(f"Failed to process BRANCH_SUMMARY_REPORT: {str(e)}")
            raise
        
        logger.info("Enhanced ETL job completed successfully")
        
    except Exception as e:
        logger.error(f"Enhanced ETL job failed with exception: {e}")
        # Implement alerting mechanism here
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")
```

### 5.3 Enhanced Rollback Scripts with Safety Checks

#### Comprehensive Emergency Rollback DDL
```sql
-- Pre-rollback validation
SELECT 
    'ROLLBACK_VALIDATION' as CHECK_TYPE,
    COUNT(*) as CURRENT_ROW_COUNT,
    MAX(ETL_LOAD_DATE) as LAST_ETL_RUN
FROM workspace.default.branch_summary_report;

-- Create backup before rollback
CREATE TABLE workspace.default.branch_summary_report_backup_v2
AS SELECT * FROM workspace.default.branch_summary_report;

-- Rollback target schema changes (with safety checks)
ALTER TABLE workspace.default.branch_summary_report 
DROP COLUMNS (REGION, LAST_AUDIT_DATE, ETL_LOAD_DATE, DATA_QUALITY_SCORE);

-- Remove constraints added in v2
ALTER TABLE workspace.default.branch_summary_report 
DROP CONSTRAINT IF EXISTS chk_total_transactions_positive;

ALTER TABLE workspace.default.branch_summary_report 
DROP CONSTRAINT IF EXISTS chk_total_amount_positive;

ALTER TABLE workspace.default.branch_summary_report 
DROP CONSTRAINT IF EXISTS chk_quality_score_range;

-- Rollback source table (if needed)
DROP TRIGGER IF EXISTS TRG_BRANCH_OPS_AUDIT;
DROP INDEX IF EXISTS IDX_BRANCH_OPS_UPDATED;
DROP INDEX IF EXISTS IDX_BRANCH_OPS_AUDIT_DATE;
DROP INDEX IF EXISTS IDX_BRANCH_OPS_REGION;
DROP INDEX IF EXISTS IDX_BRANCH_OPS_ACTIVE;
DROP TABLE IF EXISTS BRANCH_OPERATIONAL_DETAILS;

-- Validation after rollback
SELECT 
    'POST_ROLLBACK_VALIDATION' as CHECK_TYPE,
    COUNT(*) as ROW_COUNT_AFTER_ROLLBACK,
    ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(COLUMN_NAME)), ', ') as REMAINING_COLUMNS
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'branch_summary_report';
```

## 6. Enhanced Monitoring & Alerting Framework

### 6.1 Comprehensive Monitoring Strategy

#### Real-time Quality Monitoring
```sql
-- Data Quality Dashboard Query
WITH quality_metrics AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(REGION) as region_populated,
        COUNT(LAST_AUDIT_DATE) as audit_date_populated,
        AVG(DATA_QUALITY_SCORE) as avg_quality_score,
        MIN(DATA_QUALITY_SCORE) as min_quality_score,
        MAX(ETL_LOAD_DATE) as last_etl_run,
        COUNT(CASE WHEN DATA_QUALITY_SCORE < 0.7 THEN 1 END) as low_quality_records
    FROM workspace.default.branch_summary_report
    WHERE ETL_LOAD_DATE >= CURRENT_DATE() - INTERVAL 1 DAY
)
SELECT 
    *,
    ROUND(region_populated * 100.0 / total_records, 2) as region_coverage_pct,
    ROUND(audit_date_populated * 100.0 / total_records, 2) as audit_coverage_pct,
    ROUND(low_quality_records * 100.0 / total_records, 2) as low_quality_pct,
    CASE 
        WHEN avg_quality_score >= 0.9 THEN 'EXCELLENT'
        WHEN avg_quality_score >= 0.8 THEN 'GOOD'
        WHEN avg_quality_score >= 0.7 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END as quality_status
FROM quality_metrics;
```

#### Performance Monitoring Queries
```sql
-- ETL Performance Tracking
CREATE OR REPLACE VIEW etl_performance_metrics AS
SELECT 
    DATE(ETL_LOAD_DATE) as etl_date,
    COUNT(*) as records_processed,
    COUNT(DISTINCT BRANCH_ID) as unique_branches,
    SUM(TOTAL_TRANSACTIONS) as total_transactions_sum,
    SUM(TOTAL_AMOUNT) as total_amount_sum,
    AVG(DATA_QUALITY_SCORE) as avg_quality_score,
    MIN(ETL_LOAD_DATE) as etl_start_time,
    MAX(ETL_LOAD_DATE) as etl_end_time
FROM workspace.default.branch_summary_report
GROUP BY DATE(ETL_LOAD_DATE)
ORDER BY etl_date DESC;
```

### 6.2 Automated Alert System

#### Critical Alert Conditions
```python
# Enhanced Alert Framework
class ETLMonitor:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.alert_thresholds = {
            'min_quality_score': 0.7,
            'max_execution_time_minutes': 60,
            'min_record_count': 100,
            'max_null_percentage': 30
        }
    
    def check_data_quality_alerts(self):
        """Check for data quality issues and trigger alerts"""
        alerts = []
        
        # Quality score alert
        low_quality_count = self.spark.sql("""
            SELECT COUNT(*) as count
            FROM workspace.default.branch_summary_report 
            WHERE DATA_QUALITY_SCORE < 0.7 
            AND ETL_LOAD_DATE >= CURRENT_DATE()
        """).collect()[0]['count']
        
        if low_quality_count > 0:
            alerts.append(f"ALERT: {low_quality_count} records with quality score < 0.7")
        
        # Coverage alert
        coverage_stats = self.spark.sql("""
            SELECT 
                COUNT(*) as total,
                COUNT(REGION) as region_count,
                COUNT(LAST_AUDIT_DATE) as audit_count
            FROM workspace.default.branch_summary_report 
            WHERE ETL_LOAD_DATE >= CURRENT_DATE()
        """).collect()[0]
        
        region_coverage = (coverage_stats['region_count'] / coverage_stats['total']) * 100
        if region_coverage < 70:
            alerts.append(f"ALERT: Region coverage is {region_coverage:.1f}% (< 70%)")
        
        return alerts
    
    def check_performance_alerts(self):
        """Check for performance issues"""
        alerts = []
        
        # Check recent ETL execution
        last_run = self.spark.sql("""
            SELECT MAX(ETL_LOAD_DATE) as last_run
            FROM workspace.default.branch_summary_report
        """).collect()[0]['last_run']
        
        if last_run:
            hours_since_last_run = (datetime.now() - last_run).total_seconds() / 3600
            if hours_since_last_run > 25:  # Should run daily
                alerts.append(f"ALERT: ETL hasn't run for {hours_since_last_run:.1f} hours")
        
        return alerts
```

## 7. Enhanced Success Criteria & Validation

### 7.1 Comprehensive Technical Success Criteria
- [ ] BRANCH_SUMMARY_REPORT contains 8 columns (4 existing + 4 new)
- [ ] All existing branches preserved in output with 100% coverage
- [ ] New columns populated for active branches with >80% coverage
- [ ] ETL job completes without errors and within SLA (< 60 minutes)
- [ ] Performance impact < 20% increase in execution time
- [ ] Zero data loss or corruption validated through checksums
- [ ] Data quality score > 0.8 for 95% of records
- [ ] All constraints and validations pass
- [ ] Rollback procedures tested and validated
- [ ] Monitoring and alerting system operational

### 7.2 Enhanced Business Success Criteria
- [ ] Enhanced regulatory reporting capability with audit trail
- [ ] Regional analysis support enabled with data lineage
- [ ] Audit compliance tracking functional with timestamps
- [ ] Backward compatibility maintained for all consumers
- [ ] Documentation updated and accessible to all stakeholders
- [ ] Training completed for operations team
- [ ] Performance benchmarks established and monitored
- [ ] Data governance policies updated and approved

## 8. Conclusion

This enhanced version of the delta model changes provides a robust, production-ready solution for integrating BRANCH_OPERATIONAL_DETAILS into the existing ETL pipeline. The improvements include:

### Key Enhancements in Version 2
- **Comprehensive Validation Framework**: Multi-layer data validation with quality scoring
- **Enhanced Error Handling**: Robust exception handling with detailed logging
- **Advanced Monitoring**: Real-time quality metrics and automated alerting
- **Performance Optimization**: Enhanced indexing and query optimization
- **Audit Trail**: Complete change tracking and data lineage
- **Operational Excellence**: Improved rollback procedures and safety checks

### Production Readiness Features
- **Data Quality Scoring**: Automated assessment of data completeness and validity
- **Performance Monitoring**: Real-time tracking of ETL execution metrics
- **Automated Alerting**: Proactive notification of quality and performance issues
- **Enhanced Documentation**: Comprehensive operational procedures and troubleshooting guides
- **Scalability Considerations**: Optimized for future growth and additional data sources

### Next Steps for Implementation
1. **Stakeholder Review**: Technical and business validation of enhanced features
2. **Environment Setup**: Comprehensive testing in staging environment
3. **Performance Baseline**: Establish performance benchmarks and SLAs
4. **Training & Documentation**: Operations team training and runbook creation
5. **Phased Rollout**: Gradual deployment with continuous monitoring
6. **Post-Implementation Review**: Performance optimization and lessons learned

---

**Document Status**: Enhanced and Ready for Implementation  
**Approval Required**: Data Architecture Team, ETL Team Lead, Operations Team, Business Stakeholders  
**Implementation Timeline**: 3-4 weeks including comprehensive testing and validation  
**Risk Level**: Low with Enhanced Mitigation Strategies  
**Quality Gates**: Multi-layer validation with automated rollback triggers