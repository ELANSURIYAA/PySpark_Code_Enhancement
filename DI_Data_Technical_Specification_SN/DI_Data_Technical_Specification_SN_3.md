_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Technical specification for BRANCH_OPERATIONAL_DETAILS integration into BRANCH_SUMMARY_REPORT ETL pipeline
## *Version*: 3
## *Updated on*: 
## *Changes*: Added specific implementation for BRANCH_OPERATIONAL_DETAILS table integration, enhanced PySpark ETL logic for branch summary reporting, added new column mappings for REGION and LAST_AUDIT_DATE
## *Reason*: Business requirement to integrate branch operational metadata for compliance and audit readiness as per JIRA stories PCE-1 and PCE-2
_____________________________________________

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration

## Introduction

This technical specification document outlines the implementation details for integrating the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline. The enhancement focuses on extending the `BRANCH_SUMMARY_REPORT` logic to include branch-level operational metadata for improved compliance and audit readiness.

### Business Requirement
To improve compliance and audit readiness, a new Oracle source table `BRANCH_OPERATIONAL_DETAILS` has been introduced. This table includes branch-level operational metadata (region, manager name, audit date, active status) and needs to be integrated into the existing ETL pipeline.

### Objectives
- Integrate `BRANCH_OPERATIONAL_DETAILS` table into existing PySpark ETL pipeline
- Extend `BRANCH_SUMMARY_REPORT` with two new columns: `REGION` and `LAST_AUDIT_DATE`
- Maintain backward compatibility with existing records
- Ensure data integrity and audit trail compliance

### Scope
- PySpark ETL logic modifications in `RegulatoryReportingETL.py`
- Delta table structure updates for `BRANCH_SUMMARY_REPORT`
- Source-to-target mapping for new operational fields
- Data validation and reconciliation routines
- Conditional population based on `IS_ACTIVE = 'Y'`

## Code Changes Required for Enhancement

### 1. Enhanced PySpark ETL Logic

#### 1.1 Updated Main ETL Function
```python
def main():
    """
    Enhanced main ETL execution function with BRANCH_OPERATIONAL_DETAILS integration.
    """
    spark = None
    try:
        spark = get_spark_session()

        # JDBC connection properties
        jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        connection_properties = {
            "user": "your_user",
            "password": "your_password",
            "driver": "oracle.jdbc.driver.OracleDriver"
        }

        # Read source tables (existing + new)
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        
        # NEW: Read BRANCH_OPERATIONAL_DETAILS table
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # ENHANCED: Create BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_enhanced_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("Enhanced ETL job completed successfully with BRANCH_OPERATIONAL_DETAILS integration.")

    except Exception as e:
        logger.error(f"Enhanced ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
```

#### 1.2 Enhanced Branch Summary Report Function
```python
def create_enhanced_branch_summary_report(
    transaction_df: DataFrame, 
    account_df: DataFrame, 
    branch_df: DataFrame, 
    branch_operational_df: DataFrame
) -> DataFrame:
    """
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data 
    at the branch level and integrating operational metadata.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report with Operational Details.")
    
    from pyspark.sql.functions import col, count, sum as spark_sum, when, coalesce
    
    # Step 1: Create base branch summary (existing logic)
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Step 2: Filter active operational details
    active_operational_df = branch_operational_df.filter(col("IS_ACTIVE") == "Y")
    
    # Step 3: Join with operational details (LEFT JOIN to maintain all branches)
    enhanced_summary = base_summary.join(
        active_operational_df.select(
            col("BRANCH_ID"),
            col("REGION"),
            col("LAST_AUDIT_DATE")
        ),
        "BRANCH_ID",
        "left"
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        # NEW COLUMNS: Conditional population based on IS_ACTIVE = 'Y'
        coalesce(col("REGION"), lit("UNKNOWN")).alias("REGION"),
        col("LAST_AUDIT_DATE").cast("string").alias("LAST_AUDIT_DATE")
    )
    
    logger.info("Enhanced Branch Summary Report created successfully.")
    return enhanced_summary
```

#### 1.3 Data Validation Function for New Integration
```python
def validate_branch_operational_integration(branch_summary_df: DataFrame) -> bool:
    """
    Validates the enhanced branch summary report with operational details.
    
    :param branch_summary_df: The enhanced branch summary DataFrame
    :return: Boolean indicating validation success
    """
    logger.info("Validating BRANCH_OPERATIONAL_DETAILS integration.")
    
    try:
        # Check if new columns exist
        required_columns = ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", 
                          "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]
        
        missing_columns = set(required_columns) - set(branch_summary_df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Validate data types
        schema_validations = {
            "BRANCH_ID": "bigint",
            "BRANCH_NAME": "string",
            "TOTAL_TRANSACTIONS": "bigint",
            "TOTAL_AMOUNT": "double",
            "REGION": "string",
            "LAST_AUDIT_DATE": "string"
        }
        
        for col_name, expected_type in schema_validations.items():
            actual_type = dict(branch_summary_df.dtypes)[col_name]
            if actual_type != expected_type:
                logger.error(f"Column {col_name} type mismatch. Expected: {expected_type}, Actual: {actual_type}")
                return False
        
        # Check for null values in critical columns
        null_counts = branch_summary_df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in ["BRANCH_ID", "BRANCH_NAME"]]
        ).collect()[0]
        
        if null_counts["BRANCH_ID"] > 0 or null_counts["BRANCH_NAME"] > 0:
            logger.error("Critical columns contain null values")
            return False
        
        logger.info("BRANCH_OPERATIONAL_DETAILS integration validation passed.")
        return True
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return False
```

### 2. Error Handling and Logging Enhancements

#### 2.1 Enhanced Error Handling
```python
class BranchOperationalIntegrationException(Exception):
    """Custom exception for branch operational integration errors."""
    pass

def safe_branch_operational_join(base_df: DataFrame, operational_df: DataFrame) -> DataFrame:
    """
    Safely join branch summary with operational details with comprehensive error handling.
    """
    try:
        # Validate input DataFrames
        if base_df.count() == 0:
            raise BranchOperationalIntegrationException("Base branch summary DataFrame is empty")
        
        # Check for duplicate BRANCH_IDs in operational data
        operational_count = operational_df.count()
        distinct_operational_count = operational_df.select("BRANCH_ID").distinct().count()
        
        if operational_count != distinct_operational_count:
            logger.warning("Duplicate BRANCH_IDs found in operational data. Using latest record.")
            # Keep only the latest record per BRANCH_ID
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, desc
            
            window_spec = Window.partitionBy("BRANCH_ID").orderBy(desc("LAST_AUDIT_DATE"))
            operational_df = operational_df.withColumn("rn", row_number().over(window_spec)) \
                                         .filter(col("rn") == 1) \
                                         .drop("rn")
        
        # Perform the join
        result_df = base_df.join(operational_df, "BRANCH_ID", "left")
        
        logger.info(f"Successfully joined {base_df.count()} base records with {operational_df.count()} operational records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in branch operational join: {e}")
        raise BranchOperationalIntegrationException(f"Join operation failed: {e}")
```

## Data Model Updates

### 3. Source Data Model - BRANCH_OPERATIONAL_DETAILS

#### 3.1 Source Table Schema (Oracle)
```sql
-- BRANCH_OPERATIONAL_DETAILS table structure
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT NOT NULL,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y',
    CREATED_DATE DATE DEFAULT SYSDATE,
    UPDATED_DATE DATE,
    PRIMARY KEY (BRANCH_ID)
);

-- Indexes for performance
CREATE INDEX IDX_BRANCH_OP_ACTIVE ON BRANCH_OPERATIONAL_DETAILS(IS_ACTIVE);
CREATE INDEX IDX_BRANCH_OP_REGION ON BRANCH_OPERATIONAL_DETAILS(REGION);
CREATE INDEX IDX_BRANCH_OP_AUDIT_DATE ON BRANCH_OPERATIONAL_DETAILS(LAST_AUDIT_DATE);
```

#### 3.2 Source Data Characteristics
- **Primary Key**: BRANCH_ID (1:1 relationship with BRANCH table)
- **Data Volume**: Estimated 500-1000 records (one per branch)
- **Update Frequency**: Monthly (audit dates and manager changes)
- **Data Retention**: 7 years for audit compliance
- **Critical Fields**: BRANCH_ID, REGION, IS_ACTIVE

### 4. Target Data Model - Enhanced BRANCH_SUMMARY_REPORT

#### 4.1 Updated Target Table Schema (Databricks Delta)
```sql
-- Enhanced BRANCH_SUMMARY_REPORT with new columns
CREATE OR REPLACE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT NOT NULL,
    BRANCH_NAME STRING NOT NULL,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,           -- NEW COLUMN
    LAST_AUDIT_DATE STRING   -- NEW COLUMN
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

-- Add constraints for data quality
ALTER TABLE workspace.default.branch_summary_report 
ADD CONSTRAINT branch_id_not_null CHECK (BRANCH_ID IS NOT NULL);

ALTER TABLE workspace.default.branch_summary_report 
ADD CONSTRAINT branch_name_not_null CHECK (BRANCH_NAME IS NOT NULL);
```

#### 4.2 Schema Evolution Strategy
```python
def handle_schema_evolution():
    """
    Handle schema evolution for BRANCH_SUMMARY_REPORT table.
    """
    try:
        # Check if new columns exist
        existing_schema = spark.table("workspace.default.branch_summary_report").schema
        existing_columns = [field.name for field in existing_schema.fields]
        
        new_columns = ["REGION", "LAST_AUDIT_DATE"]
        missing_columns = [col for col in new_columns if col not in existing_columns]
        
        if missing_columns:
            logger.info(f"Adding missing columns: {missing_columns}")
            
            # Add missing columns with default values
            for col_name in missing_columns:
                if col_name == "REGION":
                    spark.sql("""
                        ALTER TABLE workspace.default.branch_summary_report 
                        ADD COLUMN REGION STRING DEFAULT 'UNKNOWN'
                    """)
                elif col_name == "LAST_AUDIT_DATE":
                    spark.sql("""
                        ALTER TABLE workspace.default.branch_summary_report 
                        ADD COLUMN LAST_AUDIT_DATE STRING
                    """)
            
            logger.info("Schema evolution completed successfully.")
        else:
            logger.info("Schema is up to date.")
            
    except Exception as e:
        logger.error(f"Schema evolution failed: {e}")
        raise
```

## Source-to-Target Mapping

### 5. Detailed Field Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable | Business Rule |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|---------------|
| TRANSACTION + ACCOUNT + BRANCH | - | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct from BRANCH.BRANCH_ID | BIGINT | No | Primary identifier |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING | No | Branch display name |
| TRANSACTION | COUNT(*) | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | Aggregation by BRANCH_ID | BIGINT | Yes | Count of all transactions |
| TRANSACTION | SUM(AMOUNT) | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | Aggregation by BRANCH_ID | DOUBLE | Yes | Sum of transaction amounts |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | COALESCE(REGION, 'UNKNOWN') | STRING | Yes | Only if IS_ACTIVE = 'Y' |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | CAST(LAST_AUDIT_DATE AS STRING) | STRING | Yes | Only if IS_ACTIVE = 'Y' |

### 6. Transformation Rules Detail

#### 6.1 Business Logic Implementation
```python
def apply_business_transformation_rules(df: DataFrame) -> DataFrame:
    """
    Apply specific business transformation rules for branch operational integration.
    """
    from pyspark.sql.functions import when, col, coalesce, lit, upper, trim
    
    transformed_df = df.select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        
        # REGION transformation with business rules
        when(col("IS_ACTIVE") == "Y", 
             coalesce(upper(trim(col("REGION"))), lit("UNKNOWN"))
        ).otherwise(lit("INACTIVE")).alias("REGION"),
        
        # LAST_AUDIT_DATE transformation
        when(col("IS_ACTIVE") == "Y", 
             col("LAST_AUDIT_DATE").cast("string")
        ).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
    )
    
    return transformed_df
```

#### 6.2 Data Quality Rules
```python
def apply_data_quality_rules(df: DataFrame) -> DataFrame:
    """
    Apply data quality rules specific to branch operational integration.
    """
    from pyspark.sql.functions import when, col, length, regexp_replace
    
    # Data cleansing rules
    quality_df = df.withColumn(
        "REGION",
        when(length(col("REGION")) == 0, "UNKNOWN")
        .when(col("REGION").rlike("^[A-Z\\s]+$"), col("REGION"))
        .otherwise(regexp_replace(col("REGION"), "[^A-Z\\s]", ""))
    ).withColumn(
        "LAST_AUDIT_DATE",
        when(col("LAST_AUDIT_DATE").rlike("^\\d{4}-\\d{2}-\\d{2}$"), col("LAST_AUDIT_DATE"))
        .otherwise(lit(None))
    )
    
    return quality_df
```

### 7. Data Reconciliation Strategy

#### 7.1 Reconciliation Checks
```python
def perform_data_reconciliation(source_df: DataFrame, target_df: DataFrame) -> Dict[str, Any]:
    """
    Perform comprehensive data reconciliation between source and target.
    """
    reconciliation_results = {
        "status": "PASSED",
        "checks": {},
        "discrepancies": []
    }
    
    try:
        # Record count reconciliation
        source_count = source_df.count()
        target_count = target_df.count()
        
        reconciliation_results["checks"]["record_count"] = {
            "source_count": source_count,
            "target_count": target_count,
            "match": source_count == target_count
        }
        
        # Branch ID reconciliation
        source_branches = set([row.BRANCH_ID for row in source_df.select("BRANCH_ID").collect()])
        target_branches = set([row.BRANCH_ID for row in target_df.select("BRANCH_ID").collect()])
        
        missing_in_target = source_branches - target_branches
        extra_in_target = target_branches - source_branches
        
        reconciliation_results["checks"]["branch_id_reconciliation"] = {
            "missing_in_target": list(missing_in_target),
            "extra_in_target": list(extra_in_target),
            "match": len(missing_in_target) == 0 and len(extra_in_target) == 0
        }
        
        # Region population check
        region_populated = target_df.filter(col("REGION").isNotNull() & (col("REGION") != "UNKNOWN")).count()
        total_records = target_df.count()
        region_population_rate = (region_populated / total_records) * 100 if total_records > 0 else 0
        
        reconciliation_results["checks"]["region_population"] = {
            "populated_count": region_populated,
            "total_count": total_records,
            "population_rate_percent": region_population_rate
        }
        
        # Overall status
        all_checks_passed = all(
            check.get("match", True) for check in reconciliation_results["checks"].values()
        )
        
        if not all_checks_passed:
            reconciliation_results["status"] = "FAILED"
        
        return reconciliation_results
        
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        reconciliation_results["status"] = "ERROR"
        reconciliation_results["error"] = str(e)
        return reconciliation_results
```

## Implementation Strategy

### 8. Deployment Plan

#### 8.1 Pre-Deployment Checklist
- [ ] Verify BRANCH_OPERATIONAL_DETAILS table exists in Oracle source
- [ ] Confirm JDBC connectivity to Oracle database
- [ ] Validate Delta table permissions in Databricks
- [ ] Test schema evolution scripts in development environment
- [ ] Prepare rollback scripts for emergency scenarios
- [ ] Set up monitoring and alerting for new integration

#### 8.2 Deployment Steps
```python
def execute_deployment():
    """
    Execute the deployment of BRANCH_OPERATIONAL_DETAILS integration.
    """
    deployment_steps = [
        ("validate_prerequisites", validate_deployment_prerequisites),
        ("backup_existing_data", backup_branch_summary_report),
        ("evolve_schema", handle_schema_evolution),
        ("deploy_etl_code", deploy_enhanced_etl_logic),
        ("run_initial_load", execute_full_reload),
        ("validate_results", validate_deployment_results),
        ("enable_monitoring", enable_enhanced_monitoring)
    ]
    
    for step_name, step_function in deployment_steps:
        try:
            logger.info(f"Executing deployment step: {step_name}")
            step_function()
            logger.info(f"Deployment step {step_name} completed successfully")
        except Exception as e:
            logger.error(f"Deployment step {step_name} failed: {e}")
            # Execute rollback
            execute_rollback(step_name)
            raise DeploymentException(f"Deployment failed at step {step_name}: {e}")
```

### 9. Testing Strategy

#### 9.1 Unit Tests
```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

class TestBranchOperationalIntegration:
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        return SparkSession.builder \
            .appName("BranchOperationalTests") \
            .master("local[2]") \
            .getOrCreate()
    
    def test_branch_operational_join_logic(self, spark_session):
        """Test the join logic between branch summary and operational details."""
        # Create test data
        branch_summary_data = [
            (1, "Branch A", 100, 50000.0),
            (2, "Branch B", 150, 75000.0),
            (3, "Branch C", 200, 100000.0)
        ]
        
        operational_data = [
            (1, "NORTH", "2024-01-15", "Y"),
            (2, "SOUTH", "2024-02-10", "Y"),
            (3, "EAST", "2024-03-05", "N")  # Inactive branch
        ]
        
        # Create DataFrames
        branch_summary_df = spark_session.createDataFrame(
            branch_summary_data,
            ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT"]
        )
        
        operational_df = spark_session.createDataFrame(
            operational_data,
            ["BRANCH_ID", "REGION", "LAST_AUDIT_DATE", "IS_ACTIVE"]
        )
        
        # Apply transformation
        result_df = create_enhanced_branch_summary_report(
            spark_session.createDataFrame([], StructType([])),  # Mock transaction_df
            spark_session.createDataFrame([], StructType([])),  # Mock account_df
            spark_session.createDataFrame([], StructType([])),  # Mock branch_df
            operational_df
        )
        
        # Assertions
        assert result_df.count() == 3
        
        # Check that inactive branch has proper handling
        inactive_branch = result_df.filter(col("BRANCH_ID") == 3).collect()[0]
        assert inactive_branch["REGION"] == "INACTIVE"
        assert inactive_branch["LAST_AUDIT_DATE"] is None
    
    def test_data_quality_validation(self, spark_session):
        """Test data quality validation for the enhanced report."""
        # Create test data with quality issues
        test_data = [
            (1, "Branch A", 100, 50000.0, "NORTH", "2024-01-15"),
            (2, "Branch B", 150, 75000.0, "", "invalid-date"),  # Quality issues
            (3, None, 200, 100000.0, "EAST", "2024-03-05")  # Null branch name
        ]
        
        test_df = spark_session.createDataFrame(
            test_data,
            ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]
        )
        
        # Apply quality rules
        quality_df = apply_data_quality_rules(test_df)
        
        # Validate results
        results = quality_df.collect()
        
        # Check that empty region is handled
        assert results[1]["REGION"] == "UNKNOWN"
        
        # Check that invalid date is nullified
        assert results[1]["LAST_AUDIT_DATE"] is None
```

#### 9.2 Integration Tests
```python
def test_end_to_end_integration():
    """Test complete end-to-end integration flow."""
    try:
        # Setup test environment
        spark = get_spark_session()
        
        # Create test tables
        setup_test_tables(spark)
        
        # Execute ETL pipeline
        main()
        
        # Validate results
        result_df = spark.table("workspace.default.branch_summary_report")
        
        # Comprehensive validations
        assert result_df.count() > 0, "No records in target table"
        
        # Check schema
        expected_columns = ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", 
                          "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]
        actual_columns = result_df.columns
        assert set(expected_columns).issubset(set(actual_columns)), "Missing required columns"
        
        # Check data quality
        null_branch_ids = result_df.filter(col("BRANCH_ID").isNull()).count()
        assert null_branch_ids == 0, "Found null BRANCH_IDs"
        
        logger.info("End-to-end integration test passed successfully")
        
    except Exception as e:
        logger.error(f"Integration test failed: {e}")
        raise
```

## Monitoring and Maintenance

### 10. Enhanced Monitoring

#### 10.1 Key Performance Indicators
```python
class BranchOperationalMonitor:
    def __init__(self):
        self.metrics = {}
    
    def collect_integration_metrics(self, result_df: DataFrame) -> Dict[str, Any]:
        """Collect metrics specific to branch operational integration."""
        metrics = {
            "total_branches": result_df.count(),
            "branches_with_region": result_df.filter(
                col("REGION").isNotNull() & (col("REGION") != "UNKNOWN")
            ).count(),
            "branches_with_audit_date": result_df.filter(
                col("LAST_AUDIT_DATE").isNotNull()
            ).count(),
            "region_coverage_percent": 0.0,
            "audit_date_coverage_percent": 0.0
        }
        
        if metrics["total_branches"] > 0:
            metrics["region_coverage_percent"] = (
                metrics["branches_with_region"] / metrics["total_branches"]
            ) * 100
            
            metrics["audit_date_coverage_percent"] = (
                metrics["branches_with_audit_date"] / metrics["total_branches"]
            ) * 100
        
        return metrics
    
    def check_data_freshness(self, operational_df: DataFrame) -> Dict[str, Any]:
        """Check freshness of operational data."""
        from datetime import datetime, timedelta
        
        current_date = datetime.now().date()
        stale_threshold = current_date - timedelta(days=90)  # 90 days
        
        stale_audits = operational_df.filter(
            col("LAST_AUDIT_DATE") < stale_threshold
        ).count()
        
        total_active = operational_df.filter(col("IS_ACTIVE") == "Y").count()
        
        return {
            "stale_audit_count": stale_audits,
            "total_active_branches": total_active,
            "stale_percentage": (stale_audits / total_active * 100) if total_active > 0 else 0
        }
```

#### 10.2 Alerting Configuration
```python
ALERT_THRESHOLDS = {
    "region_coverage_minimum": 85.0,  # Alert if <85% branches have region data
    "audit_date_coverage_minimum": 90.0,  # Alert if <90% branches have audit dates
    "stale_audit_maximum": 20.0,  # Alert if >20% audits are stale
    "processing_time_maximum": 1800,  # Alert if processing takes >30 minutes
    "data_quality_score_minimum": 95.0  # Alert if quality score <95%
}

def check_alert_conditions(metrics: Dict[str, Any]):
    """Check if any alert conditions are met."""
    alerts = []
    
    if metrics.get("region_coverage_percent", 0) < ALERT_THRESHOLDS["region_coverage_minimum"]:
        alerts.append({
            "type": "DATA_COVERAGE_WARNING",
            "message": f"Region coverage is {metrics['region_coverage_percent']:.1f}%, below threshold of {ALERT_THRESHOLDS['region_coverage_minimum']}%"
        })
    
    if metrics.get("audit_date_coverage_percent", 0) < ALERT_THRESHOLDS["audit_date_coverage_minimum"]:
        alerts.append({
            "type": "DATA_COVERAGE_WARNING",
            "message": f"Audit date coverage is {metrics['audit_date_coverage_percent']:.1f}%, below threshold of {ALERT_THRESHOLDS['audit_date_coverage_minimum']}%"
        })
    
    return alerts
```

## Assumptions and Constraints

### 11. Technical Assumptions
- Oracle source database is accessible via JDBC with stable connectivity
- BRANCH_OPERATIONAL_DETAILS table maintains 1:1 relationship with BRANCH table
- Databricks cluster has sufficient resources for additional join operations
- Delta table supports schema evolution without data loss
- IS_ACTIVE flag reliably indicates current operational status
- LAST_AUDIT_DATE follows consistent date format (YYYY-MM-DD)

### 12. Business Constraints
- Full reload of BRANCH_SUMMARY_REPORT is required for initial deployment
- Backward compatibility must be maintained for existing consumers
- Data processing SLA remains 4 hours for complete ETL pipeline
- Audit trail must be maintained for all data transformations
- Region data classification follows corporate data governance policies
- Historical data (pre-integration) will have NULL values for new columns

### 13. Performance Considerations
- Additional join operation may increase processing time by 10-15%
- Memory usage may increase due to additional columns and join operations
- Delta table optimization should be scheduled after major data loads
- Partition strategy may need adjustment based on data volume growth
- Index maintenance on Oracle source table is critical for performance

## Risk Mitigation

### 14. Identified Risks and Mitigation Strategies

| Risk | Impact | Probability | Mitigation Strategy |
|------|---------|-------------|--------------------|
| Oracle connectivity issues | High | Low | Implement connection retry logic and circuit breaker pattern |
| Data quality issues in operational table | Medium | Medium | Comprehensive validation and data cleansing rules |
| Schema evolution failures | High | Low | Thorough testing and rollback procedures |
| Performance degradation | Medium | Medium | Performance monitoring and optimization strategies |
| Backward compatibility issues | High | Low | Extensive regression testing with existing consumers |
| Missing operational data | Low | Medium | Default value handling and graceful degradation |

### 15. Rollback Strategy
```python
def execute_rollback(failed_step: str):
    """Execute rollback procedures based on failed deployment step."""
    rollback_procedures = {
        "evolve_schema": restore_original_schema,
        "deploy_etl_code": restore_previous_etl_version,
        "run_initial_load": restore_backup_data,
        "validate_results": revert_to_previous_state
    }
    
    if failed_step in rollback_procedures:
        logger.info(f"Executing rollback for step: {failed_step}")
        rollback_procedures[failed_step]()
        logger.info(f"Rollback completed for step: {failed_step}")
    else:
        logger.warning(f"No specific rollback procedure for step: {failed_step}")
```

## References

### 16. Business Requirements
- JIRA Story PCE-1: PySpark Code enhancement for Databricks
- JIRA Story PCE-2: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- Regulatory Compliance Requirements for Audit Trail Maintenance

### 17. Technical Documentation
- Apache Spark 3.4 SQL Reference Guide
- Databricks Delta Lake Best Practices
- Oracle JDBC Driver Documentation
- PySpark DataFrame API Reference
- Data Quality Framework Standards v2.1

### 18. Data Governance References
- Corporate Data Classification Standards
- Data Retention and Archival Policies
- ETL Development and Deployment Guidelines
- Monitoring and Alerting Framework Documentation
- Disaster Recovery and Business Continuity Procedures

---

**Document Status**: Version 3 - Ready for Review
**Review Required**: Yes
**Approval Pending**: Data Architecture Team, Business Stakeholders, Compliance Team
**Implementation Timeline**: 2-3 weeks
**Go-Live Date**: TBD based on testing completion
**Post-Implementation Review**: Scheduled 2 weeks after deployment