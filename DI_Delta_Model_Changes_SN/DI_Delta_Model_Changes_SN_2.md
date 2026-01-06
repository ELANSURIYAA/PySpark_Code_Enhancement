_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Delta model changes for Regulatory Reporting ETL Enhancement with Branch Operational Details Integration
## *Version*: 2 
## *Updated on*: 
## *Changes*: Enhanced regulatory reporting pipeline with new source table integration and target schema modifications
## *Reason*: Integration of BRANCH_OPERATIONAL_DETAILS source table and enhancement of BRANCH_SUMMARY_REPORT target table with additional fields
_____________________________________________

# Data Model Evolution Agent (DMEA) - Delta Model Changes SN

## Executive Summary

This document outlines the delta changes required to evolve the current Regulatory Reporting data model to support the new technical specifications for integrating BRANCH_OPERATIONAL_DETAILS source table and enhancing the BRANCH_SUMMARY_REPORT target table with regional and audit information.

## 1. Model Ingestion Analysis

### Current Data Model Structure

#### Source Tables (Existing)
- **CUSTOMER**
  - Primary Key: CUSTOMER_ID
  - Key Fields: CUSTOMER_ID, NAME, EMAIL, PHONE, ADDRESS, CREATED_DATE
  - Format: Oracle/SQL Database

- **BRANCH**
  - Primary Key: BRANCH_ID
  - Key Fields: BRANCH_ID, BRANCH_NAME, BRANCH_CODE, CITY, STATE, COUNTRY
  - Format: Oracle/SQL Database

- **ACCOUNT**
  - Primary Key: ACCOUNT_ID
  - Foreign Keys: CUSTOMER_ID, BRANCH_ID
  - Key Fields: ACCOUNT_ID, CUSTOMER_ID, BRANCH_ID, ACCOUNT_NUMBER, ACCOUNT_TYPE, BALANCE, OPENED_DATE
  - Format: Oracle/SQL Database

- **TRANSACTION**
  - Primary Key: TRANSACTION_ID
  - Foreign Keys: ACCOUNT_ID
  - Key Fields: TRANSACTION_ID, ACCOUNT_ID, TRANSACTION_TYPE, AMOUNT, TRANSACTION_DATE, DESCRIPTION
  - Format: Oracle/SQL Database

#### Target Tables (Existing)
- **AML_CUSTOMER_TRANSACTIONS**
  - Primary Key: Composite (CUSTOMER_ID, TRANSACTION_ID)
  - Key Fields: CUSTOMER_ID, NAME, ACCOUNT_ID, TRANSACTION_ID, AMOUNT, TRANSACTION_TYPE, TRANSACTION_DATE
  - Format: Delta Lake

- **BRANCH_SUMMARY_REPORT**
  - Primary Key: BRANCH_ID
  - Key Fields: BRANCH_ID, BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT
  - Format: Delta Lake

## 2. Spec Parsing & Delta Detection

### New Requirements from Technical Specification

#### 2.1 Additions
- **New Source Table**: BRANCH_OPERATIONAL_DETAILS
- **New Columns in Target**: REGION, LAST_AUDIT_DATE in BRANCH_SUMMARY_REPORT
- **New Join Logic**: LEFT JOIN between BRANCH and BRANCH_OPERATIONAL_DETAILS tables
- **New Validation Logic**: Operational details availability check
- **Enhanced ETL Pipeline**: Modified branch summary aggregation logic

#### 2.2 Modifications
- **ETL Pipeline Enhancement**: Modified create_branch_summary_report function to include operational details
- **Schema Evolution**: BRANCH_SUMMARY_REPORT schema extension
- **Data Quality Rules**: Addition of region and audit date completeness validation
- **Data Type Changes**: LAST_AUDIT_DATE changed from DATE to STRING in target for compatibility

#### 2.3 No Deprecations Detected
- All existing tables and columns remain unchanged
- Backward compatibility maintained

## 3. Delta Computation

### 3.1 Schema Changes Summary

| Change Type | Object | Change Description | Impact Level |
|-------------|--------|-------------------|-------------|
| ADD | BRANCH_OPERATIONAL_DETAILS | New operational metadata table | MINOR |
| ALTER | BRANCH_SUMMARY_REPORT | Add REGION and LAST_AUDIT_DATE columns | MINOR |
| MODIFY | RegulatoryReportingETL.py | Enhanced branch summary logic | MINOR |
| MODIFY | create_branch_summary_report | Add operational details join | MINOR |

### 3.2 Version Impact Assessment
- **Change Classification**: MINOR (additive changes, no breaking modifications)
- **Backward Compatibility**: MAINTAINED
- **Data Loss Risk**: NONE

## 4. Impact Assessment

### 4.1 Downstream Dependencies
- **Regulatory Reports**: Enhanced with regional and audit information
- **Existing ETL Jobs**: No breaking changes expected
- **API Consumers**: No impact (schema extension only)
- **Data Warehouse Views**: May require updates to include new columns
- **Compliance Dashboards**: Will benefit from additional audit trail information

### 4.2 Risk Analysis
- **LOW RISK**: Additive changes with graceful degradation
- **Performance Impact**: Minimal (single LEFT JOIN added)
- **Data Quality Impact**: Improved (additional operational metadata enrichment)
- **Compliance Impact**: Enhanced audit trail capabilities

### 4.3 Foreign Key Relationships
- **New Relationship**: BRANCH_OPERATIONAL_DETAILS.BRANCH_ID → BRANCH.BRANCH_ID (LEFT JOIN)
- **Referential Integrity**: Maintained through LEFT JOIN strategy

## 5. DDL Generation

### 5.1 Forward Migration Scripts

#### Create New Source Table
```sql
-- DDL-001: Create BRANCH_OPERATIONAL_DETAILS table
CREATE TABLE IF NOT EXISTS BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT NOT NULL,
    REGION STRING NOT NULL,
    MANAGER_NAME STRING,
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE STRING DEFAULT 'Y',
    PRIMARY KEY (BRANCH_ID)
)
COMMENT 'Operational details and audit information for bank branches';

-- Create index for performance
CREATE INDEX idx_branch_operational_region ON BRANCH_OPERATIONAL_DETAILS(REGION);
CREATE INDEX idx_branch_operational_active ON BRANCH_OPERATIONAL_DETAILS(IS_ACTIVE);
```

#### Alter Target Table Schema
```sql
-- DDL-002: Add operational columns to BRANCH_SUMMARY_REPORT
ALTER TABLE workspace.default.branch_summary_report 
ADD COLUMN REGION STRING COMMENT 'Regional classification of the branch' AFTER BRANCH_NAME,
ADD COLUMN LAST_AUDIT_DATE STRING COMMENT 'Last audit date for compliance tracking' AFTER TOTAL_AMOUNT;
```

#### Insert Initial Operational Data (Sample)
```sql
-- DDL-003: Insert initial operational data for existing branches
INSERT INTO BRANCH_OPERATIONAL_DETAILS VALUES
(1, 'NORTH', 'John Smith', '2024-01-15', 'Y'),
(2, 'SOUTH', 'Jane Doe', '2024-02-20', 'Y'),
(3, 'EAST', 'Mike Johnson', '2024-01-30', 'Y'),
(4, 'WEST', 'Sarah Wilson', '2024-02-10', 'Y');
```

### 5.2 Rollback Scripts

```sql
-- ROLLBACK-001: Remove operational columns from target table
ALTER TABLE workspace.default.branch_summary_report 
DROP COLUMN REGION,
DROP COLUMN LAST_AUDIT_DATE;

-- ROLLBACK-002: Drop BRANCH_OPERATIONAL_DETAILS table
DROP TABLE IF EXISTS BRANCH_OPERATIONAL_DETAILS;

-- ROLLBACK-003: Drop indexes
DROP INDEX IF EXISTS idx_branch_operational_region;
DROP INDEX IF EXISTS idx_branch_operational_active;
```

### 5.3 Data Migration Scripts

```sql
-- MIGRATION-001: Backfill operational data for historical records
UPDATE workspace.default.branch_summary_report 
SET REGION = COALESCE(
    (SELECT REGION FROM BRANCH_OPERATIONAL_DETAILS bod 
     WHERE bod.BRANCH_ID = branch_summary_report.BRANCH_ID), 
    'UNKNOWN'
),
LAST_AUDIT_DATE = COALESCE(
    (SELECT CAST(LAST_AUDIT_DATE AS STRING) FROM BRANCH_OPERATIONAL_DETAILS bod 
     WHERE bod.BRANCH_ID = branch_summary_report.BRANCH_ID), 
    'NOT_AVAILABLE'
)
WHERE REGION IS NULL OR LAST_AUDIT_DATE IS NULL;
```

## 6. ETL Pipeline Changes

### 6.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TABLES = {
    'CUSTOMER': 'CUSTOMER',
    'ACCOUNT': 'ACCOUNT', 
    'TRANSACTION': 'TRANSACTION',
    'BRANCH': 'BRANCH',
    'BRANCH_OPERATIONAL_DETAILS': 'BRANCH_OPERATIONAL_DETAILS'  # New table
}
```

### 6.2 Enhanced Branch Summary Function
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    with operational details integration.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # Base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Enhanced summary with operational details
    enhanced_summary = base_summary.join(
        branch_operational_df.select(
            "BRANCH_ID", 
            "REGION", 
            col("LAST_AUDIT_DATE").cast("string").alias("LAST_AUDIT_DATE")
        ), 
        "BRANCH_ID", 
        "left"
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        coalesce(col("REGION"), lit("UNKNOWN")).alias("REGION"),
        coalesce(col("LAST_AUDIT_DATE"), lit("NOT_AVAILABLE")).alias("LAST_AUDIT_DATE")
    )
    
    return enhanced_summary
```

### 6.3 Updated Main ETL Function
```python
def main():
    """
    Enhanced main ETL execution function with operational details integration.
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

        # Read source tables (including new operational details)
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # Create and write enhanced BRANCH_SUMMARY_REPORT
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("Enhanced ETL job completed successfully.")

    except Exception as e:
        logger.error(f"Enhanced ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
```

### 6.4 Validation Enhancement
```python
def validate_operational_data(spark: SparkSession) -> bool:
    """
    Validate operational details table availability and data quality.
    """
    try:
        operational_df = spark.table("BRANCH_OPERATIONAL_DETAILS")
        record_count = operational_df.count()
        active_branches = operational_df.filter(col("IS_ACTIVE") == "Y").count()
        
        logger.info(f"Operational details table contains {record_count} records, {active_branches} active branches")
        
        # Validate required fields
        null_regions = operational_df.filter(col("REGION").isNull()).count()
        if null_regions > 0:
            logger.warning(f"Found {null_regions} branches without region information")
        
        return True
    except Exception as e:
        logger.error(f"Operational details validation failed: {e}")
        return False
```

## 7. Data Quality & Validation Rules

### 7.1 New Validation Rules
- **Completeness**: All active branches must have region information (default to 'UNKNOWN')
- **Consistency**: Region values must follow approved regional taxonomy (NORTH, SOUTH, EAST, WEST)
- **Referential Integrity**: All branch_ids in operational details should correspond to existing branches
- **Data Freshness**: Audit dates should be monitored for compliance requirements
- **Audit Trail**: Last audit date should not be older than regulatory requirements (e.g., 12 months)

### 7.2 Monitoring Queries
```sql
-- Monitor branches without operational details
SELECT COUNT(*) as branches_without_operational_data
FROM BRANCH b
LEFT JOIN BRANCH_OPERATIONAL_DETAILS bod ON b.BRANCH_ID = bod.BRANCH_ID
WHERE bod.BRANCH_ID IS NULL;

-- Monitor regional distribution
SELECT REGION, COUNT(*) as branch_count, SUM(TOTAL_AMOUNT) as total_regional_amount
FROM workspace.default.branch_summary_report
WHERE REGION IS NOT NULL
GROUP BY REGION
ORDER BY total_regional_amount DESC;

-- Monitor audit compliance
SELECT 
    REGION,
    COUNT(*) as total_branches,
    COUNT(CASE WHEN LAST_AUDIT_DATE != 'NOT_AVAILABLE' THEN 1 END) as audited_branches,
    ROUND(COUNT(CASE WHEN LAST_AUDIT_DATE != 'NOT_AVAILABLE' THEN 1 END) * 100.0 / COUNT(*), 2) as audit_coverage_pct
FROM workspace.default.branch_summary_report
GROUP BY REGION;
```

## 8. Deployment Strategy

### 8.1 Phase 1: Infrastructure Setup (Day 1)
1. Execute DDL-001: Create BRANCH_OPERATIONAL_DETAILS table
2. Execute DDL-003: Insert initial operational data
3. Validate table creation and data insertion
4. Test connectivity and performance

### 8.2 Phase 2: Schema Evolution (Day 2)
1. Execute DDL-002: Add operational columns to target table
2. Execute MIGRATION-001: Backfill historical data
3. Validate schema changes and data migration
4. Test backward compatibility

### 8.3 Phase 3: ETL Enhancement (Day 3)
1. Deploy enhanced ETL pipeline code
2. Execute test run with validation
3. Monitor performance and data quality
4. Validate operational details integration

### 8.4 Phase 4: Validation & Monitoring (Day 4)
1. Implement monitoring queries
2. Validate end-to-end data flow
3. Update documentation and runbooks
4. Train operations team on new features

## 9. Testing Strategy

### 9.1 Unit Tests
- Operational details join logic validation
- Default value assignment testing
- Schema compatibility verification
- Data type conversion testing (DATE to STRING)

### 9.2 Integration Tests
- End-to-end pipeline execution with operational data
- Data quality validation across all tables
- Performance benchmarking with additional join
- Regulatory compliance validation

### 9.3 Regression Tests
- Backward compatibility verification
- Existing AML functionality preservation
- Historical data integrity validation
- Performance regression testing

## 10. Rollback Plan

In case of issues:
1. Revert ETL pipeline to previous version (without operational details)
2. Execute rollback scripts (ROLLBACK-001, ROLLBACK-002, ROLLBACK-003)
3. Restore target table from backup if necessary
4. Validate system stability and existing functionality
5. Communicate rollback status to stakeholders

## 11. Success Criteria

- ✅ BRANCH_OPERATIONAL_DETAILS table created successfully
- ✅ BRANCH_SUMMARY_REPORT extended with REGION and LAST_AUDIT_DATE
- ✅ ETL pipeline processes operational details enrichment
- ✅ All existing functionality preserved
- ✅ Data quality metrics maintained or improved
- ✅ Performance SLAs met (< 4 hour processing window)
- ✅ Zero data loss during migration
- ✅ Regulatory compliance enhanced
- ✅ Audit trail capabilities improved

## 12. Change Traceability Matrix

| Technical Spec Section | DDL Reference | ETL Change | Validation Rule |
|------------------------|---------------|------------|----------------|
| New Source Table Integration | DDL-001 | Configuration update | Table existence and connectivity |
| Target Schema Extension | DDL-002 | Enhanced join logic | Column presence and data type validation |
| Operational Data Enrichment | DDL-003 | Aggregation modification | Data completeness and quality check |
| Backward Compatibility | MIGRATION-001 | Graceful degradation | Historical data integrity |
| Performance Optimization | Index creation | Query optimization | Performance benchmarking |
| Regulatory Compliance | Audit date tracking | Compliance monitoring | Audit coverage validation |

## 13. Compliance and Security Considerations

### 13.1 Data Privacy
- Manager names in operational details may contain PII
- Implement data masking for non-production environments
- Ensure proper access controls for operational data

### 13.2 Regulatory Requirements
- Audit trail enhancement supports regulatory compliance
- Regional classification enables jurisdiction-specific reporting
- Last audit date tracking supports compliance monitoring

### 13.3 Data Retention
- Operational details follow same retention policy as branch data
- Audit dates preserved for regulatory timeline requirements
- Historical operational changes tracked through versioning

---

**Document Status**: Updated Version  
**Change Classification**: MINOR (Additive Enhancement with Operational Integration)  
**Risk Level**: LOW  
**Estimated Effort**: 3-4 Development Days  
**Dependencies**: Operations team for initial data population, Compliance team for audit requirements validation  
**Regulatory Impact**: Enhanced compliance and audit capabilities