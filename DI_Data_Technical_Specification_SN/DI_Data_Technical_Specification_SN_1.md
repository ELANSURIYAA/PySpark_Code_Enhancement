_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

## Introduction

This technical specification outlines the required changes to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for the `BRANCH_SUMMARY_REPORT` target table in Databricks Delta. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata including region and last audit date information.

### Business Context
- **JIRA Story**: PCE-2 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Objective**: Enhance regulatory reporting capabilities with branch operational metadata
- **Impact**: Addition of REGION and LAST_AUDIT_DATE columns to BRANCH_SUMMARY_REPORT

## Code Changes Required for the Enhancement

### 1. Modified Functions

#### 1.1 Enhanced `create_branch_summary_report` Function

**Current Function Location**: `RegulatoryReportingETL.py` - Line 65

**Required Changes**:
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and incorporating operational metadata.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # Filter active branches only
    active_branch_operational_df = branch_operational_df.filter(col("IS_ACTIVE") == "Y")
    
    # Create base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Left join with operational details to preserve all branches
    enhanced_summary = base_summary.join(
        active_branch_operational_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE"),
        "BRANCH_ID",
        "left"
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        col("REGION"),
        col("LAST_AUDIT_DATE")
    )
    
    return enhanced_summary
```

#### 1.2 Updated `main` Function

**Required Changes**:
```python
def main():
    """
    Main ETL execution function with enhanced branch operational details integration.
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

        # Read source tables (existing)
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        
        # Read new source table
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # Create and write enhanced BRANCH_SUMMARY_REPORT
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("Enhanced ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
```

### 2. Import Statements

**Additional Import Required**:
```python
from pyspark.sql.functions import col, count, sum, when, lit
```

## Data Model Updates

### 1. Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT PRIMARY KEY,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y'
);
```

**Key Characteristics**:
- Primary key: BRANCH_ID
- Relationship: One-to-One with BRANCH table
- Filter condition: IS_ACTIVE = 'Y' for active branches only

### 2. Target Data Model Changes

#### Updated Target Table: BRANCH_SUMMARY_REPORT
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,              -- NEW COLUMN
    LAST_AUDIT_DATE STRING      -- NEW COLUMN
)
USING delta
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.deletionVectors' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7',
    'delta.parquet.compression.codec' = 'zstd'
);
```

**Schema Evolution**:
- Added REGION (STRING): Branch operational region
- Added LAST_AUDIT_DATE (STRING): Last audit date for compliance tracking

## Source-to-Target Mapping

### Field Mapping Table

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|
| TRANSACTION | COUNT(*) | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | Aggregation by BRANCH_ID | BIGINT | No |
| TRANSACTION | SUM(AMOUNT) | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | Aggregation by BRANCH_ID | DOUBLE | No |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | BIGINT | No |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Conditional mapping (IS_ACTIVE='Y') | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Conditional mapping (IS_ACTIVE='Y') | STRING | Yes |

### Transformation Rules

#### 1. Join Logic
```sql
SELECT 
    b.BRANCH_ID,
    b.BRANCH_NAME,
    COUNT(t.TRANSACTION_ID) as TOTAL_TRANSACTIONS,
    SUM(t.AMOUNT) as TOTAL_AMOUNT,
    bod.REGION,
    bod.LAST_AUDIT_DATE
FROM TRANSACTION t
JOIN ACCOUNT a ON t.ACCOUNT_ID = a.ACCOUNT_ID
JOIN BRANCH b ON a.BRANCH_ID = b.BRANCH_ID
LEFT JOIN BRANCH_OPERATIONAL_DETAILS bod ON b.BRANCH_ID = bod.BRANCH_ID 
    AND bod.IS_ACTIVE = 'Y'
GROUP BY b.BRANCH_ID, b.BRANCH_NAME, bod.REGION, bod.LAST_AUDIT_DATE
```

#### 2. Data Quality Rules
- **Null Handling**: REGION and LAST_AUDIT_DATE can be NULL for branches without operational details
- **Active Filter**: Only include operational details where IS_ACTIVE = 'Y'
- **Backward Compatibility**: Existing branches without operational details will have NULL values in new columns

#### 3. Business Logic
- **Primary Join**: BRANCH_ID is the join key between BRANCH and BRANCH_OPERATIONAL_DETAILS
- **Left Join Strategy**: Preserve all branches even if operational details are missing
- **Conditional Population**: New columns populated only for active operational records

## Implementation Steps

### Phase 1: Code Changes
1. Update `create_branch_summary_report` function signature and logic
2. Modify `main` function to read BRANCH_OPERATIONAL_DETAILS table
3. Add necessary import statements
4. Update function documentation

### Phase 2: Testing
1. Unit testing for enhanced function with mock data
2. Integration testing with sample BRANCH_OPERATIONAL_DETAILS data
3. Backward compatibility testing with existing data
4. Performance testing for join operations

### Phase 3: Deployment
1. Deploy updated ETL code to development environment
2. Execute full reload of BRANCH_SUMMARY_REPORT table
3. Validate data quality and completeness
4. Promote to production environment

## Assumptions and Constraints

### Assumptions
1. BRANCH_OPERATIONAL_DETAILS table is available in the same Oracle database
2. BRANCH_ID exists in both BRANCH and BRANCH_OPERATIONAL_DETAILS tables
3. IS_ACTIVE column reliably indicates active operational records
4. Target Delta table supports schema evolution

### Constraints
1. **Performance Impact**: Additional join operation may impact ETL performance
2. **Data Availability**: New columns will be NULL for historical data
3. **Schema Evolution**: Requires Delta table schema evolution capabilities
4. **Backward Compatibility**: Existing downstream consumers must handle new columns

### Risk Mitigation
1. **Performance**: Monitor join performance and consider partitioning strategies
2. **Data Quality**: Implement data validation checks for new columns
3. **Rollback Plan**: Maintain ability to revert to previous schema if needed

## References

- **JIRA Stories**: PCE-1, PCE-2
- **Source Code**: RegulatoryReportingETL.py
- **Confluence Documentation**: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS
- **Database Schema**: Source_DDL.txt, Target_DDL.txt
- **Operational Details**: branch_operational_details.sql

## Validation Criteria

### Data Validation
1. **Row Count**: Verify BRANCH_SUMMARY_REPORT row count matches expected branches
2. **Column Population**: Validate REGION and LAST_AUDIT_DATE population for active branches
3. **Null Handling**: Confirm NULL values for inactive or missing operational details
4. **Data Types**: Ensure proper data type conversion and formatting

### Functional Validation
1. **Join Accuracy**: Verify correct matching between BRANCH and BRANCH_OPERATIONAL_DETAILS
2. **Aggregation Integrity**: Confirm transaction aggregations remain accurate
3. **Filter Logic**: Validate IS_ACTIVE = 'Y' filter is properly applied
4. **Backward Compatibility**: Ensure existing functionality is preserved