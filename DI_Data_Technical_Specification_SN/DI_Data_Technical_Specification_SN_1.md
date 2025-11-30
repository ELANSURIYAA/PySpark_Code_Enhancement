_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration Enhancement

## Introduction

This technical specification outlines the required changes to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for the `BRANCH_SUMMARY_REPORT`. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata including region, manager name, and audit date information.

### Business Context
- **JIRA Story**: PCE-2 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Objective**: Enhance regulatory reporting capabilities with branch operational metadata
- **Impact**: Addition of REGION and LAST_AUDIT_DATE columns to BRANCH_SUMMARY_REPORT

## Code Changes Required for the Enhancement

### 1. Main ETL Function Updates

**File**: `RegulatoryReportingETL.py`

#### 1.1 Add New Source Table Reading
```python
# Add to main() function after existing table reads
branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
```

#### 1.2 Update Function Signature
```python
# Modify the create_branch_summary_report function signature
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
```

#### 1.3 Enhanced Branch Summary Report Logic
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and incorporating operational metadata from BRANCH_OPERATIONAL_DETAILS.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # Filter active branches only
    active_branch_operational_df = branch_operational_df.filter(col("IS_ACTIVE") == "Y")
    
    # Create base summary with transactions
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                .join(branch_df, "BRANCH_ID") \
                                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # Left join with operational details to preserve all branches
    enhanced_summary = base_summary.join(
        active_branch_operational_df.select(
            col("BRANCH_ID"),
            col("REGION"),
            col("LAST_AUDIT_DATE")
        ),
        "BRANCH_ID",
        "left"
    )
    
    return enhanced_summary
```

#### 1.4 Update Main Function Call
```python
# Update the function call in main()
branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
```

### 2. Error Handling and Logging Enhancements

```python
# Add validation for new source table
def validate_branch_operational_data(branch_operational_df: DataFrame) -> bool:
    """
    Validates the BRANCH_OPERATIONAL_DETAILS data quality.
    """
    try:
        # Check for required columns
        required_columns = ["BRANCH_ID", "REGION", "IS_ACTIVE"]
        missing_columns = set(required_columns) - set(branch_operational_df.columns)
        
        if missing_columns:
            logger.error(f"Missing required columns in BRANCH_OPERATIONAL_DETAILS: {missing_columns}")
            return False
            
        # Check for null BRANCH_IDs
        null_branch_ids = branch_operational_df.filter(col("BRANCH_ID").isNull()).count()
        if null_branch_ids > 0:
            logger.warning(f"Found {null_branch_ids} records with null BRANCH_ID")
            
        logger.info("BRANCH_OPERATIONAL_DETAILS validation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error validating BRANCH_OPERATIONAL_DETAILS: {e}")
        return False
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

**Key Characteristics:**
- Primary key: BRANCH_ID
- Relationship: One-to-one with BRANCH table
- Filter condition: IS_ACTIVE = 'Y' for active branches only

### 2. Target Data Model Changes

#### Updated Target Table: BRANCH_SUMMARY_REPORT
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
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
    'delta.parquet.compression.codec' = 'zstd'
);
```

**Schema Evolution:**
- Added REGION (STRING): Branch operational region
- Added LAST_AUDIT_DATE (STRING): Last audit date for compliance tracking
- Maintains backward compatibility with existing records

## Source-to-Target Mapping

### Detailed Field Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rules | Data Type | Nullable |
|--------------|---------------|--------------|---------------|---------------------|-----------|----------|
| TRANSACTION | TRANSACTION_ID | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | COUNT(*) aggregation | BIGINT | No |
| TRANSACTION | AMOUNT | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | SUM(AMOUNT) aggregation | DOUBLE | No |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | BIGINT | No |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping with IS_ACTIVE='Y' filter | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Direct mapping with IS_ACTIVE='Y' filter, DATE to STRING conversion | STRING | Yes |

### Transformation Rules

#### 1. Join Logic
```sql
-- Conceptual SQL representation of the join logic
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
- **Null Handling**: REGION and LAST_AUDIT_DATE can be null for branches without operational details
- **Active Filter**: Only include operational details where IS_ACTIVE = 'Y'
- **Date Conversion**: Convert Oracle DATE to STRING format for Delta table compatibility
- **Backward Compatibility**: Existing branches without operational details will have null values in new columns

#### 3. Business Rules
- Include all branches in the summary report regardless of operational details availability
- Operational metadata is optional and should not prevent branch summary generation
- Maintain historical data integrity during schema evolution

## Implementation Steps

### Phase 1: Code Enhancement
1. Update `create_branch_summary_report` function signature
2. Implement enhanced join logic with BRANCH_OPERATIONAL_DETAILS
3. Add data validation functions
4. Update main ETL workflow

### Phase 2: Schema Evolution
1. Execute ALTER TABLE statements to add new columns
2. Verify Delta table properties and compatibility
3. Test schema evolution with sample data

### Phase 3: Testing and Validation
1. Unit testing for new transformation logic
2. Integration testing with full dataset
3. Data quality validation and reconciliation
4. Performance testing and optimization

### Phase 4: Deployment
1. Deploy code changes to development environment
2. Execute full reload of BRANCH_SUMMARY_REPORT
3. Validate data accuracy and completeness
4. Promote to production environment

## Assumptions and Constraints

### Assumptions
- BRANCH_OPERATIONAL_DETAILS table is available in the source Oracle database
- BRANCH_ID serves as the primary key and foreign key relationship
- IS_ACTIVE field reliably indicates active branches
- Delta table supports schema evolution for new columns
- Existing ETL schedule and performance requirements remain unchanged

### Constraints
- Backward compatibility must be maintained for existing consumers
- No breaking changes to existing API or data contracts
- Performance impact should be minimal (< 10% increase in processing time)
- Data quality standards must be maintained
- Compliance with existing data governance policies

### Technical Constraints
- Oracle JDBC driver compatibility
- Databricks runtime version compatibility
- Delta table feature requirements (deletion vectors, schema evolution)
- Memory and compute resource limitations

## Risk Mitigation

### Data Risks
- **Missing Operational Data**: Use LEFT JOIN to preserve all branches
- **Data Type Mismatches**: Implement explicit type conversions
- **Performance Degradation**: Monitor query execution plans and optimize joins

### Technical Risks
- **Schema Evolution Issues**: Test thoroughly in development environment
- **JDBC Connection Failures**: Implement retry logic and connection pooling
- **Memory Issues**: Monitor Spark application metrics and tune accordingly

## References

- **JIRA Stories**: PCE-1, PCE-2
- **Confluence Documentation**: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- **Source DDL**: Input/Source_DDL.txt
- **Target DDL**: Input/Target_DDL.txt
- **Existing ETL Code**: Input/RegulatoryReportingETL.py
- **Delta Lake Documentation**: Schema Evolution and Table Properties
- **PySpark SQL Functions**: Join operations and aggregations