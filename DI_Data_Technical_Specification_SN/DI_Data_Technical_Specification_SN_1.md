_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration Enhancement

## Introduction

This technical specification outlines the required changes to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline that generates the `BRANCH_SUMMARY_REPORT`. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata including region information and audit dates.

### Business Context
- **JIRA Story**: PCE-2 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Objective**: Enhance regulatory reporting with branch operational metadata
- **Impact**: Addition of REGION and LAST_AUDIT_DATE columns to BRANCH_SUMMARY_REPORT

## Code Changes Required for the Enhancement

### 1. Main ETL Function Updates

#### File: `RegulatoryReportingETL.py`

**Function: `main()`**
- Add new table read operation for BRANCH_OPERATIONAL_DETAILS
- Update function call to include new DataFrame parameter

```python
# Add after existing table reads
branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

# Update function call
branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
```

### 2. Branch Summary Report Function Enhancement

**Function: `create_branch_summary_report()`**
- Add new parameter for branch_operational_df
- Implement conditional join logic based on IS_ACTIVE status
- Add new column selections for REGION and LAST_AUDIT_DATE

```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and enriching with operational details.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Filter active branch operational details
    active_branch_ops = branch_operational_df.filter(col("IS_ACTIVE") == "Y")
    
    # Create base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                .join(branch_df, "BRANCH_ID") \
                                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # Left join with operational details to preserve all branches
    enhanced_summary = base_summary.join(active_branch_ops, "BRANCH_ID", "left") \
                                  .select(
                                      col("BRANCH_ID"),
                                      col("BRANCH_NAME"),
                                      col("TOTAL_TRANSACTIONS"),
                                      col("TOTAL_AMOUNT"),
                                      col("REGION"),
                                      col("LAST_AUDIT_DATE")
                                  )
    
    return enhanced_summary
```

### 3. Import Statements
- No additional imports required as existing PySpark functions are sufficient

### 4. Error Handling Enhancement
- Add validation for BRANCH_OPERATIONAL_DETAILS table availability
- Implement null handling for optional operational data

## Data Model Updates

### Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT,
    REGION VARCHAR2(50),
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1),
    PRIMARY KEY (BRANCH_ID)
);
```

**Key Characteristics:**
- Primary Key: BRANCH_ID
- Relationship: 1:1 with BRANCH table
- Filter Condition: IS_ACTIVE = 'Y' for active branches only

### Target Data Model Changes

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
```

**Schema Evolution:**
- Added REGION (STRING): Branch operational region
- Added LAST_AUDIT_DATE (STRING): Last audit date for compliance tracking
- Maintains backward compatibility with existing columns

## Source-to-Target Mapping

### Field Mapping Table

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping when IS_ACTIVE='Y' | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Direct mapping when IS_ACTIVE='Y', convert DATE to STRING | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Join key - no direct mapping | BIGINT | No |

### Transformation Rules

#### 1. Region Mapping
- **Source**: BRANCH_OPERATIONAL_DETAILS.REGION
- **Target**: BRANCH_SUMMARY_REPORT.REGION
- **Rule**: 
  ```sql
  CASE 
    WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' 
    THEN BRANCH_OPERATIONAL_DETAILS.REGION 
    ELSE NULL 
  END
  ```

#### 2. Last Audit Date Mapping
- **Source**: BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE
- **Target**: BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE
- **Rule**: 
  ```sql
  CASE 
    WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' 
    THEN CAST(BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE AS STRING)
    ELSE NULL 
  END
  ```

#### 3. Join Logic
- **Join Type**: LEFT JOIN
- **Join Condition**: `BRANCH.BRANCH_ID = BRANCH_OPERATIONAL_DETAILS.BRANCH_ID`
- **Filter**: `BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y'`
- **Rationale**: Preserve all branches in summary report, even if operational details are missing

### Data Flow Diagram
```
TRANSACTION ──┐
              ├── JOIN ──► AGGREGATION ──┐
ACCOUNT ──────┤                          ├── LEFT JOIN ──► BRANCH_SUMMARY_REPORT
              │                          │
BRANCH ───────┘                          │
                                         │
BRANCH_OPERATIONAL_DETAILS ──────────────┘
(WHERE IS_ACTIVE = 'Y')
```

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: BRANCH_OPERATIONAL_DETAILS table is populated and accessible via existing JDBC connection
2. **Data Quality**: BRANCH_ID values in operational details table match existing BRANCH table
3. **Performance**: Additional join operation will not significantly impact ETL performance
4. **Backward Compatibility**: Existing consumers of BRANCH_SUMMARY_REPORT can handle new nullable columns

### Constraints
1. **Schema Evolution**: Delta table schema evolution must be enabled for adding new columns
2. **Deployment**: Requires full reload of BRANCH_SUMMARY_REPORT table
3. **Data Types**: Oracle DATE type converted to STRING in target for consistency
4. **Null Handling**: New columns will be NULL for inactive branches or missing operational data

### Technical Constraints
1. **Memory**: Additional DataFrame join may increase memory usage
2. **Processing Time**: ETL runtime may increase due to additional table read and join operations
3. **Dependencies**: Oracle JDBC driver must support the new source table structure

## Implementation Steps

1. **Pre-deployment**:
   - Verify BRANCH_OPERATIONAL_DETAILS table exists and is populated
   - Test JDBC connectivity to new table
   - Backup existing BRANCH_SUMMARY_REPORT data

2. **Code Deployment**:
   - Update RegulatoryReportingETL.py with enhanced logic
   - Deploy updated PySpark application

3. **Schema Update**:
   - Execute ALTER TABLE statements to add new columns to target table
   - Verify schema evolution is successful

4. **Data Validation**:
   - Run ETL job in test environment
   - Validate data quality and completeness
   - Verify backward compatibility

5. **Production Deployment**:
   - Execute full reload of BRANCH_SUMMARY_REPORT
   - Monitor job performance and data quality

## References

- **JIRA Stories**: PCE-1, PCE-2
- **Source Files**: 
  - RegulatoryReportingETL.py
  - Source_DDL.txt
  - Target_DDL.txt
  - branch_operational_details.sql
- **Confluence Documentation**: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- **Delta Lake Documentation**: Schema Evolution and Table Properties