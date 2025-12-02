_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Delta model changes for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Data Model Evolution Agent (DMEA) - Delta Model Changes SN

## Executive Summary

This document outlines the delta changes required to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline that generates the `BRANCH_SUMMARY_REPORT`. The changes involve schema evolution, new data mappings, and ETL logic enhancements to support regulatory reporting requirements.

### Change Classification
- **Change Type**: Schema Addition & Enhancement
- **Impact Level**: Minor (Backward Compatible)
- **Risk Assessment**: Low
- **Deployment Strategy**: Blue-Green with Schema Evolution

## 1. Model Ingestion Analysis

### Current Data Model Structure

#### Existing Source Tables
```sql
-- Core Banking Tables
CUSTOMER (CUSTOMER_ID, NAME, EMAIL, PHONE, ADDRESS, CREATED_DATE)
BRANCH (BRANCH_ID, BRANCH_NAME, BRANCH_CODE, CITY, STATE, COUNTRY)
ACCOUNT (ACCOUNT_ID, CUSTOMER_ID, BRANCH_ID, ACCOUNT_NUMBER, ACCOUNT_TYPE, BALANCE, OPENED_DATE)
TRANSACTION (TRANSACTION_ID, ACCOUNT_ID, TRANSACTION_TYPE, AMOUNT, TRANSACTION_DATE, DESCRIPTION)
```

#### Current Target Table
```sql
BRANCH_SUMMARY_REPORT (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE
)
```

### Current Data Flow
```
TRANSACTION ──┐
              ├── JOIN ──► GROUP BY ──► BRANCH_SUMMARY_REPORT
ACCOUNT ──────┤           (BRANCH_ID,
              │            BRANCH_NAME)
BRANCH ───────┘
```

## 2. Spec Parsing & Delta Detection

### New Source Table Addition

#### BRANCH_OPERATIONAL_DETAILS
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT PRIMARY KEY,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y'
);
```

**Table Characteristics:**
- **Relationship**: 1:1 with BRANCH table via BRANCH_ID
- **Filter Logic**: Only active branches (IS_ACTIVE = 'Y')
- **Join Type**: LEFT JOIN to preserve all branches

### Target Schema Evolution

#### Enhanced BRANCH_SUMMARY_REPORT
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

## 3. Delta Computation

### Schema Changes Summary

| Change Type | Object | Details | Impact |
|-------------|--------|---------|--------|
| **ADD TABLE** | BRANCH_OPERATIONAL_DETAILS | New source table for operational metadata | Source expansion |
| **ADD COLUMN** | BRANCH_SUMMARY_REPORT.REGION | STRING, Nullable | Target enhancement |
| **ADD COLUMN** | BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE | STRING, Nullable | Target enhancement |
| **MODIFY ETL** | create_branch_summary_report() | Enhanced join logic | Processing change |

### Detailed Delta Analysis

#### 3.1 New Table Addition
```sql
-- Delta: ADD TABLE
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT PRIMARY KEY,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y'
);
```

#### 3.2 Target Schema Evolution
```sql
-- Delta: ADD COLUMNS
ALTER TABLE workspace.default.branch_summary_report 
ADD COLUMNS (
    REGION STRING COMMENT 'Branch operational region',
    LAST_AUDIT_DATE STRING COMMENT 'Last audit date for compliance tracking'
);
```

#### 3.3 Data Flow Evolution
```
-- BEFORE
TRANSACTION ──┐
              ├── JOIN ──► GROUP BY ──► BRANCH_SUMMARY_REPORT
ACCOUNT ──────┤           (4 columns)
              │
BRANCH ───────┘

-- AFTER
TRANSACTION ──┐
              ├── JOIN ──► GROUP BY ──┐
ACCOUNT ──────┤                      ├── LEFT JOIN ──► BRANCH_SUMMARY_REPORT
              │                      │                 (6 columns)
BRANCH ───────┘                      │
                                     │
BRANCH_OPERATIONAL_DETAILS ──────────┘
(WHERE IS_ACTIVE = 'Y')
```

## 4. Impact Assessment

### 4.1 Downstream Impact Analysis

#### Low Risk Changes
- **Schema Evolution**: Delta Lake supports automatic schema evolution
- **Backward Compatibility**: New columns are nullable, existing queries unaffected
- **Data Consumers**: Existing BI tools and reports will continue to function

#### Potential Risks
- **Performance Impact**: Additional table read and join operation
- **Memory Usage**: Increased due to additional DataFrame operations
- **Data Quality**: Dependency on BRANCH_OPERATIONAL_DETAILS data availability

### 4.2 Foreign Key & Relationship Impact

#### New Relationships
```sql
-- Logical Foreign Key (enforced in ETL logic)
BRANCH_OPERATIONAL_DETAILS.BRANCH_ID → BRANCH.BRANCH_ID
```

#### Join Strategy
- **Type**: LEFT JOIN
- **Rationale**: Preserve all branches even if operational details are missing
- **Filter**: Apply IS_ACTIVE = 'Y' filter before join

### 4.3 Data Loss Risk Assessment

#### Risk Level: **LOW**
- No existing columns are modified or dropped
- No data type narrowing
- No constraint tightening
- Additive changes only

## 5. DDL Generation & Migration Scripts

### 5.1 Forward Migration DDL

#### Source Table Creation
```sql
-- Oracle Source Database
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y',
    CONSTRAINT PK_BRANCH_OPS PRIMARY KEY (BRANCH_ID),
    CONSTRAINT FK_BRANCH_OPS_BRANCH FOREIGN KEY (BRANCH_ID) REFERENCES BRANCH(BRANCH_ID),
    CONSTRAINT CHK_IS_ACTIVE CHECK (IS_ACTIVE IN ('Y', 'N'))
);

-- Create index for performance
CREATE INDEX IDX_BRANCH_OPS_ACTIVE ON BRANCH_OPERATIONAL_DETAILS(IS_ACTIVE);
```

#### Target Schema Evolution
```sql
-- Databricks Delta Lake
ALTER TABLE workspace.default.branch_summary_report 
ADD COLUMNS (
    REGION STRING COMMENT 'Branch operational region from BRANCH_OPERATIONAL_DETAILS',
    LAST_AUDIT_DATE STRING COMMENT 'Last audit date converted from DATE to STRING format'
);

-- Update table properties for enhanced features
ALTER TABLE workspace.default.branch_summary_report 
SET TBLPROPERTIES (
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

### 5.2 ETL Code Changes

#### Enhanced PySpark Function
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
    
    # Create base aggregation with existing logic
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
                                      col("LAST_AUDIT_DATE").cast("string").alias("LAST_AUDIT_DATE")
                                  )
    
    return enhanced_summary

# Updated main() function
def main():
    # ... existing code ...
    
    # Read source tables (existing)
    customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
    account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
    transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
    branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
    
    # NEW: Read branch operational details
    branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
    
    # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
    aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
    write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
    
    # UPDATED: Create and write enhanced BRANCH_SUMMARY_REPORT
    branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
    write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")
    
    logger.info("Enhanced ETL job completed successfully.")
```

### 5.3 Rollback Scripts

#### Emergency Rollback DDL
```sql
-- Rollback target schema changes
ALTER TABLE workspace.default.branch_summary_report 
DROP COLUMNS (REGION, LAST_AUDIT_DATE);

-- Rollback source table (if needed)
DROP TABLE IF EXISTS BRANCH_OPERATIONAL_DETAILS;
```

#### Code Rollback Strategy
```python
# Revert to original function signature
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
    # Original implementation without operational details
    return transaction_df.join(account_df, "ACCOUNT_ID") \
                         .join(branch_df, "BRANCH_ID") \
                         .groupBy("BRANCH_ID", "BRANCH_NAME") \
                         .agg(
                             count("*").alias("TOTAL_TRANSACTIONS"),
                             sum("AMOUNT").alias("TOTAL_AMOUNT")
                         )
```

### 5.4 Data Migration Scripts

#### Populate Sample Data
```sql
-- Sample data for BRANCH_OPERATIONAL_DETAILS
INSERT INTO BRANCH_OPERATIONAL_DETAILS VALUES
(1, 'North America', 'John Smith', DATE '2024-01-15', 'Y'),
(2, 'Europe', 'Jane Doe', DATE '2024-02-20', 'Y'),
(3, 'Asia Pacific', 'Mike Johnson', DATE '2024-01-30', 'Y'),
(4, 'Latin America', 'Sarah Wilson', NULL, 'N');
```

#### Data Validation Queries
```sql
-- Validate join integrity
SELECT 
    b.BRANCH_ID,
    b.BRANCH_NAME,
    bod.REGION,
    bod.IS_ACTIVE,
    CASE WHEN bod.BRANCH_ID IS NULL THEN 'Missing Operational Data' ELSE 'Complete' END as DATA_STATUS
FROM BRANCH b
LEFT JOIN BRANCH_OPERATIONAL_DETAILS bod ON b.BRANCH_ID = bod.BRANCH_ID;

-- Validate active branches
SELECT COUNT(*) as ACTIVE_BRANCHES 
FROM BRANCH_OPERATIONAL_DETAILS 
WHERE IS_ACTIVE = 'Y';
```

## 6. Documentation & Traceability

### 6.1 Change Traceability Matrix

| Requirement | Technical Spec Section | DDL Statement | ETL Code Change | Test Case |
|-------------|----------------------|---------------|-----------------|----------|
| Add REGION to report | Source-to-Target Mapping | ALTER TABLE ADD COLUMN REGION | Enhanced join logic | TC_001_Region_Mapping |
| Add LAST_AUDIT_DATE | Field Mapping Table | ALTER TABLE ADD COLUMN LAST_AUDIT_DATE | Date to string conversion | TC_002_Audit_Date |
| Filter active branches | Transformation Rules | WHERE IS_ACTIVE = 'Y' | DataFrame filter | TC_003_Active_Filter |
| Preserve all branches | Join Logic | LEFT JOIN | Left join implementation | TC_004_Branch_Preservation |

### 6.2 Visual Schema Comparison

#### Before (Current State)
```
BRANCH_SUMMARY_REPORT
├── BRANCH_ID (BIGINT)
├── BRANCH_NAME (STRING)
├── TOTAL_TRANSACTIONS (BIGINT)
└── TOTAL_AMOUNT (DOUBLE)
```

#### After (Target State)
```
BRANCH_SUMMARY_REPORT
├── BRANCH_ID (BIGINT)
├── BRANCH_NAME (STRING)
├── TOTAL_TRANSACTIONS (BIGINT)
├── TOTAL_AMOUNT (DOUBLE)
├── REGION (STRING) ◄── NEW
└── LAST_AUDIT_DATE (STRING) ◄── NEW
```

### 6.3 Data Lineage Evolution

#### Enhanced Data Lineage
```
Source Systems:
├── Oracle Database
│   ├── CUSTOMER
│   ├── ACCOUNT
│   ├── TRANSACTION
│   ├── BRANCH
│   └── BRANCH_OPERATIONAL_DETAILS ◄── NEW
│
Transformation Layer:
├── PySpark ETL (RegulatoryReportingETL.py)
│   ├── create_aml_customer_transactions() [Unchanged]
│   └── create_branch_summary_report() ◄── ENHANCED
│
Target Systems:
├── Delta Lake (Databricks)
│   ├── AML_CUSTOMER_TRANSACTIONS [Unchanged]
│   └── BRANCH_SUMMARY_REPORT ◄── ENHANCED
```

## 7. Deployment Strategy

### 7.1 Pre-Deployment Checklist
- [ ] Verify BRANCH_OPERATIONAL_DETAILS table exists in source
- [ ] Validate data quality in operational details table
- [ ] Test JDBC connectivity to new table
- [ ] Backup existing BRANCH_SUMMARY_REPORT data
- [ ] Enable Delta Lake schema evolution
- [ ] Prepare rollback scripts

### 7.2 Deployment Steps

#### Phase 1: Infrastructure Setup
1. Create BRANCH_OPERATIONAL_DETAILS table in Oracle
2. Populate initial data
3. Grant necessary permissions to ETL service account
4. Test connectivity from Databricks

#### Phase 2: Schema Evolution
1. Enable auto schema evolution on Delta table
2. Execute ALTER TABLE statements
3. Validate schema changes
4. Update table documentation

#### Phase 3: Code Deployment
1. Deploy updated PySpark ETL code
2. Update job configuration
3. Execute test run in staging environment
4. Validate output data quality

#### Phase 4: Production Rollout
1. Schedule maintenance window
2. Execute full reload of BRANCH_SUMMARY_REPORT
3. Monitor job performance
4. Validate data completeness
5. Update monitoring dashboards

### 7.3 Validation Criteria

#### Data Quality Checks
```sql
-- Row count validation
SELECT COUNT(*) FROM workspace.default.branch_summary_report;

-- New column population check
SELECT 
    COUNT(*) as TOTAL_ROWS,
    COUNT(REGION) as REGION_POPULATED,
    COUNT(LAST_AUDIT_DATE) as AUDIT_DATE_POPULATED,
    ROUND(COUNT(REGION) * 100.0 / COUNT(*), 2) as REGION_COVERAGE_PCT
FROM workspace.default.branch_summary_report;

-- Data consistency validation
SELECT DISTINCT REGION FROM workspace.default.branch_summary_report WHERE REGION IS NOT NULL;
```

#### Performance Validation
- ETL execution time should not increase by more than 20%
- Memory usage should remain within acceptable limits
- No data loss or corruption

## 8. Risk Mitigation

### 8.1 Identified Risks & Mitigation

| Risk | Impact | Probability | Mitigation Strategy |
|------|--------|-------------|--------------------|
| Source table unavailable | High | Low | Implement graceful degradation, null handling |
| Performance degradation | Medium | Medium | Optimize join strategy, add indexes |
| Schema evolution failure | High | Low | Test in staging, prepare rollback scripts |
| Data quality issues | Medium | Medium | Implement data validation checks |
| Downstream system impact | Low | Low | Maintain backward compatibility |

### 8.2 Monitoring & Alerting

#### Key Metrics to Monitor
- ETL job execution time
- Data freshness of BRANCH_OPERATIONAL_DETAILS
- Row count variations in target table
- Null value percentages in new columns
- Join success rate between BRANCH and BRANCH_OPERATIONAL_DETAILS

#### Alert Conditions
```sql
-- Alert if operational data coverage drops below 80%
SELECT 
    CASE WHEN (COUNT(REGION) * 100.0 / COUNT(*)) < 80 
    THEN 'ALERT: Low operational data coverage' 
    ELSE 'OK' END as STATUS
FROM workspace.default.branch_summary_report;
```

## 9. Success Criteria

### 9.1 Technical Success Criteria
- [ ] BRANCH_SUMMARY_REPORT contains 6 columns (4 existing + 2 new)
- [ ] All existing branches preserved in output
- [ ] New columns populated for active branches
- [ ] ETL job completes without errors
- [ ] Performance impact < 20% increase in execution time
- [ ] Zero data loss or corruption

### 9.2 Business Success Criteria
- [ ] Enhanced regulatory reporting capability
- [ ] Regional analysis support enabled
- [ ] Audit compliance tracking functional
- [ ] Backward compatibility maintained
- [ ] Documentation updated and accessible

## 10. Conclusion

This delta model change represents a low-risk, high-value enhancement to the existing ETL pipeline. The additive nature of the changes ensures backward compatibility while providing enhanced regulatory reporting capabilities. The implementation follows best practices for schema evolution and maintains data integrity throughout the transformation process.

### Key Benefits
- **Enhanced Compliance**: Addition of audit date tracking
- **Regional Analytics**: Support for branch-level regional analysis
- **Maintainable Design**: Clean separation of concerns with optional operational data
- **Scalable Architecture**: Foundation for future operational metadata additions

### Next Steps
1. Stakeholder approval for implementation
2. Environment setup and testing
3. Phased deployment execution
4. Post-deployment monitoring and optimization

---

**Document Status**: Ready for Review  
**Approval Required**: Data Architecture Team, ETL Team Lead, Business Stakeholders  
**Implementation Timeline**: 2-3 weeks including testing and validation