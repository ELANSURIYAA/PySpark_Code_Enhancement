_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Delta model changes for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Data Model Evolution Analysis - BRANCH_SUMMARY_REPORT Enhancement

## Executive Summary

This document outlines the delta changes required to evolve the current data model to support the integration of the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for the `BRANCH_SUMMARY_REPORT` target table in Databricks Delta.

## Model Ingestion Analysis

### Current Data Model Structure

#### Existing Source Tables:
1. **CUSTOMER** - Customer master data
2. **BRANCH** - Branch master data
3. **ACCOUNT** - Account details linking customers to branches
4. **TRANSACTION** - Transaction records

#### Current Target Table:
- **BRANCH_SUMMARY_REPORT** - Aggregated branch transaction summary

### Current Schema Relationships:
```
TRANSACTION -> ACCOUNT -> BRANCH
                     |
                 CUSTOMER
```

## Spec Parsing & Delta Detection

### New Requirements Analysis

Based on the technical specification (DI_Data_Technical_Specification_SN_1.md), the following changes are required:

#### 1. New Source Table Addition
**Table**: `BRANCH_OPERATIONAL_DETAILS`
- **Purpose**: Provide operational metadata for branches
- **Relationship**: One-to-One with BRANCH table via BRANCH_ID
- **Key Fields**: REGION, MANAGER_NAME, LAST_AUDIT_DATE, IS_ACTIVE

#### 2. Target Schema Evolution
**Table**: `BRANCH_SUMMARY_REPORT`
- **New Columns**: REGION (STRING), LAST_AUDIT_DATE (STRING)
- **Evolution Type**: Additive schema change

## Delta Computation

### Categorized Changes

#### A. New Tables/Sources
| Change Type | Object Name | Impact Level | Description |
|-------------|-------------|--------------|-------------|
| **Addition** | BRANCH_OPERATIONAL_DETAILS | Minor | New source table for operational metadata |

#### B. Modified Schemas
| Change Type | Object Name | Field Name | Old Type | New Type | Impact Level |
|-------------|-------------|------------|----------|----------|-------------|
| **Addition** | BRANCH_SUMMARY_REPORT | REGION | N/A | STRING | Minor |
| **Addition** | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | N/A | STRING | Minor |

#### C. Modified Relationships
| Change Type | Description | Impact Level |
|-------------|-------------|-------------|
| **Addition** | BRANCH -> BRANCH_OPERATIONAL_DETAILS (1:1) | Minor |

### Version Impact Assessment
**Overall Impact**: **MINOR** - Additive changes with backward compatibility

## Impact Assessment

### Downstream Dependencies Analysis

#### Potential Breaking Changes: **NONE**
- Schema evolution is additive only
- Existing columns remain unchanged
- New columns are nullable

#### Data Loss Risk: **LOW**
- No columns being dropped
- No data type narrowing
- Historical data preserved

#### Performance Impact: **MEDIUM**
- Additional JOIN operation required
- Increased target table width
- Potential query performance impact

### Platform-Specific Considerations

#### Databricks Delta Lake:
- ✅ Supports schema evolution
- ✅ Column addition supported
- ✅ Backward compatibility maintained
- ⚠️ May require table properties update

#### Oracle Source:
- ✅ New table accessible via existing JDBC connection
- ✅ Standard SQL JOIN operations supported

## DDL Generation

### Forward Migration Scripts

#### 1. Source Table Creation (Oracle)
```sql
-- BRANCH_OPERATIONAL_DETAILS table (if not exists)
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT,
    REGION VARCHAR2(50),
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1),
    PRIMARY KEY (BRANCH_ID)
);

-- Add foreign key constraint
ALTER TABLE BRANCH_OPERATIONAL_DETAILS 
ADD CONSTRAINT FK_BRANCH_OPERATIONAL_BRANCH_ID 
FOREIGN KEY (BRANCH_ID) REFERENCES BRANCH(BRANCH_ID);
```

#### 2. Target Schema Evolution (Databricks Delta)
```sql
-- Add new columns to existing BRANCH_SUMMARY_REPORT
ALTER TABLE workspace.default.branch_summary_report 
ADD COLUMNS (
    REGION STRING COMMENT 'Branch operational region',
    LAST_AUDIT_DATE STRING COMMENT 'Last audit date for compliance tracking'
);

-- Update table properties if needed
ALTER TABLE workspace.default.branch_summary_report 
SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
);
```

### ETL Code Changes

#### 1. Enhanced Function Signature
```python
# BEFORE
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:

# AFTER  
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
```

#### 2. Updated Join Logic
```python
# Enhanced join with operational details
enhanced_summary = base_summary.join(
    active_branch_operational_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE"),
    "BRANCH_ID",
    "left"  # Preserve all branches
).select(
    col("BRANCH_ID"),
    col("BRANCH_NAME"),
    col("TOTAL_TRANSACTIONS"),
    col("TOTAL_AMOUNT"),
    col("REGION"),
    col("LAST_AUDIT_DATE")
)
```

### Rollback Scripts

#### 1. Target Schema Rollback
```sql
-- Remove added columns (if rollback needed)
ALTER TABLE workspace.default.branch_summary_report 
DROP COLUMNS (REGION, LAST_AUDIT_DATE);
```

#### 2. ETL Code Rollback
```python
# Revert to original function signature and logic
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
    return transaction_df.join(account_df, "ACCOUNT_ID") \
                         .join(branch_df, "BRANCH_ID") \
                         .groupBy("BRANCH_ID", "BRANCH_NAME") \
                         .agg(
                             count("*").alias("TOTAL_TRANSACTIONS"),
                             sum("AMOUNT").alias("TOTAL_AMOUNT")
                         )
```

## Data Migration Strategy

### Migration Approach: **Incremental Evolution**

#### Phase 1: Schema Preparation
1. Add new columns to target table with NULL defaults
2. Verify schema evolution compatibility
3. Test backward compatibility

#### Phase 2: ETL Enhancement
1. Deploy updated ETL code
2. Execute full refresh to populate new columns
3. Validate data quality and completeness

#### Phase 3: Validation & Monitoring
1. Monitor performance impact
2. Validate data accuracy
3. Update documentation and metadata

### Data Population Strategy
```sql
-- Backfill strategy for historical data
UPDATE workspace.default.branch_summary_report bsr
SET 
    bsr.REGION = bod.REGION,
    bsr.LAST_AUDIT_DATE = bod.LAST_AUDIT_DATE
FROM BRANCH_OPERATIONAL_DETAILS bod
WHERE bsr.BRANCH_ID = bod.BRANCH_ID 
  AND bod.IS_ACTIVE = 'Y';
```

## Documentation Updates

### Schema Documentation

#### Updated Target Table Schema
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT COMMENT 'Unique branch identifier',
    BRANCH_NAME STRING COMMENT 'Branch display name',
    TOTAL_TRANSACTIONS BIGINT COMMENT 'Total number of transactions',
    TOTAL_AMOUNT DOUBLE COMMENT 'Total transaction amount',
    REGION STRING COMMENT 'Branch operational region', -- NEW
    LAST_AUDIT_DATE STRING COMMENT 'Last audit date for compliance' -- NEW
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

### Change Traceability Matrix

| Tech Spec Section | Requirement | DDL Change | ETL Change | Validation |
|-------------------|-------------|------------|------------|------------|
| Data Model Updates | Add REGION column | ALTER TABLE ADD COLUMN | Enhanced JOIN logic | Column population check |
| Data Model Updates | Add LAST_AUDIT_DATE | ALTER TABLE ADD COLUMN | Enhanced JOIN logic | Date format validation |
| Source Integration | Read BRANCH_OPERATIONAL_DETAILS | N/A | New table read | Row count validation |
| Join Logic | LEFT JOIN with operational data | N/A | Updated join strategy | Relationship integrity |

## Risk Assessment & Mitigation

### Identified Risks

#### 1. Performance Degradation
**Risk Level**: Medium
**Mitigation**: 
- Monitor query execution times
- Consider partitioning strategies
- Implement query optimization

#### 2. Data Quality Issues
**Risk Level**: Low
**Mitigation**:
- Implement data validation checks
- Monitor NULL value patterns
- Establish data quality metrics

#### 3. Backward Compatibility
**Risk Level**: Low
**Mitigation**:
- Maintain existing column order
- Ensure NULL handling in downstream systems
- Comprehensive testing

### Monitoring & Alerting

#### Key Metrics to Monitor:
1. **ETL Performance**: Execution time, memory usage
2. **Data Quality**: NULL percentages, data completeness
3. **Schema Evolution**: Version compatibility, reader/writer versions
4. **Business Metrics**: Report accuracy, audit compliance

## Validation Criteria

### Technical Validation
- [ ] Schema evolution successful without errors
- [ ] ETL pipeline executes without failures
- [ ] New columns populated correctly
- [ ] Performance within acceptable thresholds
- [ ] Rollback procedures tested and verified

### Business Validation
- [ ] REGION data matches operational requirements
- [ ] LAST_AUDIT_DATE supports compliance reporting
- [ ] Historical data integrity maintained
- [ ] Downstream systems handle new schema

### Data Quality Validation
- [ ] Row counts match expected values
- [ ] JOIN accuracy verified
- [ ] NULL handling appropriate
- [ ] Data type conversions correct

## Implementation Timeline

### Recommended Phases

#### Week 1: Preparation
- Schema evolution testing
- Code development and unit testing
- Documentation updates

#### Week 2: Development Deployment
- Deploy to development environment
- Integration testing
- Performance validation

#### Week 3: Production Deployment
- Production deployment
- Full data refresh
- Monitoring and validation

#### Week 4: Stabilization
- Performance optimization
- Issue resolution
- Documentation finalization

## Conclusion

The proposed delta changes represent a **MINOR** evolution of the current data model with **LOW RISK** and **HIGH BUSINESS VALUE**. The additive nature of the changes ensures backward compatibility while enhancing the reporting capabilities with operational metadata.

### Key Success Factors:
1. **Comprehensive Testing**: Thorough validation of schema evolution and ETL changes
2. **Performance Monitoring**: Continuous monitoring of query performance and resource usage
3. **Data Quality Assurance**: Robust validation of data accuracy and completeness
4. **Change Management**: Proper communication and coordination with downstream consumers

### Next Steps:
1. Review and approve this delta analysis
2. Execute schema evolution in development environment
3. Deploy and test enhanced ETL pipeline
4. Validate business requirements and data quality
5. Proceed with production deployment

This evolution positions the data model to support enhanced regulatory reporting while maintaining system stability and performance.