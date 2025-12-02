_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced functional test cases for PySpark ETL enhancement integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT with additional validation scenarios
## *Version*: 2 
## *Updated on*: 
## *Changes*: Added comprehensive data type validation, enhanced error handling test cases, added performance benchmarking tests, included data quality validation scenarios, and expanded edge case coverage
## *Reason*: To provide more thorough test coverage based on updated requirements and ensure robust validation of the ETL enhancement
_____________________________________________

# Enhanced Functional Test Cases for PySpark Code Enhancement - BRANCH_SUMMARY_REPORT Integration

## Overview
This document contains comprehensive functional test cases for the PySpark ETL enhancement that integrates the new Oracle source table BRANCH_OPERATIONAL_DETAILS into the existing BRANCH_SUMMARY_REPORT pipeline. The enhancement adds two new columns (REGION and LAST_AUDIT_DATE) to improve compliance and audit readiness. This version includes enhanced validation scenarios and additional edge cases.

---

### Test Case ID: TC_PCE1_01
**Title:** Validate successful integration of BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT
**Description:** Ensure that the ETL job successfully joins BRANCH_OPERATIONAL_DETAILS with existing data and populates new columns in BRANCH_SUMMARY_REPORT.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table exists with valid data
- BRANCH_SUMMARY_REPORT target table schema includes REGION and LAST_AUDIT_DATE columns
- PySpark ETL job is configured with updated logic
- Source tables (CUSTOMER, BRANCH, ACCOUNT, TRANSACTION) contain test data
- Primary key constraint exists on BRANCH_OPERATIONAL_DETAILS.BRANCH_ID

**Steps to Execute:**
1. Load test data into BRANCH_OPERATIONAL_DETAILS with IS_ACTIVE = 'Y'
2. Load corresponding test data into BRANCH, ACCOUNT, and TRANSACTION tables
3. Execute the enhanced PySpark ETL job
4. Query the BRANCH_SUMMARY_REPORT table
5. Verify that REGION and LAST_AUDIT_DATE columns are populated
6. Validate data type conversion from Oracle VARCHAR2/DATE to Databricks STRING

**Expected Result:**
- ETL job completes successfully without errors
- BRANCH_SUMMARY_REPORT contains all expected records
- REGION column is populated with values from BRANCH_OPERATIONAL_DETAILS.REGION (VARCHAR2(50) → STRING)
- LAST_AUDIT_DATE column is populated with values from BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE (DATE → STRING)
- Join is performed correctly using BRANCH_ID as primary key
- Data type conversions are accurate and consistent

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_02
**Title:** Validate conditional population based on IS_ACTIVE status
**Description:** Ensure that new columns are populated only when IS_ACTIVE = 'Y' in BRANCH_OPERATIONAL_DETAILS.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table contains records with both IS_ACTIVE = 'Y' and IS_ACTIVE = 'N'
- BRANCH_SUMMARY_REPORT target table is accessible
- ETL job is configured with conditional logic for IS_ACTIVE = 'Y'

**Steps to Execute:**
1. Insert test data into BRANCH_OPERATIONAL_DETAILS with mixed IS_ACTIVE values ('Y' and 'N')
2. Ensure corresponding BRANCH records exist
3. Execute the PySpark ETL job
4. Query BRANCH_SUMMARY_REPORT for branches with IS_ACTIVE = 'Y'
5. Query BRANCH_SUMMARY_REPORT for branches with IS_ACTIVE = 'N'
6. Compare the population of REGION and LAST_AUDIT_DATE columns
7. Verify CHAR(1) data type handling for IS_ACTIVE column

**Expected Result:**
- Records with IS_ACTIVE = 'Y' have REGION and LAST_AUDIT_DATE populated
- Records with IS_ACTIVE = 'N' have REGION and LAST_AUDIT_DATE as NULL or default values
- No data corruption occurs for existing columns
- CHAR(1) IS_ACTIVE field is properly processed

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_03
**Title:** Validate backward compatibility with existing records
**Description:** Ensure that existing BRANCH_SUMMARY_REPORT records without corresponding BRANCH_OPERATIONAL_DETAILS data are handled gracefully.
**Preconditions:**
- BRANCH_SUMMARY_REPORT contains existing historical data
- Some branches in BRANCH table do not have corresponding records in BRANCH_OPERATIONAL_DETAILS
- ETL job supports backward compatibility

**Steps to Execute:**
1. Identify existing branches in BRANCH_SUMMARY_REPORT without BRANCH_OPERATIONAL_DETAILS records
2. Execute the enhanced PySpark ETL job
3. Query BRANCH_SUMMARY_REPORT for records without operational details
4. Verify existing column values remain unchanged
5. Check new column values for these records
6. Validate Delta table properties are maintained

**Expected Result:**
- Existing column values (BRANCH_ID, BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT) remain unchanged
- New columns (REGION, LAST_AUDIT_DATE) are set to NULL for records without operational details
- No existing records are lost or corrupted
- ETL job completes successfully
- Delta table properties (compression, deletion vectors) are preserved

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_04
**Title:** Validate schema update for BRANCH_SUMMARY_REPORT with Delta properties
**Description:** Ensure that the BRANCH_SUMMARY_REPORT table schema is correctly updated with new columns and Delta configurations.
**Preconditions:**
- Access to Databricks workspace
- Permissions to describe table schema
- BRANCH_SUMMARY_REPORT table exists with Delta properties

**Steps to Execute:**
1. Connect to Databricks workspace
2. Execute DESCRIBE TABLE workspace.default.branch_summary_report
3. Verify column names, data types, and constraints
4. Check Delta table properties (enableDeletionVectors, compression codec, etc.)
5. Validate column order and nullable constraints
6. Verify Delta version compatibility (minReaderVersion=3, minWriterVersion=7)

**Expected Result:**
- REGION column exists with STRING data type
- LAST_AUDIT_DATE column exists with STRING data type
- All existing columns remain unchanged
- Delta properties include: enableDeletionVectors=true, compression=zstd
- Schema version is updated appropriately
- Delta features (appendOnly, deletionVectors, invariants) are supported

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_05
**Title:** Validate Oracle to Databricks data type compatibility and transformation
**Description:** Ensure proper data type handling between Oracle source (VARCHAR2, DATE, CHAR) and Databricks Delta target (STRING).
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS in Oracle with VARCHAR2(50), VARCHAR2(100), DATE, and CHAR(1) types
- BRANCH_SUMMARY_REPORT in Databricks with STRING types
- ETL job includes comprehensive data type transformation logic

**Steps to Execute:**
1. Insert test data with various date formats in BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE
2. Insert test data with special characters and Unicode in BRANCH_OPERATIONAL_DETAILS.REGION
3. Insert test data with maximum length values (50 chars for REGION, 100 chars for MANAGER_NAME)
4. Execute the PySpark ETL job
5. Query BRANCH_SUMMARY_REPORT and examine data formats
6. Verify data integrity and format consistency
7. Test CHAR(1) to STRING conversion for IS_ACTIVE

**Expected Result:**
- Oracle DATE values are correctly converted to STRING format (consistent date format)
- VARCHAR2 values are properly handled as STRING without truncation
- CHAR(1) values are converted to STRING correctly
- No data truncation or corruption occurs
- Special characters and Unicode are preserved
- Date format is consistent across all records (e.g., YYYY-MM-DD)

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_06
**Title:** Validate ETL job performance with large datasets and Delta optimization
**Description:** Ensure that the enhanced ETL job performs efficiently with production-scale data volumes and leverages Delta optimizations.
**Preconditions:**
- Large volume test data in source tables (>100,000 records)
- BRANCH_OPERATIONAL_DETAILS contains data for all branches
- Databricks cluster is properly configured
- Delta table optimizations are enabled
- Monitoring tools are available

**Steps to Execute:**
1. Load large volume test data into all source tables
2. Monitor cluster resources before job execution
3. Execute the enhanced PySpark ETL job
4. Monitor job execution time and resource utilization
5. Verify data completeness and accuracy in target table
6. Compare performance with baseline metrics
7. Check Delta table file statistics and optimization
8. Validate compression effectiveness (zstd codec)

**Expected Result:**
- ETL job completes within acceptable time limits (<30 minutes for 100K records)
- Memory and CPU utilization remain within normal ranges (<80%)
- All records are processed correctly
- No performance degradation compared to previous version
- Delta table optimization (compaction, Z-ordering) works as expected
- Compression ratio meets expectations (>50% reduction)

**Linked Jira Ticket:** PCE-1

---

### Test Case ID: TC_PCE1_07
**Title:** Validate comprehensive error handling and logging
**Description:** Ensure proper error handling when BRANCH_OPERATIONAL_DETAILS records are missing or invalid for active branches.
**Preconditions:**
- BRANCH table contains active branches
- BRANCH_OPERATIONAL_DETAILS is missing records for some branches
- ETL job includes comprehensive error handling logic
- Structured logging is configured
- Data quality metrics are tracked

**Steps to Execute:**
1. Create test scenario with branches missing from BRANCH_OPERATIONAL_DETAILS
2. Insert invalid data (NULL BRANCH_ID, invalid IS_ACTIVE values)
3. Execute the PySpark ETL job
4. Check job logs for warnings, errors, and data quality metrics
5. Verify BRANCH_SUMMARY_REPORT output
6. Validate error handling behavior and recovery mechanisms
7. Check data quality dashboard for anomaly detection

**Expected Result:**
- ETL job logs appropriate warnings for missing operational details
- Job continues processing without failure (graceful degradation)
- Missing records are handled gracefully with NULL values
- Data quality metrics are updated with anomaly counts
- No data loss occurs for existing information
- Error notifications are sent to monitoring systems

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_08
**Title:** Validate primary key constraint and duplicate handling
**Description:** Ensure proper handling of primary key constraints and potential duplicate BRANCH_ID records in BRANCH_OPERATIONAL_DETAILS.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS has PRIMARY KEY constraint on BRANCH_ID
- ETL job includes deduplication logic
- Data validation routines are in place

**Steps to Execute:**
1. Attempt to insert duplicate BRANCH_ID records in BRANCH_OPERATIONAL_DETAILS
2. Verify primary key constraint enforcement
3. Execute the PySpark ETL job
4. Check for constraint violation handling in job logs
5. Verify BRANCH_SUMMARY_REPORT contains only one record per BRANCH_ID
6. Validate referential integrity with BRANCH table

**Expected Result:**
- Primary key constraint prevents duplicate BRANCH_ID insertion
- ETL job handles constraint violations gracefully
- No duplicate BRANCH_ID records in BRANCH_SUMMARY_REPORT
- Constraint violation handling is logged appropriately
- Data consistency is maintained across all tables
- Performance is not significantly impacted by constraint checks

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_09
**Title:** Validate comprehensive NULL value and data quality handling
**Description:** Ensure proper handling of NULL values in REGION and LAST_AUDIT_DATE columns with data quality validations.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains records with NULL REGION and LAST_AUDIT_DATE values
- ETL job includes NULL handling logic and data quality checks
- Target table allows NULL values in new columns
- Data profiling is enabled

**Steps to Execute:**
1. Insert test data with NULL values in REGION and LAST_AUDIT_DATE columns
2. Insert test data with empty strings and whitespace-only values
3. Insert test data with invalid date formats
4. Execute the PySpark ETL job with data quality validation
5. Query BRANCH_SUMMARY_REPORT for NULL value handling
6. Verify data quality reports and metrics
7. Check data profiling results

**Expected Result:**
- NULL values are preserved in target table
- Empty strings are standardized (converted to NULL or maintained)
- Invalid date formats are handled with appropriate error logging
- Data quality rules are applied appropriately
- Data profiling shows expected NULL percentages
- Downstream processes can handle NULL values correctly

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_10
**Title:** Validate incremental load functionality with Delta merge operations
**Description:** Ensure that incremental loads work correctly with the new BRANCH_OPERATIONAL_DETAILS integration using Delta merge.
**Preconditions:**
- BRANCH_SUMMARY_REPORT contains existing data
- BRANCH_OPERATIONAL_DETAILS has new/updated records
- ETL job supports incremental processing with Delta merge
- Change data capture mechanisms are in place
- Delta table versioning is enabled

**Steps to Execute:**
1. Execute initial full load of BRANCH_SUMMARY_REPORT
2. Add new records to BRANCH_OPERATIONAL_DETAILS
3. Update existing records in BRANCH_OPERATIONAL_DETAILS (change REGION, LAST_AUDIT_DATE)
4. Execute incremental ETL job using Delta merge
5. Verify only changed records are processed
6. Validate final data state in BRANCH_SUMMARY_REPORT
7. Check Delta table history and versioning

**Expected Result:**
- Only new and updated records are processed (upsert operation)
- Incremental load completes faster than full load (>70% time reduction)
- Data consistency is maintained across all operations
- No duplicate records are created
- Change tracking works correctly with new columns
- Delta table history shows proper versioning

**Linked Jira Ticket:** PCE-1, PCE-2

---

## Enhanced Edge Cases and Boundary Conditions

### Test Case ID: TC_PCE1_11
**Title:** Validate maximum length values and data truncation handling
**Description:** Test behavior with maximum length strings in REGION column and proper truncation handling.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS allows VARCHAR2(50) for REGION
- Test data with various length region names
- Data validation rules are configured

**Steps to Execute:**
1. Insert records with exactly 50-character REGION values
2. Insert records with Unicode characters in REGION (multi-byte)
3. Insert records with special characters and symbols
4. Execute ETL job with data validation
5. Verify data truncation or error handling
6. Check character encoding preservation

**Expected Result:**
- 50-character values are processed correctly
- Unicode characters are preserved without corruption
- Special characters are handled according to business rules
- No data corruption occurs during conversion
- Character encoding is maintained (UTF-8)

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_12
**Title:** Validate comprehensive date boundary conditions and format standardization
**Description:** Test behavior with edge case dates in LAST_AUDIT_DATE and ensure format standardization.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains various date formats
- ETL job handles date conversions with standardization
- Date validation rules are implemented

**Steps to Execute:**
1. Insert records with minimum date values (e.g., 1900-01-01)
2. Insert records with maximum date values (e.g., 2099-12-31)
3. Insert records with current date and future dates
4. Insert records with different Oracle date formats
5. Execute ETL job with date standardization
6. Verify date handling and format consistency
7. Test leap year dates and month-end dates

**Expected Result:**
- All valid dates are processed correctly
- Date format is standardized in target table (YYYY-MM-DD)
- Invalid dates are logged and handled appropriately
- Leap year dates are processed correctly
- Time zone considerations are handled properly

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_13
**Title:** Validate MANAGER_NAME field handling and data privacy
**Description:** Test proper handling of MANAGER_NAME field including data privacy considerations.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains VARCHAR2(100) MANAGER_NAME field
- Data privacy rules are configured
- ETL job includes data masking capabilities

**Steps to Execute:**
1. Insert records with various manager name formats
2. Insert records with special characters in names
3. Insert records with maximum length manager names (100 characters)
4. Execute ETL job (note: MANAGER_NAME not included in target but validate processing)
5. Verify data privacy compliance
6. Check that sensitive data is not inadvertently exposed

**Expected Result:**
- MANAGER_NAME field is processed correctly during ETL
- Data privacy rules are enforced
- No sensitive information leaks to target table
- Field length constraints are respected
- Special characters in names are handled properly

**Linked Jira Ticket:** PCE-1, PCE-2

---

## Advanced Data Validation Test Cases

### Test Case ID: TC_PCE1_14
**Title:** Validate comprehensive data reconciliation and audit trail
**Description:** Ensure data accuracy, completeness, and audit trail maintenance after ETL processing.
**Preconditions:**
- Source tables contain known test data volumes
- ETL job includes comprehensive data validation routines
- Audit logging is configured
- Reconciliation reports are available

**Steps to Execute:**
1. Count records in source tables before ETL
2. Execute ETL job with full audit logging enabled
3. Count records in BRANCH_SUMMARY_REPORT after ETL
4. Compare aggregated values between source and target
5. Generate comprehensive reconciliation report
6. Verify audit trail completeness
7. Check data lineage tracking

**Expected Result:**
- Record counts match expected values (100% reconciliation)
- Aggregated amounts reconcile correctly (variance <0.01%)
- No data loss during transformation
- Validation report shows 100% accuracy
- Complete audit trail is maintained
- Data lineage is properly tracked

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_15
**Title:** Validate referential integrity and foreign key relationships
**Description:** Ensure referential integrity is maintained across all joined tables with proper constraint handling.
**Preconditions:**
- BRANCH_ID exists as primary key in BRANCH table
- BRANCH_ID exists as foreign key in BRANCH_OPERATIONAL_DETAILS
- Referential integrity constraints are defined and enforced

**Steps to Execute:**
1. Verify BRANCH_ID relationships across all tables
2. Insert orphaned records (BRANCH_OPERATIONAL_DETAILS without corresponding BRANCH)
3. Execute ETL job with referential integrity checks
4. Check for orphaned records in target table
5. Validate join accuracy and constraint enforcement
6. Verify foreign key relationship preservation

**Expected Result:**
- All BRANCH_ID references are valid
- Orphaned records are identified and handled appropriately
- Join operations maintain data integrity
- Foreign key relationships are preserved
- Constraint violations are logged and reported

**Linked Jira Ticket:** PCE-2

---

## Comprehensive Regression Test Cases

### Test Case ID: TC_PCE1_16
**Title:** Validate complete backward compatibility and regression prevention
**Description:** Ensure that existing BRANCH_SUMMARY_REPORT functionality is not impacted by new changes.
**Preconditions:**
- Baseline data for existing columns
- Previous ETL job results for comparison
- Comprehensive regression test suite available
- Performance benchmarks established

**Steps to Execute:**
1. Execute ETL job with new enhancements
2. Compare existing column values with baseline (bit-by-bit comparison)
3. Verify calculation logic for TOTAL_TRANSACTIONS and TOTAL_AMOUNT
4. Check data types and formats for existing columns
5. Validate report generation functionality
6. Compare performance metrics with baseline
7. Test downstream system compatibility

**Expected Result:**
- All existing column values match baseline exactly
- Calculation logic produces identical results
- Data types and formats remain unchanged
- Report generation works as before
- No regression in existing functionality
- Performance remains within acceptable variance (±5%)

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_17
**Title:** Validate Delta table maintenance and optimization operations
**Description:** Ensure Delta table maintenance operations work correctly with the enhanced schema.
**Preconditions:**
- BRANCH_SUMMARY_REPORT is a Delta table with optimization enabled
- Table has sufficient data for optimization operations
- Maintenance operations are scheduled

**Steps to Execute:**
1. Execute OPTIMIZE command on BRANCH_SUMMARY_REPORT
2. Run VACUUM operation to clean up old files
3. Execute Z-ORDER optimization on BRANCH_ID
4. Check table statistics and file organization
5. Verify query performance after optimization
6. Test time travel functionality

**Expected Result:**
- OPTIMIZE operation completes successfully
- VACUUM operation removes old files appropriately
- Z-ORDER optimization improves query performance
- Table statistics are updated correctly
- Time travel functionality works with new schema
- File organization is optimal for query patterns

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_18
**Title:** Validate end-to-end data pipeline with monitoring and alerting
**Description:** Test the complete data pipeline from Oracle source to Databricks target with monitoring.
**Preconditions:**
- Complete data pipeline is configured
- Monitoring and alerting systems are in place
- Data quality thresholds are defined
- SLA requirements are established

**Steps to Execute:**
1. Execute full end-to-end pipeline
2. Monitor data flow through all stages
3. Verify data quality checks at each stage
4. Test alerting mechanisms for failures
5. Validate SLA compliance
6. Check monitoring dashboard accuracy

**Expected Result:**
- Pipeline completes within SLA timeframes
- Data quality checks pass at all stages
- Monitoring provides accurate real-time status
- Alerting triggers appropriately for issues
- Dashboard shows correct metrics and KPIs
- End-to-end data lineage is traceable

**Linked Jira Ticket:** PCE-1, PCE-2

---

## Security and Compliance Test Cases

### Test Case ID: TC_PCE1_19
**Title:** Validate data security and access control compliance
**Description:** Ensure that data security measures and access controls are properly implemented.
**Preconditions:**
- Security policies are defined and implemented
- Access control mechanisms are in place
- Audit logging is configured for security events

**Steps to Execute:**
1. Test data access with different user roles
2. Verify encryption in transit and at rest
3. Check audit logging for data access
4. Test data masking for sensitive fields
5. Validate compliance with data governance policies
6. Verify secure credential management

**Expected Result:**
- Access controls work as designed
- Data is encrypted appropriately
- Audit logs capture all data access events
- Sensitive data is properly masked
- Compliance requirements are met
- Credentials are managed securely

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_20
**Title:** Validate disaster recovery and business continuity
**Description:** Test disaster recovery procedures and business continuity measures.
**Preconditions:**
- Disaster recovery procedures are documented
- Backup and restore mechanisms are in place
- Business continuity plans are established

**Steps to Execute:**
1. Test backup and restore procedures
2. Simulate system failures and recovery
3. Verify data consistency after recovery
4. Test failover mechanisms
5. Validate recovery time objectives (RTO)
6. Check recovery point objectives (RPO)

**Expected Result:**
- Backup and restore work correctly
- System recovers within RTO requirements
- Data loss is within RPO limits
- Failover mechanisms function properly
- Data consistency is maintained
- Business operations can continue

**Linked Jira Ticket:** PCE-1, PCE-2