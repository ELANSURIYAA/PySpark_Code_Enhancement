_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Functional test cases for PySpark ETL enhancement integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Functional Test Cases for PySpark Code Enhancement - BRANCH_SUMMARY_REPORT Integration

## Overview
This document contains comprehensive functional test cases for the PySpark ETL enhancement that integrates the new Oracle source table BRANCH_OPERATIONAL_DETAILS into the existing BRANCH_SUMMARY_REPORT pipeline. The enhancement adds two new columns (REGION and LAST_AUDIT_DATE) to improve compliance and audit readiness.

---

### Test Case ID: TC_PCE1_01
**Title:** Validate successful integration of BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT
**Description:** Ensure that the ETL job successfully joins BRANCH_OPERATIONAL_DETAILS with existing data and populates new columns in BRANCH_SUMMARY_REPORT.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table exists with valid data
- BRANCH_SUMMARY_REPORT target table schema includes REGION and LAST_AUDIT_DATE columns
- PySpark ETL job is configured with updated logic
- Source tables (CUSTOMER, BRANCH, ACCOUNT, TRANSACTION) contain test data

**Steps to Execute:**
1. Load test data into BRANCH_OPERATIONAL_DETAILS with IS_ACTIVE = 'Y'
2. Load corresponding test data into BRANCH, ACCOUNT, and TRANSACTION tables
3. Execute the enhanced PySpark ETL job
4. Query the BRANCH_SUMMARY_REPORT table
5. Verify that REGION and LAST_AUDIT_DATE columns are populated

**Expected Result:**
- ETL job completes successfully without errors
- BRANCH_SUMMARY_REPORT contains all expected records
- REGION column is populated with values from BRANCH_OPERATIONAL_DETAILS.REGION
- LAST_AUDIT_DATE column is populated with values from BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE
- Join is performed correctly using BRANCH_ID

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_02
**Title:** Validate conditional population based on IS_ACTIVE status
**Description:** Ensure that new columns are populated only when IS_ACTIVE = 'Y' in BRANCH_OPERATIONAL_DETAILS.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table contains records with both IS_ACTIVE = 'Y' and IS_ACTIVE = 'N'
- BRANCH_SUMMARY_REPORT target table is accessible
- ETL job is configured with conditional logic

**Steps to Execute:**
1. Insert test data into BRANCH_OPERATIONAL_DETAILS with mixed IS_ACTIVE values ('Y' and 'N')
2. Ensure corresponding BRANCH records exist
3. Execute the PySpark ETL job
4. Query BRANCH_SUMMARY_REPORT for branches with IS_ACTIVE = 'Y'
5. Query BRANCH_SUMMARY_REPORT for branches with IS_ACTIVE = 'N'
6. Compare the population of REGION and LAST_AUDIT_DATE columns

**Expected Result:**
- Records with IS_ACTIVE = 'Y' have REGION and LAST_AUDIT_DATE populated
- Records with IS_ACTIVE = 'N' have REGION and LAST_AUDIT_DATE as NULL or default values
- No data corruption occurs for existing columns

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

**Expected Result:**
- Existing column values (BRANCH_ID, BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT) remain unchanged
- New columns (REGION, LAST_AUDIT_DATE) are set to NULL or default values for records without operational details
- No existing records are lost or corrupted
- ETL job completes successfully

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_04
**Title:** Validate schema update for BRANCH_SUMMARY_REPORT
**Description:** Ensure that the BRANCH_SUMMARY_REPORT table schema is correctly updated with new columns.
**Preconditions:**
- Access to Databricks workspace
- Permissions to describe table schema
- BRANCH_SUMMARY_REPORT table exists

**Steps to Execute:**
1. Connect to Databricks workspace
2. Execute DESCRIBE TABLE workspace.default.branch_summary_report
3. Verify column names, data types, and constraints
4. Check table properties and Delta configurations
5. Validate column order and nullable constraints

**Expected Result:**
- REGION column exists with STRING data type
- LAST_AUDIT_DATE column exists with STRING data type
- All existing columns remain unchanged
- Table properties maintain Delta configurations
- Schema version is updated appropriately

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_05
**Title:** Validate data type compatibility and transformation
**Description:** Ensure proper data type handling between Oracle source and Databricks Delta target.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS in Oracle with VARCHAR2 and DATE types
- BRANCH_SUMMARY_REPORT in Databricks with STRING types
- ETL job includes data type transformation logic

**Steps to Execute:**
1. Insert test data with various date formats in BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE
2. Insert test data with special characters in BRANCH_OPERATIONAL_DETAILS.REGION
3. Execute the PySpark ETL job
4. Query BRANCH_SUMMARY_REPORT and examine data formats
5. Verify data integrity and format consistency

**Expected Result:**
- DATE values from Oracle are correctly converted to STRING format
- VARCHAR2 values are properly handled as STRING
- No data truncation or corruption occurs
- Special characters and Unicode are preserved
- Date format is consistent across all records

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_06
**Title:** Validate ETL job performance with large datasets
**Description:** Ensure that the enhanced ETL job performs efficiently with production-scale data volumes.
**Preconditions:**
- Large volume test data in source tables (>10,000 records)
- BRANCH_OPERATIONAL_DETAILS contains data for all branches
- Databricks cluster is properly configured
- Monitoring tools are available

**Steps to Execute:**
1. Load large volume test data into all source tables
2. Monitor cluster resources before job execution
3. Execute the enhanced PySpark ETL job
4. Monitor job execution time and resource utilization
5. Verify data completeness and accuracy in target table
6. Compare performance with baseline metrics

**Expected Result:**
- ETL job completes within acceptable time limits
- Memory and CPU utilization remain within normal ranges
- All records are processed correctly
- No performance degradation compared to previous version
- Delta table optimization works as expected

**Linked Jira Ticket:** PCE-1

---

### Test Case ID: TC_PCE1_07
**Title:** Validate error handling for missing BRANCH_OPERATIONAL_DETAILS records
**Description:** Ensure proper error handling when BRANCH_OPERATIONAL_DETAILS records are missing for active branches.
**Preconditions:**
- BRANCH table contains active branches
- BRANCH_OPERATIONAL_DETAILS is missing records for some branches
- ETL job includes error handling logic
- Logging is configured

**Steps to Execute:**
1. Create test scenario with branches missing from BRANCH_OPERATIONAL_DETAILS
2. Execute the PySpark ETL job
3. Check job logs for warnings or errors
4. Verify BRANCH_SUMMARY_REPORT output
5. Validate error handling behavior

**Expected Result:**
- ETL job logs appropriate warnings for missing operational details
- Job continues processing without failure
- Missing records are handled gracefully with NULL values
- Data quality metrics are updated
- No data loss occurs for existing information

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_08
**Title:** Validate duplicate BRANCH_ID handling in BRANCH_OPERATIONAL_DETAILS
**Description:** Ensure proper handling of duplicate BRANCH_ID records in BRANCH_OPERATIONAL_DETAILS.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains duplicate BRANCH_ID records
- ETL job includes deduplication logic
- Primary key constraint exists on BRANCH_ID

**Steps to Execute:**
1. Insert duplicate BRANCH_ID records in BRANCH_OPERATIONAL_DETAILS with different REGION values
2. Execute the PySpark ETL job
3. Check for duplicate handling in job logs
4. Verify BRANCH_SUMMARY_REPORT contains only one record per BRANCH_ID
5. Validate which duplicate record was selected

**Expected Result:**
- ETL job handles duplicates according to business rules (e.g., latest record, active record)
- No duplicate BRANCH_ID records in BRANCH_SUMMARY_REPORT
- Deduplication logic is logged appropriately
- Data consistency is maintained
- Performance is not significantly impacted

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_09
**Title:** Validate NULL value handling in new columns
**Description:** Ensure proper handling of NULL values in REGION and LAST_AUDIT_DATE columns from source data.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains records with NULL REGION and LAST_AUDIT_DATE values
- ETL job includes NULL handling logic
- Target table allows NULL values in new columns

**Steps to Execute:**
1. Insert test data with NULL values in REGION and LAST_AUDIT_DATE columns
2. Insert test data with empty strings in these columns
3. Execute the PySpark ETL job
4. Query BRANCH_SUMMARY_REPORT for NULL value handling
5. Verify data quality and consistency

**Expected Result:**
- NULL values are preserved in target table
- Empty strings are handled consistently
- No conversion errors occur
- Data quality rules are applied appropriately
- Downstream processes can handle NULL values

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_10
**Title:** Validate incremental load functionality
**Description:** Ensure that incremental loads work correctly with the new BRANCH_OPERATIONAL_DETAILS integration.
**Preconditions:**
- BRANCH_SUMMARY_REPORT contains existing data
- BRANCH_OPERATIONAL_DETAILS has new/updated records
- ETL job supports incremental processing
- Change data capture mechanisms are in place

**Steps to Execute:**
1. Execute initial full load of BRANCH_SUMMARY_REPORT
2. Add new records to BRANCH_OPERATIONAL_DETAILS
3. Update existing records in BRANCH_OPERATIONAL_DETAILS
4. Execute incremental ETL job
5. Verify only changed records are processed
6. Validate final data state in BRANCH_SUMMARY_REPORT

**Expected Result:**
- Only new and updated records are processed
- Incremental load completes faster than full load
- Data consistency is maintained
- No duplicate records are created
- Change tracking works correctly with new columns

**Linked Jira Ticket:** PCE-1, PCE-2

---

## Edge Cases and Boundary Conditions

### Test Case ID: TC_PCE1_11
**Title:** Validate maximum length values in REGION column
**Description:** Test behavior with maximum length strings in REGION column.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS allows VARCHAR2(50) for REGION
- Test data with 50-character region names

**Steps to Execute:**
1. Insert records with exactly 50-character REGION values
2. Insert records with >50-character REGION values (if possible)
3. Execute ETL job
4. Verify data truncation or error handling

**Expected Result:**
- 50-character values are processed correctly
- Longer values are handled according to business rules
- No data corruption occurs

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE1_12
**Title:** Validate date boundary conditions
**Description:** Test behavior with edge case dates in LAST_AUDIT_DATE.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains various date formats
- ETL job handles date conversions

**Steps to Execute:**
1. Insert records with minimum date values (e.g., 1900-01-01)
2. Insert records with maximum date values (e.g., 2099-12-31)
3. Insert records with current date
4. Execute ETL job
5. Verify date handling and format consistency

**Expected Result:**
- All valid dates are processed correctly
- Date format is consistent in target table
- No date conversion errors occur

**Linked Jira Ticket:** PCE-2

---

## Data Validation Test Cases

### Test Case ID: TC_PCE1_13
**Title:** Validate data reconciliation between source and target
**Description:** Ensure data accuracy and completeness after ETL processing.
**Preconditions:**
- Source tables contain known test data volumes
- ETL job includes data validation routines
- Reconciliation reports are available

**Steps to Execute:**
1. Count records in source tables before ETL
2. Execute ETL job with validation enabled
3. Count records in BRANCH_SUMMARY_REPORT after ETL
4. Compare aggregated values between source and target
5. Generate reconciliation report

**Expected Result:**
- Record counts match expected values
- Aggregated amounts reconcile correctly
- No data loss during transformation
- Validation report shows 100% accuracy

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE1_14
**Title:** Validate referential integrity with BRANCH_ID joins
**Description:** Ensure referential integrity is maintained across all joined tables.
**Preconditions:**
- BRANCH_ID exists as primary key in BRANCH table
- BRANCH_ID exists as foreign key in BRANCH_OPERATIONAL_DETAILS
- Referential integrity constraints are defined

**Steps to Execute:**
1. Verify BRANCH_ID relationships across all tables
2. Execute ETL job
3. Check for orphaned records in target table
4. Validate join accuracy
5. Verify no referential integrity violations

**Expected Result:**
- All BRANCH_ID references are valid
- No orphaned records exist
- Join operations maintain data integrity
- Foreign key relationships are preserved

**Linked Jira Ticket:** PCE-2

---

## Regression Test Cases

### Test Case ID: TC_PCE1_15
**Title:** Validate existing functionality remains unchanged
**Description:** Ensure that existing BRANCH_SUMMARY_REPORT functionality is not impacted by new changes.
**Preconditions:**
- Baseline data for existing columns
- Previous ETL job results for comparison
- Regression test suite available

**Steps to Execute:**
1. Execute ETL job with new enhancements
2. Compare existing column values with baseline
3. Verify calculation logic for TOTAL_TRANSACTIONS and TOTAL_AMOUNT
4. Check data types and formats for existing columns
5. Validate report generation functionality

**Expected Result:**
- All existing column values match baseline
- Calculation logic produces identical results
- Data types and formats remain unchanged
- Report generation works as before
- No regression in existing functionality

**Linked Jira Ticket:** PCE-1, PCE-2