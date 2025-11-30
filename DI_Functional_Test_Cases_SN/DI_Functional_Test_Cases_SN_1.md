_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Functional test cases for ETL integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Functional Test Cases for ETL Integration - BRANCH_OPERATIONAL_DETAILS to BRANCH_SUMMARY_REPORT

## Overview
This document contains comprehensive functional test cases for the ETL process that integrates BRANCH_OPERATIONAL_DETAILS data into the BRANCH_SUMMARY_REPORT target table in Databricks Delta format.

## Test Cases

### Test Case ID: TC_ETL_001
**Title:** Validate successful data extraction from BRANCH_OPERATIONAL_DETAILS source table
**Description:** Ensure that data can be successfully extracted from the BRANCH_OPERATIONAL_DETAILS Oracle source table with all required columns.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table exists in Oracle source system
- Table contains valid test data
- ETL connection to Oracle is established

**Steps to Execute:**
1. Connect to Oracle source database
2. Execute SELECT query on BRANCH_OPERATIONAL_DETAILS table
3. Verify all columns are accessible: BRANCH_ID, REGION, MANAGER_NAME, LAST_AUDIT_DATE, IS_ACTIVE
4. Validate data types and constraints

**Expected Result:**
- Query executes successfully without errors
- All expected columns are returned
- Data types match schema definition (BRANCH_ID: INT, REGION: VARCHAR2(50), etc.)
- Primary key constraint on BRANCH_ID is respected

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_002
**Title:** Validate data transformation and mapping from source to target columns
**Description:** Ensure that source columns are correctly mapped and transformed to target table structure.
**Preconditions:**
- Source data is available in BRANCH_OPERATIONAL_DETAILS
- Target table BRANCH_SUMMARY_REPORT schema is defined
- ETL transformation logic is implemented

**Steps to Execute:**
1. Load sample data into BRANCH_OPERATIONAL_DETAILS
2. Execute ETL transformation process
3. Verify column mapping: BRANCH_OPERATIONAL_DETAILS.REGION → BRANCH_SUMMARY_REPORT.REGION
4. Verify column mapping: BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE → BRANCH_SUMMARY_REPORT.LAST_AUDIT_DATE
5. Check data type conversions (DATE to STRING for LAST_AUDIT_DATE)

**Expected Result:**
- REGION column data is correctly mapped without data loss
- LAST_AUDIT_DATE is properly converted from DATE to STRING format
- No data truncation or corruption occurs during transformation
- All mapped records maintain referential integrity

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_003
**Title:** Validate JOIN operation between BRANCH and BRANCH_OPERATIONAL_DETAILS tables
**Description:** Ensure that the JOIN operation correctly combines data from BRANCH and BRANCH_OPERATIONAL_DETAILS tables based on BRANCH_ID.
**Preconditions:**
- BRANCH table contains test data
- BRANCH_OPERATIONAL_DETAILS table contains corresponding test data
- Both tables have matching BRANCH_ID values

**Steps to Execute:**
1. Insert test data with matching BRANCH_ID in both tables
2. Execute JOIN operation on BRANCH_ID
3. Verify all matching records are included
4. Check that BRANCH_NAME from BRANCH table is preserved
5. Validate that REGION and LAST_AUDIT_DATE from BRANCH_OPERATIONAL_DETAILS are included

**Expected Result:**
- JOIN operation executes successfully
- All records with matching BRANCH_ID are included in result set
- No duplicate records are created
- All required columns from both tables are present in output

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_004
**Title:** Validate handling of NULL values in REGION and LAST_AUDIT_DATE columns
**Description:** Ensure that NULL values in source columns are properly handled during ETL process.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table contains records with NULL REGION values
- BRANCH_OPERATIONAL_DETAILS table contains records with NULL LAST_AUDIT_DATE values
- ETL process includes NULL handling logic

**Steps to Execute:**
1. Insert test records with NULL REGION values
2. Insert test records with NULL LAST_AUDIT_DATE values
3. Execute ETL process
4. Verify NULL handling in target table
5. Check if default values are applied or NULLs are preserved

**Expected Result:**
- NULL values are handled according to business rules
- No ETL process failures due to NULL values
- Target table maintains data integrity
- NULL handling is consistent across all records

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_005
**Title:** Validate Delta table properties and format in target table
**Description:** Ensure that the target BRANCH_SUMMARY_REPORT table is created with correct Delta format and properties.
**Preconditions:**
- Databricks workspace is accessible
- Target schema workspace.default exists
- Delta Lake is properly configured

**Steps to Execute:**
1. Execute ETL process to create/update target table
2. Verify table is created in Delta format
3. Check TBLPROPERTIES settings:
   - 'delta.enableDeletionVectors' = 'true'
   - 'delta.feature.appendOnly' = 'supported'
   - 'delta.feature.deletionVectors' = 'supported'
   - 'delta.feature.invariants' = 'supported'
   - 'delta.minReaderVersion' = '3'
   - 'delta.minWriterVersion' = '7'
   - 'delta.parquet.compression.codec' = 'zstd'
4. Validate column data types match specification

**Expected Result:**
- Table is successfully created in Delta format
- All specified TBLPROPERTIES are correctly set
- Column data types match specification (BIGINT, STRING, DOUBLE)
- Table is accessible for read/write operations

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_006
**Title:** Validate transaction aggregation logic for TOTAL_TRANSACTIONS and TOTAL_AMOUNT
**Description:** Ensure that transaction data is correctly aggregated by branch to calculate totals.
**Preconditions:**
- TRANSACTION table contains test data with multiple transactions per branch
- ACCOUNT table links transactions to branches
- Aggregation logic is implemented in ETL process

**Steps to Execute:**
1. Insert test transactions for multiple branches
2. Include various transaction types and amounts
3. Execute ETL aggregation process
4. Verify TOTAL_TRANSACTIONS count per branch
5. Verify TOTAL_AMOUNT sum per branch
6. Cross-validate results with manual calculations

**Expected Result:**
- TOTAL_TRANSACTIONS accurately counts all transactions per branch
- TOTAL_AMOUNT correctly sums all transaction amounts per branch
- Aggregation handles different transaction types appropriately
- Results match manual verification calculations

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_007
**Title:** Validate handling of branches with no operational details
**Description:** Ensure that branches without corresponding BRANCH_OPERATIONAL_DETAILS records are handled appropriately.
**Preconditions:**
- BRANCH table contains branches without matching BRANCH_OPERATIONAL_DETAILS
- ETL process includes logic for handling missing operational details
- Business rules for handling missing data are defined

**Steps to Execute:**
1. Create BRANCH records without corresponding BRANCH_OPERATIONAL_DETAILS
2. Execute ETL process
3. Verify how missing REGION and LAST_AUDIT_DATE are handled
4. Check if records are included or excluded from target table
5. Validate error handling and logging

**Expected Result:**
- Missing operational details are handled according to business rules
- Either default values are assigned or records are appropriately excluded
- No ETL process failures due to missing data
- Appropriate logging of missing data scenarios

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_008
**Title:** Validate IS_ACTIVE flag filtering logic
**Description:** Ensure that only active branches (IS_ACTIVE = 'Y') are included in the ETL process.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains records with IS_ACTIVE = 'Y' and 'N'
- ETL process includes filtering logic for active branches
- Test data includes both active and inactive branches

**Steps to Execute:**
1. Insert BRANCH_OPERATIONAL_DETAILS records with IS_ACTIVE = 'Y'
2. Insert BRANCH_OPERATIONAL_DETAILS records with IS_ACTIVE = 'N'
3. Execute ETL process
4. Verify only active branches are included in target table
5. Confirm inactive branches are excluded

**Expected Result:**
- Only branches with IS_ACTIVE = 'Y' are processed
- Inactive branches (IS_ACTIVE = 'N') are excluded from target table
- Filtering logic works correctly without errors
- Target table contains only active branch data

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_009
**Title:** Validate full reload functionality of BRANCH_SUMMARY_REPORT
**Description:** Ensure that the full reload process correctly replaces all data in the target table.
**Preconditions:**
- BRANCH_SUMMARY_REPORT table exists with existing data
- Full reload process is implemented
- Source data is available for complete refresh

**Steps to Execute:**
1. Populate BRANCH_SUMMARY_REPORT with initial data
2. Modify source data (add, update, delete records)
3. Execute full reload process
4. Verify old data is completely replaced
5. Confirm new data matches current source state

**Expected Result:**
- All previous data is removed from target table
- New data completely reflects current source state
- No residual data from previous loads
- Full reload completes without errors

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_010
**Title:** Validate data quality and integrity constraints
**Description:** Ensure that data quality rules and integrity constraints are enforced during ETL process.
**Preconditions:**
- Data quality rules are defined for all columns
- Source data includes both valid and invalid test cases
- ETL process includes data validation logic

**Steps to Execute:**
1. Insert valid data meeting all quality constraints
2. Insert invalid data violating constraints (e.g., negative amounts, invalid dates)
3. Execute ETL process
4. Verify valid data is processed successfully
5. Confirm invalid data is rejected or corrected
6. Check error logging and reporting

**Expected Result:**
- Valid data passes through ETL process successfully
- Invalid data is appropriately handled (rejected or corrected)
- Data quality violations are logged with details
- Target table maintains high data quality standards

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_011
**Title:** Validate performance with large data volumes
**Description:** Ensure that ETL process performs adequately with production-scale data volumes.
**Preconditions:**
- Large volume test data is available (>1M records)
- Performance benchmarks are defined
- Monitoring tools are configured

**Steps to Execute:**
1. Load large volume test data into source tables
2. Execute ETL process with performance monitoring
3. Measure execution time and resource utilization
4. Verify data completeness and accuracy
5. Compare results against performance benchmarks

**Expected Result:**
- ETL process completes within acceptable time limits
- Resource utilization remains within acceptable ranges
- All data is processed accurately despite large volume
- No performance degradation or system issues

**Linked Jira Ticket:** PCE-1, PCE-2

### Test Case ID: TC_ETL_012
**Title:** Validate error handling and recovery mechanisms
**Description:** Ensure that ETL process handles errors gracefully and provides appropriate recovery options.
**Preconditions:**
- Error scenarios are identified (connection failures, data corruption, etc.)
- Error handling logic is implemented
- Recovery mechanisms are available

**Steps to Execute:**
1. Simulate connection failure to source database
2. Introduce data corruption in source tables
3. Cause target table access issues
4. Execute ETL process for each error scenario
5. Verify error detection and handling
6. Test recovery mechanisms

**Expected Result:**
- Errors are detected and reported appropriately
- ETL process fails gracefully without data corruption
- Recovery mechanisms restore normal operation
- Comprehensive error logging is maintained

**Linked Jira Ticket:** PCE-1, PCE-2

## Test Data Requirements

### Source Data Setup
- BRANCH table: Minimum 10 branches with varied locations
- BRANCH_OPERATIONAL_DETAILS: Corresponding operational data with some NULL values
- ACCOUNT table: Multiple accounts per branch
- TRANSACTION table: Various transaction types and amounts
- CUSTOMER table: Customer data linked to accounts

### Edge Cases to Include
- Branches with no transactions
- Branches with no operational details
- NULL values in optional fields
- Maximum and minimum data values
- Special characters in text fields
- Date boundary conditions

## Validation Criteria

### Data Accuracy
- All source records are accounted for
- Calculations are mathematically correct
- Data transformations preserve meaning
- No data loss during processing

### Data Completeness
- All required fields are populated
- Optional fields handle NULLs appropriately
- No missing records in target table
- All business rules are applied

### Data Consistency
- Referential integrity is maintained
- Data types are consistent
- Format standards are followed
- Business logic is uniformly applied

## Test Environment Requirements

### Source Environment
- Oracle database with test schema
- BRANCH_OPERATIONAL_DETAILS table populated
- Related tables (BRANCH, ACCOUNT, TRANSACTION, CUSTOMER) available

### Target Environment
- Databricks workspace configured
- Delta Lake functionality enabled
- Appropriate permissions for table creation/modification

### ETL Environment
- PySpark environment configured
- Connection to both source and target systems
- ETL code deployed and accessible

---

**Document Control:**
- Total Test Cases: 12
- Coverage Areas: Data Extraction, Transformation, Loading, Error Handling, Performance
- Last Updated: Version 1
- Review Status: Initial Version