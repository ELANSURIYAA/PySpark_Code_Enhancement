_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Functional test cases for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
## *Version*: 2
## *Changes*: Updated test cases to focus on BRANCH_OPERATIONAL_DETAILS integration requirements from PCE-1 and PCE-2
## *Reason*: Requirements changed from tile metadata integration to branch operational details integration for compliance and audit readiness
## *Updated on*: 
_____________________________________________

# Functional Test Cases for BRANCH_OPERATIONAL_DETAILS Integration

## Overview
This document contains comprehensive functional test cases for PCE-1 and PCE-2: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table BRANCH_OPERATIONAL_DETAILS. The test cases validate the integration of branch operational metadata into the existing ETL pipeline and ensure proper enrichment of the BRANCH_SUMMARY_REPORT table with REGION and LAST_AUDIT_DATE columns.

---

### Test Case ID: TC_PCE1_01
**Title:** Validate BRANCH_OPERATIONAL_DETAILS table creation and structure
**Description:** Ensure that the BRANCH_OPERATIONAL_DETAILS Oracle source table is created successfully with correct schema and constraints.
**Preconditions:**
- Oracle database connection is available
- User has CREATE TABLE permissions
- Database schema exists

**Steps to Execute:**
1. Execute the BRANCH_OPERATIONAL_DETAILS DDL script in Oracle
2. Verify table exists in the source schema
3. Check table columns and data types (BRANCH_ID INT, REGION VARCHAR2(50), MANAGER_NAME VARCHAR2(100), LAST_AUDIT_DATE DATE, IS_ACTIVE CHAR(1))
4. Verify primary key constraint on BRANCH_ID
5. Confirm table structure matches specification

**Expected Result:**
- Table BRANCH_OPERATIONAL_DETAILS is created successfully in Oracle
- Contains all required columns with correct data types
- Primary key constraint is applied on BRANCH_ID
- Table structure matches the DDL specification

**Linked Jira Ticket:** PCE-1

---

### Test Case ID: TC_PCE1_02
**Title:** Validate BRANCH_OPERATIONAL_DETAILS sample data insertion
**Description:** Ensure that operational metadata can be inserted into the BRANCH_OPERATIONAL_DETAILS table successfully.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table exists
- Sample test data is prepared with various scenarios

**Steps to Execute:**
1. Insert sample records with different regions (North, South, East, West)
2. Insert records with different IS_ACTIVE values ('Y', 'N')
3. Insert records with various LAST_AUDIT_DATE values
4. Verify data insertion using SELECT statements
5. Test primary key constraint with duplicate BRANCH_ID

**Expected Result:**
- All valid sample records are inserted successfully
- Data types are preserved correctly
- Primary key constraint prevents duplicate BRANCH_ID entries
- Records can be queried successfully
- IS_ACTIVE values are properly stored as 'Y' or 'N'

**Linked Jira Ticket:** PCE-1

---

### Test Case ID: TC_PCE2_01
**Title:** Validate BRANCH_SUMMARY_REPORT schema update with new columns
**Description:** Ensure that the BRANCH_SUMMARY_REPORT Delta table schema is updated to include REGION and LAST_AUDIT_DATE columns.
**Preconditions:**
- Databricks workspace is accessible
- BRANCH_SUMMARY_REPORT table exists
- Schema evolution is enabled for Delta tables

**Steps to Execute:**
1. Check existing BRANCH_SUMMARY_REPORT schema before update
2. Execute schema update to add REGION (STRING) and LAST_AUDIT_DATE (STRING) columns
3. Verify new columns are added to the table schema
4. Check Delta table properties remain intact
5. Validate table metadata and column definitions

**Expected Result:**
- BRANCH_SUMMARY_REPORT schema includes new columns: REGION (STRING), LAST_AUDIT_DATE (STRING)
- Existing columns remain unchanged
- Delta table properties are preserved
- Schema evolution is handled gracefully
- Table remains queryable

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_02
**Title:** Validate PySpark ETL job reads BRANCH_OPERATIONAL_DETAILS successfully
**Description:** Ensure that the PySpark ETL pipeline can read from BRANCH_OPERATIONAL_DETAILS Oracle table without errors.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table exists with sample data
- Oracle JDBC connection is configured
- PySpark ETL job is updated to include new source

**Steps to Execute:**
1. Configure Oracle JDBC connection in PySpark
2. Execute ETL job to read BRANCH_OPERATIONAL_DETAILS
3. Verify data is loaded into Spark DataFrame
4. Check for any connection or schema errors
5. Validate data types mapping from Oracle to Spark

**Expected Result:**
- ETL job connects to Oracle successfully
- BRANCH_OPERATIONAL_DETAILS data is read without errors
- DataFrame contains expected columns and data types
- No connection timeouts or authentication errors
- Data mapping from Oracle to Spark is correct

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_03
**Title:** Validate join logic between BRANCH and BRANCH_OPERATIONAL_DETAILS
**Description:** Ensure that the PySpark job correctly joins BRANCH table with BRANCH_OPERATIONAL_DETAILS using BRANCH_ID.
**Preconditions:**
- BRANCH table contains test data
- BRANCH_OPERATIONAL_DETAILS contains corresponding test data
- Join logic is implemented in PySpark ETL

**Steps to Execute:**
1. Create test data with matching BRANCH_ID values in both tables
2. Create test data with BRANCH_ID values that exist only in BRANCH table
3. Execute PySpark join operation
4. Verify join results and record counts
5. Check that all BRANCH records are preserved (left join behavior)

**Expected Result:**
- Join operation executes successfully using BRANCH_ID
- All BRANCH records are preserved in the result
- Matching records get populated with operational details
- Non-matching BRANCH records have null values for operational columns
- Join performance is acceptable

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_04
**Title:** Validate conditional population based on IS_ACTIVE = 'Y'
**Description:** Ensure that REGION and LAST_AUDIT_DATE are populated only when IS_ACTIVE = 'Y' in BRANCH_OPERATIONAL_DETAILS.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains records with IS_ACTIVE = 'Y' and 'N'
- Conditional logic is implemented in PySpark ETL

**Steps to Execute:**
1. Create test data with IS_ACTIVE = 'Y' for some branches
2. Create test data with IS_ACTIVE = 'N' for other branches
3. Execute ETL pipeline with conditional logic
4. Verify REGION and LAST_AUDIT_DATE population rules
5. Check that inactive branches don't populate these fields

**Expected Result:**
- Branches with IS_ACTIVE = 'Y' get REGION and LAST_AUDIT_DATE populated
- Branches with IS_ACTIVE = 'N' have null/empty values for REGION and LAST_AUDIT_DATE
- Conditional logic works correctly
- No data leakage from inactive records

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_05
**Title:** Validate backward compatibility with existing BRANCH_SUMMARY_REPORT records
**Description:** Ensure that existing records in BRANCH_SUMMARY_REPORT are not affected and maintain backward compatibility.
**Preconditions:**
- BRANCH_SUMMARY_REPORT contains existing historical data
- ETL pipeline includes backward compatibility logic

**Steps to Execute:**
1. Backup existing BRANCH_SUMMARY_REPORT data
2. Run updated ETL pipeline
3. Compare existing records before and after update
4. Verify existing columns remain unchanged
5. Check that new columns are added with appropriate default values

**Expected Result:**
- Existing records maintain their original values
- New columns (REGION, LAST_AUDIT_DATE) are added with null/default values for historical records
- No data corruption or loss occurs
- Backward compatibility is maintained
- Historical data integrity is preserved

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_06
**Title:** Validate complete ETL pipeline execution with new integration
**Description:** Ensure that the complete ETL pipeline executes successfully with BRANCH_OPERATIONAL_DETAILS integration.
**Preconditions:**
- All source tables (CUSTOMER, BRANCH, ACCOUNT, TRANSACTION, BRANCH_OPERATIONAL_DETAILS) contain test data
- Updated PySpark ETL job is deployed

**Steps to Execute:**
1. Execute complete ETL pipeline from source to target
2. Verify all transformations and aggregations work correctly
3. Check BRANCH_SUMMARY_REPORT is populated with enhanced data
4. Validate transaction aggregations by branch
5. Confirm REGION and LAST_AUDIT_DATE are populated correctly

**Expected Result:**
- Complete ETL pipeline executes without errors
- BRANCH_SUMMARY_REPORT contains all expected records
- Transaction aggregations (TOTAL_TRANSACTIONS, TOTAL_AMOUNT) are correct
- New columns (REGION, LAST_AUDIT_DATE) are populated based on business rules
- Data quality checks pass

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_07
**Title:** Validate data validation and reconciliation routines
**Description:** Ensure that data validation and reconciliation routines work correctly with the enhanced data model.
**Preconditions:**
- ETL pipeline includes data validation checks
- Reconciliation routines are updated for new columns

**Steps to Execute:**
1. Run ETL pipeline with data validation enabled
2. Execute reconciliation checks between source and target
3. Verify record counts match expectations
4. Check for data quality issues (nulls, duplicates, invalid values)
5. Validate referential integrity between tables

**Expected Result:**
- Data validation routines pass successfully
- Record counts reconcile between source and target
- No data quality issues are detected
- Referential integrity is maintained
- Reconciliation reports show expected results

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_08
**Title:** Validate error handling for BRANCH_OPERATIONAL_DETAILS unavailability
**Description:** Ensure proper error handling when BRANCH_OPERATIONAL_DETAILS table is unavailable or inaccessible.
**Preconditions:**
- ETL pipeline includes error handling logic
- Test environment allows simulation of table unavailability

**Steps to Execute:**
1. Simulate BRANCH_OPERATIONAL_DETAILS table unavailability
2. Run ETL pipeline
3. Verify error handling behavior
4. Check that pipeline continues with existing BRANCH data
5. Validate appropriate error logging and alerting

**Expected Result:**
- Pipeline handles table unavailability gracefully
- Appropriate error messages are logged
- ETL continues processing with existing BRANCH data
- New columns remain null for affected run
- Alerts are triggered for operational team

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_09
**Title:** Validate performance impact of additional join operation
**Description:** Ensure that adding BRANCH_OPERATIONAL_DETAILS join doesn't significantly impact ETL performance.
**Preconditions:**
- Baseline ETL performance metrics are available
- Large volume test data is prepared
- Performance monitoring is enabled

**Steps to Execute:**
1. Measure ETL execution time before integration
2. Run ETL pipeline with BRANCH_OPERATIONAL_DETAILS join
3. Measure execution time after integration
4. Compare memory usage and resource consumption
5. Analyze join operation performance

**Expected Result:**
- Performance impact is within acceptable limits (<20% increase)
- Join operation is optimized and efficient
- Memory usage remains stable
- Pipeline completes within SLA timeframes
- No resource bottlenecks are introduced

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_10
**Title:** Validate Delta table properties and optimization
**Description:** Ensure that BRANCH_SUMMARY_REPORT Delta table properties are maintained and optimized after schema changes.
**Preconditions:**
- BRANCH_SUMMARY_REPORT is a Delta table with specific properties
- Schema changes are applied

**Steps to Execute:**
1. Check Delta table properties before schema update
2. Apply schema changes for new columns
3. Verify Delta table properties are preserved
4. Run OPTIMIZE and VACUUM operations
5. Check table statistics and metadata

**Expected Result:**
- Delta table properties are preserved (enableDeletionVectors, compression, etc.)
- Schema evolution is handled correctly
- OPTIMIZE operation works with new schema
- Table statistics are updated appropriately
- Metadata reflects current schema version

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_11
**Title:** Validate compliance and audit reporting capabilities
**Description:** Ensure that the enhanced BRANCH_SUMMARY_REPORT supports compliance and audit reporting requirements.
**Preconditions:**
- BRANCH_SUMMARY_REPORT contains enhanced data with REGION and LAST_AUDIT_DATE
- Compliance reporting requirements are defined

**Steps to Execute:**
1. Query BRANCH_SUMMARY_REPORT for compliance reporting scenarios
2. Generate region-wise branch performance reports
3. Create audit trail reports using LAST_AUDIT_DATE
4. Verify data supports regulatory reporting requirements
5. Test filtering and grouping by new columns

**Expected Result:**
- Region-wise reporting is supported
- Audit trail information is available and accurate
- Compliance reports can be generated successfully
- Data supports regulatory requirements
- Business users can access enhanced reporting capabilities

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE2_12
**Title:** Validate full reload requirement for BRANCH_SUMMARY_REPORT
**Description:** Ensure that the full reload of BRANCH_SUMMARY_REPORT works correctly with the new schema and data.
**Preconditions:**
- Full reload logic is implemented
- Historical data exists in source systems

**Steps to Execute:**
1. Backup existing BRANCH_SUMMARY_REPORT data
2. Execute full reload with enhanced ETL logic
3. Verify all historical data is reprocessed with new columns
4. Check data completeness and accuracy
5. Compare record counts before and after reload

**Expected Result:**
- Full reload executes successfully
- All historical data is enhanced with available operational details
- Record counts match expectations
- Data quality is maintained throughout reload
- New columns are populated for all applicable records

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_13
**Title:** Validate unit test coverage for enhanced ETL logic
**Description:** Ensure that all new ETL functionality is covered by comprehensive unit tests.
**Preconditions:**
- Unit test framework is set up for PySpark jobs
- Test cases are implemented for new features

**Steps to Execute:**
1. Run unit tests for BRANCH_OPERATIONAL_DETAILS data reading
2. Execute tests for join logic with conditional population
3. Verify tests for error handling scenarios
4. Check test coverage metrics for new code
5. Validate edge case testing

**Expected Result:**
- All unit tests pass successfully
- Test coverage meets minimum requirements (>80%)
- Edge cases and error scenarios are covered
- Regression tests validate existing functionality
- Code quality standards are maintained

**Linked Jira Ticket:** PCE-2

---

### Test Case ID: TC_PCE2_14
**Title:** Validate data lineage and metadata management
**Description:** Ensure that data lineage and metadata are properly maintained for the enhanced data model.
**Preconditions:**
- Data lineage tools are configured
- Metadata management system is available

**Steps to Execute:**
1. Verify data lineage captures new source table dependency
2. Check metadata reflects schema changes in target table
3. Validate column-level lineage for REGION and LAST_AUDIT_DATE
4. Ensure data dictionary is updated
5. Verify impact analysis capabilities

**Expected Result:**
- Data lineage includes BRANCH_OPERATIONAL_DETAILS as source
- Column-level lineage is accurate for new columns
- Metadata reflects current schema and transformations
- Data dictionary is updated with new column definitions
- Impact analysis shows correct dependencies

**Linked Jira Ticket:** PCE-1, PCE-2

---

### Test Case ID: TC_PCE2_15
**Title:** Validate end-to-end integration testing
**Description:** Ensure that the complete solution works end-to-end from Oracle source to Databricks target with all business requirements met.
**Preconditions:**
- Complete solution is deployed in test environment
- Representative test data is available

**Steps to Execute:**
1. Load comprehensive test data in all source tables
2. Execute complete ETL pipeline
3. Verify business requirements are met
4. Test various scenarios (active/inactive branches, missing operational data)
5. Validate reporting and analytics use cases

**Expected Result:**
- End-to-end pipeline executes successfully
- All business requirements are satisfied
- Compliance and audit readiness is achieved
- Enhanced reporting capabilities are functional
- Solution is ready for production deployment

**Linked Jira Ticket:** PCE-1, PCE-2

---

## Test Execution Summary

### Test Categories:
1. **Source Table Tests** (TC_PCE1_01, TC_PCE1_02)
2. **Schema Evolution Tests** (TC_PCE2_01, TC_PCE2_10)
3. **ETL Integration Tests** (TC_PCE2_02, TC_PCE2_03, TC_PCE2_06)
4. **Business Logic Tests** (TC_PCE2_04, TC_PCE2_05, TC_PCE2_11)
5. **Data Quality Tests** (TC_PCE2_07, TC_PCE2_12)
6. **Performance Tests** (TC_PCE2_09)
7. **Error Handling Tests** (TC_PCE2_08)
8. **System Integration Tests** (TC_PCE2_14, TC_PCE2_15)
9. **Unit Testing Validation** (TC_PCE2_13)

### Success Criteria:
- All functional test cases pass
- BRANCH_SUMMARY_REPORT schema includes REGION and LAST_AUDIT_DATE columns
- ETL successfully integrates BRANCH_OPERATIONAL_DETAILS data
- Backward compatibility with older records is maintained
- Performance impact is within acceptable limits
- Compliance and audit readiness requirements are met

### Dependencies:
- Oracle JDBC connectivity configuration
- Databricks workspace and cluster availability
- Data validation and reconciliation framework
- Monitoring and alerting system updates
- Business user training on enhanced reporting capabilities

### Impact Areas Validated:
- PySpark ETL logic enhancement
- Delta table structure evolution
- Data validation and reconciliation routines
- Compliance and audit reporting capabilities
- Performance and scalability considerations