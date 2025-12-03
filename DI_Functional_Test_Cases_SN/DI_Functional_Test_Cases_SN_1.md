_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Functional test cases for adding SOURCE_TILE_METADATA table and extending target summary with tile category
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Functional Test Cases for SOURCE_TILE_METADATA Integration

## Overview
This document contains comprehensive functional test cases for PCE-3: Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category. The test cases validate the integration of tile metadata into the existing ETL pipeline and ensure proper enrichment of reporting outputs.

---

### Test Case ID: TC_PCE3_01
**Title:** Validate SOURCE_TILE_METADATA table creation and structure
**Description:** Ensure that the SOURCE_TILE_METADATA table is created successfully with correct schema and properties.
**Preconditions:**
- Database analytics_db exists
- User has CREATE TABLE permissions

**Steps to Execute:**
1. Execute the SOURCE_TILE_METADATA DDL script
2. Verify table exists in analytics_db schema
3. Check table columns and data types
4. Verify table comment and properties
5. Confirm Delta format is used

**Expected Result:**
- Table analytics_db.SOURCE_TILE_METADATA is created successfully
- Contains columns: tile_id (STRING), tile_name (STRING), tile_category (STRING), is_active (BOOLEAN), updated_ts (TIMESTAMP)
- Table uses Delta format
- Table comment matches specification

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_02
**Title:** Validate SOURCE_TILE_METADATA data insertion
**Description:** Ensure that sample metadata can be inserted into the SOURCE_TILE_METADATA table successfully.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
- Sample test data is prepared

**Steps to Execute:**
1. Insert sample tile metadata records with various categories
2. Insert records with different is_active values (true/false)
3. Verify data insertion using SELECT statements
4. Check data types and constraints

**Expected Result:**
- All sample records are inserted successfully
- Data types are preserved correctly
- No constraint violations occur
- Records can be queried successfully

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_03
**Title:** Validate ETL pipeline reads SOURCE_TILE_METADATA successfully
**Description:** Ensure that the ETL pipeline can read from SOURCE_TILE_METADATA table without errors.
**Preconditions:**
- SOURCE_TILE_METADATA table exists with sample data
- ETL pipeline is updated to include metadata table

**Steps to Execute:**
1. Execute ETL pipeline with SOURCE_TILE_METADATA integration
2. Verify metadata table is read successfully
3. Check for any schema drift errors
4. Validate data loading logs

**Expected Result:**
- ETL pipeline reads SOURCE_TILE_METADATA without errors
- No schema drift warnings or errors
- Metadata records are loaded into pipeline dataframe
- Pipeline execution completes successfully

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_04
**Title:** Validate tile_category enrichment in target table
**Description:** Ensure that tile_category is correctly added to TARGET_HOME_TILE_DAILY_SUMMARY table.
**Preconditions:**
- SOURCE_TILE_METADATA contains test data
- SOURCE_HOME_TILE_EVENTS contains test data
- ETL pipeline includes metadata join logic

**Steps to Execute:**
1. Run ETL pipeline with tile events and metadata
2. Check TARGET_HOME_TILE_DAILY_SUMMARY schema includes tile_category
3. Verify tile_category values are populated correctly
4. Validate join logic works for existing tile_ids

**Expected Result:**
- TARGET_HOME_TILE_DAILY_SUMMARY includes tile_category column
- tile_category values match SOURCE_TILE_METADATA for corresponding tile_ids
- No data loss occurs during enrichment
- Schema validation passes

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_05
**Title:** Validate backward compatibility with unknown tile categories
**Description:** Ensure that tiles without metadata entries default to "UNKNOWN" category.
**Preconditions:**
- SOURCE_TILE_METADATA contains partial tile mappings
- SOURCE_HOME_TILE_EVENTS contains tiles not in metadata table

**Steps to Execute:**
1. Create test data with tile_ids not present in SOURCE_TILE_METADATA
2. Run ETL pipeline
3. Check TARGET_HOME_TILE_DAILY_SUMMARY for unmapped tiles
4. Verify tile_category defaults to "UNKNOWN"

**Expected Result:**
- Tiles without metadata mapping get tile_category = "UNKNOWN"
- No null values in tile_category column
- Record counts remain consistent
- Pipeline processes all tiles successfully

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_06
**Title:** Validate left join logic preserves all tile records
**Description:** Ensure that the left join with metadata table preserves all tile records from source events.
**Preconditions:**
- SOURCE_HOME_TILE_EVENTS contains comprehensive test data
- SOURCE_TILE_METADATA contains partial mappings

**Steps to Execute:**
1. Count distinct tile_ids in SOURCE_HOME_TILE_EVENTS
2. Run ETL pipeline with left join logic
3. Count distinct tile_ids in TARGET_HOME_TILE_DAILY_SUMMARY
4. Compare record counts before and after enrichment

**Expected Result:**
- All tile_ids from source events are preserved
- Record count matches between source and target
- No tiles are dropped due to missing metadata
- Left join logic works correctly

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_07
**Title:** Validate tile category grouping functionality
**Description:** Ensure that tiles can be correctly grouped and aggregated by tile_category.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY contains enriched data
- Multiple tile categories exist in test data

**Steps to Execute:**
1. Query TARGET_HOME_TILE_DAILY_SUMMARY grouped by tile_category
2. Verify aggregation functions work correctly
3. Test category-level CTR calculations
4. Validate business grouping scenarios

**Expected Result:**
- Tiles can be grouped by tile_category successfully
- Aggregation functions (SUM, COUNT, AVG) work correctly
- Category-level metrics are calculated accurately
- Business reporting requirements are met

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_08
**Title:** Validate interstitial events enrichment with tile category
**Description:** Ensure that interstitial events are also enriched with tile category information.
**Preconditions:**
- SOURCE_INTERSTITIAL_EVENTS contains test data
- SOURCE_TILE_METADATA contains corresponding tile mappings

**Steps to Execute:**
1. Run ETL pipeline including interstitial events processing
2. Verify interstitial metrics include tile_category
3. Check unique_interstitial_views aggregation by category
4. Validate primary and secondary click metrics by category

**Expected Result:**
- Interstitial events are enriched with tile_category
- Category-level interstitial metrics are calculated correctly
- Primary and secondary button clicks are grouped by category
- All interstitial KPIs support category-level analysis

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_09
**Title:** Validate schema evolution and partition compatibility
**Description:** Ensure that adding tile_category column doesn't break existing partitions or schema evolution.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY has existing partitioned data
- Schema evolution is enabled for Delta tables

**Steps to Execute:**
1. Check existing partition structure before schema change
2. Run ETL pipeline with new tile_category column
3. Verify partition compatibility is maintained
4. Test overwritePartition logic with new schema

**Expected Result:**
- Existing partitions remain accessible
- New partitions include tile_category column
- overwritePartition logic works correctly
- No schema drift errors occur

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_10
**Title:** Validate data quality and consistency checks
**Description:** Ensure that data quality is maintained after adding tile category enrichment.
**Preconditions:**
- Complete ETL pipeline with tile category integration
- Data quality validation rules are defined

**Steps to Execute:**
1. Run data quality checks on enriched target table
2. Verify no duplicate records are created
3. Check for null values in critical columns
4. Validate referential integrity between tables

**Expected Result:**
- No duplicate records in target table
- Critical columns maintain data quality standards
- Referential integrity is preserved
- Data consistency checks pass

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_11
**Title:** Validate performance impact of metadata join
**Description:** Ensure that adding metadata join doesn't significantly impact ETL performance.
**Preconditions:**
- Baseline ETL performance metrics are available
- Large volume test data is prepared

**Steps to Execute:**
1. Measure ETL execution time before metadata integration
2. Run ETL pipeline with metadata join
3. Measure execution time after integration
4. Compare performance metrics

**Expected Result:**
- Performance impact is within acceptable limits
- Join operation is optimized
- Memory usage remains stable
- Pipeline completes within SLA timeframes

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_12
**Title:** Validate error handling for metadata table unavailability
**Description:** Ensure proper error handling when SOURCE_TILE_METADATA table is unavailable.
**Preconditions:**
- ETL pipeline includes error handling logic
- Test environment allows table access simulation

**Steps to Execute:**
1. Simulate SOURCE_TILE_METADATA table unavailability
2. Run ETL pipeline
3. Verify error handling behavior
4. Check fallback mechanisms

**Expected Result:**
- Pipeline handles metadata table unavailability gracefully
- Appropriate error messages are logged
- Fallback to "UNKNOWN" category works
- Pipeline doesn't fail completely

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_13
**Title:** Validate unit test coverage for new functionality
**Description:** Ensure that all new functionality is covered by comprehensive unit tests.
**Preconditions:**
- Unit test framework is set up
- Test cases are implemented for new features

**Steps to Execute:**
1. Run unit tests for metadata table creation
2. Execute tests for tile category enrichment logic
3. Verify tests for default value handling
4. Check test coverage metrics

**Expected Result:**
- All unit tests pass successfully
- Test coverage meets minimum requirements (>80%)
- Edge cases are covered by tests
- Regression tests validate existing functionality

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_14
**Title:** Validate end-to-end reporting pipeline with tile categories
**Description:** Ensure that the complete pipeline from source to reporting works with tile categories.
**Preconditions:**
- Complete ETL pipeline is deployed
- Reporting tools can access target tables

**Steps to Execute:**
1. Run complete ETL pipeline from source to target
2. Verify tile_category appears in reporting outputs
3. Test category-level dashboard functionality
4. Validate business reporting scenarios

**Expected Result:**
- Complete pipeline executes successfully
- tile_category is available for reporting
- Business users can filter and group by category
- Dashboard drilldowns work correctly

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_15
**Title:** Validate inactive tile handling
**Description:** Ensure that inactive tiles (is_active = false) are handled correctly in reporting.
**Preconditions:**
- SOURCE_TILE_METADATA contains tiles with is_active = false
- Business rules for inactive tiles are defined

**Steps to Execute:**
1. Create test data with inactive tiles
2. Run ETL pipeline
3. Verify inactive tiles are processed correctly
4. Check if inactive tiles should be excluded from reporting

**Expected Result:**
- Inactive tiles are identified correctly
- Business rules for inactive tiles are applied
- Reporting logic handles active/inactive status appropriately
- Data lineage includes activity status

**Linked Jira Ticket:** PCE-3

---

## Test Execution Summary

### Test Categories:
1. **Schema and DDL Tests** (TC_PCE3_01, TC_PCE3_02)
2. **ETL Integration Tests** (TC_PCE3_03, TC_PCE3_04, TC_PCE3_08)
3. **Data Quality Tests** (TC_PCE3_05, TC_PCE3_06, TC_PCE3_10)
4. **Business Logic Tests** (TC_PCE3_07, TC_PCE3_15)
5. **Performance Tests** (TC_PCE3_11)
6. **Error Handling Tests** (TC_PCE3_12)
7. **System Integration Tests** (TC_PCE3_09, TC_PCE3_14)
8. **Unit Testing Validation** (TC_PCE3_13)

### Success Criteria:
- All functional test cases pass
- No regression in existing functionality
- Performance impact within acceptable limits
- Data quality standards maintained
- Business requirements fully satisfied

### Dependencies:
- BI team dashboard updates (out of scope)
- Product team metadata maintenance (ongoing)
- Historical data backfill (if requested by business)