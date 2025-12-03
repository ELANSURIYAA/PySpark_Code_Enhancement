_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Functional test cases for adding SOURCE_TILE_METADATA table and extending target summary with tile category
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Functional Test Cases for SOURCE_TILE_METADATA Integration

## Overview
This document contains comprehensive functional test cases for JIRA story PCE-3, which involves adding a new SOURCE_TILE_METADATA table and extending the target summary table with tile category functionality.

---

### Test Case ID: TC_PCE3_01
**Title:** Validate SOURCE_TILE_METADATA table creation
**Description:** Ensure that the SOURCE_TILE_METADATA table is created successfully with correct schema and structure.
**Preconditions:** 
- Database analytics_db exists
- User has CREATE TABLE permissions

**Steps to Execute:**
1. Execute the SOURCE_TILE_METADATA DDL script
2. Verify table exists in analytics_db schema
3. Check table structure and column definitions
4. Validate table comments and metadata

**Expected Result:**
- Table analytics_db.SOURCE_TILE_METADATA is created successfully
- All columns (tile_id, tile_name, tile_category, is_active, updated_ts) exist with correct data types
- Table uses DELTA format
- Table comment matches specification

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_02
**Title:** Validate SOURCE_TILE_METADATA data insertion
**Description:** Ensure that sample metadata can be inserted into the SOURCE_TILE_METADATA table successfully.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
- Sample tile metadata is available

**Steps to Execute:**
1. Insert sample tile metadata records
2. Verify data insertion without errors
3. Check data integrity and constraints
4. Validate timestamp fields are populated correctly

**Expected Result:**
- Sample metadata records are inserted successfully
- All mandatory fields are populated
- No constraint violations occur
- updated_ts reflects current timestamp

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_03
**Title:** Validate ETL pipeline reads SOURCE_TILE_METADATA successfully
**Description:** Ensure that the ETL pipeline can read from SOURCE_TILE_METADATA table without errors.
**Preconditions:**
- SOURCE_TILE_METADATA table exists with sample data
- ETL pipeline is configured to read metadata table

**Steps to Execute:**
1. Execute ETL pipeline with metadata table integration
2. Verify successful read operation from SOURCE_TILE_METADATA
3. Check for any schema drift errors
4. Validate data loading performance

**Expected Result:**
- ETL pipeline reads SOURCE_TILE_METADATA successfully
- No schema drift errors occur
- Data loading completes within acceptable time limits
- All metadata records are accessible

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_04
**Title:** Validate tile_category column addition to target table
**Description:** Ensure that tile_category column is successfully added to TARGET_HOME_TILE_DAILY_SUMMARY table.
**Preconditions:**
- TARGET_HOME_TILE_DAILY_SUMMARY table exists
- Schema modification permissions available

**Steps to Execute:**
1. Execute ALTER TABLE statement to add tile_category column
2. Verify column is added with correct data type (STRING)
3. Check column comment is set correctly
4. Validate existing data integrity is maintained

**Expected Result:**
- tile_category column is added successfully
- Column has STRING data type
- Column comment: 'Functional category of the tile'
- Existing records remain intact

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_05
**Title:** Validate LEFT JOIN between tile events and metadata
**Description:** Ensure that the ETL pipeline correctly performs LEFT JOIN between tile events and SOURCE_TILE_METADATA.
**Preconditions:**
- SOURCE_HOME_TILE_EVENTS table has sample data
- SOURCE_TILE_METADATA table has corresponding metadata
- ETL pipeline includes LEFT JOIN logic

**Steps to Execute:**
1. Execute ETL pipeline with LEFT JOIN implementation
2. Verify join is performed on tile_id field
3. Check that all tile events are preserved (LEFT JOIN behavior)
4. Validate tile_category is populated where metadata exists

**Expected Result:**
- LEFT JOIN executes successfully
- All tile events from source are preserved
- tile_category is populated for tiles with metadata
- No data loss occurs during join operation

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_06
**Title:** Validate default tile_category for unmapped tiles
**Description:** Ensure that tiles without metadata mapping get default category "UNKNOWN".
**Preconditions:**
- SOURCE_HOME_TILE_EVENTS contains tiles not in SOURCE_TILE_METADATA
- ETL pipeline includes default value logic

**Steps to Execute:**
1. Create tile events for tiles not in metadata table
2. Execute ETL pipeline
3. Check target table for unmapped tiles
4. Verify tile_category is set to "UNKNOWN"

**Expected Result:**
- Unmapped tiles are processed successfully
- tile_category is set to "UNKNOWN" for tiles without metadata
- No NULL values in tile_category column
- Backward compatibility is maintained

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_07
**Title:** Validate tile_category in final reporting output
**Description:** Ensure that tile_category appears correctly in the final TARGET_HOME_TILE_DAILY_SUMMARY table.
**Preconditions:**
- Complete ETL pipeline execution
- Sample data with various tile categories

**Steps to Execute:**
1. Execute full ETL pipeline end-to-end
2. Query TARGET_HOME_TILE_DAILY_SUMMARY table
3. Verify tile_category values are populated correctly
4. Check category distribution matches source metadata

**Expected Result:**
- tile_category appears in final output table
- Category values match SOURCE_TILE_METADATA definitions
- Aggregated metrics are correctly associated with categories
- Data quality checks pass

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_08
**Title:** Validate SELECT statement includes tile_category
**Description:** Ensure that all SELECT statements in ETL pipeline include tile_category column.
**Preconditions:**
- ETL pipeline code is updated
- tile_category column exists in target schema

**Steps to Execute:**
1. Review ETL SELECT statements
2. Execute pipeline with tile_category in SELECT clause
3. Verify column appears in intermediate results
4. Check final output includes tile_category

**Expected Result:**
- All relevant SELECT statements include tile_category
- Column is properly selected and propagated
- No column missing errors occur
- Output schema validation passes

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_09
**Title:** Validate overwritePartition logic with tile_category
**Description:** Ensure that partition overwrite operations correctly handle the new tile_category column.
**Preconditions:**
- Target table is partitioned by date
- tile_category column is added to schema
- Partition overwrite logic is updated

**Steps to Execute:**
1. Execute ETL pipeline with partition overwrite mode
2. Verify existing partitions are overwritten correctly
3. Check tile_category is included in overwritten data
4. Validate partition integrity after overwrite

**Expected Result:**
- Partition overwrite completes successfully
- tile_category data is preserved during overwrite
- No schema mismatch errors occur
- Partition structure remains intact

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_10
**Title:** Validate schema validation in unit tests
**Description:** Ensure that unit tests properly validate schema including tile_category column.
**Preconditions:**
- Unit test framework is set up
- Schema validation tests exist
- tile_category is added to expected schema

**Steps to Execute:**
1. Execute unit tests with updated schema validation
2. Verify tests check for tile_category column presence
3. Test schema validation with and without tile_category
4. Check test coverage for new column

**Expected Result:**
- Unit tests validate tile_category column exists
- Schema validation tests pass
- Test coverage includes new functionality
- No regression in existing tests

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_11
**Title:** Validate record count consistency
**Description:** Ensure that record counts remain consistent after adding tile_category functionality.
**Preconditions:**
- Baseline record counts are established
- ETL pipeline with tile_category is ready

**Steps to Execute:**
1. Record baseline counts from original pipeline
2. Execute updated pipeline with tile_category
3. Compare record counts before and after
4. Verify no data loss or duplication

**Expected Result:**
- Record counts match baseline expectations
- No data loss occurs due to new functionality
- No duplicate records are created
- Backward compatibility is maintained

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_12
**Title:** Validate category-level performance metrics
**Description:** Ensure that tile categories enable proper performance tracking and reporting.
**Preconditions:**
- Multiple tile categories exist in metadata
- Sample performance data is available
- Reporting queries are updated

**Steps to Execute:**
1. Execute ETL pipeline with diverse tile categories
2. Query performance metrics by category
3. Verify category-level aggregations work correctly
4. Test CTR calculations by category

**Expected Result:**
- Performance metrics can be grouped by tile_category
- Category-level CTR calculations are accurate
- Reporting queries execute successfully
- Business requirements for category tracking are met

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_13
**Title:** Validate interstitial events integration with tile_category
**Description:** Ensure that interstitial events are properly enriched with tile category information.
**Preconditions:**
- SOURCE_INTERSTITIAL_EVENTS table has sample data
- Interstitial events are linked to tiles with categories

**Steps to Execute:**
1. Execute ETL pipeline including interstitial events processing
2. Verify interstitial metrics are enriched with tile_category
3. Check category-level interstitial performance metrics
4. Validate primary/secondary button click tracking by category

**Expected Result:**
- Interstitial events are enriched with tile_category
- Category-level interstitial metrics are available
- Button click tracking works correctly by category
- No data integrity issues in interstitial processing

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_14
**Title:** Validate error handling for missing metadata
**Description:** Ensure proper error handling when tile metadata is missing or corrupted.
**Preconditions:**
- Some tiles exist without corresponding metadata
- Error handling logic is implemented

**Steps to Execute:**
1. Create scenarios with missing tile metadata
2. Execute ETL pipeline
3. Verify graceful handling of missing metadata
4. Check error logs and monitoring

**Expected Result:**
- Pipeline handles missing metadata gracefully
- Default "UNKNOWN" category is assigned appropriately
- Appropriate warnings are logged
- Pipeline execution continues without failure

**Linked Jira Ticket:** PCE-3

---

### Test Case ID: TC_PCE3_15
**Title:** Validate end-to-end data lineage with tile_category
**Description:** Ensure complete data lineage from source events to final reporting output includes tile_category.
**Preconditions:**
- Full ETL pipeline is configured
- Data lineage tracking is enabled
- Sample data exists across all source tables

**Steps to Execute:**
1. Execute complete ETL pipeline end-to-end
2. Trace data flow from SOURCE_HOME_TILE_EVENTS to TARGET_HOME_TILE_DAILY_SUMMARY
3. Verify tile_category is properly propagated throughout
4. Check data lineage documentation is updated

**Expected Result:**
- Complete data lineage includes tile_category
- Data flows correctly from source to target
- All transformations preserve tile_category information
- Lineage documentation reflects new column

**Linked Jira Ticket:** PCE-3