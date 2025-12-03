_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Delta model changes for Home Tile Reporting Enhancement - Add Tile Category Metadata
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Data Model Evolution Agent (DMEA) - Delta Model Changes SN

## Executive Summary

This document outlines the delta changes required to evolve the current Home Tile Reporting data model to support the new technical specifications for adding SOURCE_TILE_METADATA table and extending target summary with tile_category functionality.

## 1. Model Ingestion Analysis

### Current Data Model Structure

#### Source Tables (Existing)
- **analytics_db.SOURCE_HOME_TILE_EVENTS**
  - Primary Key: event_id
  - Partition: date(event_ts)
  - Key Fields: user_id, tile_id, event_type, event_ts
  - Format: Delta Lake

- **analytics_db.SOURCE_INTERSTITIAL_EVENTS**
  - Primary Key: event_id
  - Partition: date(event_ts)
  - Key Fields: user_id, tile_id, event_ts, interstitial_view_flag
  - Format: Delta Lake

#### Target Tables (Existing)
- **reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY**
  - Primary Key: date + tile_id
  - Partition: date
  - Aggregation Level: Daily tile-level metrics
  - Format: Delta Lake

- **reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS**
  - Primary Key: date
  - Partition: date
  - Aggregation Level: Daily global metrics
  - Format: Delta Lake

## 2. Spec Parsing & Delta Detection

### New Requirements from Technical Specification

#### 2.1 Additions
- **New Source Table**: analytics_db.SOURCE_TILE_METADATA
- **New Column**: tile_category in TARGET_HOME_TILE_DAILY_SUMMARY
- **New Join Logic**: LEFT JOIN between events and metadata tables
- **New Validation Logic**: Metadata table availability check

#### 2.2 Modifications
- **ETL Pipeline Enhancement**: Modified aggregation logic to include metadata enrichment
- **Schema Evolution**: TARGET_HOME_TILE_DAILY_SUMMARY schema extension
- **Data Quality Rules**: Addition of tile_category completeness validation

#### 2.3 No Deprecations Detected
- All existing tables and columns remain unchanged
- Backward compatibility maintained

## 3. Delta Computation

### 3.1 Schema Changes Summary

| Change Type | Object | Change Description | Impact Level |
|-------------|--------|-------------------|-------------|
| ADD | analytics_db.SOURCE_TILE_METADATA | New metadata table | MINOR |
| ALTER | reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY | Add tile_category column | MINOR |
| MODIFY | home_tile_reporting_etl.py | Enhanced aggregation logic | MINOR |

### 3.2 Version Impact Assessment
- **Change Classification**: MINOR (additive changes, no breaking modifications)
- **Backward Compatibility**: MAINTAINED
- **Data Loss Risk**: NONE

## 4. Impact Assessment

### 4.1 Downstream Dependencies
- **Power BI/Tableau Dashboards**: May benefit from new tile_category dimension
- **Existing ETL Jobs**: No breaking changes expected
- **API Consumers**: No impact (schema extension only)
- **Data Warehouse Views**: May require updates to include new column

### 4.2 Risk Analysis
- **LOW RISK**: Additive changes with graceful degradation
- **Performance Impact**: Minimal (single LEFT JOIN added)
- **Data Quality Impact**: Improved (additional metadata enrichment)

### 4.3 Foreign Key Relationships
- **New Relationship**: SOURCE_TILE_METADATA.tile_id → SOURCE_HOME_TILE_EVENTS.tile_id (LEFT JOIN)
- **Referential Integrity**: Maintained through LEFT JOIN strategy

## 5. DDL Generation

### 5.1 Forward Migration Scripts

#### Create New Source Table
```sql
-- DDL-001: Create SOURCE_TILE_METADATA table
CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_TILE_METADATA 
(
    tile_id        STRING    COMMENT 'Tile identifier',
    tile_name      STRING    COMMENT 'User-friendly tile name',
    tile_category  STRING    COMMENT 'Business or functional category of tile',
    is_active      BOOLEAN   COMMENT 'Indicates if tile is currently active',
    updated_ts     TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Master metadata for homepage tiles, used for business categorization and reporting enrichment';
```

#### Alter Target Table Schema
```sql
-- DDL-002: Add tile_category column to TARGET_HOME_TILE_DAILY_SUMMARY
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMN tile_category STRING COMMENT 'Functional category of the tile' AFTER tile_id;
```

#### Insert Initial Metadata (Sample)
```sql
-- DDL-003: Insert initial metadata for existing tiles
INSERT INTO analytics_db.SOURCE_TILE_METADATA VALUES
('TILE_001', 'Personal Finance Overview', 'FINANCE', true, current_timestamp()),
('TILE_002', 'Health Dashboard', 'HEALTH', true, current_timestamp()),
('TILE_003', 'Shopping Recommendations', 'COMMERCE', true, current_timestamp());
```

### 5.2 Rollback Scripts

```sql
-- ROLLBACK-001: Remove tile_category column
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
DROP COLUMN tile_category;

-- ROLLBACK-002: Drop SOURCE_TILE_METADATA table
DROP TABLE IF EXISTS analytics_db.SOURCE_TILE_METADATA;
```

### 5.3 Data Migration Scripts

```sql
-- MIGRATION-001: Backfill tile_category for historical data
UPDATE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
SET tile_category = COALESCE(
    (SELECT tile_category FROM analytics_db.SOURCE_TILE_METADATA m 
     WHERE m.tile_id = TARGET_HOME_TILE_DAILY_SUMMARY.tile_id), 
    'UNKNOWN'
)
WHERE tile_category IS NULL;
```

## 6. ETL Pipeline Changes

### 6.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

### 6.2 Enhanced Aggregation Logic
```python
# Read metadata table
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)

# Enhanced daily summary with metadata join
df_daily_summary_enhanced = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("date", F.lit(PROCESS_DATE))
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_category",  # New column
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)
```

### 6.3 Validation Enhancement
```python
# Add metadata table validation
def validate_metadata_table():
    try:
        metadata_count = spark.table(SOURCE_TILE_METADATA).count()
        print(f"Metadata table contains {metadata_count} records")
        return True
    except Exception as e:
        print(f"Warning: Metadata table not available: {e}")
        return False
```

## 7. Data Quality & Validation Rules

### 7.1 New Validation Rules
- **Completeness**: All active tiles must have tile_category (default to 'UNKNOWN')
- **Consistency**: tile_category values must follow approved taxonomy
- **Referential Integrity**: All tile_ids in events should have corresponding metadata
- **Data Freshness**: Metadata updated_ts should be monitored

### 7.2 Monitoring Queries
```sql
-- Monitor tiles without metadata
SELECT COUNT(*) as tiles_without_metadata
FROM reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY s
LEFT JOIN analytics_db.SOURCE_TILE_METADATA m ON s.tile_id = m.tile_id
WHERE m.tile_id IS NULL AND s.date = current_date();

-- Monitor category distribution
SELECT tile_category, COUNT(*) as tile_count
FROM reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY
WHERE date = current_date()
GROUP BY tile_category;
```

## 8. Deployment Strategy

### 8.1 Phase 1: Infrastructure (Day 1)
1. Execute DDL-001: Create SOURCE_TILE_METADATA table
2. Execute DDL-003: Insert initial metadata records
3. Validate table creation and data insertion

### 8.2 Phase 2: Schema Evolution (Day 2)
1. Execute DDL-002: Add tile_category column to target table
2. Execute MIGRATION-001: Backfill historical data
3. Validate schema changes

### 8.3 Phase 3: ETL Enhancement (Day 3)
1. Deploy enhanced ETL pipeline code
2. Execute test run with validation
3. Monitor performance and data quality

### 8.4 Phase 4: Validation & Monitoring (Day 4)
1. Implement monitoring queries
2. Validate end-to-end data flow
3. Update documentation and runbooks

## 9. Testing Strategy

### 9.1 Unit Tests
- Metadata join logic validation
- Default value assignment testing
- Schema compatibility verification

### 9.2 Integration Tests
- End-to-end pipeline execution
- Data quality validation
- Performance benchmarking

### 9.3 Regression Tests
- Backward compatibility verification
- Existing functionality preservation
- Historical data integrity

## 10. Rollback Plan

In case of issues:
1. Revert ETL pipeline to previous version
2. Execute rollback scripts (ROLLBACK-001, ROLLBACK-002)
3. Restore from backup if necessary
4. Validate system stability

## 11. Success Criteria

- ✅ SOURCE_TILE_METADATA table created successfully
- ✅ TARGET_HOME_TILE_DAILY_SUMMARY extended with tile_category
- ✅ ETL pipeline processes metadata enrichment
- ✅ All existing functionality preserved
- ✅ Data quality metrics maintained or improved
- ✅ Performance SLAs met
- ✅ Zero data loss during migration

## 12. Change Traceability Matrix

| Technical Spec Section | DDL Reference | ETL Change | Validation Rule |
|------------------------|---------------|------------|----------------|
| New Source Table | DDL-001 | Configuration update | Table existence check |
| Target Schema Extension | DDL-002 | Join logic enhancement | Column presence validation |
| Metadata Enrichment | DDL-003 | Aggregation modification | Data completeness check |
| Backward Compatibility | MIGRATION-001 | Graceful degradation | Historical data integrity |

---

**Document Status**: Initial Version  
**Change Classification**: MINOR (Additive Enhancement)  
**Risk Level**: LOW  
**Estimated Effort**: 2-3 Development Days  
**Dependencies**: Product Analytics team for metadata maintenance