_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Technical specification for adding SOURCE_TILE_METADATA table and extending target summary with tile_category
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Technical Specification for Home Tile Reporting Enhancement - Add Tile Category Metadata

## Introduction

This technical specification outlines the implementation details for enhancing the Home Tile Reporting ETL pipeline to include tile category metadata. The enhancement addresses the business requirement from JIRA story PCE-3 to add a new source table `SOURCE_TILE_METADATA` and extend the target summary table with `tile_category` column to enable better reporting and performance tracking at functional levels.

### Business Context
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics are aggregated only at tile_id level with no business grouping. The enhancement will enable:
- Performance metrics by category (e.g., "Personal Finance Tiles vs Health Tiles")
- Category-level CTR comparisons
- Product manager tracking of feature adoption
- Improved dashboard drilldowns in Power BI/Tableau
- Future segmentation capabilities

## Code Changes Required for the Enhancement

### 1. ETL Pipeline Modifications (home_tile_reporting_etl.py)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Source Data Reading
```python
# Add metadata table reading
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Modified daily summary with tile_category join
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

#### 1.4 Error Handling and Validation
```python
# Add validation for metadata table availability
def validate_metadata_table():
    try:
        metadata_count = spark.table(SOURCE_TILE_METADATA).count()
        print(f"Metadata table contains {metadata_count} records")
        return True
    except Exception as e:
        print(f"Warning: Metadata table not available: {e}")
        return False

# Call validation before processing
metadata_available = validate_metadata_table()
```

### 2. Impacted Modules and Functions

- **Main ETL Function**: Modify aggregation logic to include left join with metadata table
- **Data Validation Module**: Add schema validation for new tile_category column
- **Configuration Module**: Add SOURCE_TILE_METADATA table reference
- **Logging Module**: Add logging for metadata enrichment statistics

## Data Model Updates

### 1. New Source Table: analytics_db.SOURCE_TILE_METADATA

**Purpose**: Master metadata table for homepage tiles used for business categorization

**Schema**:
```sql
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

**Data Governance**:
- Primary Key: tile_id
- Data Quality: tile_category should not be null for active tiles
- Update Frequency: As needed by business teams
- Data Lineage: Maintained by Product Analytics team

### 2. Target Table Enhancement: reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY

**Modified Schema**:
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Functional category of the tile', -- NEW COLUMN
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics for reporting with category enrichment';
```

### 3. Data Model Relationships

```
SOURCE_HOME_TILE_EVENTS ──┐
                          ├── JOIN on tile_id ──> TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ──┤                           ↑
                          └── LEFT JOIN on tile_id ──────┘
SOURCE_TILE_METADATA ────────────────────────────────────┘
```

## Source-to-Target Mapping

### 1. Enhanced Daily Summary Mapping

| Source Field | Source Table | Target Field | Target Table | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------------|
| event_ts | SOURCE_HOME_TILE_EVENTS | date | TARGET_HOME_TILE_DAILY_SUMMARY | `to_date(event_ts)` |
| tile_id | SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | Direct mapping |
| tile_category | SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | `COALESCE(tile_category, 'UNKNOWN')` |
| user_id + event_type | SOURCE_HOME_TILE_EVENTS | unique_tile_views | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT CASE WHEN event_type='TILE_VIEW' THEN user_id END)` |
| user_id + event_type | SOURCE_HOME_TILE_EVENTS | unique_tile_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT CASE WHEN event_type='TILE_CLICK' THEN user_id END)` |
| user_id + interstitial_view_flag | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_views | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT CASE WHEN interstitial_view_flag=true THEN user_id END)` |
| user_id + primary_button_click_flag | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_primary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT CASE WHEN primary_button_click_flag=true THEN user_id END)` |
| user_id + secondary_button_click_flag | SOURCE_INTERSTITIAL_EVENTS | unique_interstitial_secondary_clicks | TARGET_HOME_TILE_DAILY_SUMMARY | `COUNT(DISTINCT CASE WHEN secondary_button_click_flag=true THEN user_id END)` |

### 2. Transformation Rules

#### 2.1 Tile Category Enrichment
```sql
-- Business Rule: Default to 'UNKNOWN' when no metadata exists
CASE 
    WHEN metadata.tile_category IS NOT NULL THEN metadata.tile_category
    ELSE 'UNKNOWN'
END as tile_category
```

#### 2.2 Active Tile Filter
```sql
-- Only include active tiles from metadata
WHERE metadata.is_active = true OR metadata.is_active IS NULL
```

#### 2.3 Backward Compatibility
```sql
-- Ensure all existing tiles continue to appear in reports
LEFT JOIN SOURCE_TILE_METADATA metadata ON events.tile_id = metadata.tile_id
```

### 3. Data Quality Rules

| Rule | Description | Implementation |
|------|-------------|----------------|
| Completeness | All tile_ids must have a category | Default to 'UNKNOWN' if missing |
| Consistency | Category values must be standardized | Validate against approved category list |
| Accuracy | Active flag must be boolean | Schema enforcement |
| Timeliness | Metadata updates reflected in next run | Daily refresh capability |

## Implementation Steps

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table in analytics_db
2. Insert initial metadata records for existing tiles
3. Set up data governance processes for metadata maintenance

### Phase 2: ETL Enhancement
1. Modify home_tile_reporting_etl.py to include metadata join
2. Update TARGET_HOME_TILE_DAILY_SUMMARY schema
3. Implement backward compatibility logic
4. Add data validation and error handling

### Phase 3: Testing and Validation
1. Unit tests for metadata join logic
2. Integration tests for end-to-end pipeline
3. Data quality validation tests
4. Performance impact assessment

### Phase 4: Deployment
1. Deploy schema changes to target table
2. Deploy enhanced ETL code
3. Backfill historical data (if required)
4. Monitor pipeline performance

## Assumptions and Constraints

### Assumptions
- SOURCE_TILE_METADATA will be maintained by Product Analytics team
- Tile categories will be relatively stable (low change frequency)
- Existing tile_ids will have corresponding metadata entries
- Performance impact of additional join is acceptable

### Constraints
- Backward compatibility must be maintained
- No changes to existing record counts for historical data
- Pipeline SLA must be maintained
- No impact on TARGET_HOME_TILE_GLOBAL_KPIS table (out of scope)

### Technical Constraints
- Delta Lake format requirements
- Databricks cluster resource limitations
- Data retention policies
- Schema evolution capabilities

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Metadata table unavailable | Pipeline failure | Implement graceful degradation with 'UNKNOWN' category |
| Performance degradation | SLA breach | Optimize join strategy, consider caching metadata |
| Schema drift | Data quality issues | Implement schema validation and monitoring |
| Missing metadata | Incomplete reporting | Establish data governance processes |

## Testing Strategy

### Unit Tests
```python
def test_tile_category_enrichment():
    # Test metadata join logic
    # Test default 'UNKNOWN' assignment
    # Test active tile filtering
    pass

def test_backward_compatibility():
    # Ensure existing tiles still appear
    # Validate record count consistency
    pass
```

### Integration Tests
- End-to-end pipeline execution
- Data quality validation
- Performance benchmarking
- Schema compatibility verification

## References

- JIRA Story: PCE-3 - Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category
- Source Code: home_tile_reporting_etl.py
- Source DDL: SourceDDL.sql, TargetDDL.sql
- Metadata DDL: SOURCE_TILE_METADATA.sql
- Data Governance Standards: [Internal Documentation]
- Delta Lake Best Practices: [Databricks Documentation]

---

**Document Status**: Draft v1.0  
**Next Review Date**: [To be scheduled]  
**Approval Required**: Data Architecture Team, Product Analytics Team