# Work Item Budget vs Actual Pipeline Setup

This pipeline syncs the `WORK_ITEM_BUDGET_VS_ACTUAL` table from Snowflake to BigQuery.

## Table Schema

The pipeline processes the following columns:
- REPORTING_DATE (DATE) - Primary key component (automatically set to sync date when NULL in source)
- WORK_ITEM_ID (STRING) - Primary key component  
- WORK_TITLE (STRING)
- WORK_TYPE_ID (STRING)
- WORK_TYPE (STRING)
- TASK_TYPE_ID (STRING)
- TASK_TYPE (STRING)
- TASK_TYPE_BILLABLE_FLAG (BOOLEAN)
- ROLE_ID (STRING)
- ROLE_NAME (STRING)
- ACCOUNT_ID (STRING)
- ACCOUNT_NAME (STRING)
- USER_ID (STRING)
- USER_NAME (STRING)
- INTERNAL_CLIENT_ID (STRING)
- INTERNAL_CLIENT (STRING)
- CLIENT_ID (STRING)
- CLIENT (STRING)
- BUDGETED_MINUTES (NUMERIC)
- ACTUAL_MINUTES (NUMERIC)
- BUDGETED_COST (NUMERIC)
- ACTUAL_COST (NUMERIC)

### Special Handling: REPORTING_DATE
- **Source Data**: All REPORTING_DATE values in Snowflake are NULL
- **Pipeline Behavior**: Automatically sets REPORTING_DATE to the current sync date
- **Benefit**: Provides meaningful temporal context for when data was synced
- **Tracking**: Enables proper sync tracking functionality

## Target BigQuery Tables

### Main Data Table
- **Dataset**: `karbon_data`
- **Table**: `WORK_ITEM_BUDGET_VS_ACTUAL_BQ`
- **Primary Keys**: `WORK_ITEM_ID` + `REPORTING_DATE`

### Sync Tracking Table
- **Dataset**: `karbon_data`
- **Table**: `work_item_budget_vs_actual_sync_tracker`
- **Purpose**: Tracks which WORK_ITEM_ID + REPORTING_DATE combinations have been synced
- **Schema**:
  - `unique_id` (STRING) - UUID for each sync record
  - `work_item_id` (STRING) - Work item identifier
  - `reporting_date` (DATE) - Reporting date
  - `sync_timestamp` (TIMESTAMP) - When the sync occurred
  - `sync_type` (STRING) - 'FULL' or 'INCREMENTAL'

## Pipeline Components

### 1. Full Sync
- **Function**: `sync-full-work-item-budget-vs-actual-to-bq`
- **Purpose**: Complete table refresh
- **Deployment**: `./deploy_work_item_budget_vs_actual_full.sh`

### 2. Daily Incremental Sync  
- **Function**: `sync-work-item-budget-vs-actual-daily-to-bq`
- **Purpose**: Daily updates with ±90 day window
- **Deployment**: `./deploy_work_item_budget_vs_actual_daily.sh`

### 3. Automated Scheduling
- **Script**: `./create_work_item_budget_vs_actual_scheduler.sh`
- **Schedule**: Daily at 2:00 AM UTC
- **Function**: Calls the daily incremental sync

## Setup Instructions

### Prerequisites
- Snowflake credentials stored in Google Secret Manager
- BigQuery dataset `karbon_data` exists
- Service account `karbon-bq-sync@{PROJECT_ID}.iam.gserviceaccount.com` configured

### Deployment Steps

1. **Deploy Full Sync Function**:
   ```bash
   chmod +x ./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_full.sh
   ./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_full.sh
   ```

2. **Deploy Daily Sync Function**:
   ```bash
   chmod +x ./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_daily.sh
   ./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_daily.sh
   ```

3. **Run Initial Full Sync** (optional - table will be created automatically):
   ```bash
   curl -X POST https://us-central1-{PROJECT_ID}.cloudfunctions.net/sync-full-work-item-budget-vs-actual-to-bq
   ```

4. **Set Up Automated Daily Sync**:
   ```bash
   chmod +x ./create_work_item_budget_vs_actual_scheduler.sh
   ./create_work_item_budget_vs_actual_scheduler.sh
   ```

## Data Processing Details

### Sync Logic
- **MERGE Operation**: Uses `WORK_ITEM_ID` + `REPORTING_DATE` as composite key
- **Date Filtering**: Daily sync processes ±90 days from current date
- **Batch Processing**: Processes data in batches to handle large datasets
- **Error Handling**: Comprehensive error handling with cleanup

### Data Types
- Snowflake `NUMBER`/`DECIMAL` → BigQuery `NUMERIC`
- Snowflake `VARCHAR` → BigQuery `STRING`  
- Snowflake `DATE` → BigQuery `DATE`
- Snowflake `BOOLEAN` → BigQuery `BOOL`

## Monitoring

- **Logs**: Available in Google Cloud Logging
- **Metrics**: Function execution metrics in Cloud Monitoring
- **Alerts**: Set up based on function failures or execution time

### Sync Tracking Queries

**Check recent sync activity:**
```sql
SELECT 
  work_item_id,
  reporting_date,
  sync_timestamp,
  sync_type
FROM `{PROJECT_ID}.karbon_data.work_item_budget_vs_actual_sync_tracker`
ORDER BY sync_timestamp DESC
LIMIT 100;
```

**Count unique work items synced by date:**
```sql
SELECT 
  DATE(sync_timestamp) as sync_date,
  sync_type,
  COUNT(DISTINCT work_item_id) as unique_work_items,
  COUNT(*) as total_records
FROM `{PROJECT_ID}.karbon_data.work_item_budget_vs_actual_sync_tracker`
GROUP BY sync_date, sync_type
ORDER BY sync_date DESC;
```

**Check for potential duplicates in tracking:**
```sql
SELECT 
  work_item_id,
  reporting_date,
  COUNT(*) as sync_count
FROM `{PROJECT_ID}.karbon_data.work_item_budget_vs_actual_sync_tracker`
GROUP BY work_item_id, reporting_date
HAVING COUNT(*) > 1
ORDER BY sync_count DESC;
```

## Troubleshooting

### Common Issues
1. **Schema Mismatch**: Pipeline auto-creates tables with proper schema
2. **Date Format**: Automatically converts date/datetime objects
3. **Memory Issues**: Batch size is optimized (20 for daily, 5000 for full)
4. **Timeout**: Full sync has 15-minute timeout, daily sync has 9-minute timeout

### Manual Operations
- **Force Full Refresh**: Call the full sync function directly
- **Check Sync Status**: Review Cloud Function logs
- **Validate Data**: Query BigQuery table directly 

### 2. Daily Sync
- **Function**: `sync-work-item-budget-vs-actual-daily-to-bq`
- **Purpose**: Incremental sync with ±90 day window from current date
- **Deployment**: `./deploy_work_item_budget_vs_actual_daily.sh`
- **Schedule**: Automated daily execution via Cloud Scheduler

**NEW: Deleted Work Item Handling**
- **Problem**: Work items deleted from Snowflake were persisting in BigQuery views
- **Solution**: Added DELETE logic to remove stale records that no longer exist in source
- **Behavior**: 
  - When Snowflake has no data for the date range, deletes ALL BigQuery records for that range
  - When Snowflake has partial data, deletes BigQuery records not found in the source data
  - Ensures BigQuery stays synchronized with deletions in Snowflake
- **Impact**: Prevents deleted work items from appearing in reporting views 