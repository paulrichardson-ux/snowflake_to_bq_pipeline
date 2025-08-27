# WORK_ITEM_DETAILS Pipeline Setup

This pipeline syncs work item data from Snowflake to BigQuery, providing comprehensive project tracking and work item analytics for business intelligence.

## Components Created

### 1. BigQuery Table Structure
- **Table**: `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
- **Type**: Incremental sync with duplicate detection
- **Schema**: Auto-detected from Snowflake source tables
- **Partitioning**: By `REPORTING_DATE` for optimal query performance

### 2. Cloud Functions
- **Full Sync Function**: `work-item-details-sync-full`
  - Runtime: Python 3.11
  - Memory: 1024MB
  - Timeout: 540s (9 minutes)
  
- **Daily Sync Function**: `work-item-details-sync-daily`
  - Runtime: Python 3.11
  - Memory: 1024MB
  - Timeout: 540s (9 minutes)

### 3. Cloud Scheduler
- **Job Name**: `work-item-details-daily-sync`
- **Schedule**: Daily at 7:00 AM UTC
- **Method**: Incremental updates with duplicate cleanup

## Deployment Instructions

### Step 1: Deploy Full Sync Function
```bash
cd work_item_details_pipeline
./deploy_work_item_details_full.sh
```

### Step 2: Deploy Daily Sync Function
```bash
./deploy_work_item_details_daily.sh
```

### Step 3: Create Scheduler (if needed)
```bash
gcloud scheduler jobs create http work-item-details-daily-sync \
    --location=us-central1 \
    --schedule="0 7 * * *" \
    --uri="https://us-central1-red-octane-444308-f4.cloudfunctions.net/work-item-details-sync-daily" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"source": "scheduler"}'
```

## Data Schema

The work item details table includes:

### Core Work Item Fields
- `WORK_ITEM_ID` - Unique identifier
- `WORK_ITEM_NAME` - Work item title/name
- `WORK_ITEM_TYPE` - Type of work item
- `STATUS` - Current status
- `PRIORITY` - Priority level

### Client & Assignment Fields
- `CLIENT_ID` - Associated client
- `CLIENT_NAME` - Client name
- `ASSIGNED_USER_ID` - Assigned user
- `ASSIGNED_USER_NAME` - Assigned user name

### Time & Progress Fields
- `ESTIMATED_HOURS` - Estimated work hours
- `ACTUAL_HOURS` - Actual hours worked
- `COMPLETION_PERCENTAGE` - Progress percentage
- `DUE_DATE` - Due date
- `COMPLETION_DATE` - Completion date

### Tracking Fields
- `REPORTING_DATE` - Date for reporting/partitioning
- `CREATED_DATE` - When work item was created
- `LAST_MODIFIED` - Last modification timestamp

## Sync Process

### Full Sync Process
1. **Extract** all work item records from Snowflake
2. **Transform** data with proper type casting
3. **Load** into temporary BigQuery table
4. **Replace** target table with new data
5. **Clean up** temporary resources

### Daily Incremental Process
1. **Identify** new/updated records since last sync
2. **Extract** only changed records from Snowflake
3. **Check** for duplicates in target table
4. **Remove** existing duplicates
5. **Insert** new/updated records
6. **Log** sync statistics

## Manual Execution

### Trigger Full Sync
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/work-item-details-sync-full \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger Daily Sync
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/work-item-details-sync-daily \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger via Scheduler
```bash
gcloud scheduler jobs run work-item-details-daily-sync --location=us-central1
```

## Data Usage & Queries

### Get Active Work Items
```sql
SELECT 
    WORK_ITEM_ID,
    WORK_ITEM_NAME,
    CLIENT_NAME,
    ASSIGNED_USER_NAME,
    STATUS,
    ESTIMATED_HOURS,
    ACTUAL_HOURS,
    COMPLETION_PERCENTAGE
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
WHERE STATUS IN ('In Progress', 'Open', 'Assigned')
ORDER BY DUE_DATE ASC;
```

### Work Item Analytics
```sql
SELECT 
    CLIENT_NAME,
    COUNT(*) as total_work_items,
    SUM(ESTIMATED_HOURS) as total_estimated_hours,
    SUM(ACTUAL_HOURS) as total_actual_hours,
    AVG(COMPLETION_PERCENTAGE) as avg_completion
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
WHERE REPORTING_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY CLIENT_NAME
ORDER BY total_work_items DESC;
```

### Productivity Metrics
```sql
SELECT 
    ASSIGNED_USER_NAME,
    DATE(REPORTING_DATE) as date,
    COUNT(*) as items_worked,
    SUM(ACTUAL_HOURS) as hours_logged,
    AVG(COMPLETION_PERCENTAGE) as avg_progress
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
WHERE REPORTING_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY ASSIGNED_USER_NAME, DATE(REPORTING_DATE)
ORDER BY date DESC, hours_logged DESC;
```

## Monitoring & Troubleshooting

### Check Sync Status
```sql
SELECT 
    COUNT(*) as total_records,
    MAX(REPORTING_DATE) as latest_date,
    COUNT(DISTINCT CLIENT_ID) as unique_clients,
    COUNT(DISTINCT ASSIGNED_USER_ID) as unique_users
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`;
```

### Identify Data Quality Issues
```sql
-- Check for missing required fields
SELECT 
    'Missing Work Item Name' as issue,
    COUNT(*) as count
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
WHERE WORK_ITEM_NAME IS NULL OR WORK_ITEM_NAME = ''

UNION ALL

SELECT 
    'Missing Client Assignment' as issue,
    COUNT(*) as count
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
WHERE CLIENT_ID IS NULL

UNION ALL

SELECT 
    'Negative Hours' as issue,
    COUNT(*) as count
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
WHERE ACTUAL_HOURS < 0 OR ESTIMATED_HOURS < 0;
```

## Key Features

1. **Incremental Sync**: Only processes new/changed records
2. **Duplicate Detection**: Prevents duplicate entries
3. **Auto Schema**: Adapts to Snowflake schema changes
4. **Batch Processing**: Handles large datasets efficiently
5. **Data Validation**: Basic data quality checks
6. **Partition Support**: Optimized for date-based queries

## Performance Considerations

- **Batch Size**: Default 1000 records per batch
- **Partitioning**: Table partitioned by `REPORTING_DATE`
- **Indexing**: Consider additional indexes on frequently queried fields
- **Retention**: Consider data retention policies for historical data

## Common Issues & Solutions

1. **Function Timeout**
   - Increase timeout in deployment script
   - Reduce batch size in function code
   - Consider breaking large syncs into smaller chunks

2. **Schema Mismatches**
   - Function automatically detects and adapts to schema changes
   - Monitor logs for schema evolution messages
   - Validate data types after schema changes

3. **Duplicate Data**
   - Daily sync includes duplicate cleanup logic
   - Check `WORK_ITEM_ID` + `REPORTING_DATE` uniqueness
   - Run manual cleanup if needed

4. **Missing Data**
   - Verify Snowflake source data availability
   - Check function logs for extraction errors
   - Validate date filters in incremental sync

## Integration with Other Pipelines

- **Client Dimension**: Links via `CLIENT_ID`
- **Time Details**: Related via `WORK_ITEM_ID`
- **User Data**: Connected through user assignments

## BigQuery Views

The pipeline includes pre-built views for common queries:
- `work_item_x_user_time_view` - Combines work items with time entries
- `work_item_user_time_detail_view` - Detailed work item time analysis

See `work_item_x_user_time_view.sql` for view definitions. 