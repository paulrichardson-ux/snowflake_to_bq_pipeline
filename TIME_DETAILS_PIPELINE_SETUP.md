# TIME_DETAILS Pipeline Setup

This pipeline syncs time entry data from Snowflake to BigQuery, providing comprehensive time tracking and productivity analytics for business intelligence and reporting.

## Components Created

### 1. BigQuery Table Structure
- **Table**: `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
- **Type**: Incremental sync with sophisticated duplicate detection
- **Schema**: Auto-detected from Snowflake time tracking tables
- **Partitioning**: By `REPORTING_DATE` for optimal query performance

### 2. Cloud Functions
- **Full Sync Function**: `time-details-sync-full`
  - Runtime: Python 3.11
  - Memory: 1024MB
  - Timeout: 540s (9 minutes)
  
- **Daily Sync Function**: `time-details-sync-daily`
  - Runtime: Python 3.11
  - Memory: 1024MB
  - Timeout: 540s (9 minutes)
  - Special Feature: Advanced duplicate cleanup logic

### 3. Cloud Scheduler
- **Job Name**: `time-details-daily-sync`
- **Schedule**: Daily at 8:00 AM UTC
- **Method**: Incremental updates with duplicate detection and cleanup

## Data Schema

The time entry table includes:

### Core Time Entry Fields
- `TIME_ENTRY_ID` - Unique identifier for each time entry
- `WORK_ITEM_ID` - Associated work item
- `USER_ID` - User who logged the time
- `USER_NAME` - User's display name
- `HOURS_LOGGED` - Number of hours worked
- `BILLING_RATE` - Hourly billing rate
- `BILLABLE_AMOUNT` - Calculated billable amount

### Client & Project Fields
- `CLIENT_ID` - Associated client
- `CLIENT_NAME` - Client name
- `PROJECT_ID` - Project identifier
- `PROJECT_NAME` - Project name
- `TASK_TYPE` - Type of task performed

### Time & Date Fields
- `REPORTING_DATE` - Date for reporting/partitioning
- `TIME_LOGGED_DATE` - Actual date when work was performed
- `TIME_ENTRY_CREATED` - When time entry was created
- `TIME_ENTRY_MODIFIED` - Last modification timestamp

### Productivity Fields
- `DESCRIPTION` - Work description/notes
- `CATEGORY` - Time entry category
- `BILLABLE_FLAG` - Whether time is billable
- `APPROVED_FLAG` - Whether time entry is approved
- `EFFICIENCY_SCORE` - Calculated productivity metric

## Sync Process

### Full Sync Process
1. **Extract** all time entries from Snowflake
2. **Transform** data with proper type casting and calculations
3. **Load** into temporary BigQuery table
4. **Replace** target table with complete dataset
5. **Clean up** temporary resources

### Daily Incremental Process
1. **Identify** new/updated time entries since last sync
2. **Extract** only changed records from Snowflake
3. **Run duplicate detection** using advanced logic
4. **Remove duplicates** based on multiple criteria
5. **Insert** new/updated records
6. **Calculate** productivity metrics
7. **Log** detailed sync statistics

## Duplicate Detection Logic

The pipeline uses sophisticated duplicate detection:

```sql
-- Removes duplicates based on multiple criteria
DELETE FROM target_table 
WHERE (TIME_ENTRY_ID, REPORTING_DATE, USER_ID) IN (
    SELECT TIME_ENTRY_ID, REPORTING_DATE, USER_ID 
    FROM new_data
)
```

This ensures:
- No duplicate time entries for same user/date
- Proper handling of time entry modifications
- Consistent data across sync runs

## Deployment Instructions

### Step 1: Deploy Full Sync Function
```bash
cd "snowflake_bq_sync Time details"
gcloud functions deploy time-details-sync-full \
    --source=. \
    --entry-point=main \
    --runtime=python311 \
    --trigger=http \
    --memory=1024MB \
    --timeout=540s \
    --region=us-central1
```

### Step 2: Deploy Daily Sync Function
```bash
cd "snowflake_bq_sync_daily Time Details"
gcloud functions deploy time-details-sync-daily \
    --source=. \
    --entry-point=sync_daily_incremental \
    --runtime=python311 \
    --trigger=http \
    --memory=1024MB \
    --timeout=540s \
    --region=us-central1
```

### Step 3: Create Scheduler
```bash
gcloud scheduler jobs create http time-details-daily-sync \
    --location=us-central1 \
    --schedule="0 8 * * *" \
    --uri="https://us-central1-red-octane-444308-f4.cloudfunctions.net/time-details-sync-daily" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"source": "scheduler"}'
```

## Manual Execution

### Trigger Full Sync
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/time-details-sync-full \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger Daily Sync
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/time-details-sync-daily \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

## Data Usage & Analytics

### Daily Time Summary
```sql
SELECT 
    DATE(REPORTING_DATE) as date,
    USER_NAME,
    SUM(HOURS_LOGGED) as total_hours,
    SUM(CASE WHEN BILLABLE_FLAG = true THEN HOURS_LOGGED ELSE 0 END) as billable_hours,
    SUM(BILLABLE_AMOUNT) as total_billable_amount
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE REPORTING_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY date, USER_NAME
ORDER BY date DESC, total_hours DESC;
```

### Client Productivity Analysis
```sql
SELECT 
    CLIENT_NAME,
    COUNT(DISTINCT USER_ID) as unique_users,
    SUM(HOURS_LOGGED) as total_hours,
    AVG(EFFICIENCY_SCORE) as avg_efficiency,
    SUM(BILLABLE_AMOUNT) as total_revenue
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE REPORTING_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY CLIENT_NAME
ORDER BY total_revenue DESC;
```

### User Productivity Metrics
```sql
SELECT 
    USER_NAME,
    COUNT(*) as total_entries,
    SUM(HOURS_LOGGED) as total_hours,
    AVG(HOURS_LOGGED) as avg_hours_per_entry,
    SUM(BILLABLE_AMOUNT) / NULLIF(SUM(HOURS_LOGGED), 0) as avg_hourly_rate,
    AVG(EFFICIENCY_SCORE) as avg_efficiency
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE REPORTING_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY USER_NAME
ORDER BY total_hours DESC;
```

### Project Time Tracking
```sql
SELECT 
    PROJECT_NAME,
    WORK_ITEM_ID,
    SUM(HOURS_LOGGED) as total_hours,
    COUNT(DISTINCT USER_ID) as contributors,
    MAX(TIME_LOGGED_DATE) as last_activity,
    SUM(BILLABLE_AMOUNT) as project_value
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE PROJECT_NAME IS NOT NULL
GROUP BY PROJECT_NAME, WORK_ITEM_ID
ORDER BY total_hours DESC;
```

## Monitoring & Health Checks

### Sync Status Check
```sql
SELECT 
    COUNT(*) as total_entries,
    MAX(REPORTING_DATE) as latest_date,
    COUNT(DISTINCT USER_ID) as unique_users,
    COUNT(DISTINCT CLIENT_ID) as unique_clients,
    SUM(HOURS_LOGGED) as total_hours_logged,
    SUM(BILLABLE_AMOUNT) as total_billable_amount
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`;
```

### Data Quality Checks
```sql
-- Identify potential data issues
SELECT 
    'Negative Hours' as issue,
    COUNT(*) as count
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE HOURS_LOGGED < 0

UNION ALL

SELECT 
    'Missing User Info' as issue,
    COUNT(*) as count
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE USER_ID IS NULL OR USER_NAME IS NULL

UNION ALL

SELECT 
    'Excessive Hours (>12 per day)' as issue,
    COUNT(*) as count
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE HOURS_LOGGED > 12

UNION ALL

SELECT 
    'Future Dates' as issue,
    COUNT(*) as count
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE TIME_LOGGED_DATE > CURRENT_DATE();
```

### Duplicate Detection Report
```sql
-- Check for potential duplicates
SELECT 
    TIME_ENTRY_ID,
    USER_ID,
    REPORTING_DATE,
    COUNT(*) as duplicate_count
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
GROUP BY TIME_ENTRY_ID, USER_ID, REPORTING_DATE
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

## Performance Optimization

### Batch Processing
- Default batch size: 1000 records
- Configurable via environment variables
- Automatic retry logic for failed batches

### Query Optimization
- Table partitioned by `REPORTING_DATE`
- Recommended indexes on frequently queried fields
- Efficient duplicate detection queries

### Memory Management
- Streaming inserts for large datasets
- Garbage collection between batches
- Connection pooling for Snowflake

## Key Features

1. **Advanced Duplicate Detection**: Multi-criteria duplicate identification
2. **Incremental Processing**: Only new/changed records
3. **Productivity Metrics**: Automatic efficiency calculations
4. **Data Validation**: Comprehensive quality checks
5. **Flexible Scheduling**: Configurable sync frequency
6. **Error Recovery**: Robust error handling and retry logic

## Common Issues & Solutions

### 1. Duplicate Time Entries
**Symptoms**: Multiple entries for same time period
**Solution**: 
```sql
-- Manual cleanup query
DELETE FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE (TIME_ENTRY_ID, REPORTING_DATE, USER_ID) IN (
    SELECT TIME_ENTRY_ID, REPORTING_DATE, USER_ID
    FROM (
        SELECT TIME_ENTRY_ID, REPORTING_DATE, USER_ID,
               ROW_NUMBER() OVER (PARTITION BY TIME_ENTRY_ID, USER_ID ORDER BY TIME_ENTRY_CREATED DESC) as rn
        FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
    ) WHERE rn > 1
);
```

### 2. Missing Time Entries
**Symptoms**: Gaps in time tracking data
**Solution**: 
- Run full sync to refresh complete dataset
- Check Snowflake source for data availability
- Verify date range filters in incremental sync

### 3. Performance Issues
**Symptoms**: Slow sync times or timeouts
**Solution**:
- Reduce batch size in function configuration
- Increase function timeout and memory
- Check BigQuery slot availability

### 4. Schema Evolution
**Symptoms**: New fields not appearing in BigQuery
**Solution**:
- Function automatically detects schema changes
- Monitor logs for schema update messages
- Validate new field mappings

## Integration with Other Systems

### Dashboard Integration
```sql
-- Create summary view for dashboards
CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.daily_productivity_summary` AS
SELECT 
    DATE(REPORTING_DATE) as date,
    USER_NAME,
    CLIENT_NAME,
    SUM(HOURS_LOGGED) as hours,
    SUM(BILLABLE_AMOUNT) as revenue,
    AVG(EFFICIENCY_SCORE) as efficiency
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE REPORTING_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date, USER_NAME, CLIENT_NAME;
```

### Reporting Integration
- Compatible with Google Data Studio
- Tableau connector available
- Power BI integration supported
- Custom API endpoints for real-time data

## Data Retention & Archival

### Recommended Retention Policy
```sql
-- Archive old data (older than 2 years)
CREATE OR REPLACE TABLE `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_ARCHIVE` AS
SELECT *
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE REPORTING_DATE < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR);

-- Delete archived data from main table
DELETE FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE REPORTING_DATE < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR);
```

## Security & Compliance

- **Data Encryption**: All data encrypted in transit and at rest
- **Access Control**: Role-based access to BigQuery tables
- **Audit Logging**: Complete audit trail of all sync operations
- **Data Privacy**: Personal data handling in compliance with regulations

## Support & Troubleshooting

For issues with the Time Details pipeline:
1. Check Cloud Function logs in GCP Console
2. Review BigQuery job history for errors
3. Monitor Cloud Scheduler execution status
4. Validate Snowflake source data availability
5. Use the data quality check queries above

## Future Enhancements

- Real-time sync capabilities
- Advanced analytics and ML models
- Integration with external time tracking tools
- Enhanced duplicate detection algorithms
- Automated data quality monitoring 