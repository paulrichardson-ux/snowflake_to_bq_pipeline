# USER_DIMENSION Pipeline Setup

This pipeline syncs the `DIMN_USER` table from Snowflake to the `USER_DIMENSION` table in BigQuery as a direct mirror.

## Components Created

### 1. BigQuery Table Structure
- **Table**: `red-octane-444308-f4.karbon_data.USER_DIMENSION`
- **Type**: Direct mirror of Snowflake `DIMN_USER` table
- **Schema**: 12 columns matching Snowflake structure
- **No partitioning**: Simple dimension table

### 2. Cloud Function
- **Name**: `user-dimension-sync-daily`
- **Runtime**: Python 3.11
- **Memory**: 1024MB
- **Timeout**: 540s (9 minutes)
- **Trigger**: HTTP (for both manual and scheduled execution)

### 3. Cloud Scheduler
- **Job Name**: `user-dimension-daily-sync`
- **Schedule**: Daily at 8:00 AM UTC (2 hours after CLIENT_DIMENSION)
- **Method**: Full replace (truncate and insert)

## Deployment Instructions

### Step 1: Deploy the Cloud Function
```bash
cd "Karbon Big Query"
cd snowflake_to_bq_pipeline
./deploy_user_dimension_sync.sh
```

### Step 2: Create the Daily Scheduler
```bash
./create_user_dimension_scheduler.sh
```

## Manual Execution

### Trigger via HTTP
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/user-dimension-sync-daily \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger via Scheduler
```bash
gcloud scheduler jobs run user-dimension-daily-sync --location=us-central1
```

## Data Schema

The table includes all Snowflake DIMN_USER fields:

### Core Fields
- **USER_NAME** - Full name of the user
- **USER_ID** - Unique identifier for the user
- **USER_JOB_TITLE** - Job title/position of the user
- **USER_EMAIL_ADDRESS** - Email address of the user
- **CREATED_DATE** - Date when user was created
- **ACTIVATED_DATE** - Date when user was activated
- **EXPECTED_BILLABLE_MINUTES** - Expected billable time per period
- **EXPECTED_NONBILLABLE_MINUTES** - Expected non-billable time per period
- **STATUS** - User status (Active, Archived, Invitation Pending, etc.)
- **IS_SUPPORT_USER** - Boolean flag indicating if user is a support user
- **ACCOUNT_ID** - Associated account identifier
- **ACCOUNT_NAME** - Associated account name

## Data Usage

### Access User Data
```sql
SELECT * FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION`
ORDER BY USER_NAME;
```

### Monitor Data Status
```sql
SELECT 
    COUNT(*) as total_users,
    COUNT(DISTINCT ACCOUNT_ID) as unique_accounts,
    COUNT(CASE WHEN STATUS = 'Active' THEN 1 END) as active_users,
    COUNT(CASE WHEN STATUS = 'Archived' THEN 1 END) as archived_users,
    COUNT(CASE WHEN IS_SUPPORT_USER = TRUE THEN 1 END) as support_users
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION`;
```

### User Status Analysis
```sql
SELECT 
    STATUS,
    COUNT(*) as user_count,
    ACCOUNT_NAME
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION`
GROUP BY STATUS, ACCOUNT_NAME
ORDER BY user_count DESC;
```

### Job Title Distribution
```sql
SELECT 
    USER_JOB_TITLE,
    COUNT(*) as user_count,
    COUNT(CASE WHEN STATUS = 'Active' THEN 1 END) as active_count
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION`
WHERE USER_JOB_TITLE IS NOT NULL
GROUP BY USER_JOB_TITLE
ORDER BY user_count DESC;
```

### Billable Time Expectations
```sql
SELECT 
    USER_JOB_TITLE,
    AVG(EXPECTED_BILLABLE_MINUTES) as avg_billable_minutes,
    AVG(EXPECTED_NONBILLABLE_MINUTES) as avg_nonbillable_minutes,
    COUNT(*) as user_count
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION`
WHERE STATUS = 'Active' 
  AND EXPECTED_BILLABLE_MINUTES IS NOT NULL
GROUP BY USER_JOB_TITLE
ORDER BY avg_billable_minutes DESC;
```

## Key Features

1. **Direct Mirror**: Table exactly matches Snowflake `DIMN_USER`
2. **Full Replace**: Daily truncate and replace operation
3. **Batch Processing**: Handles datasets efficiently (1000 records per batch)
4. **Simple Structure**: No versioning or tracking fields
5. **Error Handling**: Comprehensive logging and error recovery
6. **Auto-Schema Detection**: Adapts to Snowflake schema changes
7. **Boolean Conversion**: Handles IS_SUPPORT_USER boolean field correctly

## Sync Process

The function performs these steps daily:
1. **Fetch** all records from Snowflake `DIMN_USER` in batches
2. **Load** data into temporary BigQuery table
3. **Truncate** the target USER_DIMENSION table
4. **Insert** all data from temporary table
5. **Clean up** temporary table

## Current Data (as of setup)

Based on schema sample:
- **Total Users**: ~37 users
- **Job Titles**: Operations Manager, Financial Manager, Associate, Executive, etc.
- **Account**: All associated with FISKAL
- **User Types**: Mix of active users, archived users, and support users

## Monitoring

- Check Cloud Function logs in GCP Console
- Monitor BigQuery job history
- Verify record counts after sync
- Use direct table queries for reporting

## Integration with Other Pipelines

### Links to Other Tables
- **TENANT_TEAM_MEMBER_DIMENSION**: Direct join via USER_ID
- **CLIENT_DIMENSION**: Can be related via ACCOUNT_ID
- **USER_TIME_ENTRY_BQ**: Direct join via USER_ID for time tracking
- **WORK_ITEM_DETAILS_BQ**: Can be related through user assignments

### Common Join Patterns
```sql
-- Users with their team assignments
SELECT 
    u.USER_NAME,
    u.USER_JOB_TITLE,
    u.STATUS,
    STRING_AGG(tt.TENANT_TEAM_NAME, ', ') as teams
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION` u
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm 
    ON u.USER_ID = ttm.USER_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt 
    ON ttm.TENANT_TEAM_ID = tt.TENANT_TEAM_ID
GROUP BY u.USER_NAME, u.USER_JOB_TITLE, u.STATUS
ORDER BY u.USER_NAME;
```

```sql
-- User productivity analysis
SELECT 
    u.USER_NAME,
    u.USER_JOB_TITLE,
    u.EXPECTED_BILLABLE_MINUTES,
    SUM(te.HOURS_LOGGED * 60) as actual_minutes_logged,
    COUNT(DISTINCT te.WORK_ITEM_ID) as work_items_count
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION` u
LEFT JOIN `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ` te 
    ON u.USER_ID = te.USER_ID
WHERE u.STATUS = 'Active'
GROUP BY u.USER_NAME, u.USER_JOB_TITLE, u.EXPECTED_BILLABLE_MINUTES
ORDER BY actual_minutes_logged DESC;
```

## Troubleshooting

1. **Function Timeout**: Increase timeout or reduce batch size
2. **Schema Mismatch**: Function auto-adapts to Snowflake schema changes
3. **Permission Issues**: Ensure service account has BigQuery and Secret Manager access
4. **Data Quality**: Check Snowflake `DIMN_USER` table for data issues
5. **Boolean Field Issues**: Function handles IS_SUPPORT_USER conversion automatically

## Important Notes

- ⚠️ **Full Replace**: Each sync completely replaces the table
- 📊 **Direct Mirror**: Table structure matches Snowflake exactly
- 🔄 **Daily Sync**: Ensures data freshness without versioning overhead
- 📈 **Simple Queries**: Direct access without need for "latest" views
- 👤 **User Management**: Supports complete user lifecycle and role management
- ⏰ **Scheduled After Other Dimensions**: Runs 2 hours after client dimensions to ensure data consistency

## Schema Details

The table includes all Snowflake DIMN_USER fields:
- User identification (name, ID, email)
- Job and role information
- Time expectations and billing setup
- Status and lifecycle management
- Account associations
- **No tracking fields** - pure dimension data

## Performance Considerations

- **Medium Dataset**: User data is manageable in size
- **Fast Sync**: Full replacement is efficient for dimension tables
- **No Partitioning**: Simple table structure for direct queries
- **Minimal Transformation**: Direct data mapping from Snowflake with boolean handling

## Use Cases

1. **User Management**: Understanding user roles, status, and assignments
2. **Capacity Planning**: Analyzing expected vs actual billable time
3. **Team Analytics**: User-based reporting and team composition
4. **Resource Allocation**: Understanding user availability and workload
5. **HR Analytics**: User lifecycle, job roles, and organizational structure
6. **Time Tracking Integration**: Linking users with their time entries and work items

## Future Enhancements

- Integration with HR systems
- User performance metrics
- Dynamic role assignment rules
- User hierarchy and reporting structure
- Advanced capacity planning and forecasting
- User-based cost allocation and billing 