# TENANT_TEAM_MEMBER_DIMENSION Pipeline Setup

This pipeline syncs the `DIMN_TENANT_TEAM_MEMBER` table from Snowflake to the `TENANT_TEAM_MEMBER_DIMENSION` table in BigQuery as a direct mirror.

## Components Created

### 1. BigQuery Table Structure
- **Table**: `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION`
- **Type**: Direct mirror of Snowflake `DIMN_TENANT_TEAM_MEMBER` table
- **Schema**: 5 columns matching Snowflake structure
- **No partitioning**: Simple dimension table

### 2. Cloud Function
- **Name**: `tenant-team-member-dimension-sync-daily`
- **Runtime**: Python 3.11
- **Memory**: 1024MB
- **Timeout**: 540s (9 minutes)
- **Trigger**: HTTP (for both manual and scheduled execution)

### 3. Cloud Scheduler
- **Job Name**: `tenant-team-member-dimension-daily-sync`
- **Schedule**: Daily at 7:00 AM UTC (1 hour after CLIENT_DIMENSION)
- **Method**: Full replace (truncate and insert)

## Deployment Instructions

### Step 1: Deploy the Cloud Function
```bash
cd "Karbon Big Query"
cd snowflake_to_bq_pipeline
./deploy_tenant_team_member_dimension_sync.sh
```

### Step 2: Create the Daily Scheduler
```bash
./create_tenant_team_member_dimension_scheduler.sh
```

## Manual Execution

### Trigger via HTTP
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/tenant-team-member-dimension-sync-daily \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger via Scheduler
```bash
gcloud scheduler jobs run tenant-team-member-dimension-daily-sync --location=us-central1
```

## Data Schema

The table includes all Snowflake DIMN_TENANT_TEAM_MEMBER fields:

### Core Fields
- **TENANT_TEAM_MEMBER_ID** - Unique identifier for the tenant team member relationship
- **TENANT_TEAM_ID** - Identifier for the tenant team
- **USER_ID** - User identifier for the team member
- **ACCOUNT_ID** - Associated account identifier
- **ACCOUNT_NAME** - Associated account name

## Data Usage

### Access Tenant Team Member Data
```sql
SELECT * FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION`
ORDER BY TENANT_TEAM_ID, USER_ID;
```

### Monitor Data Status
```sql
SELECT 
    COUNT(*) as total_memberships,
    COUNT(DISTINCT TENANT_TEAM_ID) as unique_teams,
    COUNT(DISTINCT USER_ID) as unique_users,
    COUNT(DISTINCT ACCOUNT_ID) as unique_accounts
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION`;
```

### Team Membership Analysis
```sql
SELECT 
    tt.TENANT_TEAM_NAME,
    COUNT(ttm.USER_ID) as member_count,
    ttm.ACCOUNT_NAME
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt 
    ON ttm.TENANT_TEAM_ID = tt.TENANT_TEAM_ID
GROUP BY tt.TENANT_TEAM_NAME, ttm.ACCOUNT_NAME
ORDER BY member_count DESC;
```

### User Team Assignments
```sql
SELECT 
    ttm.USER_ID,
    COUNT(DISTINCT ttm.TENANT_TEAM_ID) as team_count,
    STRING_AGG(tt.TENANT_TEAM_NAME, ', ') as teams
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt 
    ON ttm.TENANT_TEAM_ID = tt.TENANT_TEAM_ID
GROUP BY ttm.USER_ID
ORDER BY team_count DESC;
```

## Key Features

1. **Direct Mirror**: Table exactly matches Snowflake `DIMN_TENANT_TEAM_MEMBER`
2. **Full Replace**: Daily truncate and replace operation
3. **Batch Processing**: Handles datasets efficiently (1000 records per batch)
4. **Simple Structure**: No versioning or tracking fields
5. **Error Handling**: Comprehensive logging and error recovery
6. **Auto-Schema Detection**: Adapts to Snowflake schema changes

## Sync Process

The function performs these steps daily:
1. **Fetch** all records from Snowflake `DIMN_TENANT_TEAM_MEMBER` in batches
2. **Load** data into temporary BigQuery table
3. **Truncate** the target TENANT_TEAM_MEMBER_DIMENSION table
4. **Insert** all data from temporary table
5. **Clean up** temporary table

## Current Data (as of setup)

Based on schema sample:
- **Teams**: Saas, Inventory OG, Platform, Client Ex, Ex-Co, Inventory Alchemists
- **Total Memberships**: ~20 tenant team member assignments
- **Account**: All associated with FISKAL
- **Team Structure**: Various teams with multiple members

## Monitoring

- Check Cloud Function logs in GCP Console
- Monitor BigQuery job history
- Verify record counts after sync
- Use direct table queries for reporting

## Integration with Other Pipelines

### Links to Other Tables
- **TENANT_TEAM_DIMENSION**: Direct join via TENANT_TEAM_ID
- **CLIENT_DIMENSION**: Can be related via ACCOUNT_ID
- **USER_TIME_ENTRY_BQ**: May link via USER_ID for team-based time tracking
- **WORK_ITEM_DETAILS_BQ**: Can be related through team assignments

### Common Join Patterns
```sql
-- Team members with their team details
SELECT 
    tt.TENANT_TEAM_NAME,
    ttm.USER_ID,
    ttm.ACCOUNT_NAME
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt 
    ON ttm.TENANT_TEAM_ID = tt.TENANT_TEAM_ID
ORDER BY tt.TENANT_TEAM_NAME, ttm.USER_ID;
```

```sql
-- Team-based work analytics
SELECT 
    tt.TENANT_TEAM_NAME,
    COUNT(DISTINCT ttm.USER_ID) as team_members,
    COUNT(DISTINCT wi.WORK_ITEM_ID) as work_items,
    SUM(te.HOURS_LOGGED) as total_hours
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt 
    ON ttm.TENANT_TEAM_ID = tt.TENANT_TEAM_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ` te 
    ON ttm.USER_ID = te.USER_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` wi 
    ON te.WORK_ITEM_ID = wi.WORK_ITEM_ID
GROUP BY tt.TENANT_TEAM_NAME
ORDER BY total_hours DESC;
```

## Troubleshooting

1. **Function Timeout**: Increase timeout or reduce batch size
2. **Schema Mismatch**: Function auto-adapts to Snowflake schema changes
3. **Permission Issues**: Ensure service account has BigQuery and Secret Manager access
4. **Data Quality**: Check Snowflake `DIMN_TENANT_TEAM_MEMBER` table for data issues

## Important Notes

- ⚠️ **Full Replace**: Each sync completely replaces the table
- 📊 **Direct Mirror**: Table structure matches Snowflake exactly
- 🔄 **Daily Sync**: Ensures data freshness without versioning overhead
- 📈 **Simple Queries**: Direct access without need for "latest" views
- 👥 **Team Management**: Supports organizational team structure and member assignments
- ⏰ **Scheduled After Other Dimensions**: Runs 1 hour after client dimensions to ensure data consistency

## Schema Details

The table includes all Snowflake DIMN_TENANT_TEAM_MEMBER fields:
- Team member identification (member ID, team ID, user ID)
- Account associations
- **No tracking fields** - pure dimension data

## Performance Considerations

- **Small Dataset**: Team member assignments are typically small datasets
- **Fast Sync**: Full replacement is efficient for dimension tables
- **No Partitioning**: Simple table structure for direct queries
- **Minimal Transformation**: Direct data mapping from Snowflake

## Use Cases

1. **Team Structure Analysis**: Understanding organizational team composition
2. **User Team Assignments**: Tracking which users belong to which teams
3. **Team-based Reporting**: Aggregated metrics by organizational teams
4. **Resource Allocation**: Understanding team sizes and member distribution
5. **Organizational Analytics**: Team-level dashboards and insights

## Future Enhancements

- Integration with HR systems
- Team performance metrics
- Dynamic team assignment rules
- Team hierarchy analytics
- Team-based time tracking and billing 