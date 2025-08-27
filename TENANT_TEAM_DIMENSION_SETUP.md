# TENANT_TEAM_DIMENSION Pipeline Setup

This pipeline syncs the `DIMN_TENANT_TEAM` table from Snowflake to the `TENANT_TEAM_DIMENSION` table in BigQuery as a direct mirror.

## Components Created

### 1. BigQuery Table Structure
- **Table**: `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION`
- **Type**: Direct mirror of Snowflake `DIMN_TENANT_TEAM` table
- **Schema**: 6 columns matching Snowflake structure
- **No partitioning**: Simple dimension table

### 2. Cloud Function
- **Name**: `tenant-team-dimension-sync-daily`
- **Runtime**: Python 3.11
- **Memory**: 1024MB
- **Timeout**: 540s (9 minutes)
- **Trigger**: HTTP (for both manual and scheduled execution)

### 3. Cloud Scheduler
- **Job Name**: `tenant-team-dimension-daily-sync`
- **Schedule**: Daily at 6:00 AM UTC (same as CLIENT_DIMENSION)
- **Method**: Full replace (truncate and insert)

## Deployment Instructions

### Step 1: Deploy the Cloud Function
```bash
cd "Karbon Big Query"
cd snowflake_to_bq_pipeline
./deploy_tenant_team_dimension_sync.sh
```

### Step 2: Create the Daily Scheduler
```bash
./create_tenant_team_dimension_scheduler.sh
```

## Manual Execution

### Trigger via HTTP
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/tenant-team-dimension-sync-daily \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger via Scheduler
```bash
gcloud scheduler jobs run tenant-team-dimension-daily-sync --location=us-central1
```

## Data Schema

The table includes all Snowflake DIMN_TENANT_TEAM fields:

### Core Fields
- **TENANT_TEAM_ID** - Unique identifier for the tenant team
- **TENANT_TEAM_NAME** - Name of the tenant team
- **PARENT_TENANT_TEAM_ID** - ID of parent team (for hierarchical structure)
- **PARENT_TENANT_TEAM_NAME** - Name of parent team
- **ACCOUNT_ID** - Associated account identifier
- **ACCOUNT_NAME** - Associated account name

## Data Usage

### Access Tenant Team Data
```sql
SELECT * FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION`
ORDER BY TENANT_TEAM_NAME;
```

### Monitor Data Status
```sql
SELECT 
    COUNT(*) as total_teams,
    COUNT(DISTINCT ACCOUNT_ID) as unique_accounts,
    COUNT(CASE WHEN PARENT_TENANT_TEAM_ID IS NOT NULL THEN 1 END) as child_teams,
    COUNT(CASE WHEN PARENT_TENANT_TEAM_ID IS NULL THEN 1 END) as root_teams
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION`;
```

### Team Hierarchy Analysis
```sql
SELECT 
    ACCOUNT_NAME,
    COUNT(*) as total_teams,
    COUNT(CASE WHEN PARENT_TENANT_TEAM_ID IS NULL THEN 1 END) as root_teams,
    COUNT(CASE WHEN PARENT_TENANT_TEAM_ID IS NOT NULL THEN 1 END) as child_teams
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION`
GROUP BY ACCOUNT_NAME
ORDER BY total_teams DESC;
```

### Team Structure View
```sql
SELECT 
    t1.TENANT_TEAM_NAME as team,
    t1.PARENT_TENANT_TEAM_NAME as parent_team,
    t1.ACCOUNT_NAME,
    COUNT(t2.TENANT_TEAM_ID) as child_count
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` t1
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` t2 
    ON t1.TENANT_TEAM_ID = t2.PARENT_TENANT_TEAM_ID
GROUP BY t1.TENANT_TEAM_ID, t1.TENANT_TEAM_NAME, t1.PARENT_TENANT_TEAM_NAME, t1.ACCOUNT_NAME
ORDER BY t1.ACCOUNT_NAME, t1.TENANT_TEAM_NAME;
```

## Key Features

1. **Direct Mirror**: Table exactly matches Snowflake `DIMN_TENANT_TEAM`
2. **Full Replace**: Daily truncate and replace operation
3. **Batch Processing**: Handles datasets efficiently (1000 records per batch)
4. **Simple Structure**: No versioning or tracking fields
5. **Error Handling**: Comprehensive logging and error recovery
6. **Auto-Schema Detection**: Adapts to Snowflake schema changes

## Sync Process

The function performs these steps daily:
1. **Fetch** all records from Snowflake `DIMN_TENANT_TEAM` in batches
2. **Load** data into temporary BigQuery table
3. **Truncate** the target TENANT_TEAM_DIMENSION table
4. **Insert** all data from temporary table
5. **Clean up** temporary table

## Current Data (as of setup)

Based on schema sample:
- **Teams**: Saas, Inventory OG, Platform, Client Ex, Ex-Co, Inventory Alchemists
- **Account**: All associated with FISKAL
- **Structure**: Flat hierarchy (no parent-child relationships in sample)

## Monitoring

- Check Cloud Function logs in GCP Console
- Monitor BigQuery job history
- Verify record counts after sync
- Use direct table queries for reporting

## Integration with Other Pipelines

### Links to Other Tables
- **CLIENT_DIMENSION**: Can be joined via ACCOUNT_ID
- **USER_TIME_ENTRY_BQ**: May link via team assignments
- **WORK_ITEM_DETAILS_BQ**: Can be related through team structure

### Common Join Patterns
```sql
-- Teams with their client assignments
SELECT 
    tt.TENANT_TEAM_NAME,
    tt.ACCOUNT_NAME,
    COUNT(DISTINCT c.CLIENT_ID) as client_count
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt
LEFT JOIN `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION` c 
    ON tt.ACCOUNT_ID = c.ACCOUNT_ID
GROUP BY tt.TENANT_TEAM_ID, tt.TENANT_TEAM_NAME, tt.ACCOUNT_NAME
ORDER BY client_count DESC;
```

## Troubleshooting

1. **Function Timeout**: Increase timeout or reduce batch size
2. **Schema Mismatch**: Function auto-adapts to Snowflake schema changes
3. **Permission Issues**: Ensure service account has BigQuery and Secret Manager access
4. **Data Quality**: Check Snowflake `DIMN_TENANT_TEAM` table for data issues

## Important Notes

- ⚠️ **Full Replace**: Each sync completely replaces the table
- 📊 **Direct Mirror**: Table structure matches Snowflake exactly
- 🔄 **Daily Sync**: Ensures data freshness without versioning overhead
- 📈 **Simple Queries**: Direct access without need for "latest" views
- 🏢 **Team Structure**: Supports hierarchical team organization

## Schema Details

The table includes all Snowflake DIMN_TENANT_TEAM fields:
- Team identification (ID, name)
- Hierarchical structure (parent team relationships)
- Account associations
- **No tracking fields** - pure dimension data

## Performance Considerations

- **Small Dataset**: Tenant teams are typically small datasets
- **Fast Sync**: Full replacement is efficient for dimension tables
- **No Partitioning**: Simple table structure for direct queries
- **Minimal Transformation**: Direct data mapping from Snowflake

## Use Cases

1. **Team Management**: Understanding organizational structure
2. **Resource Allocation**: Assigning work items to teams
3. **Reporting**: Team-based analytics and dashboards
4. **Access Control**: Role-based permissions by team
5. **Hierarchy Analysis**: Parent-child team relationships

## Future Enhancements

- Integration with user assignment tables
- Team performance metrics
- Workload distribution analysis
- Dynamic team structure visualization 