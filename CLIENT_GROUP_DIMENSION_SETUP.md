# CLIENT_GROUP_DIMENSION Pipeline Setup

This pipeline syncs the `DIMN_CLIENT_GROUP` table from Snowflake to the `CLIENT_GROUP_DIMENSION` table in BigQuery as a direct mirror.

## Components Created

### 1. BigQuery Table Structure
- **Table**: `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION`
- **Type**: Direct mirror of Snowflake `DIMN_CLIENT_GROUP` table
- **Schema**: 7 columns matching Snowflake structure
- **No partitioning**: Simple dimension table

### 2. Cloud Function
- **Name**: `client-group-dimension-sync-daily`
- **Runtime**: Python 3.11
- **Memory**: 1024MB
- **Timeout**: 540s (9 minutes)
- **Trigger**: HTTP (for both manual and scheduled execution)

### 3. Cloud Scheduler
- **Job Name**: `client-group-dimension-daily-sync`
- **Schedule**: Daily at 6:30 AM UTC (30 minutes after CLIENT_DIMENSION)
- **Method**: Full replace (truncate and insert)

## Deployment Instructions

### Step 1: Deploy the Cloud Function
```bash
cd "Karbon Big Query"
cd snowflake_to_bq_pipeline
./deploy_client_group_dimension_sync.sh
```

### Step 2: Create the Daily Scheduler
```bash
./create_client_group_dimension_scheduler.sh
```

## Manual Execution

### Trigger via HTTP
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/client-group-dimension-sync-daily \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger via Scheduler
```bash
gcloud scheduler jobs run client-group-dimension-daily-sync --location=us-central1
```

## Data Schema

The table includes all Snowflake DIMN_CLIENT_GROUP fields:

### Core Fields
- **CLIENT_GROUP_ID** - Unique identifier for the client group
- **CLIENT_GROUP_NAME** - Name of the client group (e.g., "RSA Clients", "USA Client", "Fiskal")
- **CLIENT_ID** - Individual client identifier within the group
- **CLIENT** - Client name or organization name
- **CLIENT_GROUP_MEMBER_TYPE** - Type of member (Client Individual, Client Organization)
- **ACCOUNT_ID** - Associated account identifier
- **ACCOUNT_NAME** - Associated account name

## Data Usage

### Access Client Group Data
```sql
SELECT * FROM `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION`
ORDER BY CLIENT_GROUP_NAME, CLIENT;
```

### Monitor Data Status
```sql
SELECT 
    COUNT(*) as total_memberships,
    COUNT(DISTINCT CLIENT_GROUP_ID) as unique_groups,
    COUNT(DISTINCT CLIENT_ID) as unique_clients,
    COUNT(DISTINCT ACCOUNT_ID) as unique_accounts
FROM `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION`;
```

### Client Group Analysis
```sql
SELECT 
    CLIENT_GROUP_NAME,
    COUNT(*) as member_count,
    COUNT(CASE WHEN CLIENT_GROUP_MEMBER_TYPE = 'Client Individual' THEN 1 END) as individuals,
    COUNT(CASE WHEN CLIENT_GROUP_MEMBER_TYPE = 'Client Organization' THEN 1 END) as organizations,
    ACCOUNT_NAME
FROM `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION`
GROUP BY CLIENT_GROUP_NAME, ACCOUNT_NAME
ORDER BY member_count DESC;
```

### Geographic Distribution
```sql
SELECT 
    CLIENT_GROUP_NAME,
    COUNT(*) as total_members,
    ACCOUNT_NAME
FROM `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION`
WHERE CLIENT_GROUP_NAME IN ('RSA Clients', 'USA Client')
GROUP BY CLIENT_GROUP_NAME, ACCOUNT_NAME
ORDER BY CLIENT_GROUP_NAME;
```

### Client Group Membership Details
```sql
SELECT 
    cg.CLIENT_GROUP_NAME,
    cg.CLIENT,
    cg.CLIENT_GROUP_MEMBER_TYPE,
    c.CLIENT_TYPE,
    c.CLIENT_SUBTYPE,
    c.CLIENT_PRIMARY_EMAIL_ADDRESS
FROM `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION` cg
LEFT JOIN `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION` c 
    ON cg.CLIENT_ID = c.CLIENT_ID
WHERE cg.CLIENT_GROUP_NAME = 'RSA Clients'
ORDER BY cg.CLIENT;
```

## Key Features

1. **Direct Mirror**: Table exactly matches Snowflake `DIMN_CLIENT_GROUP`
2. **Full Replace**: Daily truncate and replace operation
3. **Batch Processing**: Handles datasets efficiently (1000 records per batch)
4. **Simple Structure**: No versioning or tracking fields
5. **Error Handling**: Comprehensive logging and error recovery
6. **Auto-Schema Detection**: Adapts to Snowflake schema changes

## Sync Process

The function performs these steps daily:
1. **Fetch** all records from Snowflake `DIMN_CLIENT_GROUP` in batches
2. **Load** data into temporary BigQuery table
3. **Truncate** the target CLIENT_GROUP_DIMENSION table
4. **Insert** all data from temporary table
5. **Clean up** temporary table

## Current Data (as of setup)

Based on schema sample:
- **Groups**: RSA Clients, USA Client, Fiskal
- **Total Memberships**: ~108 client group assignments
- **Account**: All associated with FISKAL
- **Member Types**: Client Individual, Client Organization

## Monitoring

- Check Cloud Function logs in GCP Console
- Monitor BigQuery job history
- Verify record counts after sync
- Use direct table queries for reporting

## Integration with Other Pipelines

### Links to Other Tables
- **CLIENT_DIMENSION**: Direct join via CLIENT_ID
- **TENANT_TEAM_DIMENSION**: Can be related via ACCOUNT_ID
- **USER_TIME_ENTRY_BQ**: May link via client assignments
- **WORK_ITEM_DETAILS_BQ**: Can be related through client groupings

### Common Join Patterns
```sql
-- Clients with their group memberships
SELECT 
    c.CLIENT,
    c.CLIENT_TYPE,
    cg.CLIENT_GROUP_NAME,
    cg.CLIENT_GROUP_MEMBER_TYPE
FROM `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION` c
LEFT JOIN `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION` cg 
    ON c.CLIENT_ID = cg.CLIENT_ID
ORDER BY cg.CLIENT_GROUP_NAME, c.CLIENT;
```

```sql
-- Group-based client analytics
SELECT 
    cg.CLIENT_GROUP_NAME,
    COUNT(DISTINCT cg.CLIENT_ID) as unique_clients,
    COUNT(DISTINCT wi.WORK_ITEM_ID) as work_items,
    SUM(te.HOURS_LOGGED) as total_hours
FROM `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION` cg
LEFT JOIN `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` wi 
    ON cg.CLIENT_ID = wi.CLIENT_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ` te 
    ON wi.WORK_ITEM_ID = te.WORK_ITEM_ID
GROUP BY cg.CLIENT_GROUP_NAME
ORDER BY total_hours DESC;
```

## Troubleshooting

1. **Function Timeout**: Increase timeout or reduce batch size
2. **Schema Mismatch**: Function auto-adapts to Snowflake schema changes
3. **Permission Issues**: Ensure service account has BigQuery and Secret Manager access
4. **Data Quality**: Check Snowflake `DIMN_CLIENT_GROUP` table for data issues

## Important Notes

- ⚠️ **Full Replace**: Each sync completely replaces the table
- 📊 **Direct Mirror**: Table structure matches Snowflake exactly
- 🔄 **Daily Sync**: Ensures data freshness without versioning overhead
- 📈 **Simple Queries**: Direct access without need for "latest" views
- 🏢 **Group Management**: Supports client categorization and segmentation
- ⏰ **Scheduled After CLIENT_DIMENSION**: Runs 30 minutes after client sync to ensure data consistency

## Schema Details

The table includes all Snowflake DIMN_CLIENT_GROUP fields:
- Client group identification (ID, name)
- Individual client assignments (ID, name, type)
- Account associations
- **No tracking fields** - pure dimension data

## Performance Considerations

- **Medium Dataset**: Client group memberships are manageable datasets
- **Fast Sync**: Full replacement is efficient for dimension tables
- **No Partitioning**: Simple table structure for direct queries
- **Minimal Transformation**: Direct data mapping from Snowflake

## Use Cases

1. **Client Segmentation**: Understanding client groupings (RSA vs USA vs Internal)
2. **Regional Analytics**: Geographic-based reporting and analysis
3. **Group-based Reporting**: Aggregated metrics by client groups
4. **Account Management**: Understanding client relationships and hierarchies
5. **Business Intelligence**: Group-level dashboards and insights

## Future Enhancements

- Integration with CRM systems
- Group-based performance metrics
- Dynamic group assignment rules
- Enhanced geographic analytics
- Client lifecycle tracking by groups
``` 