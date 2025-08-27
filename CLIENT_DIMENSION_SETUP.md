# CLIENT_DIMENSION Pipeline Setup

This pipeline syncs the `DIMN_CLIENT` table from Snowflake to the `CLIENT_DIMENSION` table in BigQuery as a direct mirror.

## Components Created

### 1. BigQuery Table Structure
- **Table**: `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION`
- **Type**: Direct mirror of Snowflake `DIMN_CLIENT` table
- **Schema**: 35 columns exactly matching Snowflake structure
- **No partitioning**: Simple dimension table

### 2. Cloud Function
- **Name**: `client-dimension-sync-daily`
- **Runtime**: Python 3.11
- **Memory**: 1024MB
- **Timeout**: 540s (9 minutes)
- **Trigger**: HTTP (for both manual and scheduled execution)

### 3. Cloud Scheduler
- **Job Name**: `client-dimension-daily-sync`
- **Schedule**: Daily at 6:00 AM UTC
- **Method**: Full replace (truncate and insert)

## Deployment Instructions

### Step 1: Deploy the Cloud Function
```bash
cd "Karbon Big Query"
cd snowflake_to_bq_pipeline
./deploy_client_dimension_sync.sh
```

### Step 2: Create the Daily Scheduler
```bash
./create_client_dimension_scheduler.sh
```

## Manual Execution

### Trigger via HTTP
```bash
curl -X POST https://us-central1-red-octane-444308-f4.cloudfunctions.net/client-dimension-sync-daily \
     -H "Content-Type: application/json" \
     -d '{"source": "manual"}'
```

### Trigger via Scheduler
```bash
gcloud scheduler jobs run client-dimension-daily-sync --location=us-central1
```

## Data Usage

### Access Client Data
```sql
SELECT * FROM `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION`
WHERE CLIENT_TYPE = 'Client Organization'
ORDER BY CLIENT DESC;
```

### Monitor Data Status
```sql
SELECT 
    COUNT(*) as total_clients,
    COUNT(CASE WHEN CLIENT_TYPE = 'Client Organization' THEN 1 END) as organizations,
    COUNT(CASE WHEN CLIENT_TYPE = 'Client Individual' THEN 1 END) as individuals
FROM `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION`;
```

## Key Features

1. **Direct Mirror**: Table exactly matches Snowflake `DIMN_CLIENT`
2. **Full Replace**: Daily truncate and replace operation
3. **Batch Processing**: Handles large datasets efficiently (1000 records per batch)
4. **Simple Structure**: No versioning or tracking fields
5. **Error Handling**: Comprehensive logging and error recovery

## Sync Process

The function performs these steps daily:
1. **Fetch** all records from Snowflake `DIMN_CLIENT` in batches
2. **Load** data into temporary BigQuery table
3. **Truncate** the target CLIENT_DIMENSION table
4. **Insert** all data from temporary table
5. **Clean up** temporary table

## Current Data (as of last sync)

- **Total Clients**: 249
- **Organizations**: 131 
- **Individuals**: 113
- **Accounts**: All associated with FISKAL

## Monitoring

- Check Cloud Function logs in GCP Console
- Monitor BigQuery job history
- Verify record counts after sync
- Use direct table queries for reporting

## Schema Details

The table includes all Snowflake DIMN_CLIENT fields:
- Client identification (ID, name, type, subtype)
- Contact information (email, phone, address)
- Ownership and management details
- Account associations
- **No tracking fields** - pure dimension data

## Troubleshooting

1. **Function Timeout**: Increase timeout or reduce batch size
2. **Schema Mismatch**: Function auto-adapts to Snowflake schema changes
3. **Permission Issues**: Ensure service account has BigQuery and Secret Manager access
4. **Data Quality**: Check Snowflake `DIMN_CLIENT` table for data issues

## Important Notes

- ⚠️ **Full Replace**: Each sync completely replaces the table
- 📊 **Direct Mirror**: Table structure matches Snowflake exactly
- 🔄 **Daily Sync**: Ensures data freshness without versioning overhead
- 📈 **Simple Queries**: Direct access without need for "latest" views 