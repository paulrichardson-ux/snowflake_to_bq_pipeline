# Pipeline Schema Verification Instructions

This document provides instructions for verifying that your Snowflake to BigQuery pipelines are up to date with the current Snowflake schemas.

## Prerequisites

1. **Google Cloud Project**: Set your project ID environment variable
   ```bash
   export GOOGLE_CLOUD_PROJECT="red-octane-444308-f4"
   # OR
   export GCP_PROJECT="red-octane-444308-f4"
   ```

2. **Snowflake Secrets**: Ensure the following secrets exist in Google Secret Manager:
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`

3. **Python Dependencies**: Install required packages
   ```bash
   pip install -r verify_requirements.txt
   ```

## Running the Verification

### Step 1: Execute the Schema Verification Script
```bash
python3 verify_schemas.py
```

### Step 2: Interpret the Results

The script will output detailed comparisons for each table:

#### ✅ Perfect Match
```
✅ SCHEMAS MATCH PERFECTLY!
   Total columns: 35
```

#### ⚠️ Schema Issues Found
```
❌ MISSING COLUMNS in Snowflake table:
   - NEW_COLUMN (STRING)

⚠️ EXTRA COLUMNS in Snowflake table (not in pipeline):
   - DEPRECATED_COLUMN (VARCHAR)

⚠️ TYPE MISMATCHES:
┌─────────────────┬─────────────────┬───────────────┐
│ Column          │ Snowflake Type  │ Expected Type │
├─────────────────┼─────────────────┼───────────────┤
│ BUDGET_AMOUNT   │ NUMERIC         │ FLOAT64       │
└─────────────────┴─────────────────┴───────────────┘
```

### Step 3: Address Any Issues

#### For Missing Columns
1. **Update pipeline code** to add the new columns to the schema definition
2. **Redeploy** the affected Cloud Functions
3. **Run a full sync** to populate the new columns

#### For Extra Columns  
1. **Update pipeline code** to include the new columns
2. **Redeploy** the Cloud Functions
3. **Run a full sync** to sync the additional data

#### For Type Mismatches
1. **Review data types** in both systems
2. **Update pipeline mappings** if needed
3. **Consider data transformation** requirements
4. **Test with sample data** before full deployment

## Manual Schema Inspection

### Check Snowflake Schema Directly
```sql
-- Connect to Snowflake and run:
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'DIMN_CLIENT'
  AND table_schema = 'YOUR_SCHEMA_NAME'
ORDER BY ordinal_position;
```

### Check BigQuery Schema
```sql
-- In BigQuery, run:
SELECT column_name, data_type, is_nullable
FROM `red-octane-444308-f4.karbon_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'DIMN_CLIENT'
ORDER BY ordinal_position;
```

## Common Issues and Solutions

### Issue: "Secret not found"
**Solution**: Verify secret names and ensure the service account has access
```bash
gcloud secrets list
gcloud secrets versions access latest --secret="SNOWFLAKE_USER"
```

### Issue: "Connection timeout to Snowflake"
**Solution**: Check network connectivity and credentials
- Verify VPC/firewall rules if running from GCP
- Test Snowflake connectivity from your environment
- Confirm account name format (e.g., `account.region.cloud`)

### Issue: "Table not found"
**Solution**: Verify table names and schema
- Check if table exists: `SHOW TABLES LIKE 'DIMN_CLIENT'`
- Verify schema name is correct
- Ensure proper permissions to read the table

### Issue: "Pipeline deployment fails after schema updates"
**Solution**: Update pipeline configurations
1. Check Cloud Function memory/timeout settings
2. Verify BigQuery dataset exists and has proper permissions
3. Test with smaller batch sizes initially

## Next Steps After Verification

1. **Document any schema changes** found
2. **Update pipeline code** to handle new/changed columns
3. **Update documentation** to reflect current schemas
4. **Schedule regular verification** (monthly/quarterly)
5. **Consider implementing automated schema drift detection**

## Pipeline Update Workflow

When schema changes are detected:

1. **Development**: Update and test pipeline code locally
2. **Staging**: Deploy to staging environment and test
3. **Production**: 
   - Deploy updated Cloud Functions
   - Run full sync to populate new data
   - Monitor for errors
   - Verify data integrity

## Troubleshooting

### Enable Debug Logging
Add debugging to the verification script:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Check Cloud Function Logs
```bash
gcloud functions logs read sync-full-client-dimension-to-bq --limit=50
```

### Verify Service Account Permissions
```bash
gcloud projects get-iam-policy red-octane-444308-f4 \
  --filter="bindings.members:karbon-bq-sync@red-octane-444308-f4.iam.gserviceaccount.com"
```