# Snowflake to BigQuery Data Pipeline

A comprehensive ETL pipeline system that synchronizes data from Snowflake to BigQuery for Fiskal's business intelligence and reporting needs.

## Architecture Overview

This project consists of five main data synchronization pipelines:

1. **Client Dimension Pipeline** - Syncs customer/client data
2. **Client Group Dimension Pipeline** - Syncs client group memberships and categorization
3. **Tenant Team Dimension Pipeline** - Syncs organizational team structure
4. **Work Item Details Pipeline** - Syncs project work items and tasks
5. **Time Details Pipeline** - Syncs time entries and productivity data

Each pipeline supports both full synchronization and incremental daily updates.

## Project Structure

```
snowflake_to_bq_pipeline/
├── client_dimension_pipeline/          # Client data sync
│   ├── client_dimension_sync_full/     # Full sync implementation
│   └── deploy_client_dimension_full.sh # Deployment script
├── client_dimension_sync_daily/        # Daily incremental sync
├── client_group_dimension_pipeline/    # Client group sync
│   └── client_group_dimension_sync_full/ # Full sync implementation
├── client_group_dimension_sync_daily/  # Daily client group sync
├── tenant_team_dimension_pipeline/     # Team structure sync
│   └── tenant_team_dimension_sync_full/ # Full sync implementation
├── tenant_team_dimension_sync_daily/   # Daily team sync
├── work_item_details_pipeline/         # Work item sync
│   ├── work_item_details_sync_full/    # Full sync implementation
│   ├── work_item_details_sync_daily/   # Daily incremental sync
│   └── work_item_x_user_time_view.sql  # BigQuery views
├── snowflake_bq_sync Time details/     # Time entry sync (full)
├── snowflake_bq_sync_daily Time Details/ # Time entry sync (daily)
└── Schema examples/                    # Sample data structures
```

## Key Features

- **Multi-Pipeline Architecture**: Separate pipelines for different data types
- **Dual Sync Modes**: Full replacement and incremental updates
- **Auto-Schema Detection**: Automatically adapts to Snowflake schema changes
- **Error Handling**: Comprehensive logging and retry mechanisms
- **Batch Processing**: Efficient handling of large datasets
- **Secret Management**: Secure credential handling via Google Secret Manager

## Technology Stack

- **Cloud Platform**: Google Cloud Platform
- **Compute**: Cloud Functions (Python 3.11)
- **Data Warehouse**: BigQuery
- **Source Database**: Snowflake
- **Orchestration**: Cloud Scheduler
- **Security**: Secret Manager

## Data Flow

1. **Extract**: Fetch data from Snowflake tables using batch processing
2. **Transform**: Minimal transformation - mostly direct data mapping
3. **Load**: Insert into BigQuery with automatic schema detection
4. **Schedule**: Automated daily runs via Cloud Scheduler

## Getting Started

### Prerequisites

- Google Cloud Project with BigQuery and Cloud Functions enabled
- Snowflake account with appropriate permissions
- `gcloud` CLI configured

### Quick Setup

1. **Deploy Client Dimension Pipeline:**
   ```bash
   ./deploy_client_dimension_sync.sh
   ./create_client_dimension_scheduler.sh
   ```

2. **Deploy Client Group Pipeline:**
   ```bash
   ./deploy_client_group_dimension_sync.sh
   ./create_client_group_dimension_scheduler.sh
   ```

3. **Deploy Tenant Team Pipeline:**
   ```bash
   ./deploy_tenant_team_dimension_sync.sh
   ./create_tenant_team_dimension_scheduler.sh
   ```

4. **Deploy Work Item Pipeline:**
   ```bash
   ./work_item_details_pipeline/deploy_work_item_details_full.sh
   ./work_item_details_pipeline/deploy_work_item_details_daily.sh
   ```

3. **Configure Secrets:**
   - Set up Snowflake credentials in Secret Manager
   - Configure BigQuery permissions

## Pipeline Details

### Client Dimension Pipeline
- **Source**: `DIMN_CLIENT` table in Snowflake
- **Target**: `CLIENT_DIMENSION` table in BigQuery
- **Frequency**: Daily full replacement
- **Documentation**: See `CLIENT_DIMENSION_SETUP.md`

### Client Group Dimension Pipeline
- **Source**: `DIMN_CLIENT_GROUP` table in Snowflake
- **Target**: `CLIENT_GROUP_DIMENSION` table in BigQuery
- **Frequency**: Daily full replacement
- **Documentation**: See `CLIENT_GROUP_DIMENSION_SETUP.md`

### Tenant Team Dimension Pipeline
- **Source**: `DIMN_TENANT_TEAM` table in Snowflake
- **Target**: `TENANT_TEAM_DIMENSION` table in BigQuery
- **Frequency**: Daily full replacement
- **Documentation**: See `TENANT_TEAM_DIMENSION_SETUP.md`

### Work Item Details Pipeline
- **Source**: Work item tables in Snowflake
- **Target**: `WORK_ITEM_DETAILS_BQ` table in BigQuery
- **Frequency**: Daily incremental updates
- **Features**: Duplicate detection and cleanup

### Time Details Pipeline
- **Source**: Time entry tables in Snowflake
- **Target**: `USER_TIME_ENTRY_BQ` table in BigQuery
- **Frequency**: Daily incremental updates
- **Features**: Duplicate cleanup, productivity metrics

## Monitoring & Maintenance

### Health Checks
```bash
# Check recent sync status
bq query --use_legacy_sql=false "
SELECT 
  COUNT(*) as total_records,
  MAX(CAST(LAST_MODIFIED_TIME AS TIMESTAMP)) as last_updated
FROM \`red-octane-444308-f4.karbon_data.CLIENT_DIMENSION\`
"
```

### Common Issues
- **Schema Mismatches**: Functions auto-adapt to Snowflake schema changes
- **Timeout Errors**: Increase Cloud Function timeout or reduce batch size
- **Permission Issues**: Verify Secret Manager and BigQuery permissions

## Development

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"

# Run tests
python -m pytest tests/
```

### Adding New Pipelines
1. Create new directory with `main.py` and `requirements.txt`
2. Implement sync logic following existing patterns
3. Create deployment scripts
4. Add to Cloud Scheduler

## Contributing

1. Follow existing code patterns
2. Update documentation for new features
3. Test thoroughly before deployment
4. Use meaningful commit messages

## Support

For issues or questions:
- Check Cloud Function logs in GCP Console
- Review BigQuery job history
- Consult individual pipeline documentation
- Monitor Cloud Scheduler job status

## License

Internal use only - Fiskal Finance 