#!/bin/bash

# Create Cloud Scheduler job for Tenant Team Dimension daily sync

echo "Creating Cloud Scheduler job for Tenant Team Dimension daily sync..."

gcloud scheduler jobs create http tenant-team-dimension-daily-sync \
    --location=us-central1 \
    --schedule="0 6 * * *" \
    --uri="https://us-central1-red-octane-444308-f4.cloudfunctions.net/tenant-team-dimension-sync-daily" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"source": "scheduler"}' \
    --description="Daily sync of TENANT_TEAM dimension from Snowflake to BigQuery"

echo "Tenant Team Dimension scheduler created successfully!"
echo "Schedule: Daily at 6:00 AM UTC (same as CLIENT_DIMENSION)"
echo ""
echo "To manually trigger the sync:"
echo "gcloud scheduler jobs run tenant-team-dimension-daily-sync --location=us-central1" 