#!/bin/bash

# Create Cloud Scheduler job for Client Group Dimension daily sync

echo "Creating Cloud Scheduler job for Client Group Dimension daily sync..."

gcloud scheduler jobs create http client-group-dimension-daily-sync \
    --location=us-central1 \
    --schedule="30 6 * * *" \
    --uri="https://us-central1-red-octane-444308-f4.cloudfunctions.net/client-group-dimension-sync-daily" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"source": "scheduler"}' \
    --description="Daily sync of CLIENT_GROUP dimension from Snowflake to BigQuery"

echo "Client Group Dimension scheduler created successfully!"
echo "Schedule: Daily at 6:30 AM UTC (30 minutes after CLIENT_DIMENSION)"
echo ""
echo "To manually trigger the sync:"
echo "gcloud scheduler jobs run client-group-dimension-daily-sync --location=us-central1" 