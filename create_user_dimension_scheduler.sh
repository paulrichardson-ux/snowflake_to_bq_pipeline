#!/bin/bash

# Create Cloud Scheduler job for USER_DIMENSION daily sync
JOB_NAME="user-dimension-daily-sync"
FUNCTION_NAME="user-dimension-sync-daily"
REGION="us-central1"
PROJECT_ID="red-octane-444308-f4"
SCHEDULE="0 8 * * *"  # Daily at 8 AM UTC (2 hours after CLIENT_DIMENSION)
TIMEZONE="UTC"

echo "Creating Cloud Scheduler job for USER_DIMENSION daily sync..."

gcloud scheduler jobs create http $JOB_NAME \
  --location=$REGION \
  --schedule="$SCHEDULE" \
  --time-zone=$TIMEZONE \
  --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"source": "scheduler"}' \
  --description="Daily sync of USER_DIMENSION from Snowflake to BigQuery"

if [ $? -eq 0 ]; then
    echo "✅ Cloud Scheduler job '$JOB_NAME' created successfully!"
    echo "Schedule: Daily at 8 AM UTC"
    echo "Function: $FUNCTION_NAME"
else
    echo "❌ Scheduler job creation failed!"
    exit 1
fi

echo ""
echo "To manually trigger the job:"
echo "gcloud scheduler jobs run $JOB_NAME --location=$REGION" 