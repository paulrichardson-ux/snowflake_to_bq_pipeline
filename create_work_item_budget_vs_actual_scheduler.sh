#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
SCHEDULER_NAME="work-item-budget-vs-actual-daily-sync"
FUNCTION_NAME="sync-work-item-budget-vs-actual-daily-to-bq"
SCHEDULE="0 2 * * *"  # Daily at 2:00 AM UTC
TIME_ZONE="UTC"
DESCRIPTION="Daily sync for Work Item Budget vs Actual data from Snowflake to BigQuery"

# Function URL
FUNCTION_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"

echo "Creating Cloud Scheduler job: ${SCHEDULER_NAME}"
echo "  Function: ${FUNCTION_NAME}"
echo "  Schedule: ${SCHEDULE} (${TIME_ZONE})"
echo "  URL: ${FUNCTION_URL}"

# Create the scheduler job
gcloud scheduler jobs create http ${SCHEDULER_NAME} \
  --location=${REGION} \
  --schedule="${SCHEDULE}" \
  --time-zone="${TIME_ZONE}" \
  --uri="${FUNCTION_URL}" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{}' \
  --description="${DESCRIPTION}" \
  --project=${PROJECT_ID}

echo "Scheduler job ${SCHEDULER_NAME} created successfully."
echo "The job will run daily at 2:00 AM UTC to sync Work Item Budget vs Actual data." 