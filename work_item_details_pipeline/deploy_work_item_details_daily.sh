#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project) # Or set explicitly 'red-octane-444308-f4'
REGION="us-central1" # Or your preferred region
FUNCTION_NAME="sync-work-item-details-daily-to-bq"
SOURCE_DIR="./work_item_details_pipeline/work_item_details_sync_daily" # Relative path from where script is run (likely workspace root)
ENTRY_POINT="sync_daily_incremental"
RUNTIME="python311" # Match the runtime from the other script
MEMORY="1024MB" # Increased memory slightly, can adjust
TIMEOUT="900s"  # Increased timeout to match full sync (15 mins) for handling backlogs
SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com" # Use the same SA
BQ_DATASET="karbon_data"

# Environment variables needed by the function
ENV_VARS="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${BQ_DATASET}"

echo "Deploying Cloud Function: ${FUNCTION_NAME}..."
echo "  Source: ${SOURCE_DIR}"
echo "  Dataset: ${BQ_DATASET}"
echo "  Project: ${PROJECT_ID}"
echo "  Service Account: ${SERVICE_ACCOUNT}"


gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=${RUNTIME} \
  --region=${REGION} \
  --source=${SOURCE_DIR} \
  --entry-point=${ENTRY_POINT} \
  --trigger-http \
  --memory=${MEMORY} \
  --timeout=${TIMEOUT} \
  --min-instances=0 \
  --max-instances=5 \
  --project=${PROJECT_ID} \
  --service-account=${SERVICE_ACCOUNT} \
  --set-env-vars=${ENV_VARS}

echo "${FUNCTION_NAME} deployed successfully."
echo "Target BQ Table: ${PROJECT_ID}.${BQ_DATASET}.WORK_ITEM_DETAILS_BQ" 