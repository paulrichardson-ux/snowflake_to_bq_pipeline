#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project) # Or set explicitly 'red-octane-444308-f4'
REGION="us-central1" # Or your preferred region
FUNCTION_NAME="sync-full-work-item-budget-vs-actual-to-bq" # Updated Function Name
SOURCE_DIR="./work_item_budget_vs_actual_sync_full" # Updated Source Dir
ENTRY_POINT="sync_full_work_item_budget_vs_actual" # Updated Entry Point
RUNTIME="python311" # Match the runtime from the other script
MEMORY="512MB" # Reduced memory allocation
TIMEOUT="900s"  # Increased timeout for potentially longer full sync (15 mins)
SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com" # Use the same SA
BQ_DATASET="karbon_data"

# Environment variables needed by the function
ENV_VARS="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${BQ_DATASET}"

echo "Deploying Cloud Function: ${FUNCTION_NAME}..."
echo "  Source: ${SOURCE_DIR}"
echo "  Entry Point: ${ENTRY_POINT}"
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
  --max-instances=3 \
  --project=${PROJECT_ID} --service-account=${SERVICE_ACCOUNT} --set-env-vars=${ENV_VARS}

echo "${FUNCTION_NAME} deployed successfully."
echo "Target BQ Table: ${PROJECT_ID}.${BQ_DATASET}.WORK_ITEM_BUDGET_VS_ACTUAL_BQ" 