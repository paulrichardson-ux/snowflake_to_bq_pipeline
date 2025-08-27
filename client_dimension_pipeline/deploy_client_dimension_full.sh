#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project) # Or set explicitly 'red-octane-444308-f4'
REGION="us-central1" # Or your preferred region
FUNCTION_NAME="sync-full-client-dimension-to-bq"
SOURCE_DIR="./client_dimension_pipeline/client_dimension_sync_full"
ENTRY_POINT="sync_full_client_dimension"
RUNTIME="python311"
MEMORY="512MB"
TIMEOUT="900s"  # 15 minutes timeout
SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com"
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
echo "Target BQ Table: ${PROJECT_ID}.${BQ_DATASET}.DIMN_CLIENT" 