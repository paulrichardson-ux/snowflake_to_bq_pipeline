#!/bin/bash

# Unified Pipeline Deployment Script
# ==================================
# This script deploys the unified pipeline to Google Cloud Functions

set -e

# Configuration
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
FUNCTION_NAME="unified-snowflake-bq-pipeline"
DATASET_ID="karbon_data"
SERVICE_ACCOUNT="pipeline-service-account@${PROJECT_ID}.iam.gserviceaccount.com"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Deploying Unified Snowflake to BigQuery Pipeline${NC}"
echo "=================================================="

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}❌ gcloud CLI is not installed. Please install it first.${NC}"
    exit 1
fi

# Set the project
echo -e "${YELLOW}Setting project to ${PROJECT_ID}...${NC}"
gcloud config set project ${PROJECT_ID}

# Enable required APIs
echo -e "${YELLOW}Enabling required APIs...${NC}"
gcloud services enable cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com \
    cloudscheduler.googleapis.com \
    logging.googleapis.com \
    monitoring.googleapis.com \
    clouderrorreporting.googleapis.com

# Create service account if it doesn't exist
echo -e "${YELLOW}Creating service account...${NC}"
gcloud iam service-accounts create pipeline-service-account \
    --display-name="Pipeline Service Account" \
    --description="Service account for unified pipeline" \
    2>/dev/null || echo "Service account already exists"

# Grant necessary permissions
echo -e "${YELLOW}Granting IAM permissions...${NC}"
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.dataEditor" \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.jobUser" \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/logging.logWriter" \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/monitoring.metricWriter" \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/clouderrorreporting.writer" \
    --quiet

# Deploy the main pipeline function
echo -e "${YELLOW}Deploying main pipeline function...${NC}"
gcloud functions deploy ${FUNCTION_NAME} \
    --gen2 \
    --region=${REGION} \
    --runtime=python311 \
    --source=. \
    --entry-point=pipeline_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=${SERVICE_ACCOUNT} \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${DATASET_ID}" \
    --memory=2GB \
    --timeout=540s \
    --max-instances=10 \
    --min-instances=0 \
    --concurrency=1

# Deploy batch pipeline function
echo -e "${YELLOW}Deploying batch pipeline function...${NC}"
gcloud functions deploy ${FUNCTION_NAME}-batch \
    --gen2 \
    --region=${REGION} \
    --runtime=python311 \
    --source=. \
    --entry-point=batch_pipeline_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=${SERVICE_ACCOUNT} \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${DATASET_ID}" \
    --memory=4GB \
    --timeout=540s \
    --max-instances=5 \
    --min-instances=0 \
    --concurrency=1

# Deploy status function
echo -e "${YELLOW}Deploying status function...${NC}"
gcloud functions deploy ${FUNCTION_NAME}-status \
    --gen2 \
    --region=${REGION} \
    --runtime=python311 \
    --source=. \
    --entry-point=pipeline_status_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=${SERVICE_ACCOUNT} \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${DATASET_ID}" \
    --memory=512MB \
    --timeout=60s \
    --max-instances=5

# Get function URLs
MAIN_URL=$(gcloud functions describe ${FUNCTION_NAME} --region=${REGION} --format='value(serviceConfig.uri)')
BATCH_URL=$(gcloud functions describe ${FUNCTION_NAME}-batch --region=${REGION} --format='value(serviceConfig.uri)')
STATUS_URL=$(gcloud functions describe ${FUNCTION_NAME}-status --region=${REGION} --format='value(serviceConfig.uri)')

echo -e "${GREEN}✅ Deployment completed successfully!${NC}"
echo ""
echo "Function URLs:"
echo "=============="
echo "Main Pipeline:   ${MAIN_URL}"
echo "Batch Pipeline:  ${BATCH_URL}"
echo "Status:          ${STATUS_URL}"
echo ""
echo "Example usage:"
echo "============="
echo "# Run a single pipeline:"
echo "curl -X POST ${MAIN_URL} \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"pipeline\": \"client_dimension\"}'"
echo ""
echo "# Run multiple pipelines:"
echo "curl -X POST ${BATCH_URL} \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"pipelines\": [\"client_dimension\", \"user_dimension\"], \"parallel\": true}'"
echo ""
echo "# Check status:"
echo "curl ${STATUS_URL}"