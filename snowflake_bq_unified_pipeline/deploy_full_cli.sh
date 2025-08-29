#!/bin/bash

# Full CLI Deployment Script for Unified Pipeline
# ================================================
# This script deploys everything from scratch via CLI

set -e

# Configuration
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
LOCATION="US"  # BigQuery dataset location
NEW_DATASET_ID="unified_pipeline_data"  # New dataset for unified pipeline
FUNCTION_NAME="unified-snowflake-bq-pipeline"
SERVICE_ACCOUNT_NAME="unified-pipeline-sa"
TIME_ZONE="America/Los_Angeles"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}   🚀 UNIFIED PIPELINE FULL DEPLOYMENT via CLI${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Function to check if command succeeded
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Success${NC}"
    else
        echo -e "${RED}❌ Failed${NC}"
        exit 1
    fi
}

# Function to print section headers
print_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Step 1: Set up project and enable APIs
print_section "STEP 1: Setting up Project and APIs"

echo -e "${YELLOW}→ Setting project to ${PROJECT_ID}...${NC}"
gcloud config set project ${PROJECT_ID}
check_status

echo -e "${YELLOW}→ Enabling required Google Cloud APIs...${NC}"
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com \
    cloudscheduler.googleapis.com \
    logging.googleapis.com \
    monitoring.googleapis.com \
    clouderrorreporting.googleapis.com \
    artifactregistry.googleapis.com \
    run.googleapis.com \
    eventarc.googleapis.com \
    pubsub.googleapis.com \
    --project=${PROJECT_ID}
check_status

# Step 2: Create new BigQuery dataset
print_section "STEP 2: Creating BigQuery Dataset"

echo -e "${YELLOW}→ Creating new BigQuery dataset: ${NEW_DATASET_ID}...${NC}"

# Check if dataset exists
if bq ls -d --project_id=${PROJECT_ID} | grep -w ${NEW_DATASET_ID} > /dev/null 2>&1; then
    echo -e "${GREEN}  Dataset ${NEW_DATASET_ID} already exists${NC}"
else
    bq mk \
        --project_id=${PROJECT_ID} \
        --location=${LOCATION} \
        --description="Unified pipeline data storage" \
        --dataset ${NEW_DATASET_ID}
    check_status
    echo -e "${GREEN}  Created dataset: ${PROJECT_ID}:${NEW_DATASET_ID}${NC}"
fi

# Step 3: Create service account
print_section "STEP 3: Setting up Service Account"

SERVICE_ACCOUNT="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo -e "${YELLOW}→ Creating service account: ${SERVICE_ACCOUNT_NAME}...${NC}"
if gcloud iam service-accounts describe ${SERVICE_ACCOUNT} --project=${PROJECT_ID} > /dev/null 2>&1; then
    echo -e "${GREEN}  Service account already exists${NC}"
else
    gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
        --display-name="Unified Pipeline Service Account" \
        --description="Service account for unified Snowflake to BigQuery pipeline" \
        --project=${PROJECT_ID}
    check_status
fi

echo -e "${YELLOW}→ Granting IAM permissions to service account...${NC}"

# BigQuery permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.dataEditor" \
    --condition=None \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.jobUser" \
    --condition=None \
    --quiet

# Secret Manager permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --condition=None \
    --quiet

# Logging and monitoring permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/logging.logWriter" \
    --condition=None \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/monitoring.metricWriter" \
    --condition=None \
    --quiet

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/clouderrorreporting.writer" \
    --condition=None \
    --quiet

echo -e "${GREEN}  IAM permissions granted${NC}"

# Step 4: Check and create secrets if needed
print_section "STEP 4: Verifying Secret Manager Secrets"

echo -e "${YELLOW}→ Checking required secrets...${NC}"

REQUIRED_SECRETS=(
    "SNOWFLAKE_USER"
    "SNOWFLAKE_PASSWORD"
    "SNOWFLAKE_ACCOUNT"
    "SNOWFLAKE_WAREHOUSE"
    "SNOWFLAKE_DATABASE"
    "SNOWFLAKE_SCHEMA"
)

MISSING_SECRETS=()

for secret in "${REQUIRED_SECRETS[@]}"; do
    if gcloud secrets describe ${secret} --project=${PROJECT_ID} > /dev/null 2>&1; then
        echo -e "${GREEN}  ✓ ${secret} exists${NC}"
    else
        echo -e "${RED}  ✗ ${secret} is missing${NC}"
        MISSING_SECRETS+=("${secret}")
    fi
done

if [ ${#MISSING_SECRETS[@]} -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}The following secrets need to be created:${NC}"
    for secret in "${MISSING_SECRETS[@]}"; do
        echo "  - ${secret}"
    done
    echo ""
    echo -e "${YELLOW}Please create them using:${NC}"
    echo -e "${BLUE}echo 'YOUR_VALUE' | gcloud secrets create SECRET_NAME --data-file=- --project=${PROJECT_ID}${NC}"
    echo ""
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Step 5: Update configuration file with new dataset
print_section "STEP 5: Updating Configuration"

echo -e "${YELLOW}→ Updating pipeline_config.yaml with new dataset...${NC}"

# Update the config file to use the new dataset
cat > /workspace/snowflake_bq_unified_pipeline/config/pipeline_config.yaml << EOF
# Unified Pipeline Configuration
# This centralized configuration manages all pipeline settings

project:
  gcp_project_id: "${PROJECT_ID}"
  bigquery_dataset: "${NEW_DATASET_ID}"
  region: "${REGION}"

snowflake:
  warehouse: "COMPUTE_WH"
  database: "KARBON"
  schema: "PUBLIC"
  connection_timeout: 300
  query_timeout: 600
  
# Pipeline definitions with their specific configurations
pipelines:
  client_dimension:
    source_table: "DIMN_CLIENT"
    target_table: "CLIENT_DIMENSION"
    primary_key: "CLIENT_ID"
    sync_type: "full"
    batch_size: 5000
    schedule: "0 8 * * *"
    
  client_group_dimension:
    source_table: "DIMN_CLIENT_GROUP"
    target_table: "CLIENT_GROUP_DIMENSION"
    primary_key: "CLIENT_GROUP_ID"
    sync_type: "full"
    batch_size: 5000
    schedule: "30 6 * * *"
    
  tenant_team_dimension:
    source_table: "DIMN_TENANT_TEAM"
    target_table: "TENANT_TEAM_DIMENSION"
    primary_key: "TEAM_ID"
    sync_type: "full"
    batch_size: 5000
    schedule: "0 6 * * *"
    
  tenant_team_member_dimension:
    source_table: "DIMN_TENANT_TEAM_MEMBER"
    target_table: "TENANT_TEAM_MEMBER_DIMENSION"
    primary_key: "TEAM_MEMBER_ID"
    sync_type: "full"
    batch_size: 5000
    schedule: "0 7 * * *"
    
  user_dimension:
    source_table: "DIMN_USER"
    target_table: "USER_DIMENSION"
    primary_key: "USER_ID"
    sync_type: "full"
    batch_size: 5000
    schedule: "0 8 * * *"
    
  work_item_details:
    source_table: "WORK_ITEM_DETAILS"
    target_table: "WORK_ITEM_DETAILS_BQ"
    primary_key: "WORK_ITEM_ID"
    sync_type: "incremental"
    batch_size: 10000
    schedule: "30 6 * * *"
    incremental_column: "LAST_MODIFIED_TIME"
    lookback_days: 7
    
  work_item_budget_vs_actual:
    source_table: "WORK_ITEM_BUDGET_VS_ACTUAL"
    target_table: "WORK_ITEM_BUDGET_VS_ACTUAL_BQ"
    primary_key: "WORK_ITEM_ID"
    sync_type: "full"
    batch_size: 10000
    schedule: "0 6 * * *"
    
  user_time_entry:
    source_table: "USER_TIME_ENTRY_DETAIL"
    target_table: "USER_TIME_ENTRY_BQ"
    primary_key: "TIME_ENTRY_ID"
    sync_type: "incremental"
    batch_size: 10000
    schedule: "0 2 * * *"
    incremental_column: "REPORTING_DATE"
    lookback_days: 30

# Performance settings
performance:
  max_parallel_batches: 5
  connection_pool_size: 3
  retry_attempts: 3
  retry_delay_seconds: 5
  temp_table_expiration_hours: 2

# Monitoring settings
monitoring:
  enable_metrics: true
  enable_structured_logging: true
  alert_on_failure: true
  slack_webhook_secret: "SLACK_WEBHOOK_URL"
  
# Data quality settings
data_quality:
  enable_deduplication: true
  enable_validation: true
  validation_threshold_percent: 5
EOF

echo -e "${GREEN}  Configuration updated${NC}"

# Step 6: Deploy Cloud Functions
print_section "STEP 6: Deploying Cloud Functions"

cd /workspace/snowflake_bq_unified_pipeline

echo -e "${YELLOW}→ Deploying main pipeline function...${NC}"
gcloud functions deploy ${FUNCTION_NAME} \
    --gen2 \
    --region=${REGION} \
    --runtime=python311 \
    --source=. \
    --entry-point=pipeline_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=${SERVICE_ACCOUNT} \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${NEW_DATASET_ID}" \
    --memory=2GB \
    --timeout=540s \
    --max-instances=10 \
    --min-instances=0 \
    --concurrency=1 \
    --project=${PROJECT_ID}
check_status

echo -e "${YELLOW}→ Deploying batch pipeline function...${NC}"
gcloud functions deploy ${FUNCTION_NAME}-batch \
    --gen2 \
    --region=${REGION} \
    --runtime=python311 \
    --source=. \
    --entry-point=batch_pipeline_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=${SERVICE_ACCOUNT} \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${NEW_DATASET_ID}" \
    --memory=4GB \
    --timeout=540s \
    --max-instances=5 \
    --min-instances=0 \
    --concurrency=1 \
    --project=${PROJECT_ID}
check_status

echo -e "${YELLOW}→ Deploying status monitoring function...${NC}"
gcloud functions deploy ${FUNCTION_NAME}-status \
    --gen2 \
    --region=${REGION} \
    --runtime=python311 \
    --source=. \
    --entry-point=pipeline_status_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=${SERVICE_ACCOUNT} \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${NEW_DATASET_ID}" \
    --memory=512MB \
    --timeout=60s \
    --max-instances=5 \
    --project=${PROJECT_ID}
check_status

# Step 7: Get function URLs
print_section "STEP 7: Retrieving Function URLs"

MAIN_URL=$(gcloud functions describe ${FUNCTION_NAME} --region=${REGION} --format='value(serviceConfig.uri)' --project=${PROJECT_ID})
BATCH_URL=$(gcloud functions describe ${FUNCTION_NAME}-batch --region=${REGION} --format='value(serviceConfig.uri)' --project=${PROJECT_ID})
STATUS_URL=$(gcloud functions describe ${FUNCTION_NAME}-status --region=${REGION} --format='value(serviceConfig.uri)' --project=${PROJECT_ID})

# Step 8: Create Cloud Scheduler jobs
print_section "STEP 8: Creating Cloud Scheduler Jobs"

echo -e "${YELLOW}→ Creating scheduler jobs for automated pipeline runs...${NC}"

# Function to create scheduler job
create_scheduler() {
    local JOB_NAME=$1
    local SCHEDULE=$2
    local PIPELINE=$3
    local DESCRIPTION=$4
    
    echo -e "${YELLOW}  Creating ${JOB_NAME}...${NC}"
    
    # Delete if exists
    gcloud scheduler jobs delete ${JOB_NAME} \
        --location=${REGION} \
        --project=${PROJECT_ID} \
        --quiet 2>/dev/null || true
    
    # Create new job
    gcloud scheduler jobs create http ${JOB_NAME} \
        --location=${REGION} \
        --schedule="${SCHEDULE}" \
        --uri="${MAIN_URL}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body="{\"pipeline\": \"${PIPELINE}\"}" \
        --time-zone="${TIME_ZONE}" \
        --description="${DESCRIPTION}" \
        --attempt-deadline=540s \
        --max-retry-attempts=3 \
        --project=${PROJECT_ID}
}

# Create individual pipeline schedulers
create_scheduler "unified-client-dimension-daily" "0 8 * * *" "client_dimension" "Daily CLIENT_DIMENSION sync"
create_scheduler "unified-user-dimension-daily" "0 8 * * *" "user_dimension" "Daily USER_DIMENSION sync"
create_scheduler "unified-work-item-details-daily" "30 6 * * *" "work_item_details" "Daily WORK_ITEM_DETAILS sync"
create_scheduler "unified-user-time-entry-daily" "0 2 * * *" "user_time_entry" "Daily USER_TIME_ENTRY sync"

echo -e "${GREEN}  Scheduler jobs created${NC}"

# Step 9: Create test script
print_section "STEP 9: Creating Test Script"

cat > /workspace/snowflake_bq_unified_pipeline/test_pipeline.sh << 'EOFTEST'
#!/bin/bash

# Test script for unified pipeline
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
FUNCTION_NAME="unified-snowflake-bq-pipeline"

echo "🧪 Testing Unified Pipeline"
echo "=========================="

# Get function URL
FUNCTION_URL=$(gcloud functions describe ${FUNCTION_NAME} --region=${REGION} --format='value(serviceConfig.uri)' --project=${PROJECT_ID})

echo "Testing client_dimension pipeline..."
curl -X POST ${FUNCTION_URL} \
  -H 'Content-Type: application/json' \
  -d '{"pipeline": "client_dimension", "dry_run": true}' \
  -w "\nHTTP Status: %{http_code}\n"

echo ""
echo "To run a full sync (not dry run), use:"
echo "curl -X POST ${FUNCTION_URL} \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"pipeline\": \"client_dimension\"}'"
EOFTEST

chmod +x /workspace/snowflake_bq_unified_pipeline/test_pipeline.sh

# Step 10: Summary
print_section "DEPLOYMENT COMPLETE! 🎉"

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ DEPLOYMENT SUCCESSFUL${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}📊 Deployment Summary:${NC}"
echo -e "  • Project ID:        ${PROJECT_ID}"
echo -e "  • Region:            ${REGION}"
echo -e "  • Dataset:           ${NEW_DATASET_ID}"
echo -e "  • Service Account:   ${SERVICE_ACCOUNT}"
echo ""
echo -e "${BLUE}🔗 Function URLs:${NC}"
echo -e "  • Main Pipeline:     ${MAIN_URL}"
echo -e "  • Batch Pipeline:    ${BATCH_URL}"
echo -e "  • Status Monitor:    ${STATUS_URL}"
echo ""
echo -e "${BLUE}📅 Scheduled Jobs:${NC}"
gcloud scheduler jobs list --location=${REGION} --filter="name:unified-*" --format="table(name,schedule,timeZone)" --project=${PROJECT_ID}
echo ""
echo -e "${BLUE}🧪 Test Commands:${NC}"
echo -e "  • Run test script:   ${GREEN}./test_pipeline.sh${NC}"
echo -e "  • Check status:      ${GREEN}curl ${STATUS_URL}${NC}"
echo -e "  • View logs:         ${GREEN}gcloud functions logs read ${FUNCTION_NAME} --region=${REGION}${NC}"
echo ""
echo -e "${BLUE}📚 Next Steps:${NC}"
echo -e "  1. Run test script to validate deployment"
echo -e "  2. Monitor first scheduled runs"
echo -e "  3. Check BigQuery tables in dataset: ${NEW_DATASET_ID}"
echo -e "  4. Review logs for any issues"
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"