#!/bin/bash

# Cloud Scheduler Setup Script
# ============================
# This script creates Cloud Scheduler jobs for all pipelines

set -e

# Configuration
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
FUNCTION_NAME="unified-snowflake-bq-pipeline"
TIME_ZONE="America/Los_Angeles"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}📅 Setting up Cloud Scheduler jobs for Unified Pipeline${NC}"
echo "======================================================"

# Get function URL
FUNCTION_URL=$(gcloud functions describe ${FUNCTION_NAME} --region=${REGION} --format='value(serviceConfig.uri)')

if [ -z "$FUNCTION_URL" ]; then
    echo "❌ Error: Could not get function URL. Make sure the function is deployed."
    exit 1
fi

echo -e "${YELLOW}Function URL: ${FUNCTION_URL}${NC}"
echo ""

# Function to create or update a scheduler job
create_scheduler_job() {
    local JOB_NAME=$1
    local SCHEDULE=$2
    local PIPELINE=$3
    local DESCRIPTION=$4
    
    echo -e "${YELLOW}Creating scheduler job: ${JOB_NAME}${NC}"
    
    # Delete existing job if it exists
    gcloud scheduler jobs delete ${JOB_NAME} \
        --location=${REGION} \
        --quiet 2>/dev/null || true
    
    # Create new job
    gcloud scheduler jobs create http ${JOB_NAME} \
        --location=${REGION} \
        --schedule="${SCHEDULE}" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body="{\"pipeline\": \"${PIPELINE}\"}" \
        --time-zone="${TIME_ZONE}" \
        --description="${DESCRIPTION}" \
        --attempt-deadline=540s \
        --max-retry-attempts=3
    
    echo "✅ Created ${JOB_NAME}"
    echo ""
}

# Create scheduler jobs for each pipeline

# Dimension tables (full sync)
create_scheduler_job \
    "unified-client-dimension-daily" \
    "0 8 * * *" \
    "client_dimension" \
    "Daily sync of CLIENT_DIMENSION table"

create_scheduler_job \
    "unified-client-group-dimension-daily" \
    "30 6 * * *" \
    "client_group_dimension" \
    "Daily sync of CLIENT_GROUP_DIMENSION table"

create_scheduler_job \
    "unified-tenant-team-dimension-daily" \
    "0 6 * * *" \
    "tenant_team_dimension" \
    "Daily sync of TENANT_TEAM_DIMENSION table"

create_scheduler_job \
    "unified-tenant-team-member-dimension-daily" \
    "0 7 * * *" \
    "tenant_team_member_dimension" \
    "Daily sync of TENANT_TEAM_MEMBER_DIMENSION table"

create_scheduler_job \
    "unified-user-dimension-daily" \
    "0 8 * * *" \
    "user_dimension" \
    "Daily sync of USER_DIMENSION table"

# Fact tables (incremental sync)
create_scheduler_job \
    "unified-work-item-details-daily" \
    "30 6 * * *" \
    "work_item_details" \
    "Daily incremental sync of WORK_ITEM_DETAILS"

create_scheduler_job \
    "unified-work-item-budget-daily" \
    "0 6 * * *" \
    "work_item_budget_vs_actual" \
    "Daily sync of WORK_ITEM_BUDGET_VS_ACTUAL"

create_scheduler_job \
    "unified-user-time-entry-daily" \
    "0 2 * * *" \
    "user_time_entry" \
    "Daily incremental sync of USER_TIME_ENTRY"

# Create a batch job for all dimension tables
echo -e "${YELLOW}Creating batch scheduler job for all dimensions${NC}"

BATCH_URL=$(gcloud functions describe ${FUNCTION_NAME}-batch --region=${REGION} --format='value(serviceConfig.uri)')

gcloud scheduler jobs delete unified-all-dimensions-daily \
    --location=${REGION} \
    --quiet 2>/dev/null || true

gcloud scheduler jobs create http unified-all-dimensions-daily \
    --location=${REGION} \
    --schedule="0 9 * * *" \
    --uri="${BATCH_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{
        "pipelines": [
            "client_dimension",
            "client_group_dimension",
            "tenant_team_dimension",
            "tenant_team_member_dimension",
            "user_dimension"
        ],
        "parallel": true
    }' \
    --time-zone="${TIME_ZONE}" \
    --description="Batch sync of all dimension tables" \
    --attempt-deadline=540s \
    --max-retry-attempts=2

echo "✅ Created unified-all-dimensions-daily"
echo ""

# List all scheduler jobs
echo -e "${GREEN}📋 All scheduler jobs:${NC}"
gcloud scheduler jobs list --location=${REGION} --filter="name:unified-*"

echo ""
echo -e "${GREEN}✅ Scheduler setup completed!${NC}"
echo ""
echo "Commands:"
echo "========="
echo "# List all jobs:"
echo "gcloud scheduler jobs list --location=${REGION}"
echo ""
echo "# Run a job manually:"
echo "gcloud scheduler jobs run unified-client-dimension-daily --location=${REGION}"
echo ""
echo "# Pause a job:"
echo "gcloud scheduler jobs pause unified-client-dimension-daily --location=${REGION}"
echo ""
echo "# Resume a job:"
echo "gcloud scheduler jobs resume unified-client-dimension-daily --location=${REGION}"