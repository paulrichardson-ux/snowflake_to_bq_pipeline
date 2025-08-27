#!/bin/bash

# =============================================================================
# DEPLOY SNOWFLAKE-BIGQUERY DEDUPLICATION SYNC
# =============================================================================
# This script deploys the deduplication sync Cloud Function

set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
FUNCTION_NAME="snowflake-bq-deduplication-sync"
RUNTIME="python311"
MEMORY="1GB"
TIMEOUT="900s"  # 15 minutes for comprehensive sync
SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🚀 Deploying Snowflake-BigQuery Deduplication Sync..."
echo "  Function: ${FUNCTION_NAME}"
echo "  Project: ${PROJECT_ID}"
echo "  Region: ${REGION}"
echo "  Service Account: ${SERVICE_ACCOUNT}"

# Create a temporary directory for the function
TEMP_DIR=$(mktemp -d)
echo "📁 Creating function package in: ${TEMP_DIR}"

# Copy the main script
cp snowflake_bq_deduplication_sync.py "${TEMP_DIR}/main.py"

# Create requirements.txt
cat > "${TEMP_DIR}/requirements.txt" << EOF
google-cloud-bigquery>=3.4.0
google-cloud-secret-manager>=2.16.0
snowflake-connector-python>=3.0.0
EOF

echo "📦 Function package created"

# Deploy the function
echo "🚀 Deploying Cloud Function..."

gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=${RUNTIME} \
  --region=${REGION} \
  --source=${TEMP_DIR} \
  --entry-point=deduplication_sync_cloud_function \
  --trigger-http \
  --memory=${MEMORY} \
  --timeout=${TIMEOUT} \
  --min-instances=0 \
  --max-instances=1 \
  --project=${PROJECT_ID} \
  --service-account=${SERVICE_ACCOUNT} \
  --allow-unauthenticated

echo "✅ Cloud Function deployed successfully!"

# Clean up
rm -rf "${TEMP_DIR}"

# Create scheduler job for weekly deduplication
echo "⏰ Creating weekly scheduler job..."

SCHEDULER_JOB_NAME="snowflake-bq-deduplication-weekly"
FUNCTION_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"

# Check if scheduler job already exists
if gcloud scheduler jobs describe ${SCHEDULER_JOB_NAME} --location=${REGION} &>/dev/null; then
    echo "⚠️ Scheduler job already exists, updating..."
    gcloud scheduler jobs update http ${SCHEDULER_JOB_NAME} \
        --location=${REGION} \
        --schedule="0 2 * * 0" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT}" \
        --message-body='{"dry_run": false, "days_back": 30}'
else
    echo "📅 Creating new scheduler job..."
    gcloud scheduler jobs create http ${SCHEDULER_JOB_NAME} \
        --location=${REGION} \
        --schedule="0 2 * * 0" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT}" \
        --message-body='{"dry_run": false, "days_back": 30}'
fi

echo ""
echo "🎉 DEDUPLICATION SYNC DEPLOYMENT COMPLETE!"
echo "==========================================="
echo ""
echo "✅ Function URL: ${FUNCTION_URL}"
echo "✅ Scheduler: Weekly on Sunday at 2:00 AM UTC"
echo "✅ Service Account: ${SERVICE_ACCOUNT}"
echo ""
echo "📊 FEATURES:"
echo "   • Validates data consistency between Snowflake and BigQuery"
echo "   • Removes orphaned BigQuery records not found in Snowflake"
echo "   • Provides detailed reporting on cleanup actions"
echo "   • Handles all core tables automatically"
echo ""
echo "🔍 USAGE:"
echo "   • Manual trigger (dry run): curl -X POST '${FUNCTION_URL}' -d '{\"dry_run\": true}'"
echo "   • Manual trigger (live): curl -X POST '${FUNCTION_URL}' -d '{\"dry_run\": false}'"
echo "   • Validate specific item: curl -X POST '${FUNCTION_URL}' -d '{\"work_item_id\": \"JvqmhFJBFGP\"}'"
echo "   • Check logs: gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=${FUNCTION_NAME}'"
echo ""
echo "⚠️ SAFETY:"
echo "   • Defaults to dry run mode for safety"
echo "   • Weekly automated cleanup on Sundays"
echo "   • Comprehensive validation before deletion"
echo ""
