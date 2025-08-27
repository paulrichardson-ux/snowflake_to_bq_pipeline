#!/bin/bash

# =============================================================================
# DEPLOY PIPELINE FALLBACK MONITOR
# =============================================================================
# This script deploys the pipeline monitoring and fallback Cloud Function

set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
FUNCTION_NAME="pipeline-fallback-monitor"
RUNTIME="python311"
MEMORY="512MB"
TIMEOUT="300s"  # 5 minutes should be enough for monitoring
SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com"
BQ_DATASET="karbon_data"

# Environment variables
ENV_VARS="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BQ_DATASET=${BQ_DATASET},FUNCTION_REGION=${REGION}"

echo "🚀 Deploying Pipeline Fallback Monitor..."
echo "  Function: ${FUNCTION_NAME}"
echo "  Project: ${PROJECT_ID}"
echo "  Region: ${REGION}"
echo "  Service Account: ${SERVICE_ACCOUNT}"

# Create a temporary directory for the function
TEMP_DIR=$(mktemp -d)
echo "📁 Creating function package in: ${TEMP_DIR}"

# Copy the main script
cp pipeline_fallback_monitor.py "${TEMP_DIR}/main.py"

# Create requirements.txt
cat > "${TEMP_DIR}/requirements.txt" << EOF
google-cloud-bigquery>=3.4.0
google-cloud-functions>=1.8.0
requests>=2.28.0
EOF

echo "📦 Function package created"

# Deploy the function
echo "🚀 Deploying Cloud Function..."

gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=${RUNTIME} \
  --region=${REGION} \
  --source=${TEMP_DIR} \
  --entry-point=pipeline_fallback_monitor \
  --trigger-http \
  --memory=${MEMORY} \
  --timeout=${TIMEOUT} \
  --min-instances=0 \
  --max-instances=1 \
  --project=${PROJECT_ID} \
  --service-account=${SERVICE_ACCOUNT} \
  --set-env-vars=${ENV_VARS}

echo "✅ Cloud Function deployed successfully!"

# Clean up
rm -rf "${TEMP_DIR}"

# Create scheduler job for daily monitoring
echo "⏰ Creating daily scheduler job..."

SCHEDULER_JOB_NAME="pipeline-fallback-monitor-daily"
FUNCTION_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"

# Check if scheduler job already exists
if gcloud scheduler jobs describe ${SCHEDULER_JOB_NAME} --location=${REGION} &>/dev/null; then
    echo "⚠️ Scheduler job already exists, updating..."
    gcloud scheduler jobs update http ${SCHEDULER_JOB_NAME} \
        --location=${REGION} \
        --schedule="0 10 * * *" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT}" \
        --headers="Content-Type=application/json" \
        --message-body='{"source": "daily_scheduler", "check_type": "full_monitoring"}'
else
    echo "📅 Creating new scheduler job..."
    gcloud scheduler jobs create http ${SCHEDULER_JOB_NAME} \
        --location=${REGION} \
        --schedule="0 10 * * *" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT}" \
        --headers="Content-Type=application/json" \
        --message-body='{"source": "daily_scheduler", "check_type": "full_monitoring"}'
fi

echo ""
echo "🎉 PIPELINE MONITOR DEPLOYMENT COMPLETE!"
echo "========================================"
echo ""
echo "✅ Function URL: ${FUNCTION_URL}"
echo "✅ Scheduler: Daily at 10:00 AM UTC"
echo "✅ Service Account: ${SERVICE_ACCOUNT}"
echo ""
echo "📊 FEATURES:"
echo "   • Monitors data freshness across key tables"
echo "   • Automatically triggers fallback syncs when needed"
echo "   • Sends structured alerts via Cloud Logging"
echo "   • Configurable thresholds (1 day warning, 3 days critical)"
echo ""
echo "🔍 MONITORING:"
echo "   • View logs: gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=${FUNCTION_NAME}'"
echo "   • Manual trigger: curl -X POST '${FUNCTION_URL}' -H 'Content-Type: application/json' -d '{\"source\": \"manual\"}'"
echo "   • Check scheduler: gcloud scheduler jobs describe ${SCHEDULER_JOB_NAME} --location=${REGION}"
echo ""
echo "⚠️ ALERT LEVELS:"
echo "   • OK: Data is current (≤1 day behind)"
echo "   • WARNING: Data is 1-3 days behind"
echo "   • CRITICAL: Data is >3 days behind (triggers fallback)"
echo "" 