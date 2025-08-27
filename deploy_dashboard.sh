#!/bin/bash

# =============================================================================
# DEPLOY KARBON PIPELINE DASHBOARD
# =============================================================================
# This script deploys a comprehensive web dashboard for monitoring all
# pipeline functions, schedulers, and their real-time status

set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
FUNCTION_NAME="karbon-pipeline-dashboard"
RUNTIME="python311"
MEMORY="1GB"
TIMEOUT="60s"
SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🚀 Deploying Karbon Pipeline Dashboard..."
echo "  Function: ${FUNCTION_NAME}"
echo "  Project: ${PROJECT_ID}"
echo "  Region: ${REGION}"
echo "  Service Account: ${SERVICE_ACCOUNT}"

# Check if dashboard directory exists
if [ ! -d "dashboard" ]; then
    echo "❌ Error: dashboard directory not found!"
    echo "Please ensure you're running this script from the pipeline root directory."
    exit 1
fi

# Deploy the dashboard function
echo "🚀 Deploying Cloud Function..."

gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=${RUNTIME} \
  --region=${REGION} \
  --source=dashboard \
  --entry-point=pipeline_dashboard \
  --trigger-http \
  --memory=${MEMORY} \
  --timeout=${TIMEOUT} \
  --min-instances=0 \
  --max-instances=3 \
  --project=${PROJECT_ID} \
  --service-account=${SERVICE_ACCOUNT} \
  --allow-unauthenticated \
  --set-env-vars=GOOGLE_CLOUD_PROJECT=${PROJECT_ID}

echo "✅ Dashboard deployed successfully!"

# Get the function URL
FUNCTION_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"

echo ""
echo "🎉 KARBON PIPELINE DASHBOARD DEPLOYMENT COMPLETE!"
echo "=================================================="
echo ""
echo "✅ Dashboard URL: ${FUNCTION_URL}"
echo "✅ Service Account: ${SERVICE_ACCOUNT}"
echo "✅ Auto-refresh: Every 30 seconds"
echo ""
echo "📊 DASHBOARD FEATURES:"
echo "   • Real-time function status monitoring"
echo "   • Scheduler job health tracking"
echo "   • Last run and next run times"
echo "   • Visual status indicators"
echo "   • Auto-refresh capability"
echo "   • Mobile-responsive design"
echo ""
echo "🔍 MONITORING CAPABILITIES:"
echo "   • All Cloud Functions status"
echo "   • All Scheduler jobs status"
echo "   • Pipeline health overview"
echo "   • Real-time updates"
echo ""
echo "🌐 ACCESS:"
echo "   Open in browser: ${FUNCTION_URL}"
echo "   API endpoint: ${FUNCTION_URL}/api/status"
echo ""
echo "🔧 MANAGEMENT:"
echo "   • View logs: gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=${FUNCTION_NAME}'"
echo "   • Update function: Re-run this script"
echo "   • Delete function: gcloud functions delete ${FUNCTION_NAME} --region=${REGION}"
echo ""

# Test the deployment
echo "🧪 Testing dashboard deployment..."
if curl -s -o /dev/null -w "%{http_code}" "${FUNCTION_URL}" | grep -q "200"; then
    echo "✅ Dashboard is responding successfully!"
else
    echo "⚠️ Dashboard may need a moment to initialize. Try accessing it in a few seconds."
fi

echo ""
echo "🎯 NEXT STEPS:"
echo "1. Open ${FUNCTION_URL} in your browser"
echo "2. Bookmark the URL for easy access"
echo "3. The dashboard will auto-refresh every 30 seconds"
echo "4. Use the refresh button for immediate updates"
echo ""
echo "📱 The dashboard is fully responsive and works on mobile devices!"
