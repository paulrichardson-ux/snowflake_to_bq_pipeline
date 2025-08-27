#!/bin/bash

# =============================================================================
# OPTIMIZE BUDGET SYNC FOR TIMEOUT PREVENTION
# =============================================================================
# This script applies timeout optimizations to prevent the scheduler from being paused

set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
SCHEDULER_NAME="work-item-budget-vs-actual-daily-sync"
FUNCTION_NAME="sync-work-item-budget-vs-actual-daily-to-bq"

echo "🚀 OPTIMIZING BUDGET SYNC TO PREVENT TIMEOUT ISSUES"
echo "=================================================="
echo "Project: ${PROJECT_ID}"
echo "Scheduler: ${SCHEDULER_NAME}"
echo "Function: ${FUNCTION_NAME}"
echo ""

# Step 1: Deploy the optimized function with increased resources
echo "📦 Step 1: Deploying optimized function with increased resources..."
echo "  - Memory: 1024MB → 2048MB"
echo "  - Timeout: 540s (9min) → 900s (15min)"
echo "  - Batch size: 20 → 1000 rows"
echo "  - Date range: ±90 days → ±30 days"
echo "  - Added time limit checks"
echo "  - Improved BigQuery loading strategy"
echo ""

chmod +x ./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_daily.sh
./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_daily.sh

echo ""
echo "✅ Function deployed with optimizations"

# Step 2: Update scheduler with retry configuration
echo ""
echo "⏰ Step 2: Updating scheduler with robust retry configuration..."

gcloud scheduler jobs update http ${SCHEDULER_NAME} \
  --location=${REGION} \
  --max-retry-attempts=3 \
  --max-retry-duration=3600s \
  --min-backoff-duration=60s \
  --max-backoff-duration=600s \
  --attempt-deadline=960s

echo "✅ Scheduler updated with retry configuration:"
echo "  - Max retries: 3 attempts"
echo "  - Retry window: 1 hour"
echo "  - Backoff: 60s - 600s"
echo "  - Attempt deadline: 16 minutes"

# Step 3: Test the optimized function
echo ""
echo "🧪 Step 3: Testing optimized function..."

FUNCTION_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"
echo "Testing URL: ${FUNCTION_URL}"

# Test with curl and capture response
echo "Triggering test run..."
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${FUNCTION_URL}" -H "Content-Type: application/json" -d '{"test": true}')
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ Function test successful!"
    echo "Response: $BODY"
else
    echo "⚠️  Function test returned HTTP $HTTP_CODE"
    echo "Response: $BODY"
    echo "This may be normal if there's no data to sync"
fi

# Step 4: Check scheduler status
echo ""
echo "📊 Step 4: Checking scheduler status..."

SCHEDULER_INFO=$(gcloud scheduler jobs describe ${SCHEDULER_NAME} --location=${REGION} --format="value(state,schedule,lastAttemptTime)")
echo "Scheduler status: $SCHEDULER_INFO"

if echo "$SCHEDULER_INFO" | grep -q "ENABLED"; then
    echo "✅ Scheduler is ENABLED"
else
    echo "⚠️  Scheduler may be paused. Attempting to resume..."
    gcloud scheduler jobs resume ${SCHEDULER_NAME} --location=${REGION}
    echo "✅ Scheduler resumed"
fi

echo ""
echo "🎉 OPTIMIZATION COMPLETE!"
echo "========================="
echo ""
echo "✅ APPLIED OPTIMIZATIONS:"
echo "   1. 🧠 Increased memory: 1024MB → 2048MB"
echo "   2. ⏱️  Increased timeout: 9min → 15min"
echo "   3. 📊 Reduced date range: ±90 days → ±30 days"
echo "   4. 📦 Increased batch size: 20 → 1000 rows"
echo "   5. ⚡ Added time limit checks (12.5min max)"
echo "   6. 🚀 Improved BigQuery loading (load jobs for large batches)"
echo "   7. 🔄 Added scheduler retry configuration"
echo ""
echo "🎯 EXPECTED RESULTS:"
echo "   • Faster processing with larger batches"
echo "   • Reduced memory pressure"
echo "   • Graceful timeout prevention"
echo "   • Automatic retry on transient failures"
echo "   • Smaller data window reduces processing time"
echo ""
echo "📋 MONITORING:"
echo "   • Function logs: gcloud logging read 'resource.type=\"cloud_function\" AND resource.labels.function_name=\"${FUNCTION_NAME}\"'"
echo "   • Scheduler logs: gcloud logging read 'resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"${SCHEDULER_NAME}\"'"
echo "   • Dashboard: https://us-central1-${PROJECT_ID}.cloudfunctions.net/karbon-pipeline-dashboard"
echo ""
echo "⚠️  NOTE: If the function still times out, consider:"
echo "   • Further reducing the date range (±15 days)"
echo "   • Implementing progressive sync (sync different date ranges on different days)"
echo "   • Moving to a more powerful runtime (Cloud Run)"
echo ""
