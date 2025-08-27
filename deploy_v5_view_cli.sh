#!/bin/bash

# =============================================================================
# CLI DEPLOYMENT SCRIPT: Work Item Individual Budget Time Tracking View V5
# =============================================================================
# This script deploys the V5 view using BigQuery CLI tools

set -e  # Exit on any error

# Configuration
PROJECT_ID="red-octane-444308-f4"
DATASET_ID="karbon_data"
VIEW_NAME="WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5"

echo "🚀 DEPLOYING V5 INDIVIDUAL BUDGET VIEW VIA CLI"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_ID"
echo "View: $VIEW_NAME"
echo ""

# Check if bq CLI is installed and authenticated
echo "🔍 Checking BigQuery CLI setup..."
if ! command -v bq &> /dev/null; then
    echo "❌ ERROR: 'bq' command not found. Please install Google Cloud SDK."
    exit 1
fi

echo "✅ BigQuery CLI found"

# Test authentication
echo "🔐 Testing BigQuery authentication..."
if ! bq ls --project_id=$PROJECT_ID > /dev/null 2>&1; then
    echo "❌ ERROR: Authentication failed. Please run 'gcloud auth login' and 'gcloud auth application-default login'"
    exit 1
fi

echo "✅ Authentication successful"

# Deploy the view
echo "📋 Deploying WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5..."

# Use temp file to avoid command line parsing issues
TEMP_SQL=$(mktemp)
cat work_item_individual_budget_time_tracking_view_v5.sql > "$TEMP_SQL"

bq query \
    --project_id=$PROJECT_ID \
    --use_legacy_sql=false \
    --replace \
    < "$TEMP_SQL"

# Clean up temp file
rm "$TEMP_SQL"

echo "✅ View deployment completed!"

# Test the view
echo "🧪 Testing the deployed view..."

bq query \
    --project_id=$PROJECT_ID \
    --use_legacy_sql=false \
    "SELECT 
       COUNT(*) as total_records,
       COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
       COUNT(DISTINCT budget_user_id) as unique_users_with_budgets,
       COUNT(DISTINCT CLIENT) as unique_clients
     FROM \`$PROJECT_ID.$DATASET_ID.$VIEW_NAME\`"

echo ""
echo "🎯 Testing May 2025 individual budgets..."

bq query \
    --project_id=$PROJECT_ID \
    --use_legacy_sql=false \
    "SELECT 
       budget_user_name,
       COUNT(DISTINCT WORK_ITEM_ID) as work_items_with_budget,
       ROUND(SUM(individual_budgeted_hours), 2) as total_individual_budget_hours,
       ROUND(SUM(individual_hours_logged_actual), 2) as total_individual_logged_hours
     FROM \`$PROJECT_ID.$DATASET_ID.$VIEW_NAME\`
     WHERE EXTRACT(YEAR FROM DUE_DATETIME) = 2025 
       AND EXTRACT(MONTH FROM DUE_DATETIME) = 5
       AND budget_user_name IS NOT NULL
     GROUP BY budget_user_name
     ORDER BY total_individual_budget_hours DESC
     LIMIT 10"

echo ""
echo "🎉 V5 VIEW DEPLOYMENT COMPLETE!"
echo "================================"
echo ""
echo "✅ Your new view is ready: $PROJECT_ID.$DATASET_ID.$VIEW_NAME"
echo ""
echo "📊 KEY FEATURES:"
echo "   • Individual user budget allocations per work item"
echo "   • Actual time tracking per user per work item"  
echo "   • Individual budget vs actual analysis"
echo "   • All V4 fields preserved for work item context"
echo ""
echo "🔍 SAMPLE QUERIES:"
echo "   • Individual budgets: SELECT * FROM \`$PROJECT_ID.$DATASET_ID.$VIEW_NAME\` WHERE has_individual_budget = true"
echo "   • May 2025 user budgets: SELECT budget_user_name, SUM(individual_budgeted_hours) FROM ... WHERE due_year = 2025 AND due_month = 5"
echo "   • Budget utilization by user: SELECT budget_user_name, AVG(individual_budget_utilization_percentage) FROM ..."
echo "" 