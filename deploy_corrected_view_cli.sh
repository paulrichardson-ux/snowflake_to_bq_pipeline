#!/bin/bash

# =============================================================================
# CLI DEPLOYMENT SCRIPT: Work Item Budget vs Actual Corrected View
# =============================================================================
# This script deploys the corrected view using BigQuery CLI tools

set -e  # Exit on any error

# Configuration
PROJECT_ID="red-octane-444308-f4"
DATASET_ID="karbon_data"
VIEW_NAME="work_item_budget_vs_actual_corrected_view"

echo "🚀 DEPLOYING CORRECTED VIEW VIA CLI"
echo "====================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_ID"
echo "View: $VIEW_NAME"
echo ""

# Check if bq CLI is installed and authenticated
echo "🔍 Checking BigQuery CLI setup..."
if ! command -v bq &> /dev/null; then
    echo "❌ ERROR: 'bq' command not found!"
    echo "📥 Install Google Cloud SDK first:"
    echo "   https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check authentication
echo "🔐 Checking authentication..."
if ! bq ls --project_id=$PROJECT_ID $DATASET_ID &> /dev/null; then
    echo "❌ ERROR: Not authenticated or no access to project/dataset!"
    echo "🔐 Run: gcloud auth login"
    echo "🔐 And: gcloud config set project $PROJECT_ID"
    exit 1
fi

echo "✅ CLI setup verified!"
echo ""

# Create temporary SQL file
TEMP_SQL_FILE="/tmp/corrected_view_deployment.sql"

echo "📝 Creating deployment SQL..."
cat > "$TEMP_SQL_FILE" << 'EOF'
CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view` AS
SELECT 
  -- All fields from WORK_ITEM_BUDGET_VS_ACTUAL_BQ
  bva.REPORTING_DATE as sync_reporting_date,  -- Keep original sync date for reference
  bva.WORK_ITEM_ID,
  bva.WORK_TITLE,
  bva.WORK_TYPE_ID,
  bva.WORK_TYPE,
  bva.TASK_TYPE_ID,
  bva.TASK_TYPE,
  bva.TASK_TYPE_BILLABLE_FLAG,
  bva.ROLE_ID,
  bva.ROLE_NAME,
  bva.ACCOUNT_ID,
  bva.ACCOUNT_NAME,
  bva.USER_ID,
  bva.USER_NAME,
  bva.INTERNAL_CLIENT_ID,
  bva.INTERNAL_CLIENT,
  bva.CLIENT_ID,
  bva.CLIENT,
  bva.BUDGETED_MINUTES,
  bva.ACTUAL_MINUTES,
  bva.BUDGETED_COST,
  bva.ACTUAL_COST,
  
  -- Key corrected date fields from work item details
  wi.DUE_DATETIME,
  wi.DEADLINE_DATETIME,
  wi.START_DATETIME,
  wi.CREATED_DATETIME,
  wi.COMPLETED_DATETIME,
  
  -- Date components for easy filtering
  DATE(wi.DUE_DATETIME) as due_date,
  EXTRACT(YEAR FROM wi.DUE_DATETIME) as due_year,
  EXTRACT(MONTH FROM wi.DUE_DATETIME) as due_month,
  EXTRACT(DAY FROM wi.DUE_DATETIME) as due_day,
  DATE_TRUNC(wi.DUE_DATETIME, MONTH) as due_month_start,
  DATE_TRUNC(wi.DUE_DATETIME, WEEK) as due_week_start,
  DATE_TRUNC(wi.DUE_DATETIME, QUARTER) as due_quarter_start,
  
  -- Additional work item context fields
  wi.PRIMARY_STATUS_ID,
  wi.SECONDARY_STATUS_ID,
  wi.PRIMARY_STATUS,
  wi.SECONDARY_STATUS,
  wi.ASSIGNED_TO_ID,
  wi.ASSIGNED_TO,
  wi.CREATED_BY_ID,
  wi.CREATED_BY,
  wi.COMPLETED_BY_ID,
  wi.COMPLETED_BY,
  wi.WORK_TEMPLATE_ID,
  wi.WORK_TEMPLATE,
  wi.WORK_DESCRIPTION,
  
  -- Budget calculations in hours for easier analysis
  COALESCE(bva.BUDGETED_MINUTES, 0) / 60.0 as budgeted_hours,
  COALESCE(bva.ACTUAL_MINUTES, 0) / 60.0 as actual_hours,
  COALESCE(bva.BUDGETED_COST, 0) as budgeted_cost_calculated,
  COALESCE(bva.ACTUAL_COST, 0) as actual_cost_calculated,
  
  -- Budget variance calculations
  COALESCE(bva.ACTUAL_MINUTES, 0) - COALESCE(bva.BUDGETED_MINUTES, 0) as variance_minutes,
  (COALESCE(bva.ACTUAL_MINUTES, 0) - COALESCE(bva.BUDGETED_MINUTES, 0)) / 60.0 as variance_hours,
  COALESCE(bva.ACTUAL_COST, 0) - COALESCE(bva.BUDGETED_COST, 0) as variance_cost_calculated,
  
  -- Budget utilization percentage
  CASE 
    WHEN COALESCE(bva.BUDGETED_MINUTES, 0) > 0 
    THEN (COALESCE(bva.ACTUAL_MINUTES, 0) / bva.BUDGETED_MINUTES) * 100.0
    ELSE NULL 
  END as budget_utilization_percentage,
  
  -- Status indicators
  CASE 
    WHEN wi.DUE_DATETIME IS NOT NULL AND wi.DUE_DATETIME < CURRENT_TIMESTAMP() AND wi.PRIMARY_STATUS NOT IN ('Completed', 'Closed')
    THEN 'OVERDUE'
    WHEN wi.DUE_DATETIME IS NOT NULL AND wi.DUE_DATETIME <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND wi.PRIMARY_STATUS NOT IN ('Completed', 'Closed')
    THEN 'DUE_SOON'
    WHEN wi.PRIMARY_STATUS IN ('Completed', 'Closed')
    THEN 'COMPLETED'
    ELSE 'ON_TRACK'
  END AS schedule_status,
  
  CASE 
    WHEN COALESCE(bva.BUDGETED_MINUTES, 0) > 0 AND COALESCE(bva.ACTUAL_MINUTES, 0) > bva.BUDGETED_MINUTES 
    THEN 'OVER_BUDGET'
    WHEN COALESCE(bva.BUDGETED_MINUTES, 0) > 0 AND COALESCE(bva.ACTUAL_MINUTES, 0) > (bva.BUDGETED_MINUTES * 0.8)
    THEN 'APPROACHING_BUDGET'
    WHEN COALESCE(bva.BUDGETED_MINUTES, 0) > 0 
    THEN 'WITHIN_BUDGET'
    ELSE 'NO_BUDGET_SET'
  END AS budget_status,
  
  -- Useful date flags for reporting
  CASE WHEN wi.DUE_DATETIME IS NOT NULL THEN true ELSE false END as has_due_date,
  CASE WHEN wi.PRIMARY_STATUS IN ('Completed', 'Closed') THEN true ELSE false END as is_completed,
  CASE WHEN wi.DUE_DATETIME < CURRENT_TIMESTAMP() THEN true ELSE false END as is_past_due

FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ` bva
LEFT JOIN `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_LATEST_VIEW` wi 
  ON bva.WORK_ITEM_ID = wi.WORK_ITEM_ID

-- Order by due date for better performance and logical ordering
ORDER BY wi.DUE_DATETIME DESC, bva.CLIENT, bva.WORK_TITLE;
EOF

echo "✅ SQL file created: $TEMP_SQL_FILE"
echo ""

# Deploy the view
echo "🚀 Deploying corrected view..."
if bq query --project_id=$PROJECT_ID --use_legacy_sql=false < "$TEMP_SQL_FILE"; then
    echo "✅ VIEW DEPLOYED SUCCESSFULLY!"
else
    echo "❌ VIEW DEPLOYMENT FAILED!"
    exit 1
fi

echo ""

# Create verification SQL
VERIFICATION_SQL_FILE="/tmp/verification_query.sql"
cat > "$VERIFICATION_SQL_FILE" << 'EOF'
WITH v4_totals AS (
  SELECT 
    'V4_TIME_TRACKING_VIEW' as source_view,
    COUNT(DISTINCT CLIENT) as unique_clients,
    COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
    ROUND(SUM(budget_hours), 2) as total_budget_hours,
    ROUND(SUM(COALESCE(BUDGETED_COST, 0)), 2) as total_budget_cost
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`
  WHERE EXTRACT(YEAR FROM DUE_DATETIME) = 2025 
    AND EXTRACT(MONTH FROM DUE_DATETIME) = 5
),
corrected_totals AS (
  SELECT 
    'CORRECTED_VIEW' as source_view,
    COUNT(DISTINCT CLIENT) as unique_clients,
    COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
    ROUND(SUM(budgeted_hours), 2) as total_budget_hours,
         ROUND(SUM(budgeted_cost_calculated), 2) as total_budget_cost
  FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
  WHERE due_year = 2025 AND due_month = 5
),
original_bva AS (
  SELECT 
    'ORIGINAL_BVA_VIEW' as source_view,
    COUNT(DISTINCT CLIENT) as unique_clients,
    COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
    ROUND(SUM(BUDGETED_MINUTES)/60.0, 2) as total_budget_hours,
    ROUND(SUM(BUDGETED_COST), 2) as total_budget_cost
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ`
  WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
    AND EXTRACT(MONTH FROM REPORTING_DATE) = 5
)
SELECT * FROM v4_totals
UNION ALL
SELECT * FROM corrected_totals 
UNION ALL
SELECT * FROM original_bva
ORDER BY source_view;
EOF

# Run verification
echo "🔍 Running verification query..."
echo "Expected: V4_TIME_TRACKING_VIEW and CORRECTED_VIEW should have IDENTICAL totals"
echo "========================================================================="
bq query --project_id=$PROJECT_ID --use_legacy_sql=false --format=prettyjson < "$VERIFICATION_SQL_FILE"

# Cleanup
rm -f "$TEMP_SQL_FILE" "$VERIFICATION_SQL_FILE"

echo ""
echo "🎉 DEPLOYMENT COMPLETE!"
echo "======================="
echo "✅ Corrected view deployed successfully"
echo "✅ Verification completed"
echo ""
echo "📊 Now you can query May 2025 budgets with:"
echo "   SELECT CLIENT, SUM(budgeted_hours) as budget_hours"
echo "   FROM \`$PROJECT_ID.$DATASET_ID.work_item_budget_vs_actual_corrected_view\`"
echo "   WHERE due_year = 2025 AND due_month = 5"
echo "   GROUP BY CLIENT ORDER BY budget_hours DESC;" 