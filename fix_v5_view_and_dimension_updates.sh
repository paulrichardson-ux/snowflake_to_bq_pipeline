#!/bin/bash

# =============================================================================
# FIX V5 VIEW AND DIMENSION TABLE UPDATES
# =============================================================================
# This script fixes the V5 view duplicates and ensures dimension tables are updating properly
# Issues addressed:
# 1. V5 view was using non-existent WORK_ITEM_DETAILS_LATEST_VIEW
# 2. Dimension tables may not be updating on schedule
# 3. Creates missing WORK_ITEM_DETAILS_LATEST_VIEW for other views

set -e

echo "🔧 Starting V5 View and Dimension Table Update Fix..."
echo "============================================="

# Set project and region
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
DATASET="karbon_data"

echo "📋 Project: $PROJECT_ID"
echo "📋 Region: $REGION"
echo "📋 Dataset: $DATASET"
echo ""

# =============================================================================
# STEP 1: Deploy the missing WORK_ITEM_DETAILS_LATEST_VIEW
# =============================================================================
echo "🚀 Step 1: Creating missing WORK_ITEM_DETAILS_LATEST_VIEW..."

bq query --use_legacy_sql=false --project_id=$PROJECT_ID < work_item_details_latest_view.sql

echo "✅ WORK_ITEM_DETAILS_LATEST_VIEW created successfully!"
echo ""

# =============================================================================
# STEP 2: Deploy the corrected V5 view
# =============================================================================
echo "🚀 Step 2: Deploying corrected V5 view..."

bq query --use_legacy_sql=false --project_id=$PROJECT_ID < work_item_individual_budget_time_tracking_view_v5.sql

echo "✅ V5 view deployed successfully!"
echo ""

# =============================================================================
# STEP 3: Check and fix dimension table update schedules
# =============================================================================
echo "🚀 Step 3: Checking dimension table update schedules..."

# Function to check if scheduler job exists
check_scheduler_job() {
    local job_name=$1
    if gcloud scheduler jobs describe $job_name --location=$REGION --quiet > /dev/null 2>&1; then
        echo "✅ Scheduler job '$job_name' exists"
        return 0
    else
        echo "❌ Scheduler job '$job_name' does not exist"
        return 1
    fi
}

# Function to run scheduler job
run_scheduler_job() {
    local job_name=$1
    echo "🔄 Running scheduler job: $job_name"
    gcloud scheduler jobs run $job_name --location=$REGION --quiet
    if [ $? -eq 0 ]; then
        echo "✅ Successfully triggered: $job_name"
    else
        echo "❌ Failed to trigger: $job_name"
    fi
}

# Check all dimension table schedulers
echo "📋 Checking dimension table schedulers..."

SCHEDULERS=(
    "client-dimension-daily-sync"
    "client-group-dimension-daily-sync"
    "tenant-team-dimension-daily-sync"
    "tenant-team-member-dimension-daily-sync"
    "user-dimension-daily-sync"
)

missing_schedulers=()
existing_schedulers=()

for scheduler in "${SCHEDULERS[@]}"; do
    if check_scheduler_job $scheduler; then
        existing_schedulers+=($scheduler)
    else
        missing_schedulers+=($scheduler)
    fi
done

echo ""

# =============================================================================
# STEP 4: Create missing schedulers
# =============================================================================
if [ ${#missing_schedulers[@]} -gt 0 ]; then
    echo "🚀 Step 4: Creating missing schedulers..."
    
    for scheduler in "${missing_schedulers[@]}"; do
        case $scheduler in
            "client-dimension-daily-sync")
                echo "Creating CLIENT_DIMENSION scheduler..."
                gcloud scheduler jobs create http $scheduler \
                    --location=$REGION \
                    --schedule="0 6 * * *" \
                    --time-zone="UTC" \
                    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/client-dimension-sync-daily" \
                    --http-method=POST \
                    --headers="Content-Type=application/json" \
                    --message-body='{"source": "scheduler"}' \
                    --description="Daily sync of CLIENT_DIMENSION from Snowflake to BigQuery"
                ;;
            "client-group-dimension-daily-sync")
                echo "Creating CLIENT_GROUP_DIMENSION scheduler..."
                gcloud scheduler jobs create http $scheduler \
                    --location=$REGION \
                    --schedule="0 6 * * *" \
                    --time-zone="UTC" \
                    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/client-group-dimension-sync-daily" \
                    --http-method=POST \
                    --headers="Content-Type=application/json" \
                    --message-body='{"source": "scheduler"}' \
                    --description="Daily sync of CLIENT_GROUP_DIMENSION from Snowflake to BigQuery"
                ;;
            "tenant-team-dimension-daily-sync")
                echo "Creating TENANT_TEAM_DIMENSION scheduler..."
                gcloud scheduler jobs create http $scheduler \
                    --location=$REGION \
                    --schedule="0 6 * * *" \
                    --time-zone="UTC" \
                    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/tenant-team-dimension-sync-daily" \
                    --http-method=POST \
                    --headers="Content-Type=application/json" \
                    --message-body='{"source": "scheduler"}' \
                    --description="Daily sync of TENANT_TEAM_DIMENSION from Snowflake to BigQuery"
                ;;
            "tenant-team-member-dimension-daily-sync")
                echo "Creating TENANT_TEAM_MEMBER_DIMENSION scheduler..."
                gcloud scheduler jobs create http $scheduler \
                    --location=$REGION \
                    --schedule="0 7 * * *" \
                    --time-zone="UTC" \
                    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/tenant-team-member-dimension-sync-daily" \
                    --http-method=POST \
                    --headers="Content-Type=application/json" \
                    --message-body='{"source": "scheduler"}' \
                    --description="Daily sync of TENANT_TEAM_MEMBER_DIMENSION from Snowflake to BigQuery"
                ;;
            "user-dimension-daily-sync")
                echo "Creating USER_DIMENSION scheduler..."
                gcloud scheduler jobs create http $scheduler \
                    --location=$REGION \
                    --schedule="0 8 * * *" \
                    --time-zone="UTC" \
                    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/user-dimension-sync-daily" \
                    --http-method=POST \
                    --headers="Content-Type=application/json" \
                    --message-body='{"source": "scheduler"}' \
                    --description="Daily sync of USER_DIMENSION from Snowflake to BigQuery"
                ;;
        esac
        echo "✅ Created scheduler: $scheduler"
    done
else
    echo "✅ Step 4: All schedulers already exist - skipping creation"
fi

echo ""

# =============================================================================
# STEP 5: Trigger dimension table updates
# =============================================================================
echo "🚀 Step 5: Triggering dimension table updates..."

for scheduler in "${existing_schedulers[@]}"; do
    run_scheduler_job $scheduler
    sleep 5  # Wait 5 seconds between jobs to avoid overwhelming the system
done

echo ""

# =============================================================================
# STEP 6: Verification queries
# =============================================================================
echo "🚀 Step 6: Running verification queries..."

echo "📊 Checking V5 view record counts..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID << 'EOF'
SELECT 
  'V5_VIEW_TOTAL_RECORDS' as metric,
  COUNT(*) as count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT budget_user_id) as unique_users_with_budgets
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`

UNION ALL

SELECT 
  'V5_VIEW_RECORDS_WITH_INDIVIDUAL_BUDGET' as metric,
  COUNT(*) as count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT budget_user_id) as unique_users_with_budgets
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
WHERE has_individual_budget = true

UNION ALL

SELECT 
  'V4_VIEW_TOTAL_RECORDS' as metric,
  COUNT(*) as count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  0 as unique_users_with_budgets
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`

UNION ALL

SELECT 
  'BASE_WORK_ITEM_DETAILS_BQ_TOTAL' as metric,
  COUNT(*) as count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  0 as unique_users_with_budgets
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`

UNION ALL

SELECT 
  'LATEST_VIEW_TOTAL_RECORDS' as metric,
  COUNT(*) as count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  0 as unique_users_with_budgets
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_LATEST_VIEW`

ORDER BY metric;
EOF

echo ""
echo "📊 Checking dimension table freshness..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID << 'EOF'
SELECT 
  'CLIENT_DIMENSION' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT CLIENT_ID) as unique_clients
FROM `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION`

UNION ALL

SELECT 
  'CLIENT_GROUP_DIMENSION' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT CLIENT_GROUP_ID) as unique_clients
FROM `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION`

UNION ALL

SELECT 
  'TENANT_TEAM_DIMENSION' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT TENANT_TEAM_ID) as unique_clients
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION`

UNION ALL

SELECT 
  'TENANT_TEAM_MEMBER_DIMENSION' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT TENANT_TEAM_MEMBER_ID) as unique_clients
FROM `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION`

UNION ALL

SELECT 
  'USER_DIMENSION' as table_name,
  COUNT(*) as record_count,
  COUNT(DISTINCT USER_ID) as unique_clients
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION`

ORDER BY table_name;
EOF

echo ""

# =============================================================================
# COMPLETION
# =============================================================================
echo "🎉 Fix completed successfully!"
echo "============================================="
echo ""
echo "📋 Summary of actions taken:"
echo "1. ✅ Created missing WORK_ITEM_DETAILS_LATEST_VIEW"
echo "2. ✅ Deployed corrected V5 view with proper latest record filtering"
echo "3. ✅ Checked and fixed dimension table schedulers"
echo "4. ✅ Triggered dimension table updates"
echo "5. ✅ Ran verification queries"
echo ""
echo "🔍 The V5 view should now:"
echo "   - Show only the latest version of each work item (no duplicates)"
echo "   - Properly join with individual user budgets"
echo "   - Include all dimension data from updated tables"
echo ""
echo "⚠️  Note: If dimension tables are still showing old data, wait 10-15 minutes"
echo "    for the triggered sync jobs to complete, then rerun the verification queries."
echo ""
echo "📞 If you still see issues, check the Cloud Function logs:"
echo "    gcloud functions logs read [function-name] --region=$REGION --limit=50"
echo ""
echo "✅ Fix script completed!" 