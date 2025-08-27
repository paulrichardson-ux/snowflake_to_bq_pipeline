#!/bin/bash

# V5 Budget Data Pipeline Fix Script
# This script addresses common causes of missing budget data in V5 view

echo "=================================================="
echo "V5 BUDGET DATA PIPELINE FIX"
echo "=================================================="
echo "Date: $(date)"
echo ""

# Check if we're authenticated with gcloud
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    echo "❌ ERROR: Not authenticated with gcloud. Please run: gcloud auth login"
    exit 1
fi

echo "✅ Authenticated with gcloud"
echo ""

echo "🔧 FIXING POTENTIAL BUDGET DATA ISSUES..."
echo ""

# 1. Redeploy the corrected view (in case it's corrupted)
echo "1️⃣  Redeploying work_item_budget_vs_actual_corrected_view..."
if [ -f "work_item_budget_vs_actual_corrected_view.sql" ]; then
    bq query --use_legacy_sql=false < work_item_budget_vs_actual_corrected_view.sql
    if [ $? -eq 0 ]; then
        echo "   ✅ Corrected view deployed successfully"
    else
        echo "   ❌ Failed to deploy corrected view"
    fi
else
    echo "   ⚠️  work_item_budget_vs_actual_corrected_view.sql not found"
fi
echo ""

# 2. Trigger budget vs actual data sync
echo "2️⃣  Triggering budget vs actual data sync..."
if gcloud scheduler jobs list --location=us-central1 | grep -q "work-item-budget-vs-actual"; then
    echo "   Found work-item-budget-vs-actual scheduler job"
    gcloud scheduler jobs run work-item-budget-vs-actual-sync-daily --location=us-central1
    if [ $? -eq 0 ]; then
        echo "   ✅ Budget sync job triggered"
        echo "   ⏳ Waiting 30 seconds for sync to complete..."
        sleep 30
    else
        echo "   ❌ Failed to trigger budget sync job"
    fi
else
    echo "   ⚠️  Budget sync scheduler not found - may need to be created"
fi
echo ""

# 3. Check if work item details are current
echo "3️⃣  Checking work item details freshness..."
LATEST_WORK_ITEM_DATE=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 "SELECT MAX(REPORTING_DATE) FROM \`red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ\`" | tail -n 1)
echo "   Latest work item data: $LATEST_WORK_ITEM_DATE"

# Check if it's older than 3 days
if [ -n "$LATEST_WORK_ITEM_DATE" ] && [ "$LATEST_WORK_ITEM_DATE" != "MAX_REPORTING_DATE_" ]; then
    echo "   ✅ Work item data appears current"
else
    echo "   ⚠️  Work item data may be stale - triggering sync..."
    # Try to find and run work item details sync
    if gcloud scheduler jobs list --location=us-central1 | grep -q "work-item-details"; then
        gcloud scheduler jobs run work-item-details-sync-daily --location=us-central1 2>/dev/null || echo "   ⚠️  Could not trigger work item sync"
    fi
fi
echo ""

# 4. Redeploy the V5 view
echo "4️⃣  Redeploying V5 view..."
if [ -f "work_item_individual_budget_time_tracking_view_v5.sql" ]; then
    bq query --use_legacy_sql=false < work_item_individual_budget_time_tracking_view_v5.sql
    if [ $? -eq 0 ]; then
        echo "   ✅ V5 view deployed successfully"
    else
        echo "   ❌ Failed to deploy V5 view"
        exit 1
    fi
else
    echo "   ❌ work_item_individual_budget_time_tracking_view_v5.sql not found"
    exit 1
fi
echo ""

# 5. Run a quick verification
echo "5️⃣  Running verification check..."
BUDGET_COUNT=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 "SELECT COUNT(*) as count FROM \`red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5\` WHERE individual_budgeted_minutes > 0" | tail -n 1)

if [ -n "$BUDGET_COUNT" ] && [ "$BUDGET_COUNT" != "count" ] && [ "$BUDGET_COUNT" -gt 0 ]; then
    echo "   ✅ SUCCESS: Found $BUDGET_COUNT records with individual budgets"
    
    # Get percentage
    TOTAL_COUNT=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 "SELECT COUNT(*) as count FROM \`red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5\`" | tail -n 1)
    if [ -n "$TOTAL_COUNT" ] && [ "$TOTAL_COUNT" != "count" ] && [ "$TOTAL_COUNT" -gt 0 ]; then
        PERCENTAGE=$(echo "scale=1; $BUDGET_COUNT * 100 / $TOTAL_COUNT" | bc -l 2>/dev/null || echo "N/A")
        echo "   📊 Budget coverage: $BUDGET_COUNT out of $TOTAL_COUNT records ($PERCENTAGE%)"
    fi
else
    echo "   ❌ ISSUE PERSISTS: Still no records with individual budgets found"
    echo "   🔍 Running detailed investigation..."
    echo ""
    
    # Run the investigation script
    if [ -f "investigate_v5_budget_issue.sh" ]; then
        ./investigate_v5_budget_issue.sh
    else
        echo "   ⚠️  Investigation script not found"
    fi
fi
echo ""

echo "=================================================="
echo "FIX ATTEMPT COMPLETE"
echo "=================================================="
echo ""

echo "📋 SUMMARY:"
echo "- Redeployed corrected budget view"
echo "- Triggered budget data sync"
echo "- Checked work item data freshness"  
echo "- Redeployed V5 view"
echo "- Ran verification check"
echo ""

if [ -n "$BUDGET_COUNT" ] && [ "$BUDGET_COUNT" -gt 0 ]; then
    echo "✅ SUCCESS: Budget data is now appearing in V5 view"
    echo "📊 Records with budgets: $BUDGET_COUNT"
else
    echo "❌ ISSUE PERSISTS: Budget data still not appearing"
    echo ""
    echo "🔍 Next steps:"
    echo "1. Check the investigation results above"
    echo "2. Verify source Snowflake data has budget allocations"
    echo "3. Check Cloud Function logs for sync errors"
    echo "4. Consider running full budget vs actual sync"
fi
echo ""
