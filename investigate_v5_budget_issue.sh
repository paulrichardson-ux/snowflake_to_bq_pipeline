#!/bin/bash

# V5 Budget Issue Investigation Script
# This script runs comprehensive diagnostics to find why budgets disappeared

echo "=================================================="
echo "V5 BUDGET ISSUE INVESTIGATION"
echo "=================================================="
echo "Date: $(date)"
echo ""

echo "Running comprehensive diagnostics to identify why budgets are not showing in V5 view..."
echo ""

# Check if we're authenticated with gcloud
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    echo "❌ ERROR: Not authenticated with gcloud. Please run: gcloud auth login"
    exit 1
fi

echo "✅ Authenticated with gcloud"
echo ""

# Run the investigation query
echo "🔍 Running budget investigation diagnostics..."
echo ""

bq query \
    --use_legacy_sql=false \
    --format=table \
    --max_rows=1000 \
    "$(cat v5_budget_issue_investigation.sql)"

echo ""
echo "=================================================="
echo "INVESTIGATION COMPLETE"
echo "=================================================="
echo ""

echo "📊 ANALYSIS SUMMARY:"
echo ""
echo "1. Check the 'V5_VIEW_CURRENT_STATE' results:"
echo "   - If pct_with_budget is 0%, then no budgets are showing"
echo "   - Compare with historical 87.4% from August"
echo ""
echo "2. Check 'SOURCE_BUDGET_DATA_CHECK':"
echo "   - If this shows 0 records, the source budget data is missing"
echo "   - Check latest_sync_date to see if data is stale"
echo ""
echo "3. Check 'INDIVIDUAL_BUDGET_SUMMARY_TEST':"
echo "   - This tests the CTE logic that processes budget data"
echo "   - Should show individual budget records if source data exists"
echo ""
echo "4. Check 'JOIN_ANALYSIS':"
echo "   - Shows if work items with budgets are joining with latest work items"
echo "   - If 'Work items in both' is 0, the join is failing"
echo ""

echo "🛠️  POTENTIAL SOLUTIONS:"
echo ""
echo "If source budget data is missing:"
echo "   ./deploy_work_item_budget_vs_actual_sync.sh"
echo ""
echo "If join is failing:"
echo "   ./fix_v5_view_and_dimension_updates.sh"
echo ""
echo "If CTE logic is broken:"
echo "   Check the work_item_budget_vs_actual_corrected_view deployment"
echo ""

echo "📧 Next steps:"
echo "1. Review the diagnostic results above"
echo "2. Run the appropriate fix script based on findings"
echo "3. Re-run this investigation to confirm the fix"
echo ""
