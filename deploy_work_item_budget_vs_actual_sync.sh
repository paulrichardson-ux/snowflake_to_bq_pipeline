#!/bin/bash
set -e

echo "====================================="
echo "Work Item Budget vs Actual Pipeline Deployment"
echo "====================================="

PROJECT_ID=$(gcloud config get-value project)
echo "Using Project: ${PROJECT_ID}"

# Deploy Full Sync Function
echo ""
echo "1. Deploying Full Sync Function..."
echo "-------------------------------------"
chmod +x ./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_full.sh
./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_full.sh

# Deploy Daily Sync Function  
echo ""
echo "2. Deploying Daily Sync Function..."
echo "-------------------------------------"
chmod +x ./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_daily.sh
./work_item_budget_vs_actual_pipeline/deploy_work_item_budget_vs_actual_daily.sh

echo ""
echo "====================================="
echo "Deployment Complete!"
echo "====================================="
echo "Next steps:"
echo "1. Run initial full sync (optional):"
echo "   curl -X POST https://us-central1-${PROJECT_ID}.cloudfunctions.net/sync-full-work-item-budget-vs-actual-to-bq"
echo ""
echo "2. Set up automated daily sync:"
echo "   chmod +x ./create_work_item_budget_vs_actual_scheduler.sh"
echo "   ./create_work_item_budget_vs_actual_scheduler.sh"
echo ""
echo "Target BigQuery table: ${PROJECT_ID}.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ" 