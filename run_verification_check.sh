#!/bin/bash

# Comprehensive Verification Script for WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5
# This script runs all verification checks using Google Cloud CLI

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="red-octane-444308-f4"
DATASET="karbon_data"
VIEW_NAME="WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5"

echo -e "${BLUE}==============================================================================${NC}"
echo -e "${BLUE}WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5 Verification Report${NC}"
echo -e "${BLUE}==============================================================================${NC}"
echo -e "Project: ${PROJECT_ID}"
echo -e "Dataset: ${DATASET}"
echo -e "View: ${VIEW_NAME}"
echo -e "Timestamp: $(date)"
echo -e "${BLUE}==============================================================================${NC}\n"

# Function to run a BigQuery query and display results
run_bq_query() {
    local query_description="$1"
    local query="$2"
    local output_format="${3:-table}"
    
    echo -e "${YELLOW}Running: ${query_description}${NC}"
    echo -e "${BLUE}Query:${NC} ${query}"
    echo ""
    
    if [ "$output_format" = "csv" ]; then
        bq query --use_legacy_sql=false --format=csv --max_rows=50 "$query"
    else
        bq query --use_legacy_sql=false --format=pretty --max_rows=50 "$query"
    fi
    
    echo -e "\n${BLUE}---${NC}\n"
}

# Check if gcloud and bq CLI are available
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI is not installed or not in PATH${NC}"
    exit 1
fi

if ! command -v bq &> /dev/null; then
    echo -e "${RED}Error: bq CLI is not installed or not in PATH${NC}"
    exit 1
fi

# Verify authentication
echo -e "${YELLOW}Checking Google Cloud authentication...${NC}"
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1 > /dev/null; then
    echo -e "${RED}Error: No active Google Cloud authentication found${NC}"
    echo -e "${YELLOW}Please run: gcloud auth login${NC}"
    exit 1
fi

ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1)
echo -e "${GREEN}Authenticated as: ${ACTIVE_ACCOUNT}${NC}\n"

# Set the project
gcloud config set project $PROJECT_ID

# Verify the view exists
echo -e "${YELLOW}Verifying view exists...${NC}"
if ! bq show --format=json ${PROJECT_ID}:${DATASET}.${VIEW_NAME} > /dev/null 2>&1; then
    echo -e "${RED}Error: View ${VIEW_NAME} does not exist in ${PROJECT_ID}:${DATASET}${NC}"
    exit 1
fi
echo -e "${GREEN}✓ View exists${NC}\n"

# =============================================================================
# SECTION 1: DATA FRESHNESS VERIFICATION
# =============================================================================

echo -e "${BLUE}=== SECTION 1: DATA FRESHNESS VERIFICATION ===${NC}\n"

run_bq_query "Data Freshness Check - All Source Tables" "
SELECT 
  'WORK_ITEM_DETAILS_BQ' as table_name,
  MAX(REPORTING_DATE) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  CURRENT_TIMESTAMP() as check_timestamp
FROM \`${PROJECT_ID}.${DATASET}.WORK_ITEM_DETAILS_BQ\`

UNION ALL

SELECT 
  'USER_TIME_ENTRY_BQ' as table_name,
  MAX(REPORTING_DATE) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT CONCAT(WORK_ITEM_ID, '-', USER_ID, '-', CAST(REPORTING_DATE AS STRING))) as unique_entries,
  CURRENT_TIMESTAMP() as check_timestamp
FROM \`${PROJECT_ID}.${DATASET}.USER_TIME_ENTRY_BQ\`

UNION ALL

SELECT 
  'work_item_budget_vs_actual_corrected_view' as table_name,
  MAX(sync_reporting_date) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT CONCAT(WORK_ITEM_ID, '-', CAST(USER_ID AS STRING))) as unique_user_work_items,
  CURRENT_TIMESTAMP() as check_timestamp
FROM \`${PROJECT_ID}.${DATASET}.work_item_budget_vs_actual_corrected_view\`
WHERE USER_NAME IS NOT NULL AND BUDGETED_MINUTES > 0

ORDER BY table_name"

# =============================================================================
# SECTION 2: DEDUPLICATION VERIFICATION
# =============================================================================

echo -e "${BLUE}=== SECTION 2: DEDUPLICATION VERIFICATION ===${NC}\n"

run_bq_query "Check for WORK_ITEM_DETAILS_BQ Duplicates" "
SELECT 
  'WORK_ITEM_DETAILS_BQ_DUPLICATES' as check_name,
  WORK_ITEM_ID,
  COUNT(*) as record_count,
  STRING_AGG(DISTINCT CAST(REPORTING_DATE AS STRING), ', ' ORDER BY REPORTING_DATE DESC) as reporting_dates
FROM \`${PROJECT_ID}.${DATASET}.WORK_ITEM_DETAILS_BQ\`
GROUP BY WORK_ITEM_ID
HAVING COUNT(*) > 1
ORDER BY record_count DESC
LIMIT 10"

run_bq_query "Check for Budget vs Actual Duplicates" "
SELECT 
  'BUDGET_VS_ACTUAL_DUPLICATES' as check_name,
  WORK_ITEM_ID,
  USER_ID,
  USER_NAME,
  TASK_TYPE_ID,
  ROLE_ID,
  BUDGETED_MINUTES,
  BUDGETED_COST,
  COUNT(*) as duplicate_count
FROM \`${PROJECT_ID}.${DATASET}.work_item_budget_vs_actual_corrected_view\`
WHERE USER_NAME IS NOT NULL AND BUDGETED_MINUTES > 0
GROUP BY WORK_ITEM_ID, USER_ID, USER_NAME, TASK_TYPE_ID, ROLE_ID, BUDGETED_MINUTES, BUDGETED_COST
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10"

# =============================================================================
# SECTION 3: VIEW VERIFICATION
# =============================================================================

echo -e "${BLUE}=== SECTION 3: VIEW VERIFICATION ===${NC}\n"

run_bq_query "Check for V5 View User-Work Item Duplicates" "
SELECT 
  'V5_VIEW_USER_WORK_ITEM_DUPLICATES' as check_name,
  WORK_ITEM_ID,
  budget_user_id,
  budget_user_name,
  COUNT(*) as duplicate_count
FROM \`${PROJECT_ID}.${DATASET}.${VIEW_NAME}\`
GROUP BY WORK_ITEM_ID, budget_user_id, budget_user_name
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10"

run_bq_query "View Coverage Summary" "
SELECT 
  'VIEW_COVERAGE_SUMMARY' as check_name,
  COUNT(*) as total_view_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT budget_user_id) as unique_users,
  COUNT(CASE WHEN budget_source = 'INDIVIDUAL_BUDGET' THEN 1 END) as records_with_individual_budget,
  COUNT(CASE WHEN individual_hours_logged_actual > 0 THEN 1 END) as records_with_time_logged,
  COUNT(CASE WHEN budget_source = 'INDIVIDUAL_BUDGET' AND individual_hours_logged_actual > 0 THEN 1 END) as records_with_both,
  COUNT(CASE WHEN budget_source = 'NO_BUDGET' AND individual_hours_logged_actual > 0 THEN 1 END) as time_only_records
FROM \`${PROJECT_ID}.${DATASET}.${VIEW_NAME}\`"

# =============================================================================
# SECTION 4: DATA CONSISTENCY VERIFICATION
# =============================================================================

echo -e "${BLUE}=== SECTION 4: DATA CONSISTENCY VERIFICATION ===${NC}\n"

run_bq_query "Budget Consistency Check (Individual vs Work Item Totals)" "
WITH budget_comparison AS (
  SELECT 
    v5.WORK_ITEM_ID,
    v5.WORK_TITLE,
    v5.work_item_total_budgeted_minutes,
    SUM(v5.individual_budgeted_minutes) as sum_individual_budgets,
    v5.work_item_total_budgeted_minutes - SUM(v5.individual_budgeted_minutes) as budget_difference,
    COUNT(*) as user_count,
    COUNT(CASE WHEN v5.individual_budgeted_minutes > 0 THEN 1 END) as users_with_budget
  FROM \`${PROJECT_ID}.${DATASET}.${VIEW_NAME}\` v5
  GROUP BY v5.WORK_ITEM_ID, v5.WORK_TITLE, v5.work_item_total_budgeted_minutes
)
SELECT 
  'BUDGET_CONSISTENCY_CHECK' as check_name,
  WORK_ITEM_ID,
  WORK_TITLE,
  work_item_total_budgeted_minutes,
  sum_individual_budgets,
  budget_difference,
  user_count,
  users_with_budget,
  CASE 
    WHEN ABS(budget_difference) > 60 THEN 'SIGNIFICANT_VARIANCE'
    WHEN budget_difference != 0 THEN 'MINOR_VARIANCE'
    ELSE 'CONSISTENT'
  END as consistency_status
FROM budget_comparison
WHERE work_item_total_budgeted_minutes > 0
ORDER BY ABS(budget_difference) DESC
LIMIT 15"

# =============================================================================
# SECTION 5: RECENT ACTIVITY CHECK
# =============================================================================

echo -e "${BLUE}=== SECTION 5: RECENT ACTIVITY CHECK ===${NC}\n"

run_bq_query "Recent Activity (Last 30 Days)" "
SELECT 
  'RECENT_ACTIVITY_CHECK' as check_name,
  DATE(individual_last_time_entry) as time_entry_date,
  COUNT(*) as records_with_activity,
  COUNT(DISTINCT WORK_ITEM_ID) as work_items_with_activity,
  COUNT(DISTINCT budget_user_id) as users_with_activity,
  ROUND(SUM(individual_hours_logged_actual), 2) as total_hours_logged
FROM \`${PROJECT_ID}.${DATASET}.${VIEW_NAME}\`
WHERE individual_last_time_entry >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND individual_hours_logged_actual > 0
GROUP BY DATE(individual_last_time_entry)
ORDER BY time_entry_date DESC
LIMIT 10"

# =============================================================================
# SECTION 6: HEALTH SUMMARY
# =============================================================================

echo -e "${BLUE}=== SECTION 6: OVERALL HEALTH SUMMARY ===${NC}\n"

run_bq_query "View Health Summary" "
WITH health_metrics AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
    COUNT(DISTINCT budget_user_id) as unique_users,
    AVG(individual_budgeted_hours) as avg_individual_budget_hours,
    AVG(individual_hours_logged_actual) as avg_individual_hours_logged,
    COUNT(CASE WHEN individual_budget_status = 'OVER_BUDGET' THEN 1 END) as over_budget_count,
    COUNT(CASE WHEN individual_budget_status = 'APPROACHING_BUDGET' THEN 1 END) as approaching_budget_count,
    COUNT(CASE WHEN individual_budget_status = 'WITHIN_BUDGET' THEN 1 END) as within_budget_count,
    COUNT(CASE WHEN individual_budget_status = 'NO_INDIVIDUAL_BUDGET_SET' THEN 1 END) as no_budget_count
  FROM \`${PROJECT_ID}.${DATASET}.${VIEW_NAME}\`
)
SELECT 
  'VIEW_HEALTH_SUMMARY' as check_name,
  total_records,
  unique_work_items,
  unique_users,
  ROUND(avg_individual_budget_hours, 2) as avg_individual_budget_hours,
  ROUND(avg_individual_hours_logged, 2) as avg_individual_hours_logged,
  over_budget_count,
  approaching_budget_count,
  within_budget_count,
  no_budget_count,
  ROUND((over_budget_count + approaching_budget_count + within_budget_count) * 100.0 / total_records, 2) as budget_coverage_percentage
FROM health_metrics"

# =============================================================================
# FINAL SUMMARY
# =============================================================================

echo -e "${BLUE}==============================================================================${NC}"
echo -e "${GREEN}✓ Verification completed successfully!${NC}"
echo -e "${BLUE}==============================================================================${NC}"
echo -e "View: ${VIEW_NAME}"
echo -e "Verification completed at: $(date)"
echo -e ""
echo -e "${YELLOW}Key Points to Review:${NC}"
echo -e "1. Check data freshness - ensure all source tables are recently synced"
echo -e "2. Verify no duplicates found in source tables or view"
echo -e "3. Confirm budget consistency between individual and work item totals"
echo -e "4. Review recent activity to ensure data is being updated"
echo -e "5. Check overall health metrics for data quality"
echo -e ""
echo -e "${BLUE}If any issues are found, review the specific queries above for detailed analysis.${NC}"
echo -e "${BLUE}==============================================================================${NC}"