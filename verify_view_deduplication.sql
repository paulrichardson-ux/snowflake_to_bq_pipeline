-- Comprehensive Verification Script for WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5
-- This script verifies deduplication and data freshness of the view and its related tables
-- Run with: bq query --use_legacy_sql=false < verify_view_deduplication.sql

-- =============================================================================
-- SECTION 1: DATA FRESHNESS VERIFICATION
-- =============================================================================

-- Check 1: Latest sync dates for all source tables
SELECT 
  'WORK_ITEM_DETAILS_BQ' as table_name,
  MAX(REPORTING_DATE) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  CURRENT_TIMESTAMP() as check_timestamp
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`

UNION ALL

SELECT 
  'USER_TIME_ENTRY_BQ' as table_name,
  MAX(REPORTING_DATE) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT CONCAT(WORK_ITEM_ID, '-', USER_ID, '-', REPORTING_DATE)) as unique_entries,
  CURRENT_TIMESTAMP() as check_timestamp
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`

UNION ALL

SELECT 
  'work_item_budget_vs_actual_corrected_view' as table_name,
  MAX(sync_reporting_date) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT CONCAT(WORK_ITEM_ID, '-', USER_ID)) as unique_user_work_items,
  CURRENT_TIMESTAMP() as check_timestamp
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE USER_NAME IS NOT NULL AND BUDGETED_MINUTES > 0

UNION ALL

SELECT 
  'CLIENT_DIMENSION' as table_name,
  MAX(REPORTING_DATE) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT CLIENT_ID) as unique_clients,
  CURRENT_TIMESTAMP() as check_timestamp
FROM `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION`

UNION ALL

SELECT 
  'USER_DIMENSION' as table_name,
  MAX(REPORTING_DATE) as latest_sync_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT USER_ID) as unique_users,
  CURRENT_TIMESTAMP() as check_timestamp
FROM `red-octane-444308-f4.karbon_data.USER_DIMENSION`

ORDER BY table_name;

-- =============================================================================
-- SECTION 2: DEDUPLICATION VERIFICATION - SOURCE TABLES
-- =============================================================================

-- Check 2: Verify WORK_ITEM_DETAILS_BQ deduplication
SELECT 
  'WORK_ITEM_DETAILS_BQ_DUPLICATES' as check_name,
  WORK_ITEM_ID,
  COUNT(*) as record_count,
  STRING_AGG(DISTINCT CAST(REPORTING_DATE AS STRING), ', ' ORDER BY REPORTING_DATE DESC) as reporting_dates
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
GROUP BY WORK_ITEM_ID
HAVING COUNT(*) > 1
ORDER BY record_count DESC
LIMIT 10;

-- Check 3: Verify budget vs actual corrected view deduplication
SELECT 
  'BUDGET_VS_ACTUAL_DUPLICATES' as check_name,
  WORK_ITEM_ID,
  USER_ID,
  USER_NAME,
  TASK_TYPE_ID,
  ROLE_ID,
  BUDGETED_MINUTES,
  BUDGETED_COST,
  COUNT(*) as duplicate_count,
  STRING_AGG(DISTINCT CAST(sync_reporting_date AS STRING), ', ' ORDER BY sync_reporting_date DESC) as sync_dates
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE USER_NAME IS NOT NULL AND BUDGETED_MINUTES > 0
GROUP BY WORK_ITEM_ID, USER_ID, USER_NAME, TASK_TYPE_ID, ROLE_ID, BUDGETED_MINUTES, BUDGETED_COST
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;

-- =============================================================================
-- SECTION 3: VIEW DEDUPLICATION VERIFICATION
-- =============================================================================

-- Check 4: Verify V5 view has no duplicate user-work item combinations
SELECT 
  'V5_VIEW_USER_WORK_ITEM_DUPLICATES' as check_name,
  WORK_ITEM_ID,
  budget_user_id,
  budget_user_name,
  COUNT(*) as duplicate_count
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
GROUP BY WORK_ITEM_ID, budget_user_id, budget_user_name
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;

-- Check 5: Verify individual budget summary deduplication logic
WITH IndividualBudgetSummary AS (
  WITH deduplicated_budget_data AS (
    SELECT 
      WORK_ITEM_ID,
      USER_ID,
      USER_NAME,
      TASK_TYPE_ID,
      TASK_TYPE,
      ROLE_ID,
      ROLE_NAME,
      BUDGETED_MINUTES,
      ACTUAL_MINUTES,
      BUDGETED_COST,
      ACTUAL_COST,
      sync_reporting_date,
      ROW_NUMBER() OVER (
        PARTITION BY WORK_ITEM_ID, USER_ID, USER_NAME, TASK_TYPE_ID, ROLE_ID, BUDGETED_MINUTES, BUDGETED_COST
        ORDER BY sync_reporting_date DESC
      ) as rn
    FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
    WHERE USER_NAME IS NOT NULL 
      AND BUDGETED_MINUTES > 0
  )
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    SUM(BUDGETED_MINUTES) as total_individual_budgeted_minutes,
    COUNT(*) as task_type_allocations,
    STRING_AGG(DISTINCT CONCAT(TASK_TYPE, ' (', CAST(BUDGETED_MINUTES AS STRING), ' min)'), ', ') as task_breakdown
  FROM deduplicated_budget_data
  WHERE rn = 1
  GROUP BY WORK_ITEM_ID, USER_ID, USER_NAME
)
SELECT 
  'INDIVIDUAL_BUDGET_SUMMARY_CHECK' as check_name,
  WORK_ITEM_ID,
  USER_NAME,
  total_individual_budgeted_minutes,
  task_type_allocations,
  task_breakdown
FROM IndividualBudgetSummary
WHERE task_type_allocations > 3  -- Show users with multiple task type allocations
ORDER BY task_type_allocations DESC, total_individual_budgeted_minutes DESC
LIMIT 10;

-- =============================================================================
-- SECTION 4: DATA CONSISTENCY VERIFICATION
-- =============================================================================

-- Check 6: Compare individual budget totals vs work item budget totals
WITH budget_comparison AS (
  SELECT 
    v5.WORK_ITEM_ID,
    v5.WORK_TITLE,
    v5.work_item_total_budgeted_minutes,
    SUM(v5.individual_budgeted_minutes) as sum_individual_budgets,
    v5.work_item_total_budgeted_minutes - SUM(v5.individual_budgeted_minutes) as budget_difference,
    COUNT(*) as user_count,
    COUNT(CASE WHEN v5.individual_budgeted_minutes > 0 THEN 1 END) as users_with_budget
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5` v5
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
LIMIT 15;

-- Check 7: Verify time tracking data consistency
SELECT 
  'TIME_TRACKING_CONSISTENCY' as check_name,
  v5.WORK_ITEM_ID,
  v5.WORK_TITLE,
  v5.work_item_total_hours_logged_summary,
  SUM(v5.individual_hours_logged_actual) as sum_individual_hours,
  v5.work_item_total_hours_logged_summary - SUM(v5.individual_hours_logged_actual) as time_difference,
  COUNT(*) as user_count,
  COUNT(CASE WHEN v5.individual_hours_logged_actual > 0 THEN 1 END) as users_with_time
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5` v5
GROUP BY v5.WORK_ITEM_ID, v5.WORK_TITLE, v5.work_item_total_hours_logged_summary
HAVING v5.work_item_total_hours_logged_summary > 0
  AND ABS(v5.work_item_total_hours_logged_summary - SUM(v5.individual_hours_logged_actual)) > 0.1
ORDER BY ABS(time_difference) DESC
LIMIT 10;

-- =============================================================================
-- SECTION 5: VIEW PERFORMANCE AND COMPLETENESS
-- =============================================================================

-- Check 8: View record counts and coverage
SELECT 
  'VIEW_COVERAGE_SUMMARY' as check_name,
  COUNT(*) as total_view_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT budget_user_id) as unique_users,
  COUNT(CASE WHEN budget_source = 'INDIVIDUAL_BUDGET' THEN 1 END) as records_with_individual_budget,
  COUNT(CASE WHEN individual_hours_logged_actual > 0 THEN 1 END) as records_with_time_logged,
  COUNT(CASE WHEN budget_source = 'INDIVIDUAL_BUDGET' AND individual_hours_logged_actual > 0 THEN 1 END) as records_with_both,
  COUNT(CASE WHEN budget_source = 'NO_BUDGET' AND individual_hours_logged_actual > 0 THEN 1 END) as time_only_records
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`;

-- Check 9: Recent activity verification (last 30 days)
SELECT 
  'RECENT_ACTIVITY_CHECK' as check_name,
  DATE(individual_last_time_entry) as time_entry_date,
  COUNT(*) as records_with_activity,
  COUNT(DISTINCT WORK_ITEM_ID) as work_items_with_activity,
  COUNT(DISTINCT budget_user_id) as users_with_activity,
  SUM(individual_hours_logged_actual) as total_hours_logged
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
WHERE individual_last_time_entry >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY DATE(individual_last_time_entry)
ORDER BY time_entry_date DESC
LIMIT 10;

-- =============================================================================
-- SECTION 6: SUMMARY HEALTH CHECK
-- =============================================================================

-- Check 10: Overall view health summary
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
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
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
FROM health_metrics;