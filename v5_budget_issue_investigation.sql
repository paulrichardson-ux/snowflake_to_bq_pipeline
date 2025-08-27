-- V5 BUDGET ISSUE INVESTIGATION
-- Comprehensive diagnostic to find why budgets disappeared from V5 view
-- Based on August verification that showed 87.4% records had budgets

-- 1. Check current state of V5 view
SELECT 
  'V5_VIEW_CURRENT_STATE' as analysis_type,
  COUNT(*) as total_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT budget_user_name) as unique_users,
  SUM(CASE WHEN individual_budgeted_minutes > 0 THEN 1 ELSE 0 END) as records_with_individual_budget,
  SUM(CASE WHEN has_individual_budget = true THEN 1 ELSE 0 END) as records_flagged_has_budget,
  ROUND(AVG(CASE WHEN individual_budgeted_minutes > 0 THEN 1.0 ELSE 0.0 END) * 100, 2) as pct_with_budget
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`;

-- 2. Compare with historical verification data (August showed 22,608 records with budgets)
SELECT 
  'HISTORICAL_COMPARISON' as analysis_type,
  'August 2025 had 22,608 records with budgets (87.4%)' as historical_note,
  'Current investigation' as current_status;

-- 3. Check the source budget data availability
SELECT 
  'SOURCE_BUDGET_DATA_CHECK' as analysis_type,
  COUNT(*) as total_budget_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items_with_budget,
  COUNT(DISTINCT USER_NAME) as unique_users_with_budget,
  MAX(sync_reporting_date) as latest_sync_date,
  MIN(sync_reporting_date) as earliest_sync_date
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE BUDGETED_MINUTES > 0 AND USER_NAME IS NOT NULL;

-- 4. Test the IndividualBudgetSummary CTE logic step by step
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
),
IndividualBudgetSummary AS (
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    SUM(BUDGETED_MINUTES) as total_individual_budgeted_minutes,
    SUM(ACTUAL_MINUTES) as total_individual_actual_minutes_from_budget_data,
    SUM(BUDGETED_COST) as total_individual_budgeted_cost,
    SUM(ACTUAL_COST) as total_individual_actual_cost,
    COUNT(*) as task_allocations_count
  FROM deduplicated_budget_data
  WHERE rn = 1
  GROUP BY WORK_ITEM_ID, USER_ID, USER_NAME
)
SELECT 
  'INDIVIDUAL_BUDGET_SUMMARY_TEST' as analysis_type,
  COUNT(*) as individual_budget_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT USER_NAME) as unique_users,
  SUM(total_individual_budgeted_minutes) as total_budgeted_minutes,
  AVG(task_allocations_count) as avg_task_allocations_per_user
FROM IndividualBudgetSummary;

-- 5. Check if the issue is with AllWorkItemUsers CTE
WITH AllWorkItemUsers AS (
  SELECT DISTINCT
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME
  FROM (
    -- Users with individual budgets
    SELECT 
      WORK_ITEM_ID,
      USER_ID,
      USER_NAME
    FROM (
      SELECT 
        WORK_ITEM_ID,
        USER_ID,
        USER_NAME,
        ROW_NUMBER() OVER (
          PARTITION BY WORK_ITEM_ID, USER_ID, USER_NAME, TASK_TYPE_ID, ROLE_ID, BUDGETED_MINUTES, BUDGETED_COST
          ORDER BY sync_reporting_date DESC
        ) as rn
      FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
      WHERE USER_NAME IS NOT NULL AND BUDGETED_MINUTES > 0
    ) WHERE rn = 1
    
    UNION ALL
    
    -- Users with time logged
    SELECT 
      WORK_ITEM_ID,
      USER_ID,
      USER_NAME
    FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
  )
)
SELECT 
  'ALL_WORK_ITEM_USERS_TEST' as analysis_type,
  COUNT(*) as total_user_work_item_combinations,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT USER_NAME) as unique_users
FROM AllWorkItemUsers;

-- 6. Check if work items exist in base table
SELECT 
  'WORK_ITEM_DETAILS_CHECK' as analysis_type,
  COUNT(*) as total_work_item_records,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  MAX(REPORTING_DATE) as latest_reporting_date,
  MIN(REPORTING_DATE) as earliest_reporting_date
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`;

-- 7. Check the join between work items and budget data
WITH budget_work_items AS (
  SELECT DISTINCT WORK_ITEM_ID
  FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
  WHERE BUDGETED_MINUTES > 0 AND USER_NAME IS NOT NULL
),
latest_work_items AS (
  SELECT DISTINCT WORK_ITEM_ID
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` budget
  WHERE budget.REPORTING_DATE = (
    SELECT MAX(REPORTING_DATE) 
    FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` latest
    WHERE latest.WORK_ITEM_ID = budget.WORK_ITEM_ID
  )
)
SELECT 
  'JOIN_ANALYSIS' as analysis_type,
  'Work items with budget data' as category,
  COUNT(*) as count
FROM budget_work_items

UNION ALL

SELECT 
  'JOIN_ANALYSIS' as analysis_type,
  'Latest work items' as category,
  COUNT(*) as count
FROM latest_work_items

UNION ALL

SELECT 
  'JOIN_ANALYSIS' as analysis_type,
  'Work items in both (should join)' as category,
  COUNT(*) as count
FROM budget_work_items b
INNER JOIN latest_work_items w ON b.WORK_ITEM_ID = w.WORK_ITEM_ID;

-- 8. Sample records to see actual data
SELECT 
  'SAMPLE_V5_RECORDS' as analysis_type,
  WORK_ITEM_ID,
  budget_user_name,
  individual_budgeted_minutes,
  individual_budgeted_hours,
  has_individual_budget,
  budget_source
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
WHERE individual_budgeted_minutes > 0
ORDER BY individual_budgeted_minutes DESC
LIMIT 10;

-- 9. Check if corrected view has recent data
SELECT 
  'CORRECTED_VIEW_RECENT_DATA' as analysis_type,
  DATE(sync_reporting_date) as sync_date,
  COUNT(*) as records_count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  SUM(BUDGETED_MINUTES) as total_budgeted_minutes
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE BUDGETED_MINUTES > 0 
  AND USER_NAME IS NOT NULL
  AND sync_reporting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAYS)
GROUP BY DATE(sync_reporting_date)
ORDER BY sync_date DESC
LIMIT 10;
