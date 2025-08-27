-- BUDGET DATA DIAGNOSTIC QUERIES
-- These queries help diagnose why budgets are not appearing in V5 view

-- 1. Check if there's any budget data in the raw source table
SELECT 
  'Raw Budget Data Count' as check_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT USER_NAME) as unique_users,
  SUM(CASE WHEN BUDGETED_MINUTES > 0 THEN 1 ELSE 0 END) as records_with_budget
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ`

UNION ALL

-- 2. Check the corrected view
SELECT 
  'Corrected View Data Count' as check_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT USER_NAME) as unique_users,
  SUM(CASE WHEN BUDGETED_MINUTES > 0 THEN 1 ELSE 0 END) as records_with_budget
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`

UNION ALL

-- 3. Check IndividualBudgetSummary CTE logic (simulated)
SELECT 
  'Individual Budget Summary (simulated)' as check_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(DISTINCT USER_NAME) as unique_users,
  COUNT(*) as records_with_budget
FROM (
  WITH deduplicated_budget_data AS (
    SELECT 
      WORK_ITEM_ID,
      USER_ID,
      USER_NAME,
      TASK_TYPE_ID,
      ROLE_ID,
      BUDGETED_MINUTES,
      BUDGETED_COST,
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
    SUM(BUDGETED_MINUTES) as total_individual_budgeted_minutes
  FROM deduplicated_budget_data
  WHERE rn = 1
  GROUP BY WORK_ITEM_ID, USER_ID, USER_NAME
);

-- 4. Sample budget records from corrected view
SELECT 
  'Sample Budget Records' as check_type,
  WORK_ITEM_ID,
  USER_NAME,
  BUDGETED_MINUTES,
  BUDGETED_COST,
  sync_reporting_date
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE BUDGETED_MINUTES > 0
  AND USER_NAME IS NOT NULL
ORDER BY sync_reporting_date DESC
LIMIT 10;

-- 5. Check if work items have any budget data at all
SELECT 
  'Work Items with Budget vs Without' as analysis,
  CASE 
    WHEN has_budget_data THEN 'HAS_BUDGET_DATA'
    ELSE 'NO_BUDGET_DATA'
  END as budget_status,
  COUNT(*) as work_item_count
FROM (
  SELECT 
    wi.WORK_ITEM_ID,
    CASE WHEN bva.WORK_ITEM_ID IS NOT NULL THEN true ELSE false END as has_budget_data
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` wi
  LEFT JOIN (
    SELECT DISTINCT WORK_ITEM_ID 
    FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
    WHERE BUDGETED_MINUTES > 0
  ) bva ON wi.WORK_ITEM_ID = bva.WORK_ITEM_ID
  WHERE wi.REPORTING_DATE = (
    SELECT MAX(REPORTING_DATE) 
    FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` latest
    WHERE latest.WORK_ITEM_ID = wi.WORK_ITEM_ID
  )
)
GROUP BY budget_status;

-- 6. Check if the issue is with the AllWorkItemUsers CTE
SELECT 
  'AllWorkItemUsers Analysis' as check_type,
  source_type,
  COUNT(*) as user_work_item_combinations
FROM (
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    'BUDGET' as source_type
  FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
  WHERE USER_NAME IS NOT NULL AND BUDGETED_MINUTES > 0
  
  UNION ALL
  
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    'TIME_ENTRY' as source_type
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
)
GROUP BY source_type;
