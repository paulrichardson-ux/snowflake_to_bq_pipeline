-- =============================================================================
-- DEPLOYMENT SCRIPT: Work Item Budget vs Actual Corrected View
-- =============================================================================
-- Execute this entire script in BigQuery Console to deploy and verify the fix

-- Step 1: Create the corrected view
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
  COALESCE(bva.BUDGETED_COST, 0) as budgeted_cost,
  COALESCE(bva.ACTUAL_COST, 0) as actual_cost,
  
  -- Budget variance calculations
  COALESCE(bva.ACTUAL_MINUTES, 0) - COALESCE(bva.BUDGETED_MINUTES, 0) as variance_minutes,
  (COALESCE(bva.ACTUAL_MINUTES, 0) - COALESCE(bva.BUDGETED_MINUTES, 0)) / 60.0 as variance_hours,
  COALESCE(bva.ACTUAL_COST, 0) - COALESCE(bva.BUDGETED_COST, 0) as variance_cost,
  
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

-- =============================================================================
-- IMMEDIATE VERIFICATION QUERIES
-- =============================================================================

-- Query 1: Quick alignment check for May 2025
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
    ROUND(SUM(budgeted_cost), 2) as total_budget_cost
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

-- Query 2: Show the problem - sync dates vs due dates
SELECT DISTINCT
  sync_reporting_date,
  COUNT(DISTINCT WORK_ITEM_ID) as work_items_with_sync_date,
  'Sync dates are probably recent, not May 2025' as note
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
GROUP BY sync_reporting_date
ORDER BY sync_reporting_date DESC
LIMIT 10;

-- Query 3: Top 10 clients with May 2025 budget (corrected view)
SELECT 
  CLIENT,
  COUNT(DISTINCT WORK_ITEM_ID) as work_items,
  ROUND(SUM(budgeted_hours), 2) as total_budget_hours,
  ROUND(SUM(budgeted_cost), 2) as total_budget_cost,
  ROUND(AVG(budget_utilization_percentage), 1) as avg_utilization_pct
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE due_year = 2025 AND due_month = 5
GROUP BY CLIENT
ORDER BY total_budget_hours DESC
LIMIT 10;

-- Query 4: Sample records showing the fix
SELECT 
  WORK_ITEM_ID,
  WORK_TITLE,
  CLIENT,
  sync_reporting_date,
  DUE_DATETIME,
  due_year,
  due_month,
  ROUND(budgeted_hours, 2) as budgeted_hours,
  schedule_status,
  budget_status
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE due_year = 2025 AND due_month = 5
ORDER BY budgeted_hours DESC
LIMIT 15;

-- DEPLOYMENT COMPLETE!
-- The corrected view should now show proper May 2025 budget alignment with V4 view 