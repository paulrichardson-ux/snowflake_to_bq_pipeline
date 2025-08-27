-- Create WORK_ITEM_BUDGET_VS_ACTUAL_CORRECTED_VIEW
-- This view corrects the issue where REPORTING_DATE in the budget vs actual table
-- is set to sync date instead of actual work item due dates
-- This enables proper filtering by due date (e.g., May 2025) for budget analysis
-- FIXED: Added deduplication logic to handle duplicate records from sync process

CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view` AS

-- First, deduplicate the raw budget vs actual data
WITH deduplicated_budget_vs_actual AS (
  SELECT DISTINCT
    REPORTING_DATE,
    WORK_ITEM_ID,
    WORK_TITLE,
    WORK_TYPE_ID,
    WORK_TYPE,
    TASK_TYPE_ID,
    TASK_TYPE,
    TASK_TYPE_BILLABLE_FLAG,
    ROLE_ID,
    ROLE_NAME,
    ACCOUNT_ID,
    ACCOUNT_NAME,
    USER_ID,
    USER_NAME,
    INTERNAL_CLIENT_ID,
    INTERNAL_CLIENT,
    CLIENT_ID,
    CLIENT,
    BUDGETED_MINUTES,
    ACTUAL_MINUTES,
    BUDGETED_COST,
    ACTUAL_COST
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ`
)

SELECT 
  -- All fields from WORK_ITEM_BUDGET_VS_ACTUAL_BQ (now deduplicated)
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
  COALESCE(bva.BUDGETED_MINUTES, 0) - COALESCE(bva.ACTUAL_MINUTES, 0) as variance_minutes,
  (COALESCE(bva.BUDGETED_MINUTES, 0) - COALESCE(bva.ACTUAL_MINUTES, 0)) / 60.0 as variance_hours,
  COALESCE(bva.BUDGETED_COST, 0) - COALESCE(bva.ACTUAL_COST, 0) as variance_cost,
  
  -- Budget utilization percentage
  CASE 
    WHEN COALESCE(bva.BUDGETED_MINUTES, 0) > 0 
    THEN (COALESCE(bva.ACTUAL_MINUTES, 0) / bva.BUDGETED_MINUTES) * 100
    ELSE NULL 
  END AS budget_utilization_percentage,
  
  -- Budget status classification
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

FROM deduplicated_budget_vs_actual bva
LEFT JOIN `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_LATEST_VIEW` wi 
  ON bva.WORK_ITEM_ID = wi.WORK_ITEM_ID

-- Order by due date for better performance and logical ordering
ORDER BY wi.DUE_DATETIME DESC, bva.CLIENT, bva.WORK_TITLE; 