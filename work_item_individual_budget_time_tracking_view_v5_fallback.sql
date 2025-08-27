-- FALLBACK VERSION: WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5_FALLBACK
-- This version shows users who logged time when individual budget allocations are not available
-- It falls back to work item level budgets distributed among users who actually logged time

CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5_FALLBACK` AS

WITH IndividualTimeEntrySummary AS (
  -- Aggregate time entries by work item AND user for individual analytics
  SELECT 
    WORK_ITEM_ID,
    USER_NAME,
    USER_ID,
    SUM(CASE WHEN IS_BILLABLE = true THEN MINUTES ELSE 0 END) / 60.0 AS billable_hours_logged,
    SUM(MINUTES) / 60.0 AS total_hours_logged,
    COUNT(*) AS total_time_entries,
    MIN(REPORTING_DATE) AS first_time_entry_date,
    MAX(REPORTING_DATE) AS last_time_entry_date,
    -- Monthly breakdown for 2025
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 7 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_jul_2025
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
  GROUP BY WORK_ITEM_ID, USER_NAME, USER_ID
),

WorkItemTimeEntrySummary AS (
  -- Aggregate time entries by work item for work item level analytics
  SELECT 
    WORK_ITEM_ID,
    SUM(CASE WHEN IS_BILLABLE = true THEN MINUTES ELSE 0 END) / 60.0 AS work_item_billable_hours_logged,
    SUM(MINUTES) / 60.0 AS work_item_total_hours_logged,
    COUNT(*) AS work_item_total_time_entries,
    COUNT(DISTINCT USER_NAME) AS unique_contributors
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
  GROUP BY WORK_ITEM_ID
),

IndividualBudgetSummary AS (
  -- Try to get individual user budgets, but also include fallback logic
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    SUM(BUDGETED_MINUTES) as total_individual_budgeted_minutes,
    SUM(ACTUAL_MINUTES) as total_individual_actual_minutes_from_budget_data,
    SUM(BUDGETED_COST) as total_individual_budgeted_cost,
    SUM(ACTUAL_COST) as total_individual_actual_cost,
    ANY_VALUE(WORK_TITLE) as work_title,
    ANY_VALUE(CLIENT) as client,
    ANY_VALUE(DUE_DATETIME) as due_datetime,
    ANY_VALUE(due_year) as due_year,
    ANY_VALUE(due_month) as due_month,
    ANY_VALUE(due_date) as due_date
  FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
  WHERE USER_NAME IS NOT NULL
    AND BUDGETED_MINUTES > 0
  GROUP BY WORK_ITEM_ID, USER_ID, USER_NAME
),

FallbackUserBudgets AS (
  -- For work items where individual budgets don't exist, create fallback budgets
  -- based on proportional distribution among users who logged time
  SELECT 
    wi.WORK_ITEM_ID,
    ite.USER_ID,
    ite.USER_NAME,
    -- Distribute work item budget proportionally based on time logged
    CASE 
      WHEN wite.work_item_total_hours_logged > 0 
      THEN (wi.BUDGETED_MINUTES * (ite.total_hours_logged / wite.work_item_total_hours_logged))
      ELSE 0
    END as estimated_individual_budgeted_minutes,
    0 as total_individual_actual_minutes_from_budget_data,
    0 as total_individual_budgeted_cost,
    0 as total_individual_actual_cost,
    wi.WORK_TITLE as work_title,
    wi.CLIENT as client,
    wi.DUE_DATETIME as due_datetime,
    EXTRACT(YEAR FROM wi.DUE_DATETIME) as due_year,
    EXTRACT(MONTH FROM wi.DUE_DATETIME) as due_month,
    DATE(wi.DUE_DATETIME) as due_date
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` wi
  JOIN IndividualTimeEntrySummary ite ON wi.WORK_ITEM_ID = ite.WORK_ITEM_ID
  JOIN WorkItemTimeEntrySummary wite ON wi.WORK_ITEM_ID = wite.WORK_ITEM_ID
  WHERE wi.REPORTING_DATE = (
    SELECT MAX(REPORTING_DATE) 
    FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` latest
    WHERE latest.WORK_ITEM_ID = wi.WORK_ITEM_ID
  )
  -- Only include if no individual budget exists
  AND wi.WORK_ITEM_ID NOT IN (
    SELECT WORK_ITEM_ID 
    FROM IndividualBudgetSummary
  )
),

CombinedBudgetSummary AS (
  -- Combine actual individual budgets with fallback budgets
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    total_individual_budgeted_minutes,
    total_individual_actual_minutes_from_budget_data,
    total_individual_budgeted_cost,
    total_individual_actual_cost,
    work_title,
    client,
    due_datetime,
    due_year,
    due_month,
    due_date,
    'ACTUAL_INDIVIDUAL_BUDGET' as budget_source
  FROM IndividualBudgetSummary
  
  UNION ALL
  
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    estimated_individual_budgeted_minutes as total_individual_budgeted_minutes,
    total_individual_actual_minutes_from_budget_data,
    total_individual_budgeted_cost,
    total_individual_actual_cost,
    work_title,
    client,
    due_datetime,
    due_year,
    due_month,
    due_date,
    'ESTIMATED_FROM_TIME_LOGGED' as budget_source
  FROM FallbackUserBudgets
)

SELECT 
  -- Core work item identification
  budget.WORK_ITEM_ID,
  budget.REPORTING_DATE,
  budget.WORK_TITLE,
  budget.CLIENT_ID,
  budget.CLIENT,
  budget.DUE_DATETIME,
  budget.PRIMARY_STATUS,
  budget.ASSIGNED_TO,
  budget.ASSIGNED_TO_ID,
  
  -- Individual User Budget Information (actual or estimated)
  COALESCE(cbs.USER_ID, ite.USER_ID) as budget_user_id,
  COALESCE(cbs.USER_NAME, ite.USER_NAME) as budget_user_name,
  COALESCE(cbs.total_individual_budgeted_minutes, 0) as individual_budgeted_minutes,
  COALESCE(cbs.total_individual_budgeted_minutes, 0) / 60.0 as individual_budgeted_hours,
  cbs.budget_source,
  
  -- Individual Time Tracking (actual logged time per user)
  COALESCE(ite.total_hours_logged, 0) AS individual_hours_logged_actual,
  COALESCE(ite.billable_hours_logged, 0) AS individual_billable_hours_logged,
  COALESCE(ite.total_time_entries, 0) AS individual_time_entries_count,
  COALESCE(ite.hours_logged_jul_2025, 0) AS hours_logged_jul_2025,
  
  -- Individual Budget vs Actual Analysis
  COALESCE(cbs.total_individual_budgeted_minutes, 0) - (COALESCE(ite.total_hours_logged, 0) * 60) as individual_budget_variance_minutes,
  (COALESCE(cbs.total_individual_budgeted_minutes, 0) / 60.0) - COALESCE(ite.total_hours_logged, 0) as individual_budget_variance_hours,
  
  -- Work Item Level Context
  budget.BUDGETED_MINUTES as work_item_total_budgeted_minutes,
  COALESCE(budget.BUDGETED_MINUTES, 0) / 60.0 as work_item_total_budget_hours,
  COALESCE(wite.work_item_total_hours_logged, 0) AS work_item_total_hours_logged_summary,
  
  -- Status flags
  CASE WHEN cbs.USER_ID IS NOT NULL THEN true ELSE false END as has_individual_budget,
  CASE WHEN ite.USER_ID IS NOT NULL THEN true ELSE false END as has_individual_time_logged,
  CASE WHEN cbs.budget_source = 'ACTUAL_INDIVIDUAL_BUDGET' THEN true ELSE false END as has_actual_individual_budget

FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` budget

-- Left join with combined budget summary (actual or estimated)
LEFT JOIN CombinedBudgetSummary cbs
  ON budget.WORK_ITEM_ID = cbs.WORK_ITEM_ID

-- Left join with individual time tracking - join on user from budget OR time entries
LEFT JOIN IndividualTimeEntrySummary ite 
  ON budget.WORK_ITEM_ID = ite.WORK_ITEM_ID 
  AND (cbs.USER_NAME = ite.USER_NAME OR cbs.USER_NAME IS NULL)

-- Left join with work item level time summary
LEFT JOIN WorkItemTimeEntrySummary wite 
  ON budget.WORK_ITEM_ID = wite.WORK_ITEM_ID

-- Filter to only show the latest version of each work item
WHERE budget.REPORTING_DATE = (
  SELECT MAX(REPORTING_DATE) 
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` latest
  WHERE latest.WORK_ITEM_ID = budget.WORK_ITEM_ID
)

-- Only show records where we have either individual budget or time logged
AND (cbs.USER_ID IS NOT NULL OR ite.USER_ID IS NOT NULL)

-- Order by work item and then by user for logical grouping
ORDER BY budget.WORK_ITEM_ID, COALESCE(cbs.USER_NAME, ite.USER_NAME); 