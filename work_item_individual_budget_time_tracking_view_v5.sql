-- Create WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5
-- FIXED VERSION: This fixes the join issue where users with time logged but no individual budget don't appear
-- Uses FULL OUTER JOIN logic to show ALL users from both budget and time tracking data
-- This enables budget vs actual tracking at the individual user level per work item
-- FIXED: Now uses WORK_ITEM_DETAILS_BQ with proper latest record filter to avoid duplicates
-- FIXED: Removed fallback budget logic entirely to prevent over-counting (was adding 500+ extra hours)
-- FIXED: Resolved sync date duplication issue by excluding sync_reporting_date from PARTITION BY clause

CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5` AS

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
    -- Add time entry date aggregations for filtering by actual tracking dates
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 1 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_jan_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 2 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_feb_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 3 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_mar_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 4 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_apr_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 5 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_may_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 6 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_jun_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 7 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_jul_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 8 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_aug_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 9 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_sep_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 10 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_oct_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 11 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_nov_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM REPORTING_DATE) = 2025 AND EXTRACT(MONTH FROM REPORTING_DATE) = 12 THEN MINUTES ELSE 0 END) / 60.0 AS hours_logged_dec_2025
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
  GROUP BY WORK_ITEM_ID, USER_NAME, USER_ID
),

WorkItemTimeEntrySummary AS (
  -- Aggregate time entries by work item for work item level analytics (from V4)
  SELECT 
    WORK_ITEM_ID,
    SUM(CASE WHEN IS_BILLABLE = true THEN MINUTES ELSE 0 END) / 60.0 AS work_item_billable_hours_logged,
    SUM(MINUTES) / 60.0 AS work_item_total_hours_logged,
    COUNT(*) AS work_item_total_time_entries,
    COUNT(DISTINCT USER_NAME) AS unique_contributors,
    MIN(REPORTING_DATE) AS first_time_entry_date,
    MAX(REPORTING_DATE) AS last_time_entry_date
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
  GROUP BY WORK_ITEM_ID
),

IndividualBudgetSummary AS (
  -- FIXED: Advanced deduplication that handles sync date duplicates properly
  -- Removes identical budget allocations synced on different dates while preserving different task type allocations
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
      WORK_TITLE,
      CLIENT,
      DUE_DATETIME,
      due_year,
      due_month,
      due_date,
      sync_reporting_date,
      -- FIXED: Exclude ACTUAL_MINUTES and ACTUAL_COST from PARTITION BY since they change as time is logged
      -- Only deduplicate based on budget allocation (BUDGETED_MINUTES, BUDGETED_COST) not actual time
      ROW_NUMBER() OVER (
        PARTITION BY WORK_ITEM_ID, USER_ID, USER_NAME, TASK_TYPE_ID, ROLE_ID, BUDGETED_MINUTES, BUDGETED_COST
        ORDER BY sync_reporting_date DESC
      ) as rn
    FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
    WHERE USER_NAME IS NOT NULL 
      AND BUDGETED_MINUTES > 0
  )
  -- Now aggregate the deduplicated data (preserving all legitimate task type allocations)
  SELECT 
    WORK_ITEM_ID,
    USER_ID,
    USER_NAME,
    SUM(BUDGETED_MINUTES) as total_individual_budgeted_minutes,
    SUM(ACTUAL_MINUTES) as total_individual_actual_minutes_from_budget_data,
    SUM(BUDGETED_COST) as total_individual_budgeted_cost,
    SUM(ACTUAL_COST) as total_individual_actual_cost,
    -- Keep one representative record for other fields
    ANY_VALUE(WORK_TITLE) as work_title,
    ANY_VALUE(CLIENT) as client,
    ANY_VALUE(DUE_DATETIME) as due_datetime,
    ANY_VALUE(due_year) as due_year,
    ANY_VALUE(due_month) as due_month,
    ANY_VALUE(due_date) as due_date
  FROM deduplicated_budget_data
  WHERE rn = 1  -- Only keep one instance of each true duplicate
  GROUP BY WORK_ITEM_ID, USER_ID, USER_NAME
),

-- REMOVED: Fallback budget logic was causing over-counting by double-counting unassigned budgets
-- The fallback logic redistributed unassigned budgets but still counted them as additional budget
-- This caused 500+ hours of over-counting across the entire view

-- CRITICAL FIX: Create a comprehensive user-work item combination that includes ALL users
-- This ensures users with time logged but no individual budget still appear
-- FIXED: Consolidate users to prevent duplicates when a user has BOTH budget AND time logged
AllWorkItemUsers AS (
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
    FROM IndividualBudgetSummary
    
    UNION ALL
    
    -- Users with time logged (even if no individual budget)
    SELECT 
      WORK_ITEM_ID,
      USER_ID,
      USER_NAME
    FROM IndividualTimeEntrySummary
  )
)

SELECT 
  -- Core work item identification (keep from V4)
  budget.WORK_ITEM_ID,
  budget.REPORTING_DATE,
  budget.WORK_TITLE,
  budget.CLIENT_ID,
  budget.CLIENT,
  budget.CLIENT_TYPE,
  budget.INTERNAL_CLIENT_ID,
  budget.INTERNAL_CLIENT,
  budget.ACCOUNT_ID,
  budget.ACCOUNT_NAME,
  budget.WORK_TYPE_ID,
  budget.WORK_TYPE,
  budget.PRIMARY_STATUS_ID,
  budget.SECONDARY_STATUS_ID,
  budget.PRIMARY_STATUS,
  budget.SECONDARY_STATUS,
  budget.SECONDARY_STATUS_ORDER,
  budget.CURRENT_STATUS_ENTRY_DATE,
  budget.REPEAT_SCHEDULE,
  budget.CREATED_DATETIME,
  budget.CREATED_BY_ID,
  budget.CREATED_BY,
  budget.START_DATETIME,
  budget.ASSIGNED_TO_ID,
  budget.ASSIGNED_TO,
  budget.DUE_DATETIME,
  budget.DEADLINE_DATETIME,
  budget.COMPLETED_DATETIME,
  budget.COMPLETED_BY_ID,
  budget.COMPLETED_BY,
  budget.USER_DEFINED_CLIENT_ID,
  budget.WORK_TEMPLATE_ID,
  budget.WORK_TEMPLATE,
  budget.WORK_DESCRIPTION,
  budget.FIXED_FEE_ESTIMATED_COST,
  budget.EXPENSE_AMOUNT,
  budget.BILLABLE_EXPENSE_AMOUNT,
  budget.INTERNAL_TASKS_COMPLETED_COUNT,
  budget.INTERNAL_TASKS_PENDING_COUNT,
  budget.IS_WORK_ITEM_OVERDUE,
  
  -- ENHANCED: Individual User Budget Information (FIXED: Only individual budgets, no fallback)
  awu.USER_ID as budget_user_id,
  awu.USER_NAME as budget_user_name,
  -- FIXED: Only use individual budgets (no fallback logic that caused over-counting)
  COALESCE(ibs.total_individual_budgeted_minutes, 0) as individual_budgeted_minutes,
  COALESCE(ibs.total_individual_actual_minutes_from_budget_data, 0) as individual_actual_minutes_from_budget_data,
  COALESCE(ibs.total_individual_budgeted_cost, 0) as individual_budgeted_cost,
  COALESCE(ibs.total_individual_actual_cost, 0) as individual_actual_cost,
  
  -- NEW V5: Individual User Budget Calculations (FIXED: Only individual budgets)
  COALESCE(ibs.total_individual_budgeted_minutes, 0) / 60.0 as individual_budgeted_hours,
  COALESCE(ibs.total_individual_actual_minutes_from_budget_data, 0) / 60.0 as individual_actual_hours_from_budget_data,
  COALESCE(ibs.total_individual_budgeted_cost, 0) as individual_budgeted_cost_calculated,
  COALESCE(ibs.total_individual_actual_cost, 0) as individual_actual_cost_calculated,
  
  -- NEW: Budget source tracking (simplified)
  CASE 
    WHEN ibs.USER_ID IS NOT NULL THEN 'INDIVIDUAL_BUDGET'
    ELSE 'NO_BUDGET'
  END as budget_source,
  
  -- FIXED: Individual Time Tracking (now shows ALL users with time logged)
  COALESCE(ite.total_hours_logged, 0) AS individual_hours_logged_actual,
  COALESCE(ite.billable_hours_logged, 0) AS individual_billable_hours_logged,
  COALESCE(ite.total_time_entries, 0) AS individual_time_entries_count,
  ite.first_time_entry_date as individual_first_time_entry,
  ite.last_time_entry_date as individual_last_time_entry,
  
  -- NEW V5: Time tracking by actual reporting month (for filtering by when time was tracked)
  COALESCE(ite.hours_logged_jan_2025, 0) AS hours_logged_jan_2025,
  COALESCE(ite.hours_logged_feb_2025, 0) AS hours_logged_feb_2025,
  COALESCE(ite.hours_logged_mar_2025, 0) AS hours_logged_mar_2025,
  COALESCE(ite.hours_logged_apr_2025, 0) AS hours_logged_apr_2025,
  COALESCE(ite.hours_logged_may_2025, 0) AS hours_logged_may_2025,
  COALESCE(ite.hours_logged_jun_2025, 0) AS hours_logged_jun_2025,
  COALESCE(ite.hours_logged_jul_2025, 0) AS hours_logged_jul_2025,
  COALESCE(ite.hours_logged_aug_2025, 0) AS hours_logged_aug_2025,
  COALESCE(ite.hours_logged_sep_2025, 0) AS hours_logged_sep_2025,
  COALESCE(ite.hours_logged_oct_2025, 0) AS hours_logged_oct_2025,
  COALESCE(ite.hours_logged_nov_2025, 0) AS hours_logged_nov_2025,
  COALESCE(ite.hours_logged_dec_2025, 0) AS hours_logged_dec_2025,
  
  -- ENHANCED V5: Individual Budget vs Actual Analysis (FIXED: Only individual budgets)
  COALESCE(ibs.total_individual_budgeted_minutes, 0) - (COALESCE(ite.total_hours_logged, 0) * 60) as individual_budget_variance_minutes,
  (COALESCE(ibs.total_individual_budgeted_minutes, 0) / 60.0) - COALESCE(ite.total_hours_logged, 0) as individual_budget_variance_hours,
  
  -- ENHANCED V5: Individual Budget Utilization (FIXED: Only individual budgets)
  CASE 
    WHEN COALESCE(ibs.total_individual_budgeted_minutes, 0) > 0 
    THEN (COALESCE(ite.total_hours_logged, 0) / (COALESCE(ibs.total_individual_budgeted_minutes, 0) / 60.0)) * 100
    ELSE NULL 
  END AS individual_budget_utilization_percentage,
  
  -- ENHANCED V5: Individual Budget Status (FIXED: Only individual budgets)
  CASE 
    WHEN COALESCE(ibs.total_individual_budgeted_minutes, 0) > 0 AND COALESCE(ite.total_hours_logged, 0) > (COALESCE(ibs.total_individual_budgeted_minutes, 0) / 60.0) 
    THEN 'OVER_BUDGET'
    WHEN COALESCE(ibs.total_individual_budgeted_minutes, 0) > 0 AND COALESCE(ite.total_hours_logged, 0) > (COALESCE(ibs.total_individual_budgeted_minutes, 0) / 60.0 * 0.8)
    THEN 'APPROACHING_BUDGET'
    WHEN COALESCE(ibs.total_individual_budgeted_minutes, 0) > 0 
    THEN 'WITHIN_BUDGET'
    ELSE 'NO_INDIVIDUAL_BUDGET_SET'
  END AS individual_budget_status,
  
  -- KEEP V4: Work Item Level Totals (for comparison/context)
  budget.BUDGETED_MINUTES as work_item_total_budgeted_minutes,
  budget.BUDGETED_COST as work_item_total_budgeted_cost,
  budget.TIME_ENTRY_MINUTES as work_item_total_time_entry_minutes,
  budget.TIME_ENTRY_COST as work_item_total_time_entry_cost,
  budget.BUDGET_REMAINING_HOURS as work_item_budget_remaining_hours,
  COALESCE(budget.BUDGETED_MINUTES, 0) / 60.0 as work_item_total_budget_hours,
  COALESCE(budget.TIME_ENTRY_MINUTES, 0) / 60.0 as work_item_total_actual_hours,
  
  -- KEEP V4: Work Item Time Summary (from aggregated time entries)
  COALESCE(wite.work_item_total_hours_logged, 0) AS work_item_total_hours_logged_summary,
  COALESCE(wite.work_item_billable_hours_logged, 0) AS work_item_billable_hours_logged_summary,
  COALESCE(wite.work_item_total_time_entries, 0) AS work_item_total_time_entries,
  COALESCE(wite.unique_contributors, 0) AS work_item_unique_contributors,
  wite.first_time_entry_date as work_item_first_time_entry_date,
  wite.last_time_entry_date as work_item_last_time_entry_date,
  
  -- KEEP V4: Corrected budget remaining hours calculation
  CASE
    WHEN budget.BUDGETED_MINUTES IS NULL AND (budget.TIME_ENTRY_MINUTES IS NULL OR budget.TIME_ENTRY_MINUTES = 0) THEN NULL
    WHEN budget.BUDGETED_MINUTES IS NULL THEN -SAFE_DIVIDE(budget.TIME_ENTRY_MINUTES, 60)
    ELSE SAFE_DIVIDE(budget.BUDGETED_MINUTES, 60) - SAFE_DIVIDE(budget.TIME_ENTRY_MINUTES, 60)
  END as work_item_budget_remaining_hours_corrected,
  
  -- KEEP V4: Productivity fields from PRODUCTIVITY_REPEATS_SYNC
  prod.CATEGORY,
  prod.PROJECT,
  prod.MEASURE_TYPE,
  prod.POINTS,
  prod.WEIGHTING,
  prod.TOTAL_POINTS,
  prod.SYNC_TIMESTAMP as PRODUCTIVITY_SYNC_TIMESTAMP,
  prod.LAST_MODIFIED_BY as PRODUCTIVITY_LAST_MODIFIED_BY,
  
  -- KEEP V4: Calculated productivity metrics
  CASE
    WHEN prod.POINTS IS NOT NULL AND budget.TIME_ENTRY_MINUTES IS NOT NULL AND budget.TIME_ENTRY_MINUTES > 0
    THEN SAFE_DIVIDE(prod.POINTS, SAFE_DIVIDE(budget.TIME_ENTRY_MINUTES, 60.0))
    ELSE NULL
  END as POINTS_PER_HOUR_ACTUAL,
  
  CASE
    WHEN prod.POINTS IS NOT NULL AND budget.BUDGETED_MINUTES IS NOT NULL AND budget.BUDGETED_MINUTES > 0
    THEN SAFE_DIVIDE(prod.POINTS, SAFE_DIVIDE(budget.BUDGETED_MINUTES, 60.0))
    ELSE NULL
  END as POINTS_PER_HOUR_BUDGETED,
  
  CASE
    WHEN prod.TOTAL_POINTS IS NOT NULL AND budget.TIME_ENTRY_MINUTES IS NOT NULL AND budget.TIME_ENTRY_MINUTES > 0
    THEN SAFE_DIVIDE(prod.TOTAL_POINTS, SAFE_DIVIDE(budget.TIME_ENTRY_MINUTES, 60.0))
    ELSE NULL
  END as TOTAL_POINTS_PER_HOUR_ACTUAL,
  
  -- KEEP V4: Productivity status indicators
  CASE
    WHEN prod.MEASURE_TYPE IS NOT NULL THEN TRUE
    ELSE FALSE
  END as HAS_PRODUCTIVITY_DATA,
  
  CASE
    WHEN prod.MEASURE_TYPE IN ('Productivity', 'Productivity SLA') THEN TRUE
    ELSE FALSE
  END as IS_PRODUCTIVITY_TASK,
  
  CASE
    WHEN prod.MEASURE_TYPE = 'SLA' OR prod.MEASURE_TYPE = 'Productivity SLA' THEN TRUE
    ELSE FALSE
  END as IS_SLA_TASK,
  
  -- KEEP V4: Client Group Information from CLIENT_GROUP_DIMENSION
  cg.CLIENT_GROUP_ID,
  cg.CLIENT_GROUP_NAME AS client_group_name,
  cg.CLIENT_GROUP_MEMBER_TYPE AS client_group_member_type,
  
  -- KEEP V4: Additional Client Details from CLIENT_DIMENSION
  cd.CLIENT_SUBTYPE,
  cd.CLIENT_OWNER_USER_NAME,
  cd.CLIENT_OWNER_USER_ID,
  cd.CLIENT_MANAGER_USER_NAME,
  cd.CLIENT_MANAGER_USER_ID,
  cd.PRIMARY_ADDRESS_COUNTRY_CODE,
  cd.PRIMARY_ADDRESS_STATE_PROVINCE_COUNTY,
  cd.PRIMARY_ADDRESS_CITY,
  
  -- KEEP V4: Client Owner's Tenant Team Information
  ttm.TENANT_TEAM_ID AS client_owner_tenant_team_id,
  tt.TENANT_TEAM_NAME AS client_owner_tenant_team_name,
  
  -- KEEP V4: Client Manager's Tenant Team Information
  ttm_mgr.TENANT_TEAM_ID AS client_manager_tenant_team_id,
  tt_mgr.TENANT_TEAM_NAME AS client_manager_tenant_team_name,
  
  -- KEEP V4: Assigned User Information (from USER_DIMENSION)
  ud_assigned.USER_JOB_TITLE AS assigned_user_job_title,
  COALESCE(ud_assigned.EXPECTED_BILLABLE_MINUTES, 0) / 60.0 AS assigned_user_expected_billable_hours,
  COALESCE(ud_assigned.EXPECTED_NONBILLABLE_MINUTES, 0) / 60.0 AS assigned_user_expected_nonbillable_hours,
  
  -- KEEP V4: Client Owner User Information (from USER_DIMENSION)
  ud_client_owner.USER_JOB_TITLE AS client_owner_job_title,
  COALESCE(ud_client_owner.EXPECTED_BILLABLE_MINUTES, 0) / 60.0 AS client_owner_expected_billable_hours,
  COALESCE(ud_client_owner.EXPECTED_NONBILLABLE_MINUTES, 0) / 60.0 AS client_owner_expected_nonbillable_hours,
  
  -- KEEP V4: Client Manager User Information (from USER_DIMENSION)
  ud_client_manager.USER_JOB_TITLE AS client_manager_job_title,
  COALESCE(ud_client_manager.EXPECTED_BILLABLE_MINUTES, 0) / 60.0 AS client_manager_expected_billable_hours,
  COALESCE(ud_client_manager.EXPECTED_NONBILLABLE_MINUTES, 0) / 60.0 AS client_manager_expected_nonbillable_hours,
  
  -- KEEP V4: Work Item Level Budget Analytics
  CASE 
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
    THEN COALESCE(wite.work_item_total_hours_logged, 0) / (budget.BUDGETED_MINUTES / 60.0) * 100
    ELSE NULL 
  END AS work_item_budget_utilization_percentage,
  
  CASE 
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
    THEN (budget.BUDGETED_MINUTES / 60.0) - COALESCE(wite.work_item_total_hours_logged, 0)
    ELSE NULL 
  END AS work_item_budget_remaining_hours_calculated,
  
  -- KEEP V4: Status Indicators
  CASE 
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 AND COALESCE(wite.work_item_total_hours_logged, 0) > (budget.BUDGETED_MINUTES / 60.0) 
    THEN 'OVER_BUDGET'
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 AND COALESCE(wite.work_item_total_hours_logged, 0) > (budget.BUDGETED_MINUTES / 60.0 * 0.8)
    THEN 'APPROACHING_BUDGET'
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
    THEN 'WITHIN_BUDGET'
    ELSE 'NO_BUDGET_SET'
  END AS work_item_budget_status,
  
  CASE 
    WHEN budget.DUE_DATETIME IS NOT NULL AND budget.DUE_DATETIME < CURRENT_TIMESTAMP() AND budget.PRIMARY_STATUS NOT IN ('Completed', 'Closed')
    THEN 'OVERDUE'
    WHEN budget.DUE_DATETIME IS NOT NULL AND budget.DUE_DATETIME <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY) AND budget.PRIMARY_STATUS NOT IN ('Completed', 'Closed')
    THEN 'DUE_SOON'
    ELSE 'ON_TRACK'
  END AS schedule_status,
  
  -- KEEP V4: Client Group Account Information
  cg.ACCOUNT_ID AS client_group_account_id,
  cg.ACCOUNT_NAME AS client_group_account_name,
  
  -- KEEP V4: Useful date flags for reporting
  CASE WHEN budget.DUE_DATETIME IS NOT NULL THEN true ELSE false END as has_due_date,
  CASE WHEN budget.PRIMARY_STATUS IN ('Completed', 'Closed') THEN true ELSE false END as is_completed,
  CASE WHEN budget.DUE_DATETIME < CURRENT_TIMESTAMP() THEN true ELSE false END as is_past_due,
  
  -- ENHANCED: Individual user flags (FIXED: Simplified without fallback budgets)
  CASE WHEN ibs.USER_ID IS NOT NULL THEN true ELSE false END as has_individual_budget,
  CASE WHEN ite.USER_ID IS NOT NULL THEN true ELSE false END as has_individual_time_logged,
  CASE WHEN ibs.USER_ID IS NOT NULL THEN true ELSE false END as has_actual_individual_budget

-- FIXED: Base table now uses WORK_ITEM_DETAILS_BQ with proper latest record filter
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` budget

-- CRITICAL FIX: Join with ALL users (budget + time tracking combined)
INNER JOIN AllWorkItemUsers awu
  ON budget.WORK_ITEM_ID = awu.WORK_ITEM_ID

-- FIXED: Left join with aggregated individual user budgets
LEFT JOIN IndividualBudgetSummary ibs
  ON budget.WORK_ITEM_ID = ibs.WORK_ITEM_ID 
  AND awu.USER_NAME = ibs.USER_NAME

-- FIXED: Left join with individual user time tracking
LEFT JOIN IndividualTimeEntrySummary ite 
  ON budget.WORK_ITEM_ID = ite.WORK_ITEM_ID 
  AND awu.USER_NAME = ite.USER_NAME

-- REMOVED: Fallback user budgets join (was causing over-counting)

-- KEEP V4: Join with work item level time summary
LEFT JOIN WorkItemTimeEntrySummary wite 
  ON budget.WORK_ITEM_ID = wite.WORK_ITEM_ID

-- KEEP V4: All other joins from V4
LEFT JOIN `red-octane-444308-f4.karbon_data.PRODUCTIVITY_REPEATS_SYNC` prod
  ON budget.WORK_ITEM_ID = prod.WORK_ITEM_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION` cg 
  ON budget.CLIENT_ID = cg.CLIENT_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION` cd 
  ON budget.CLIENT_ID = cd.CLIENT_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm 
  ON cd.CLIENT_OWNER_USER_ID = ttm.USER_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt 
  ON ttm.TENANT_TEAM_ID = tt.TENANT_TEAM_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm_mgr 
  ON cd.CLIENT_MANAGER_USER_ID = ttm_mgr.USER_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt_mgr 
  ON ttm_mgr.TENANT_TEAM_ID = tt_mgr.TENANT_TEAM_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.USER_DIMENSION` ud_assigned 
  ON budget.ASSIGNED_TO_ID = ud_assigned.USER_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.USER_DIMENSION` ud_client_owner 
  ON cd.CLIENT_OWNER_USER_ID = ud_client_owner.USER_ID

LEFT JOIN `red-octane-444308-f4.karbon_data.USER_DIMENSION` ud_client_manager 
  ON cd.CLIENT_MANAGER_USER_ID = ud_client_manager.USER_ID

-- FIXED: Filter to only show the latest version of each work item (same as V4)
WHERE budget.REPORTING_DATE = (
  SELECT MAX(REPORTING_DATE) 
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` latest
  WHERE latest.WORK_ITEM_ID = budget.WORK_ITEM_ID
)

-- Order by work item and then by user for logical grouping
ORDER BY budget.WORK_ITEM_ID, awu.USER_NAME; 