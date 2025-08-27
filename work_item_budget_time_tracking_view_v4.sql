-- Create WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4
-- This view extends the V3 functionality by adding client group information
-- from the CLIENT_GROUP_DIMENSION table and additional client details from CLIENT_DIMENSION

CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4` AS

WITH TimeEntrySummary AS (
  -- Aggregate time entries by work item for additional analytics
  SELECT 
    WORK_ITEM_ID,
    SUM(CASE WHEN IS_BILLABLE = true THEN MINUTES ELSE 0 END) / 60.0 AS billable_hours_logged,
    SUM(MINUTES) / 60.0 AS total_hours_logged,
    COUNT(*) AS total_time_entries,
    COUNT(DISTINCT USER_NAME) AS unique_contributors,
    MIN(REPORTING_DATE) AS first_time_entry_date,
    MAX(REPORTING_DATE) AS last_time_entry_date
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
  GROUP BY WORK_ITEM_ID
)

SELECT 
  -- All fields from WORK_ITEM_DETAILS_LATEST_VIEW (matching V3 structure)
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
  budget.BUDGETED_MINUTES,
  budget.BUDGETED_COST,
  budget.EXPENSE_AMOUNT,
  budget.BILLABLE_EXPENSE_AMOUNT,
  budget.TIME_ENTRY_MINUTES,
  budget.TIME_ENTRY_COST,
  budget.BUDGET_REMAINING_HOURS,
  budget.INTERNAL_TASKS_COMPLETED_COUNT,
  budget.INTERNAL_TASKS_PENDING_COUNT,
  budget.IS_WORK_ITEM_OVERDUE,
  
  -- Add the corrected budget remaining hours calculation (from V3)
  CASE
    WHEN budget.BUDGETED_MINUTES IS NULL AND (budget.TIME_ENTRY_MINUTES IS NULL OR budget.TIME_ENTRY_MINUTES = 0) THEN NULL
    WHEN budget.BUDGETED_MINUTES IS NULL THEN -SAFE_DIVIDE(budget.TIME_ENTRY_MINUTES, 60)
    ELSE SAFE_DIVIDE(budget.BUDGETED_MINUTES, 60) - SAFE_DIVIDE(budget.TIME_ENTRY_MINUTES, 60)
  END as BUDGET_REMAINING_HOURS_CORRECTED,
  
  -- Productivity fields from PRODUCTIVITY_REPEATS_SYNC (from V3)
  prod.MEASURE_TYPE,
  prod.POINTS,
  prod.WEIGHTING,
  prod.TOTAL_POINTS,
  
  -- Additional metadata from productivity sync (from V3)
  prod.SYNC_TIMESTAMP as PRODUCTIVITY_SYNC_TIMESTAMP,
  prod.LAST_MODIFIED_BY as PRODUCTIVITY_LAST_MODIFIED_BY,
  
  -- Calculated productivity metrics (from V3)
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
  
  -- Productivity status indicators (from V3)
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
  
  -- NEW: Client Group Information from CLIENT_GROUP_DIMENSION
  cg.CLIENT_GROUP_ID,
  cg.CLIENT_GROUP_NAME AS client_group_name,
  cg.CLIENT_GROUP_MEMBER_TYPE AS client_group_member_type,
  
  -- NEW: Additional Client Details from CLIENT_DIMENSION
  cd.CLIENT_SUBTYPE,
  cd.CLIENT_OWNER_USER_NAME,
  cd.CLIENT_OWNER_USER_ID,
  cd.CLIENT_MANAGER_USER_NAME,
  cd.CLIENT_MANAGER_USER_ID,
  cd.PRIMARY_ADDRESS_COUNTRY_CODE,
  cd.PRIMARY_ADDRESS_STATE_PROVINCE_COUNTY,
  cd.PRIMARY_ADDRESS_CITY,
  
  -- NEW: Client Owner's Tenant Team Information
  ttm.TENANT_TEAM_ID AS client_owner_tenant_team_id,
  tt.TENANT_TEAM_NAME AS client_owner_tenant_team_name,
  
  -- NEW: Client Manager's Tenant Team Information
  ttm_mgr.TENANT_TEAM_ID AS client_manager_tenant_team_id,
  tt_mgr.TENANT_TEAM_NAME AS client_manager_tenant_team_name,
  
  -- NEW: Assigned User Information (from USER_DIMENSION)
  ud_assigned.USER_JOB_TITLE AS assigned_user_job_title,
  COALESCE(ud_assigned.EXPECTED_BILLABLE_MINUTES, 0) / 60.0 AS assigned_user_expected_billable_hours,
  COALESCE(ud_assigned.EXPECTED_NONBILLABLE_MINUTES, 0) / 60.0 AS assigned_user_expected_nonbillable_hours,
  
  -- NEW: Client Owner User Information (from USER_DIMENSION)
  ud_client_owner.USER_JOB_TITLE AS client_owner_job_title,
  COALESCE(ud_client_owner.EXPECTED_BILLABLE_MINUTES, 0) / 60.0 AS client_owner_expected_billable_hours,
  COALESCE(ud_client_owner.EXPECTED_NONBILLABLE_MINUTES, 0) / 60.0 AS client_owner_expected_nonbillable_hours,
  
  -- NEW: Client Manager User Information (from USER_DIMENSION)
  ud_client_manager.USER_JOB_TITLE AS client_manager_job_title,
  COALESCE(ud_client_manager.EXPECTED_BILLABLE_MINUTES, 0) / 60.0 AS client_manager_expected_billable_hours,
  COALESCE(ud_client_manager.EXPECTED_NONBILLABLE_MINUTES, 0) / 60.0 AS client_manager_expected_nonbillable_hours,
  
  -- NEW: Additional Time Tracking Information (V4 enhancements)
  COALESCE(te.total_hours_logged, 0) AS total_hours_logged_summary,
  COALESCE(te.billable_hours_logged, 0) AS billable_hours_logged_summary,
  COALESCE(te.total_time_entries, 0) AS total_time_entries,
  COALESCE(te.unique_contributors, 0) AS unique_contributors,
  te.first_time_entry_date,
  te.last_time_entry_date,
  
  -- NEW: Advanced Budget Analytics (V4 enhancements)
  CASE 
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
    THEN COALESCE(te.total_hours_logged, 0) / (budget.BUDGETED_MINUTES / 60.0) * 100
    ELSE NULL 
  END AS budget_utilization_percentage,
  
  CASE 
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
    THEN (budget.BUDGETED_MINUTES / 60.0) - COALESCE(te.total_hours_logged, 0)
    ELSE NULL 
  END AS budget_remaining_hours_calculated,
  
  -- NEW: Status Indicators (V4 enhancements)
  CASE 
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 AND COALESCE(te.total_hours_logged, 0) > (budget.BUDGETED_MINUTES / 60.0) 
    THEN 'OVER_BUDGET'
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 AND COALESCE(te.total_hours_logged, 0) > (budget.BUDGETED_MINUTES / 60.0 * 0.8)
    THEN 'APPROACHING_BUDGET'
    WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
    THEN 'WITHIN_BUDGET'
    ELSE 'NO_BUDGET_SET'
  END AS budget_status,
  
  -- NEW: 3-Month Average Budget Variance Trend (V4 enhancement)
  -- Work Item Level: Only shows rolling average if values actually vary over time
  -- Positive = Under Budget, Negative = Over Budget
  CASE 
    WHEN STDDEV(COALESCE(te.total_hours_logged, 0)) OVER (
      PARTITION BY budget.WORK_ITEM_ID 
      ORDER BY budget.REPORTING_DATE 
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) > 0.01 -- Only calculate rolling average if there's actual variance
    THEN AVG(
      CASE 
        WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
        THEN (budget.BUDGETED_MINUTES / 60.0) - COALESCE(te.total_hours_logged, 0)
        ELSE NULL 
      END
    ) OVER (
      PARTITION BY budget.WORK_ITEM_ID 
      ORDER BY budget.REPORTING_DATE 
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    )
    ELSE 
      CASE 
        WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
        THEN (budget.BUDGETED_MINUTES / 60.0) - COALESCE(te.total_hours_logged, 0)
        ELSE NULL 
      END
  END AS avg_budget_variance_3_months_hours,
  
  -- NEW: Client-Level 3-Month Average Budget Variance Trend (V4 enhancement)
  -- Client Level: Shows aggregated variance across all work items for the client
  -- Positive = Under Budget, Negative = Over Budget
  AVG(
    CASE 
      WHEN COALESCE(budget.BUDGETED_MINUTES, 0) > 0 
      THEN (budget.BUDGETED_MINUTES / 60.0) - COALESCE(te.total_hours_logged, 0)
      ELSE NULL 
    END
  ) OVER (
    PARTITION BY budget.CLIENT_ID 
    ORDER BY budget.REPORTING_DATE 
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS client_avg_budget_variance_3_months_hours,
  
  CASE 
    WHEN budget.DUE_DATETIME IS NOT NULL AND budget.DUE_DATETIME < CURRENT_TIMESTAMP() AND budget.PRIMARY_STATUS NOT IN ('Completed', 'Closed')
    THEN 'OVERDUE'
    WHEN budget.DUE_DATETIME IS NOT NULL AND budget.DUE_DATETIME <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY) AND budget.PRIMARY_STATUS NOT IN ('Completed', 'Closed')
    THEN 'DUE_SOON'
    ELSE 'ON_TRACK'
  END AS schedule_status,
  
  -- NEW: Client Group Account Information (V4 enhancements)
  cg.ACCOUNT_ID AS client_group_account_id,
  cg.ACCOUNT_NAME AS client_group_account_name,
  
  -- MISSING ALIASED FIELDS - Adding user-friendly field names
  budget.COMPLETED_DATETIME AS completion_date,
  budget.ASSIGNED_TO AS assignee,
  budget.CLIENT AS client_name,
  COALESCE(budget.BUDGETED_MINUTES, 0) / 60.0 AS budget_hours,
  COALESCE(budget.TIME_ENTRY_MINUTES, 0) / 60.0 AS actual_hours_from_work_item,
  COALESCE(budget.TIME_ENTRY_COST, 0) AS actual_cost_from_work_item,
  COALESCE(budget.BUDGET_REMAINING_HOURS, 0) AS budget_remaining_hours_from_work_item,
  budget.REPORTING_DATE AS work_item_reporting_date,
  budget.CREATED_DATETIME AS work_item_created_date

FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` budget

LEFT JOIN `red-octane-444308-f4.karbon_data.PRODUCTIVITY_REPEATS_SYNC` prod
  ON budget.WORK_ITEM_ID = prod.WORK_ITEM_ID

LEFT JOIN TimeEntrySummary te ON budget.WORK_ITEM_ID = te.WORK_ITEM_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.CLIENT_GROUP_DIMENSION` cg ON budget.CLIENT_ID = cg.CLIENT_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.CLIENT_DIMENSION` cd ON budget.CLIENT_ID = cd.CLIENT_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm ON cd.CLIENT_OWNER_USER_ID = ttm.USER_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt ON ttm.TENANT_TEAM_ID = tt.TENANT_TEAM_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_MEMBER_DIMENSION` ttm_mgr ON cd.CLIENT_MANAGER_USER_ID = ttm_mgr.USER_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.TENANT_TEAM_DIMENSION` tt_mgr ON ttm_mgr.TENANT_TEAM_ID = tt_mgr.TENANT_TEAM_ID

-- NEW: Add USER_DIMENSION joins for assigned user, client owner, and client manager
LEFT JOIN `red-octane-444308-f4.karbon_data.USER_DIMENSION` ud_assigned ON budget.ASSIGNED_TO_ID = ud_assigned.USER_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.USER_DIMENSION` ud_client_owner ON cd.CLIENT_OWNER_USER_ID = ud_client_owner.USER_ID
LEFT JOIN `red-octane-444308-f4.karbon_data.USER_DIMENSION` ud_client_manager ON cd.CLIENT_MANAGER_USER_ID = ud_client_manager.USER_ID

-- Filter to only show the latest version of each work item
WHERE budget.REPORTING_DATE = (
  SELECT MAX(REPORTING_DATE) 
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` latest
  WHERE latest.WORK_ITEM_ID = budget.WORK_ITEM_ID
)

-- Order by client and work title for consistent results (matching V3)
ORDER BY budget.CLIENT, budget.WORK_TITLE; 