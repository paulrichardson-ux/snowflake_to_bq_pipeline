-- Creates or replaces the view that joins work item details with aggregated user time entries.
CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.work_item_x_user_time_view` AS
SELECT
  te.WORK_ITEM_ID,
  ANY_VALUE(wi.WORK_TITLE) AS work_item_title,
  ANY_VALUE(wi.CLIENT) AS client_name,
  ANY_VALUE(wi.PRIMARY_STATUS) AS status,
  ANY_VALUE(wi.START_DATETIME) AS start_date,
  ANY_VALUE(wi.DUE_DATETIME) AS due_date,
  ANY_VALUE(wi.ASSIGNED_TO) AS assignee,
  ANY_VALUE(wi.BUDGETED_MINUTES) / 60.0 AS total_budget_hours,
  ANY_VALUE(wi.WORK_TYPE) AS work_type,
  te.USER_NAME,
  te.REPORTING_DATE AS time_entry_date,
  te.IS_BILLABLE,
  SUM(te.MINUTES) / 60.0 AS hours_logged
FROM
  `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ` te -- Source: User Time Entries
LEFT JOIN
  `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ` wi -- Source: Work Item Details
ON
  te.WORK_ITEM_ID = wi.WORK_ITEM_ID
GROUP BY
  te.WORK_ITEM_ID,
  te.USER_NAME,
  te.REPORTING_DATE,
  te.IS_BILLABLE;

CREATE OR REPLACE VIEW `red-octane-444308-f4.karbon_data.work_item_user_time_detail_view` AS
WITH WorkItemLatest AS (
  SELECT * EXCEPT (row_num)
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY WORK_ITEM_ID ORDER BY REPORTING_DATE DESC) as row_num
    FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
  ) WHERE row_num = 1
)
SELECT 
  wi.WORK_ITEM_ID,
  wi.WORK_TITLE,
  wi.CLIENT AS client_name,
  wi.WORK_TYPE,
  wi.PRIMARY_STATUS AS status,
  wi.BUDGETED_MINUTES / 60.0 AS budget_hours,
  te.USER_NAME,
  te.REPORTING_DATE AS time_entry_date,
  te.IS_BILLABLE,
  te.MINUTES / 60.0 AS hours_logged
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ` te
LEFT JOIN WorkItemLatest wi ON te.WORK_ITEM_ID = wi.WORK_ITEM_ID; 