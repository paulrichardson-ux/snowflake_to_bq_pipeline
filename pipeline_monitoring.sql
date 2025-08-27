-- KARBON DATA PIPELINE MONITORING DASHBOARD
-- This query provides comprehensive monitoring for the work item pipeline
-- Run this daily to check pipeline health and data freshness

-- =============================================================================
-- MAIN MONITORING QUERY
-- =============================================================================

WITH pipeline_health AS (
  -- Check main data tables freshness
  SELECT 
    'WORK_ITEM_DETAILS_BQ' as table_name,
    MAX(REPORTING_DATE) as latest_date,
    COUNT(*) as total_records,
    COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
    CURRENT_DATE() as check_date,
    DATE_DIFF(CURRENT_DATE(), MAX(REPORTING_DATE), DAY) as days_behind
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
  
  UNION ALL
  
  SELECT 
    'WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4' as table_name,
    MAX(REPORTING_DATE) as latest_date,
    COUNT(*) as total_records,
    COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
    CURRENT_DATE() as check_date,
    DATE_DIFF(CURRENT_DATE(), MAX(REPORTING_DATE), DAY) as days_behind
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`
  
  UNION ALL
  
  SELECT 
    'USER_TIME_ENTRY_BQ' as table_name,
    MAX(REPORTING_DATE) as latest_date,
    COUNT(*) as total_records,
    COUNT(DISTINCT USER_ID) as unique_work_items,
    CURRENT_DATE() as check_date,
    DATE_DIFF(CURRENT_DATE(), MAX(REPORTING_DATE), DAY) as days_behind
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
),

data_quality_checks AS (
  -- Check for data quality issues
  SELECT 
    'WORK_ITEM_DETAILS_BQ' as table_name,
    'NULL_WORK_ITEM_ID' as check_type,
    COUNT(*) as issue_count
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_DETAILS_BQ`
  WHERE WORK_ITEM_ID IS NULL OR WORK_ITEM_ID = ''
  
  UNION ALL
  
  SELECT 
    'WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4' as table_name,
    'NULL_CLIENT_ID' as check_type,
    COUNT(*) as issue_count
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`
  WHERE CLIENT_ID IS NULL OR CLIENT_ID = ''
  
  UNION ALL
  
  SELECT 
    'USER_TIME_ENTRY_BQ' as table_name,
    'FUTURE_DATES' as check_type,
    COUNT(*) as issue_count
  FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
  WHERE REPORTING_DATE > CURRENT_DATE()
),

alert_conditions AS (
  -- Define alert conditions
  SELECT 
    table_name,
    latest_date,
    total_records,
    days_behind,
    CASE 
      WHEN days_behind > 3 THEN 'CRITICAL'
      WHEN days_behind > 1 THEN 'WARNING'
      ELSE 'OK'
    END as alert_level,
    CASE 
      WHEN days_behind > 3 THEN 'Data is more than 3 days old - investigate pipeline'
      WHEN days_behind > 1 THEN 'Data is more than 1 day old - monitor closely'
      ELSE 'Data freshness is acceptable'
    END as alert_message
  FROM pipeline_health
)

-- =============================================================================
-- FINAL MONITORING REPORT
-- =============================================================================

SELECT 
  '🔍 PIPELINE HEALTH REPORT' as report_section,
  CURRENT_DATETIME() as report_timestamp,
  '' as table_name,
  NULL as latest_date,
  NULL as total_records,
  NULL as days_behind,
  '' as alert_level,
  'Generated automatically for daily monitoring' as alert_message

UNION ALL

SELECT 
  '📊 DATA FRESHNESS' as report_section,
  CURRENT_DATETIME() as report_timestamp,
  table_name,
  latest_date,
  total_records,
  days_behind,
  alert_level,
  alert_message
FROM alert_conditions

UNION ALL

SELECT 
  '⚠️ DATA QUALITY ISSUES' as report_section,
  CURRENT_DATETIME() as report_timestamp,
  table_name,
  NULL as latest_date,
  issue_count as total_records,
  NULL as days_behind,
  CASE WHEN issue_count > 0 THEN 'WARNING' ELSE 'OK' END as alert_level,
  CONCAT(check_type, ': ', CAST(issue_count AS STRING), ' issues found') as alert_message
FROM data_quality_checks

ORDER BY report_section, table_name;

-- =============================================================================
-- QUICK HEALTH CHECK (Simple version for automation)
-- =============================================================================

/*
-- Use this simpler query for automated alerts:

SELECT 
  table_name,
  latest_date,
  days_behind,
  CASE 
    WHEN days_behind > 3 THEN 'CRITICAL'
    WHEN days_behind > 1 THEN 'WARNING'
    ELSE 'OK'
  END as status
FROM (
  SELECT 
    'WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4' as table_name,
    MAX(REPORTING_DATE) as latest_date,
    DATE_DIFF(CURRENT_DATE(), MAX(REPORTING_DATE), DAY) as days_behind
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`
)
WHERE days_behind > 1;

*/ 