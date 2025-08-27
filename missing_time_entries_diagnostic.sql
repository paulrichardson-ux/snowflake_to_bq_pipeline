-- MISSING TIME ENTRIES DIAGNOSTIC
-- This query helps identify why the second view is missing 94.77 hours
-- Run this to find the root cause of the missing data

-- =============================================================================
-- 1. VALIDATE THE CORRECT DATA (1,134 hours for July 2025)
-- =============================================================================

SELECT 
  'JULY_2025_VALIDATION' as analysis_type,
  COUNT(*) as total_entries,
  COUNT(DISTINCT TIME_ENTRY_ID) as unique_time_entries,
  COUNT(DISTINCT USER_ID) as unique_users,
  SUM(CASE WHEN IS_BILLABLE = true THEN MINUTES ELSE 0 END) / 60.0 as billable_hours,
  SUM(CASE WHEN IS_BILLABLE = false THEN MINUTES ELSE 0 END) / 60.0 as non_billable_hours,
  SUM(MINUTES) / 60.0 as total_hours,
  MIN(REPORTING_DATE) as earliest_date,
  MAX(REPORTING_DATE) as latest_date
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 7;

-- =============================================================================
-- 2. RECENT DATA ANALYSIS (Last 10 days of July 2025)
-- =============================================================================

SELECT 
  DATE(REPORTING_DATE) as entry_date,
  COUNT(*) as entries_count,
  COUNT(DISTINCT USER_ID) as unique_users,
  SUM(CASE WHEN IS_BILLABLE = true THEN MINUTES ELSE 0 END) / 60.0 as billable_hours,
  SUM(CASE WHEN IS_BILLABLE = false THEN MINUTES ELSE 0 END) / 60.0 as non_billable_hours,
  SUM(MINUTES) / 60.0 as daily_total_hours
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 7
  AND REPORTING_DATE >= '2025-07-20'  -- Focus on recent dates
GROUP BY DATE(REPORTING_DATE)
ORDER BY entry_date DESC;

-- =============================================================================
-- 3. USER BREAKDOWN - TOP CONTRIBUTORS
-- =============================================================================

SELECT 
  USER_NAME,
  COUNT(*) as total_entries,
  COUNT(DISTINCT DATE(REPORTING_DATE)) as days_with_entries,
  SUM(CASE WHEN IS_BILLABLE = true THEN MINUTES ELSE 0 END) / 60.0 as billable_hours,
  SUM(CASE WHEN IS_BILLABLE = false THEN MINUTES ELSE 0 END) / 60.0 as non_billable_hours,
  SUM(MINUTES) / 60.0 as user_total_hours,
  MIN(REPORTING_DATE) as first_entry_date,
  MAX(REPORTING_DATE) as last_entry_date
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 7
GROUP BY USER_NAME
ORDER BY user_total_hours DESC
LIMIT 15;

-- =============================================================================
-- 4. POTENTIAL FILTERING ISSUES
-- =============================================================================

-- Check for entries that might be filtered out by common WHERE clauses
SELECT 
  'POTENTIAL_FILTER_ISSUES' as analysis_type,
  COUNT(CASE WHEN WORK_ITEM_ID IS NULL THEN 1 END) as entries_with_null_work_item,
  COUNT(CASE WHEN USER_ID IS NULL THEN 1 END) as entries_with_null_user,
  COUNT(CASE WHEN MINUTES <= 0 THEN 1 END) as entries_with_zero_or_negative_minutes,
  COUNT(CASE WHEN IS_BILLABLE IS NULL THEN 1 END) as entries_with_null_billable_flag,
  COUNT(CASE WHEN REPORTING_DATE != DATE(REPORTING_DATE) THEN 1 END) as entries_with_time_component,
  SUM(CASE WHEN WORK_ITEM_ID IS NULL THEN MINUTES ELSE 0 END) / 60.0 as hours_with_null_work_item,
  SUM(CASE WHEN USER_ID IS NULL THEN MINUTES ELSE 0 END) / 60.0 as hours_with_null_user,
  SUM(CASE WHEN MINUTES <= 0 THEN MINUTES ELSE 0 END) / 60.0 as hours_with_zero_or_negative_minutes
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 7;

-- =============================================================================
-- 5. WORK ITEM ANALYSIS
-- =============================================================================

SELECT 
  'WORK_ITEM_ANALYSIS' as analysis_type,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  COUNT(CASE WHEN WORK_ITEM_ID IS NULL THEN 1 END) as entries_without_work_item,
  SUM(CASE WHEN WORK_ITEM_ID IS NULL THEN MINUTES ELSE 0 END) / 60.0 as hours_without_work_item,
  AVG(MINUTES) / 60.0 as avg_hours_per_entry
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 7;

-- =============================================================================
-- 6. IDENTIFY SPECIFIC MISSING PATTERNS
-- =============================================================================

-- Look for patterns in the missing ~94.77 hours
SELECT 
  'MISSING_HOURS_ANALYSIS' as analysis_type,
  CASE 
    WHEN SUM(MINUTES) / 60.0 BETWEEN 1130 AND 1140 THEN 'CORRECT_TOTAL_CONFIRMED'
    WHEN SUM(MINUTES) / 60.0 BETWEEN 1035 AND 1045 THEN 'MATCHES_SECOND_VIEW'
    ELSE 'UNEXPECTED_TOTAL'
  END as data_validation,
  SUM(MINUTES) / 60.0 as calculated_total_hours,
  1134.0 - (SUM(MINUTES) / 60.0) as variance_from_expected
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 7;

-- =============================================================================
-- 7. TIME ENTRY SYNC STATUS CHECK
-- =============================================================================

-- Check if there are any sync-related issues
SELECT 
  'SYNC_STATUS_CHECK' as analysis_type,
  COUNT(*) as total_records,
  MAX(REPORTING_DATE) as latest_entry_date,
  CURRENT_DATE() as current_date,
  DATE_DIFF(CURRENT_DATE(), MAX(REPORTING_DATE), DAY) as days_since_last_entry,
  -- Check if bq_ingestion_timestamp exists (indicates recent sync activity)
  COUNT(CASE WHEN bq_ingestion_timestamp IS NOT NULL THEN 1 END) as records_with_ingestion_timestamp
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 7; 