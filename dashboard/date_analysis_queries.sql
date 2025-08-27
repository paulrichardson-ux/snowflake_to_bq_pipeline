-- =============================================================================
-- DATE ANALYSIS QUERIES: BigQuery vs Snowflake Hours Recognition Timing
-- =============================================================================
-- These queries investigate potential date/timing differences between BQ and SF
-- that could cause discrepancies in hours logged recognition

-- PART 1: BIGQUERY DATE ANALYSIS
-- =============================================================================

-- 1. Analyze REPORTING_DATE distribution in BigQuery
-- This shows when time entries are being recognized/reported
SELECT 
    'BigQuery Time Entries' as source,
    REPORTING_DATE,
    COUNT(*) as time_entry_count,
    SUM(MINUTES) / 60.0 as total_hours,
    COUNT(DISTINCT USER_NAME) as unique_users,
    COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
    MIN(MINUTES) as min_minutes,
    MAX(MINUTES) as max_minutes,
    AVG(MINUTES) as avg_minutes
FROM `red-octane-444308-f4.karbon_data.USER_TIME_ENTRY_BQ`
WHERE REPORTING_DATE >= '2024-12-01'  -- Recent data
GROUP BY REPORTING_DATE
ORDER BY REPORTING_DATE DESC
LIMIT 30;

-- 2. Analyze different date fields in BigQuery view
-- This compares REPORTING_DATE vs actual time entry dates vs work item dates
SELECT 
    'BigQuery View Analysis' as source,
    REPORTING_DATE,
    individual_first_time_entry as first_actual_time_entry,
    individual_last_time_entry as last_actual_time_entry,
    DATE_DIFF(individual_last_time_entry, individual_first_time_entry, DAY) as time_entry_span_days,
    DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY) as days_between_last_entry_and_reporting,
    COUNT(*) as record_count,
    SUM(individual_hours_logged_actual) as total_hours,
    AVG(individual_hours_logged_actual) as avg_hours_per_user
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
WHERE REPORTING_DATE >= '2024-12-01'
    AND individual_hours_logged_actual > 0
    AND individual_first_time_entry IS NOT NULL
GROUP BY 
    REPORTING_DATE,
    individual_first_time_entry,
    individual_last_time_entry
ORDER BY REPORTING_DATE DESC, total_hours DESC
LIMIT 50;

-- 3. Monthly time entry patterns in BigQuery
-- This shows how hours are distributed by actual time entry month vs reporting month
SELECT 
    'BigQuery Monthly Patterns' as source,
    EXTRACT(YEAR FROM REPORTING_DATE) as reporting_year,
    EXTRACT(MONTH FROM REPORTING_DATE) as reporting_month,
    -- Hours by actual time entry month (from the view's monthly breakdowns)
    SUM(hours_logged_jan_2025) as jan_2025_hours,
    SUM(hours_logged_feb_2025) as feb_2025_hours,
    SUM(hours_logged_mar_2025) as mar_2025_hours,
    SUM(hours_logged_apr_2025) as apr_2025_hours,
    SUM(hours_logged_may_2025) as may_2025_hours,
    SUM(hours_logged_jun_2025) as jun_2025_hours,
    SUM(hours_logged_jul_2025) as jul_2025_hours,
    SUM(hours_logged_aug_2025) as aug_2025_hours,
    SUM(hours_logged_sep_2025) as sep_2025_hours,
    SUM(hours_logged_oct_2025) as oct_2025_hours,
    SUM(hours_logged_nov_2025) as nov_2025_hours,
    SUM(hours_logged_dec_2025) as dec_2025_hours,
    -- Total hours in this reporting period
    SUM(individual_hours_logged_actual) as total_reported_hours,
    COUNT(*) as record_count
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
WHERE REPORTING_DATE >= '2024-12-01'
    AND individual_hours_logged_actual > 0
GROUP BY 
    EXTRACT(YEAR FROM REPORTING_DATE),
    EXTRACT(MONTH FROM REPORTING_DATE)
ORDER BY reporting_year DESC, reporting_month DESC;

-- PART 2: COMPARISON ANALYSIS
-- =============================================================================

-- 4. Side-by-side date comparison for specific clients
-- This compares the latest reporting dates between BQ and what we expect from SF
WITH bq_latest AS (
    SELECT 
        CLIENT,
        MAX(REPORTING_DATE) as bq_latest_reporting_date,
        MAX(individual_last_time_entry) as bq_latest_actual_time_entry,
        SUM(individual_hours_logged_actual) as bq_total_hours,
        COUNT(*) as bq_record_count
    FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
    WHERE individual_hours_logged_actual > 0
        AND CLIENT IS NOT NULL
    GROUP BY CLIENT
),
bq_current_filter AS (
    -- This is exactly what the comparison query uses
    SELECT 
        CLIENT,
        SUM(individual_hours_logged_actual) as bq_comparison_hours,
        COUNT(*) as bq_comparison_records,
        MAX(REPORTING_DATE) as bq_comparison_date
    FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
    WHERE REPORTING_DATE = (
        SELECT MAX(REPORTING_DATE) 
        FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
    )
    AND individual_budgeted_hours > 0
    AND budget_user_name IS NOT NULL
    AND CLIENT IS NOT NULL
    GROUP BY CLIENT
)
SELECT 
    COALESCE(bq_latest.CLIENT, bq_current.CLIENT) as client,
    bq_latest.bq_latest_reporting_date,
    bq_latest.bq_latest_actual_time_entry,
    bq_current.bq_comparison_date,
    bq_latest.bq_total_hours as all_bq_hours,
    bq_current.bq_comparison_hours as filtered_bq_hours,
    bq_latest.bq_total_hours - COALESCE(bq_current.bq_comparison_hours, 0) as hours_difference,
    bq_latest.bq_record_count as all_bq_records,
    bq_current.bq_comparison_records as filtered_bq_records,
    CASE 
        WHEN bq_current.bq_comparison_hours IS NULL THEN 'EXCLUDED_BY_FILTER'
        WHEN bq_latest.bq_total_hours > COALESCE(bq_current.bq_comparison_hours, 0) THEN 'HOURS_MISSING_IN_COMPARISON'
        ELSE 'MATCHES'
    END as status
FROM bq_latest
FULL OUTER JOIN bq_current ON bq_latest.CLIENT = bq_current.CLIENT
WHERE COALESCE(bq_latest.bq_total_hours, 0) > 0 
    OR COALESCE(bq_current.bq_comparison_hours, 0) > 0
ORDER BY hours_difference DESC NULLS LAST;

-- PART 3: TIME LAG ANALYSIS
-- =============================================================================

-- 5. Time lag between actual time entry and reporting
-- This identifies delays in time entry recognition
SELECT 
    'Time Entry Lag Analysis' as analysis_type,
    REPORTING_DATE,
    individual_last_time_entry,
    DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY) as lag_days,
    CASE 
        WHEN DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY) = 0 THEN 'SAME_DAY'
        WHEN DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY) = 1 THEN '1_DAY_LAG'
        WHEN DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY) <= 7 THEN 'WITHIN_WEEK'
        WHEN DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY) <= 30 THEN 'WITHIN_MONTH'
        ELSE 'OVER_MONTH'
    END as lag_category,
    COUNT(*) as record_count,
    SUM(individual_hours_logged_actual) as total_hours,
    AVG(individual_hours_logged_actual) as avg_hours
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
WHERE REPORTING_DATE >= '2024-12-01'
    AND individual_hours_logged_actual > 0
    AND individual_last_time_entry IS NOT NULL
GROUP BY 
    REPORTING_DATE,
    individual_last_time_entry,
    DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY)
ORDER BY lag_days DESC, total_hours DESC
LIMIT 50;

-- 6. Summary of potential date-related discrepancies
-- This provides a high-level view of timing issues
SELECT 
    'Summary Analysis' as analysis_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN individual_hours_logged_actual > 0 THEN 1 END) as records_with_hours,
    COUNT(CASE WHEN individual_budgeted_hours > 0 THEN 1 END) as records_with_budget,
    COUNT(CASE WHEN individual_hours_logged_actual > 0 AND individual_budgeted_hours > 0 THEN 1 END) as records_with_both,
    COUNT(CASE WHEN individual_hours_logged_actual > 0 AND individual_budgeted_hours = 0 THEN 1 END) as hours_no_budget,
    COUNT(CASE WHEN individual_hours_logged_actual = 0 AND individual_budgeted_hours > 0 THEN 1 END) as budget_no_hours,
    SUM(individual_hours_logged_actual) as total_hours_logged,
    SUM(individual_budgeted_hours) as total_hours_budgeted,
    AVG(DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY)) as avg_reporting_lag_days,
    MIN(REPORTING_DATE) as min_reporting_date,
    MAX(REPORTING_DATE) as max_reporting_date,
    MIN(individual_first_time_entry) as min_actual_time_entry,
    MAX(individual_last_time_entry) as max_actual_time_entry
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
WHERE REPORTING_DATE >= '2024-12-01';
