-- Validation Queries for WORK_ITEM_BUDGET_VS_ACTUAL_CORRECTED_VIEW
-- Use these queries to verify the corrected view fixes the May 2025 budget alignment issue

-- ================================================================================================
-- 1. COMPARISON: May 2025 Budget Totals Between Original Views and Corrected View
-- ================================================================================================

-- Query 1A: May 2025 totals from WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4
SELECT 
  'V4_TIME_TRACKING_VIEW' as source_view,
  COUNT(DISTINCT CLIENT) as unique_clients,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  SUM(budget_hours) as total_budget_hours,
  SUM(COALESCE(BUDGETED_COST, 0)) as total_budget_cost
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`
WHERE EXTRACT(YEAR FROM DUE_DATETIME) = 2025 
  AND EXTRACT(MONTH FROM DUE_DATETIME) = 5;

-- Query 1B: May 2025 totals from corrected view
SELECT 
  'CORRECTED_VIEW' as source_view,
  COUNT(DISTINCT CLIENT) as unique_clients,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  SUM(budgeted_hours) as total_budget_hours,
  SUM(budgeted_cost) as total_budget_cost
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE due_year = 2025 
  AND due_month = 5;

-- Query 1C: May 2025 totals from original budget vs actual (likely showing sync date issue)
SELECT 
  'ORIGINAL_BVA_VIEW' as source_view,
  COUNT(DISTINCT CLIENT) as unique_clients,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  SUM(BUDGETED_MINUTES)/60.0 as total_budget_hours,
  SUM(BUDGETED_COST) as total_budget_cost
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ`
WHERE EXTRACT(YEAR FROM REPORTING_DATE) = 2025 
  AND EXTRACT(MONTH FROM REPORTING_DATE) = 5;

-- ================================================================================================
-- 2. DETAILED CLIENT BREAKDOWN: May 2025 Budget per Client
-- ================================================================================================

-- Query 2A: Client breakdown from corrected view
SELECT 
  CLIENT,
  COUNT(DISTINCT WORK_ITEM_ID) as work_items,
  SUM(budgeted_hours) as total_budget_hours,
  SUM(actual_hours) as total_actual_hours,
  SUM(budgeted_cost) as total_budget_cost,
  SUM(actual_cost) as total_actual_cost,
  SUM(variance_hours) as total_variance_hours,
  SUM(variance_cost) as total_variance_cost
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE due_year = 2025 AND due_month = 5
GROUP BY CLIENT
ORDER BY total_budget_hours DESC;

-- Query 2B: Client breakdown from V4 view for comparison
SELECT 
  CLIENT,
  COUNT(DISTINCT WORK_ITEM_ID) as work_items,
  SUM(budget_hours) as total_budget_hours,
  SUM(actual_hours_from_work_item) as total_actual_hours,
  SUM(COALESCE(BUDGETED_COST, 0)) as total_budget_cost,
  SUM(COALESCE(actual_cost_from_work_item, 0)) as total_actual_cost
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`
WHERE EXTRACT(YEAR FROM DUE_DATETIME) = 2025 
  AND EXTRACT(MONTH FROM DUE_DATETIME) = 5
GROUP BY CLIENT
ORDER BY total_budget_hours DESC;

-- ================================================================================================
-- 3. DIAGNOSTIC QUERIES: Understanding the Data Issues
-- ================================================================================================

-- Query 3A: Check REPORTING_DATE distribution in original budget vs actual table
SELECT 
  REPORTING_DATE,
  COUNT(*) as record_count,
  COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
  MIN(WORK_TITLE) as sample_work_title
FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ`
GROUP BY REPORTING_DATE
ORDER BY REPORTING_DATE DESC
LIMIT 20;

-- Query 3B: Check for records with NULL due dates in corrected view
SELECT 
  'NULL_DUE_DATES' as issue_type,
  COUNT(*) as affected_records,
  COUNT(DISTINCT WORK_ITEM_ID) as affected_work_items,
  SUM(budgeted_hours) as total_budget_hours_affected
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE DUE_DATETIME IS NULL;

-- Query 3C: Sample records showing sync date vs due date comparison
SELECT 
  WORK_ITEM_ID,
  WORK_TITLE,
  CLIENT,
  sync_reporting_date,
  DUE_DATETIME,
  due_date,
  due_year,
  due_month,
  budgeted_hours,
  schedule_status
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE due_year = 2025 AND due_month = 5
ORDER BY budgeted_hours DESC
LIMIT 20;

-- ================================================================================================
-- 4. MONTHLY TREND ANALYSIS: Budget Distribution by Due Month
-- ================================================================================================

-- Query 4: Monthly budget distribution from corrected view
SELECT 
  due_year,
  due_month,
  due_month_start,
  COUNT(DISTINCT WORK_ITEM_ID) as work_items,
  COUNT(DISTINCT CLIENT) as unique_clients,
  SUM(budgeted_hours) as total_budget_hours,
  SUM(actual_hours) as total_actual_hours,
  SUM(budgeted_cost) as total_budget_cost,
  AVG(budget_utilization_percentage) as avg_budget_utilization
FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
WHERE due_year IN (2024, 2025)
  AND DUE_DATETIME IS NOT NULL
GROUP BY due_year, due_month, due_month_start
ORDER BY due_year, due_month;

-- ================================================================================================
-- 5. QUICK VERIFICATION: Are the views now aligned for May 2025?
-- ================================================================================================

-- Query 5: Side-by-side comparison of key metrics
WITH v4_totals AS (
  SELECT 
    SUM(budget_hours) as v4_budget_hours,
    COUNT(DISTINCT WORK_ITEM_ID) as v4_work_items,
    COUNT(DISTINCT CLIENT) as v4_clients
  FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4`
  WHERE EXTRACT(YEAR FROM DUE_DATETIME) = 2025 
    AND EXTRACT(MONTH FROM DUE_DATETIME) = 5
),
corrected_totals AS (
  SELECT 
    SUM(budgeted_hours) as corrected_budget_hours,
    COUNT(DISTINCT WORK_ITEM_ID) as corrected_work_items,
    COUNT(DISTINCT CLIENT) as corrected_clients
  FROM `red-octane-444308-f4.karbon_data.work_item_budget_vs_actual_corrected_view`
  WHERE due_year = 2025 AND due_month = 5
)
SELECT 
  v4.v4_budget_hours,
  c.corrected_budget_hours,
  v4.v4_budget_hours - c.corrected_budget_hours as budget_hours_difference,
  v4.v4_work_items,
  c.corrected_work_items,
  v4.v4_work_items - c.corrected_work_items as work_items_difference,
  v4.v4_clients,
  c.corrected_clients,
  v4.v4_clients - c.corrected_clients as clients_difference,
  CASE 
    WHEN ABS(v4.v4_budget_hours - c.corrected_budget_hours) < 0.01 THEN 'ALIGNED ✓'
    ELSE 'MISALIGNED ✗'
  END as alignment_status
FROM v4_totals v4
CROSS JOIN corrected_totals c; 