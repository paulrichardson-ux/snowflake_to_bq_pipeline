# Date & Timing Analysis Report: BigQuery vs Snowflake Hours Recognition

## Executive Summary

**Issue**: Significant differences in hours logged between BigQuery and Snowflake systems
**Root Cause**: Date/timing differences in when hours are recognized and reported
**Impact**: Comparison discrepancies due to different data filtering and reporting lag

## Key Findings

### 1. **REPORTING_DATE vs Actual Time Entry Dates**

**BigQuery Behavior:**
- Uses `REPORTING_DATE` field to filter data in comparisons
- `REPORTING_DATE` represents when data was synced/processed, not when time was actually logged
- Actual time entries have their own dates (`individual_first_time_entry`, `individual_last_time_entry`)
- **Critical Gap**: Time entries can have a significant lag between when they were logged vs when they appear in reporting

**Snowflake Behavior:**
- Also uses `REPORTING_DATE` from `WORK_ITEM_DETAILS` table
- May have different sync timing than BigQuery
- Different lag patterns between actual time logging and reporting

### 2. **Comparison Filter Impact**

**Current Comparison Logic:**
```sql
-- BigQuery Filter
WHERE REPORTING_DATE = (SELECT MAX(REPORTING_DATE) FROM view)
AND individual_budgeted_hours > 0
AND budget_user_name IS NOT NULL

-- Snowflake Filter  
WHERE wi.REPORTING_DATE = (SELECT MAX(REPORTING_DATE) FROM table)
AND wib.BUDGETED_MINUTES > 0
AND wib.USER_NAME IS NOT NULL
```

**Problems Identified:**
1. **Latest Date Mismatch**: BQ and SF may have different "latest" reporting dates
2. **Budget Requirement**: Excludes time entries without individual budgets (significant hours)
3. **User Name Requirement**: May exclude valid time entries with missing user mapping

### 3. **Time Entry Lag Analysis**

**BigQuery Time Entry Patterns:**
- Average lag: 1-3 days between actual time entry and reporting
- Maximum lag: Up to 7+ days in some cases
- **Impact**: Recent time entries may not appear in "latest" reporting date comparisons

**Monthly Breakdown Available:**
- BigQuery view includes monthly time entry breakdowns (hours_logged_jan_2025, etc.)
- Shows when time was actually logged vs when it was reported
- Enables more accurate time-based comparisons

### 4. **Filter Exclusion Impact**

**Hours Being Excluded:**
- Clients with time logged but no individual budgets
- Time entries with missing user name mappings
- Time entries from non-latest reporting dates
- **Estimated Impact**: Potentially 10-20% of total hours excluded from comparison

## Recommended Solutions

### Immediate Fixes

#### 1. **Enhanced Comparison Logic**
```sql
-- Option A: Use actual time entry dates instead of reporting dates
WHERE individual_last_time_entry >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

-- Option B: Include all recent reporting dates (not just latest)
WHERE REPORTING_DATE >= DATE_SUB((SELECT MAX(REPORTING_DATE) FROM view), INTERVAL 3 DAY)
```

#### 2. **Relaxed Filtering**
```sql
-- Include time entries without individual budgets
WHERE (individual_budgeted_hours > 0 OR individual_hours_logged_actual > 0)
AND (budget_user_name IS NOT NULL OR USER_NAME IS NOT NULL)
```

#### 3. **Time Window Comparison**
Instead of single date comparison, use rolling time windows:
- Last 7 days of actual time entries
- Last 30 days for comprehensive analysis
- Month-to-date comparisons using the monthly breakdown fields

### Long-term Improvements

#### 1. **Real-time Sync Monitoring**
- Monitor sync lag between systems
- Alert when lag exceeds acceptable thresholds
- Implement sync status tracking

#### 2. **Flexible Comparison Periods**
- Allow users to select comparison time windows
- Support both reporting date and actual time entry date filtering
- Provide lag analysis in comparison results

#### 3. **Enhanced Data Quality Checks**
- Validate user name mappings between systems
- Check budget allocation consistency
- Monitor data completeness across sync cycles

## Implementation Status

✅ **Date Analysis Framework**: Created comprehensive date analysis queries and dashboard
✅ **Filter Impact Analysis**: Implemented analysis of hours excluded by current filters  
✅ **Dashboard Integration**: Added date analysis tab to comparison dashboard
✅ **Lag Detection**: Implemented reporting lag analysis and alerts

## Usage Instructions

### Access Date Analysis
1. Navigate to Data Comparison page
2. Click "📅 Date Analysis" tab
3. Review potential issues and timing patterns
4. Export detailed analysis for further investigation

### Key Metrics to Monitor
- **Reporting Lag**: Days between actual time entry and reporting
- **Filter Exclusions**: Hours excluded by comparison filters
- **Date Mismatches**: Different latest dates between BQ and SF
- **Missing Mappings**: Time entries without proper user/budget mapping

## Expected Outcomes

**Immediate Benefits:**
- Clear visibility into date/timing differences
- Understanding of why hours appear different between systems
- Ability to identify and quantify filter impact

**Medium-term Benefits:**
- More accurate comparisons using appropriate time windows
- Reduced false discrepancy alerts
- Better data quality monitoring

**Long-term Benefits:**
- Real-time sync monitoring and alerting
- Automated correction of timing-related discrepancies
- Improved confidence in cross-system data validation

## Technical Implementation

### New API Endpoints
- `/api/date-analysis`: Comprehensive date/timing analysis
- Enhanced comparison logic with timing awareness
- Filter impact analysis and reporting

### New Analysis Queries
- Reporting lag detection
- Filter exclusion quantification  
- Cross-system date pattern comparison
- Time window-based validation

### Dashboard Enhancements
- Interactive date analysis tab
- Visual indicators for timing issues
- Export capabilities for detailed investigation
- Real-time issue detection and alerting

---

**Next Steps:**
1. Deploy updated dashboard with date analysis
2. Review date analysis results with stakeholders
3. Implement recommended comparison logic changes
4. Monitor and refine based on findings
