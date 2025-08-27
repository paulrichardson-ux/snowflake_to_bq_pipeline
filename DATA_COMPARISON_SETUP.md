# 📊 Data Comparison Dashboard Setup

This guide explains how to set up and use the enhanced Karbon Pipeline Dashboard with BigQuery vs Snowflake data comparison capabilities.

## 🎯 Overview

The enhanced dashboard now includes a powerful data comparison feature that validates the accuracy of your BigQuery views against the source Snowflake data. This ensures data integrity and helps identify any discrepancies in your data pipeline.

## ✨ New Features

### 📊 Data Comparison Page
- **Real-time Comparison**: Compare BigQuery `WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5` with Snowflake source data
- **Discrepancy Detection**: Automatically identify differences in budget hours, logged hours, and variance calculations
- **Match Rate Analytics**: View overall data accuracy percentage and detailed statistics
- **Interactive Filtering**: Filter results by match status and search specific work items
- **Visual Indicators**: Clear visual indicators for matching and mismatched data

### 🔍 Comparison Metrics
- **Budget Hours**: Compare budgeted hours between systems
- **Logged Hours**: Validate time tracking data accuracy
- **Variance Calculations**: Ensure budget variance calculations are consistent
- **User-Level Analysis**: Individual user budget and time tracking validation

## 🚀 Setup Instructions

### Step 1: Secure Credential Setup

1. **🔐 Store Snowflake credentials in Secret Manager (Recommended):**
   ```bash
   ./setup_secrets.sh
   ```
   This securely stores your Snowflake credentials in Google Cloud Secret Manager.

2. **Alternative: Environment Configuration (Less Secure):**
   ```bash
   cd dashboard
   ./setup_environment.sh
   ```
   Then update the `.env` file with your credentials.

**🛡️ Security Note**: Using Secret Manager is highly recommended for production deployments as it provides encryption, audit logging, and fine-grained access control.

### Step 2: Deploy Enhanced Dashboard

1. **🔐 Deploy with Secret Manager (Recommended):**
   ```bash
   ./deploy_secure_dashboard_with_secrets.sh
   ```

2. **Alternative: Deploy with environment variables:**
   ```bash
   ./deploy_secure_dashboard_with_comparison.sh
   ```

3. **The script will:**
   - Deploy the enhanced Cloud Function
   - Configure Secret Manager access (if using secrets)
   - Set up IAM permissions
   - Enable necessary Google Cloud APIs
   - Test the deployment
   - Provide access URLs

### Step 3: Access the Comparison Features

1. **Navigate to your dashboard URL**
2. **Sign in with your @fiskalfinance.com Google account**
3. **Click on the "📊 Data Comparison" tab**
4. **View real-time comparison results**

## 📊 Using the Comparison Dashboard

### Summary Statistics
The dashboard displays key metrics:
- **BigQuery Records**: Total records in the BQ view
- **Snowflake Records**: Total records from Snowflake
- **Common Records**: Records present in both systems
- **Matching Records**: Records with identical data
- **Discrepancies**: Records with data differences
- **Match Rate**: Overall accuracy percentage

### Discrepancy Analysis
When discrepancies are found, the dashboard shows:
- **Work Item Details**: ID and title
- **User Information**: Budget/time allocation by user
- **Side-by-side Comparison**: BQ vs SF values
- **Match Indicators**: Visual indicators for each metric
- **Detailed Breakdown**: Budget, hours, and variance comparisons

### Filtering and Search
- **Filter by Status**: View all records, matches only, or discrepancies only
- **Search Work Items**: Find specific work items by ID
- **Real-time Updates**: Data refreshes automatically

## 🔧 Technical Details

### Database Connections
- **BigQuery**: Uses service account authentication
- **Snowflake**: Uses username/password authentication
- **Connection Pooling**: Efficient connection management
- **Error Handling**: Graceful handling of connection issues

### Query Logic
The comparison uses optimized queries to:
- **BigQuery**: Query the latest data from `WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
- **Snowflake**: Aggregate equivalent data from source tables
- **Matching**: Compare records using work item ID and user name
- **Tolerance**: Use 1% tolerance for floating-point comparisons

### Security
- **🔐 Credentials**: Stored securely in Google Cloud Secret Manager (recommended) or environment variables
- **🔍 Access Control**: Same authentication as main dashboard
- **📚 Read-only**: Snowflake connection uses read-only permissions
- **🔒 Encryption**: All data in transit and at rest is encrypted
- **📊 Audit Logging**: Secret access is logged and auditable
- **🎯 IAM Integration**: Fine-grained access control via Google Cloud IAM

## 🚨 Troubleshooting

### Common Issues

#### "No data available from Snowflake"
- **Check credentials**: Verify SNOWFLAKE_USER and SNOWFLAKE_PASSWORD
- **Check permissions**: Ensure user has read access to KARBON database
- **Check connection**: Verify SNOWFLAKE_ACCOUNT is correct

#### "Comparison failed" error
- **Check logs**: View Cloud Function logs for detailed errors
- **Check network**: Ensure Cloud Function can reach Snowflake
- **Check queries**: Verify table names and schema are correct

#### High discrepancy count
- **Check sync timing**: Ensure both systems are up-to-date
- **Check data pipeline**: Verify ETL processes are running correctly
- **Check transformations**: Review any data transformations for accuracy

### Debugging Steps

1. **Check Function Logs:**
   ```bash
   gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard' --limit=50
   ```

2. **Test Connections Individually:**
   - Test BigQuery connection in Cloud Console
   - Test Snowflake connection with provided credentials

3. **Verify Data Sources:**
   - Check latest data in BigQuery view
   - Check corresponding data in Snowflake tables

## 📈 Performance Considerations

### Query Optimization
- **Limit Results**: Default limit of 1000 records for performance
- **Indexed Queries**: Use indexed columns for filtering
- **Parallel Processing**: BigQuery and Snowflake queries run in parallel
- **Caching**: Results cached for improved response times

### Resource Usage
- **Memory**: 1024MB allocated for Cloud Function
- **Timeout**: 540s timeout for complex queries
- **Concurrency**: Handles multiple concurrent requests
- **Cost**: Optimized to minimize query costs

## 🔄 Maintenance

### Regular Tasks
- **Monitor Discrepancies**: Review daily for data quality issues
- **Update Credentials**: Rotate passwords regularly
- **Check Performance**: Monitor query execution times
- **Review Logs**: Check for any error patterns

### Updates
- **Redeploy**: Use deployment script for updates
- **Environment Variables**: Update via Cloud Console or script
- **Dependencies**: Keep Python packages updated

## 📞 Support

For issues with the data comparison features:

1. **Check this documentation** for common solutions
2. **Review function logs** for detailed error messages
3. **Verify credentials** and permissions
4. **Test individual components** (BQ, Snowflake) separately

## 🎉 Success Indicators

After setup, you should see:
- ✅ Comparison page loads successfully
- ✅ Summary statistics display correctly
- ✅ Data from both BigQuery and Snowflake loads
- ✅ Discrepancies (if any) are clearly highlighted
- ✅ Filtering and search work properly
- ✅ Navigation between dashboard pages works

## 🔮 Future Enhancements

Planned features:
- **CSV Export**: Export comparison results
- **Historical Tracking**: Track discrepancies over time
- **Alerting**: Email notifications for significant discrepancies
- **Advanced Filtering**: More filtering options
- **Drill-down Analysis**: Detailed record-level investigation

---

**🎯 Your enhanced Karbon pipeline dashboard with data comparison is now ready to ensure data quality and pipeline accuracy!**
