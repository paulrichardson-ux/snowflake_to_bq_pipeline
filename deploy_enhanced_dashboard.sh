#!/bin/bash

# Deploy Enhanced Karbon Pipeline Dashboard with Detailed Data Exports
# This script deploys the dashboard with BigQuery vs Snowflake comparison and detailed user-level exports

set -e

echo "🚀 Deploying Enhanced Karbon Pipeline Dashboard with Detailed Data Exports..."

# Configuration
FUNCTION_NAME="karbon-pipeline-dashboard"
REGION="us-central1"
PROJECT_ID="red-octane-444308-f4"

# Check if we're in the right directory
if [ ! -d "dashboard" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    exit 1
fi

# Change to dashboard directory
cd dashboard

# Check if required files exist
if [ ! -f "main.py" ] || [ ! -f "requirements.txt" ]; then
    echo "❌ Error: main.py or requirements.txt not found in dashboard directory"
    exit 1
fi

echo "📋 Checking requirements..."
echo "✅ main.py found"
echo "✅ requirements.txt found"

# Check if environment variables are set or get from Secret Manager
echo "🔐 Checking credentials configuration..."

# For production, we'll use Secret Manager instead of environment variables
echo "📝 Note: Using Google Secret Manager for credentials (recommended for production)"
echo "   Make sure these secrets exist in Secret Manager:"
echo "   - SNOWFLAKE_USER"
echo "   - SNOWFLAKE_PASSWORD" 
echo "   - SNOWFLAKE_ACCOUNT"
echo "   - SNOWFLAKE_WAREHOUSE"
echo "   - SNOWFLAKE_DATABASE"
echo "   - SNOWFLAKE_SCHEMA"

# Get Google Client ID if not set
if [ -z "$GOOGLE_CLIENT_ID" ]; then
    echo "⚠️  Warning: GOOGLE_CLIENT_ID not set."
    read -p "Enter your Google OAuth Client ID: " GOOGLE_CLIENT_ID
fi

# Generate secure secret key if not set
if [ -z "$SECRET_KEY" ]; then
    echo "📝 Generating secure secret key..."
    SECRET_KEY=$(python3 -c "import secrets; import base64; print(base64.b64encode(secrets.token_bytes(32)).decode())")
    echo "✅ Generated secure secret key"
fi

echo "🔧 Configuration:"
echo "   Function Name: $FUNCTION_NAME"
echo "   Region: $REGION"
echo "   Project: $PROJECT_ID"
echo "   Using Secret Manager for Snowflake credentials"
echo ""

# Check gcloud authentication
echo "🔐 Checking Google Cloud authentication..."
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null 2>&1; then
    echo "❌ Error: gcloud not authenticated. Please run: gcloud auth login"
    exit 1
fi

# Set the project
gcloud config set project $PROJECT_ID

echo "✅ Authentication verified"

# Enable required APIs
echo "🔌 Enabling required Google Cloud APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable logging.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable secretmanager.googleapis.com

echo "✅ APIs enabled"

# Deploy the function with enhanced environment variables
echo "🚀 Deploying Enhanced Cloud Function..."
echo "   Features included:"
echo "   ✅ Pipeline monitoring dashboard"
echo "   ✅ BigQuery vs Snowflake data comparison"
echo "   ✅ Date & timing analysis (NEW)"
echo "   ✅ Client-level summary exports"
echo "   ✅ Detailed user-level data exports"
echo "   ✅ Real-time discrepancy detection"
echo "   ✅ Interactive filtering and search"
echo "   ✅ Hours recognition timing analysis"
echo ""

gcloud functions deploy $FUNCTION_NAME \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --region $REGION \
  --memory 1024MB \
  --timeout 540s \
  --set-env-vars "GOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID,SECRET_KEY=$SECRET_KEY,GOOGLE_CLOUD_PROJECT=$PROJECT_ID"

# Get the function URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(httpsTrigger.url)")

echo ""
echo "🎉 Enhanced Dashboard Deployment Successful!"
echo ""
echo "📊 Dashboard URLs:"
echo "   Main Dashboard: $FUNCTION_URL"
echo "   Data Comparison: $FUNCTION_URL/comparison"
echo "   Date Analysis: $FUNCTION_URL/comparison (click Date Analysis tab)"
echo "   Login Page: $FUNCTION_URL/login"
echo "   Test Endpoint: $FUNCTION_URL/api/test"
echo ""
echo "🔒 Security Features:"
echo "   ✅ Google SSO Authentication"
echo "   ✅ Domain restriction (@fiskalfinance.com)"
echo "   ✅ Secure session management"
echo "   ✅ API endpoint protection"
echo "   ✅ Secret Manager integration"
echo ""
echo "📊 Enhanced Data Comparison Features:"
echo "   ✅ BigQuery vs Snowflake comparison"
echo "   ✅ Client-level summary comparison"
echo "   ✅ Real-time discrepancy detection"
echo "   ✅ Budget vs actual hours validation"
echo "   ✅ Interactive filtering and search"
echo ""
echo "📈 New Detailed Export Features:"
echo "   ✅ Export client-level summary data (CSV/JSON)"
echo "   ✅ Export detailed user-level data (CSV/JSON)"
echo "   ✅ Export BigQuery data only"
echo "   ✅ Export Snowflake data only"
echo "   ✅ Export combined BigQuery + Snowflake data"
echo "   ✅ Export discrepancies only"
echo "   ✅ Individual user budget tracking by work item"
echo ""
echo "🧪 Testing the deployment..."

# Test the function
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/test")
if [ "$HTTP_STATUS" = "200" ]; then
    echo "✅ Function is responding correctly (HTTP $HTTP_STATUS)"
else
    echo "⚠️  Function returned HTTP $HTTP_STATUS - check logs if needed"
fi

# Test main dashboard
DASHBOARD_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL")
if [ "$DASHBOARD_STATUS" = "200" ]; then
    echo "✅ Main dashboard is accessible (HTTP $DASHBOARD_STATUS)"
else
    echo "⚠️  Dashboard returned HTTP $DASHBOARD_STATUS"
fi

# Test comparison endpoint
COMPARISON_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/comparison")
if [ "$COMPARISON_STATUS" = "200" ]; then
    echo "✅ Comparison endpoint is responding correctly (HTTP $COMPARISON_STATUS)"
else
    echo "⚠️  Comparison endpoint returned HTTP $COMPARISON_STATUS - check Snowflake credentials in Secret Manager"
fi

# Test detailed data endpoint
DETAILED_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/detailed-data?source=bigquery")
if [ "$DETAILED_STATUS" = "200" ]; then
    echo "✅ Detailed data endpoint is responding correctly (HTTP $DETAILED_STATUS)"
else
    echo "⚠️  Detailed data endpoint returned HTTP $DETAILED_STATUS"
fi

# Test date analysis endpoint
DATE_ANALYSIS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/date-analysis")
if [ "$DATE_ANALYSIS_STATUS" = "200" ]; then
    echo "✅ Date analysis endpoint is responding correctly (HTTP $DATE_ANALYSIS_STATUS)"
else
    echo "⚠️  Date analysis endpoint returned HTTP $DATE_ANALYSIS_STATUS"
fi

echo ""
echo "📝 View logs:"
echo "   gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=$FUNCTION_NAME' --limit=50"
echo ""
echo "🔄 Update function:"
echo "   Re-run this script to deploy updates"
echo ""
echo "🗑️  Delete function:"
echo "   gcloud functions delete $FUNCTION_NAME --region=$REGION"
echo ""
echo "🎯 Next Steps:"
echo "   1. Access the dashboard at: $FUNCTION_URL"
echo "   2. Sign in with your @fiskalfinance.com Google account"
echo "   3. Navigate to the Data Comparison tab"
echo "   4. Click the '📅 Date Analysis' tab to investigate timing differences"
echo "   5. Review potential issues with hours recognition timing"
echo "   6. Review any discrepancies between BigQuery and Snowflake"
echo "   7. Use the new detailed export features:"
echo "      • Export client-level summary data"
echo "      • Export detailed user-level budget tracking data"
echo "      • Export data from BigQuery, Snowflake, or both"
echo "   8. Use filtering options to focus on specific clients or users"
echo ""
echo "🔧 Secret Manager Setup (if not done already):"
echo "   Create these secrets in Google Cloud Secret Manager:"
echo "   gcloud secrets create SNOWFLAKE_USER --data-file=<(echo 'your-username')"
echo "   gcloud secrets create SNOWFLAKE_PASSWORD --data-file=<(echo 'your-password')"
echo "   gcloud secrets create SNOWFLAKE_ACCOUNT --data-file=<(echo 'your-account-id')"
echo "   gcloud secrets create SNOWFLAKE_WAREHOUSE --data-file=<(echo 'COMPUTE_WH')"
echo "   gcloud secrets create SNOWFLAKE_DATABASE --data-file=<(echo 'KPI_DATABASE')"
echo "   gcloud secrets create SNOWFLAKE_SCHEMA --data-file=<(echo 'SECURE_VIEWS')"
echo ""
echo "🔧 Troubleshooting:"
echo "   - If comparison fails, check Snowflake credentials in Secret Manager"
echo "   - Ensure the Snowflake user has read access to required tables"
echo "   - Check function logs for detailed error messages"
echo "   - Verify BigQuery permissions for the service account"
echo "   - For detailed exports, ensure sufficient memory/timeout settings"
echo ""
echo "📊 Export Data Structure:"
echo "   Detailed exports include:"
echo "   • CLIENT - Client name"
echo "   • USER_NAME - Individual user/team member"
echo "   • WORK_ITEM_ID - Specific work item identifier"
echo "   • WORK_TITLE - Work item description"
echo "   • Budgeted_Hours - Hours budgeted for this user"
echo "   • Hours_Logged_Actual - Actual hours logged"
echo "   • Budget_Variance_Hours - Difference between budgeted and actual"
echo "   • Budget_Utilization_Percentage - Utilization percentage"
echo "   • REPORTING_DATE - Data reporting date"

cd ..

echo ""
echo "🎉 Enhanced Dashboard with Detailed Data Exports is now live!"
echo "🚀 Access it at: $FUNCTION_URL"
