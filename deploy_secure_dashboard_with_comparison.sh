#!/bin/bash

# Deploy Secure Karbon Pipeline Dashboard with Data Comparison Features
# This script deploys the enhanced dashboard with BigQuery vs Snowflake comparison

set -e

echo "🚀 Deploying Secure Karbon Pipeline Dashboard with Data Comparison..."

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

# Check if environment variables are set
if [ -z "$GOOGLE_CLIENT_ID" ]; then
    echo "⚠️  Warning: GOOGLE_CLIENT_ID not set. Please set it before deployment."
    read -p "Enter your Google OAuth Client ID: " GOOGLE_CLIENT_ID
fi

if [ -z "$SNOWFLAKE_USER" ]; then
    echo "⚠️  Warning: SNOWFLAKE_USER not set. Please set it before deployment."
    read -p "Enter your Snowflake username: " SNOWFLAKE_USER
fi

if [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo "⚠️  Warning: SNOWFLAKE_PASSWORD not set. Please set it before deployment."
    read -s -p "Enter your Snowflake password: " SNOWFLAKE_PASSWORD
    echo ""
fi

if [ -z "$SNOWFLAKE_ACCOUNT" ]; then
    echo "⚠️  Warning: SNOWFLAKE_ACCOUNT not set. Please set it before deployment."
    read -p "Enter your Snowflake account identifier: " SNOWFLAKE_ACCOUNT
fi

if [ -z "$SECRET_KEY" ]; then
    echo "📝 Generating secure secret key..."
    SECRET_KEY=$(python3 -c "import secrets; import base64; print(base64.b64encode(secrets.token_bytes(32)).decode())")
fi

# Set default Snowflake configuration if not provided
SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE:-"COMPUTE_WH"}
SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE:-"KARBON"}
SNOWFLAKE_SCHEMA=${SNOWFLAKE_SCHEMA:-"PUBLIC"}

echo "🔧 Configuration:"
echo "   Function Name: $FUNCTION_NAME"
echo "   Region: $REGION"
echo "   Project: $PROJECT_ID"
echo "   Snowflake Database: $SNOWFLAKE_DATABASE"
echo "   Snowflake Schema: $SNOWFLAKE_SCHEMA"
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

echo "✅ APIs enabled"

# Deploy the function with enhanced environment variables
echo "🚀 Deploying Cloud Function with comparison features..."
gcloud functions deploy $FUNCTION_NAME \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --region $REGION \
  --memory 1024MB \
  --timeout 540s \
  --set-env-vars \
    GOOGLE_CLIENT_ID="$GOOGLE_CLIENT_ID",\
    SNOWFLAKE_USER="$SNOWFLAKE_USER",\
    SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD",\
    SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT",\
    SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE",\
    SNOWFLAKE_DATABASE="$SNOWFLAKE_DATABASE",\
    SNOWFLAKE_SCHEMA="$SNOWFLAKE_SCHEMA",\
    SECRET_KEY="$SECRET_KEY",\
    GOOGLE_CLOUD_PROJECT="$PROJECT_ID"

# Get the function URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(httpsTrigger.url)")

echo ""
echo "🎉 Deployment Successful!"
echo ""
echo "📊 Dashboard URLs:"
echo "   Main Dashboard: $FUNCTION_URL"
echo "   Data Comparison: $FUNCTION_URL/comparison"
echo "   Login Page: $FUNCTION_URL/login"
echo ""
echo "🔒 Security Features:"
echo "   ✅ Google SSO Authentication"
echo "   ✅ Domain restriction (@fiskalfinance.com)"
echo "   ✅ Secure session management"
echo "   ✅ API endpoint protection"
echo ""
echo "📊 New Comparison Features:"
echo "   ✅ BigQuery vs Snowflake data comparison"
echo "   ✅ Real-time discrepancy detection"
echo "   ✅ Budget vs actual hours validation"
echo "   ✅ Interactive filtering and search"
echo "   ✅ Export functionality (coming soon)"
echo ""
echo "🧪 Testing the deployment..."

# Test the function
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/status")
if [ "$HTTP_STATUS" = "200" ]; then
    echo "✅ Function is responding correctly (HTTP $HTTP_STATUS)"
else
    echo "⚠️  Function returned HTTP $HTTP_STATUS - check logs if needed"
fi

# Test comparison endpoint
COMPARISON_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/comparison")
if [ "$COMPARISON_STATUS" = "200" ]; then
    echo "✅ Comparison endpoint is responding correctly (HTTP $COMPARISON_STATUS)"
else
    echo "⚠️  Comparison endpoint returned HTTP $COMPARISON_STATUS - check Snowflake credentials"
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
echo "   4. Review any discrepancies between BigQuery and Snowflake"
echo "   5. Use the filtering options to focus on specific work items"
echo ""
echo "🔧 Troubleshooting:"
echo "   - If comparison fails, check Snowflake credentials and permissions"
echo "   - Ensure the Snowflake user has read access to KARBON database"
echo "   - Check function logs for detailed error messages"
echo "   - Verify BigQuery permissions for the service account"

cd ..

echo ""
echo "🎉 Secure Dashboard with Data Comparison is now live!"
