#!/bin/bash

# Deploy Secure Karbon Pipeline Dashboard with Secret Manager Integration
# This script deploys the dashboard using Google Cloud Secret Manager for Snowflake credentials

set -e

echo "🚀 Deploying Secure Karbon Pipeline Dashboard with Secret Manager integration..."

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

# Check if Google OAuth Client ID is set
if [ -z "$GOOGLE_CLIENT_ID" ]; then
    echo "⚠️  Warning: GOOGLE_CLIENT_ID not set. Please set it before deployment."
    read -p "Enter your Google OAuth Client ID: " GOOGLE_CLIENT_ID
fi

# Generate secure secret key if not provided
if [ -z "$SECRET_KEY" ]; then
    echo "📝 Generating secure secret key..."
    SECRET_KEY=$(python3 -c "import secrets; import base64; print(base64.b64encode(secrets.token_bytes(32)).decode())")
fi

echo "🔧 Configuration:"
echo "   Function Name: $FUNCTION_NAME"
echo "   Region: $REGION"
echo "   Project: $PROJECT_ID"
echo "   Using Secret Manager: ✅"
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

# Check if secrets exist
echo "🔍 Checking if Snowflake secrets exist in Secret Manager..."
required_secrets=("snowflake-user" "snowflake-password" "snowflake-account")
missing_secrets=()

for secret in "${required_secrets[@]}"; do
    if ! gcloud secrets describe $secret --project=$PROJECT_ID >/dev/null 2>&1; then
        missing_secrets+=($secret)
    fi
done

if [ ${#missing_secrets[@]} -ne 0 ]; then
    echo "❌ Missing required secrets: ${missing_secrets[*]}"
    echo ""
    echo "Please run the setup script first:"
    echo "   ./setup_secrets.sh"
    echo ""
    read -p "Do you want to run the setup script now? (y/N): " run_setup
    if [[ $run_setup =~ ^[Yy]$ ]]; then
        cd ..
        ./setup_secrets.sh
        cd dashboard
    else
        echo "❌ Cannot deploy without required secrets. Exiting."
        exit 1
    fi
fi

echo "✅ All required secrets found in Secret Manager"

# Enable required APIs
echo "🔌 Enabling required Google Cloud APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable logging.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable secretmanager.googleapis.com

echo "✅ APIs enabled"

# Verify service account has Secret Manager access
SERVICE_ACCOUNT="$PROJECT_ID@appspot.gserviceaccount.com"
echo "🔐 Verifying service account permissions..."

# Check if service account has secretmanager.secretAccessor role
if gcloud projects get-iam-policy $PROJECT_ID --flatten="bindings[].members" --format="table(bindings.role)" --filter="bindings.members:serviceAccount:$SERVICE_ACCOUNT AND bindings.role:roles/secretmanager.secretAccessor" | grep -q "secretmanager.secretAccessor"; then
    echo "✅ Service account has Secret Manager access"
else
    echo "⚠️  Granting Secret Manager access to service account..."
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/secretmanager.secretAccessor"
    echo "✅ Secret Manager permissions granted"
fi

# Deploy the function with minimal environment variables (only non-sensitive ones)
echo "🚀 Deploying Cloud Function with Secret Manager integration..."
gcloud functions deploy $FUNCTION_NAME \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --region $REGION \
  --memory 1024MB \
  --timeout 540s \
  --set-env-vars \
    GOOGLE_CLIENT_ID="$GOOGLE_CLIENT_ID",\
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
echo "   ✅ Secret Manager for credentials"
echo ""
echo "📊 Comparison Features:"
echo "   ✅ BigQuery vs Snowflake data comparison"
echo "   ✅ Real-time discrepancy detection"
echo "   ✅ Budget vs actual hours validation"
echo "   ✅ Interactive filtering and search"
echo "   ✅ Secure credential management"
echo ""
echo "🧪 Testing the deployment..."

# Test the function
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/status")
if [ "$HTTP_STATUS" = "200" ]; then
    echo "✅ Function is responding correctly (HTTP $HTTP_STATUS)"
else
    echo "⚠️  Function returned HTTP $HTTP_STATUS - check logs if needed"
fi

# Test comparison endpoint (this will test Secret Manager integration)
echo "🧪 Testing Secret Manager integration..."
COMPARISON_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_URL/api/comparison")
if [ "$COMPARISON_STATUS" = "200" ]; then
    echo "✅ Secret Manager integration working (HTTP $COMPARISON_STATUS)"
elif [ "$COMPARISON_STATUS" = "500" ]; then
    echo "⚠️  Comparison endpoint returned HTTP $COMPARISON_STATUS - check Snowflake credentials or Secret Manager access"
else
    echo "ℹ️  Comparison endpoint returned HTTP $COMPARISON_STATUS - this may be expected if Snowflake is unreachable"
fi

echo ""
echo "📝 View logs:"
echo "   gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=$FUNCTION_NAME' --limit=50"
echo ""
echo "🔍 Monitor secret access:"
echo "   gcloud logging read 'protoPayload.serviceName=\"secretmanager.googleapis.com\"' --limit=20"
echo ""
echo "🔄 Update secrets:"
echo "   ./setup_secrets.sh  # To update existing secrets"
echo ""
echo "🔧 Manage secrets:"
echo "   gcloud secrets list --filter='labels.component=karbon-dashboard'"
echo "   gcloud secrets versions access latest --secret=snowflake-user"
echo ""
echo "🗑️  Delete function:"
echo "   gcloud functions delete $FUNCTION_NAME --region=$REGION"
echo ""
echo "🎯 Next Steps:"
echo "   1. Access the dashboard at: $FUNCTION_URL"
echo "   2. Sign in with your @fiskalfinance.com Google account"
echo "   3. Navigate to the Data Comparison tab"
echo "   4. Review any discrepancies between BigQuery and Snowflake"
echo "   5. Monitor secret access in Cloud Logging"
echo ""
echo "🔐 Security Best Practices:"
echo "   • Regularly rotate Snowflake passwords"
echo "   • Monitor secret access logs"
echo "   • Use least-privilege IAM roles"
echo "   • Keep Google OAuth credentials secure"
echo "   • Review Cloud Function logs regularly"
echo ""
echo "🔧 Troubleshooting:"
echo "   - If comparison fails, check secret values: ./setup_secrets.sh"
echo "   - Verify IAM permissions for service account"
echo "   - Check Secret Manager API is enabled"
echo "   - Ensure Snowflake user has read permissions"
echo "   - Review function logs for detailed error messages"

cd ..

echo ""
echo "🎉 Secure Dashboard with Secret Manager is now live!"
echo "🔐 Your Snowflake credentials are safely stored in Google Cloud Secret Manager!"
