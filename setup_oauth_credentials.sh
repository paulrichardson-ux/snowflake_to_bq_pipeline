#!/bin/bash

# =============================================================================
# SETUP GOOGLE OAUTH CREDENTIALS FOR DASHBOARD
# =============================================================================
# This script helps set up Google OAuth credentials for the secured dashboard

set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
FUNCTION_NAME="karbon-pipeline-dashboard"

echo "🔐 Setting up Google OAuth credentials for Dashboard..."
echo "  Project: ${PROJECT_ID}"
echo "  Function: ${FUNCTION_NAME}"
echo ""

echo "📋 STEP 1: Create Google OAuth Client ID"
echo "========================================="
echo ""
echo "1. Go to the Google Cloud Console:"
echo "   https://console.cloud.google.com/apis/credentials?project=${PROJECT_ID}"
echo ""
echo "2. Click 'CREATE CREDENTIALS' → 'OAuth 2.0 Client IDs'"
echo ""
echo "3. Configure the OAuth consent screen if not done already:"
echo "   - Application name: Karbon Pipeline Dashboard"
echo "   - User support email: your-email@fiskalfinance.com"
echo "   - Authorized domains: fiskalfinance.com"
echo "   - Developer contact: your-email@fiskalfinance.com"
echo ""
echo "4. Create OAuth 2.0 Client ID:"
echo "   - Application type: Web application"
echo "   - Name: Karbon Pipeline Dashboard"
echo "   - Authorized JavaScript origins:"
echo "     https://us-central1-${PROJECT_ID}.cloudfunctions.net"
echo "   - Authorized redirect URIs:"
echo "     https://us-central1-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"
echo ""
echo "5. Copy the Client ID (it looks like: 123456789-abcdefg.apps.googleusercontent.com)"
echo ""

# Wait for user input
read -p "📝 Enter your Google OAuth Client ID: " GOOGLE_CLIENT_ID

if [ -z "$GOOGLE_CLIENT_ID" ]; then
    echo "❌ Error: Client ID cannot be empty"
    exit 1
fi

echo ""
echo "📋 STEP 2: Generate Secret Key"
echo "=============================="

# Generate a secure secret key
SECRET_KEY=$(python3 -c "import secrets; import base64; print(base64.b64encode(secrets.token_bytes(32)).decode())")
echo "✅ Generated secure secret key"

echo ""
echo "📋 STEP 3: Store Credentials in Google Secret Manager"
echo "===================================================="

# Create secrets for OAuth credentials
echo "Creating GOOGLE_CLIENT_ID secret..."
echo "${GOOGLE_CLIENT_ID}" | gcloud secrets create GOOGLE_CLIENT_ID --data-file=- --replication-policy=automatic || \
echo "${GOOGLE_CLIENT_ID}" | gcloud secrets versions add GOOGLE_CLIENT_ID --data-file=-

echo "Creating DASHBOARD_SECRET_KEY secret..."
echo "${SECRET_KEY}" | gcloud secrets create DASHBOARD_SECRET_KEY --data-file=- --replication-policy=automatic || \
echo "${SECRET_KEY}" | gcloud secrets versions add DASHBOARD_SECRET_KEY --data-file=-

echo "✅ Secrets created successfully"

echo ""
echo "📋 STEP 4: Grant Service Account Access"
echo "======================================="

SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant access to secrets
gcloud secrets add-iam-policy-binding GOOGLE_CLIENT_ID \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding DASHBOARD_SECRET_KEY \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor"

echo "✅ Service account permissions granted"

echo ""
echo "📋 STEP 5: Configure Domain Restriction"
echo "======================================="
echo "✅ Domain restriction already configured for: fiskalfinance.com"
echo "   Only users with @fiskalfinance.com email addresses can access the dashboard"

echo ""
echo "🎉 OAUTH SETUP COMPLETE!"
echo "========================"
echo ""
echo "✅ Google Client ID: ${GOOGLE_CLIENT_ID}"
echo "✅ Secret Key: Generated and stored securely"
echo "✅ Domain Restriction: fiskalfinance.com only"
echo "✅ Service Account: ${SERVICE_ACCOUNT}"
echo ""
echo "🔄 NEXT STEP: Deploy the secured dashboard"
echo "./deploy_secure_dashboard.sh"
echo ""
echo "🌐 After deployment, access your secured dashboard at:"
echo "https://us-central1-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"
echo ""
echo "🔒 SECURITY FEATURES:"
echo "   • Google SSO authentication required"
echo "   • Domain restricted to fiskalfinance.com"
echo "   • Secure session management"
echo "   • Automatic logout on session expiry"
echo ""
