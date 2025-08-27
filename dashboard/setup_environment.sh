#!/bin/bash

# Setup Environment Variables for Dashboard with Data Comparison
# This script sets up the required environment variables for Snowflake and BigQuery connections

echo "🔧 Setting up environment variables for Karbon Dashboard with Data Comparison..."

# Check if we're in the right directory
if [ ! -f "main.py" ]; then
    echo "❌ Error: Please run this script from the dashboard directory"
    exit 1
fi

# Create .env file for local development
cat > .env << EOF
# Google Cloud Configuration
GOOGLE_CLOUD_PROJECT=red-octane-444308-f4
GOOGLE_CLIENT_ID=your-google-oauth-client-id

# Snowflake Configuration (for data comparison)
SNOWFLAKE_USER=your-snowflake-username
SNOWFLAKE_PASSWORD=your-snowflake-password
SNOWFLAKE_ACCOUNT=your-snowflake-account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=KARBON
SNOWFLAKE_SCHEMA=PUBLIC

# Flask Configuration
SECRET_KEY=your-secret-key-here
EOF

echo "✅ Created .env file with template environment variables"
echo ""
echo "📝 Please update the .env file with your actual credentials:"
echo "   - GOOGLE_CLIENT_ID: Your Google OAuth Client ID"
echo "   - SNOWFLAKE_USER: Your Snowflake username"
echo "   - SNOWFLAKE_PASSWORD: Your Snowflake password"
echo "   - SNOWFLAKE_ACCOUNT: Your Snowflake account identifier"
echo "   - SECRET_KEY: A secure random string for Flask sessions"
echo ""

# For Cloud Functions deployment, we'll set these via environment variables
echo "🚀 For Cloud Functions deployment, set these environment variables:"
echo ""
echo "gcloud functions deploy karbon-pipeline-dashboard \\"
echo "  --runtime python311 \\"
echo "  --trigger-http \\"
echo "  --allow-unauthenticated \\"
echo "  --region us-central1 \\"
echo "  --memory 1024MB \\"
echo "  --timeout 540s \\"
echo "  --set-env-vars \\"
echo "    GOOGLE_CLIENT_ID=your-google-oauth-client-id,\\"
echo "    SNOWFLAKE_USER=your-snowflake-username,\\"
echo "    SNOWFLAKE_PASSWORD=your-snowflake-password,\\"
echo "    SNOWFLAKE_ACCOUNT=your-snowflake-account,\\"
echo "    SNOWFLAKE_WAREHOUSE=COMPUTE_WH,\\"
echo "    SNOWFLAKE_DATABASE=KARBON,\\"
echo "    SNOWFLAKE_SCHEMA=PUBLIC,\\"
echo "    SECRET_KEY=your-secure-secret-key"
echo ""

# Check if gcloud is installed and authenticated
if command -v gcloud &> /dev/null; then
    echo "✅ gcloud CLI is available"
    
    # Check authentication
    if gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null 2>&1; then
        echo "✅ gcloud is authenticated"
        
        # Get current project
        CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
        if [ ! -z "$CURRENT_PROJECT" ]; then
            echo "✅ Current project: $CURRENT_PROJECT"
        else
            echo "⚠️  No default project set. Run: gcloud config set project red-octane-444308-f4"
        fi
    else
        echo "⚠️  gcloud not authenticated. Run: gcloud auth login"
    fi
else
    echo "⚠️  gcloud CLI not found. Please install it to deploy to Cloud Functions"
fi

echo ""
echo "🔐 Security Notes:"
echo "   - Never commit the .env file to version control"
echo "   - Use Google Secret Manager for production credentials"
echo "   - Rotate credentials regularly"
echo "   - Limit Snowflake user permissions to read-only for comparison queries"
echo ""
echo "📊 Data Comparison Features:"
echo "   - Compare BigQuery view with Snowflake source data"
echo "   - Identify discrepancies in budget and time tracking data"
echo "   - Real-time validation of data pipeline accuracy"
echo "   - Export comparison results for analysis"
echo ""
echo "🎯 Next Steps:"
echo "   1. Update .env file with your credentials"
echo "   2. Test locally: functions-framework --target=pipeline_dashboard --port=8080"
echo "   3. Deploy to Cloud Functions with the command above"
echo "   4. Access dashboard at the provided URL"
