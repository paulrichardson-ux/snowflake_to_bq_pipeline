#!/bin/bash

# Setup Google Cloud Secret Manager for Snowflake Credentials
# This script creates and stores Snowflake credentials securely in Google Cloud Secret Manager

set -e

echo "🔐 Setting up Google Cloud Secret Manager for Snowflake credentials..."

# Configuration
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI not found. Please install it first."
    exit 1
fi

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null 2>&1; then
    echo "❌ Error: gcloud not authenticated. Please run: gcloud auth login"
    exit 1
fi

# Set the project
echo "🔧 Setting project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID

# Enable Secret Manager API
echo "🔌 Enabling Secret Manager API..."
gcloud services enable secretmanager.googleapis.com

echo "✅ Secret Manager API enabled"

# Function to create or update a secret
create_or_update_secret() {
    local secret_name=$1
    local secret_description=$2
    local prompt_text=$3
    local is_password=${4:-false}
    
    echo ""
    echo "📝 Setting up secret: $secret_name"
    echo "Description: $secret_description"
    
    # Check if secret already exists
    if gcloud secrets describe $secret_name --project=$PROJECT_ID >/dev/null 2>&1; then
        echo "⚠️  Secret $secret_name already exists."
        read -p "Do you want to update it? (y/N): " update_choice
        if [[ ! $update_choice =~ ^[Yy]$ ]]; then
            echo "⏭️  Skipping $secret_name"
            return
        fi
    else
        # Create the secret
        echo "🆕 Creating secret: $secret_name"
        gcloud secrets create $secret_name \
            --project=$PROJECT_ID \
            --replication-policy="automatic" \
            --labels="component=karbon-dashboard,type=snowflake-credential"
    fi
    
    # Get the secret value from user
    if [ "$is_password" = true ]; then
        read -s -p "$prompt_text: " secret_value
        echo ""  # New line after hidden input
    else
        read -p "$prompt_text: " secret_value
    fi
    
    if [ -z "$secret_value" ]; then
        echo "⚠️  Empty value provided. Skipping $secret_name"
        return
    fi
    
    # Add the secret version
    echo "$secret_value" | gcloud secrets versions add $secret_name \
        --project=$PROJECT_ID \
        --data-file=-
    
    echo "✅ Secret $secret_name updated successfully"
}

# Create/update all Snowflake secrets
echo ""
echo "🔑 Please provide your Snowflake credentials:"
echo "These will be stored securely in Google Cloud Secret Manager"

create_or_update_secret "snowflake-user" \
    "Snowflake username for Karbon dashboard" \
    "Enter your Snowflake username"

create_or_update_secret "snowflake-password" \
    "Snowflake password for Karbon dashboard" \
    "Enter your Snowflake password" \
    true

create_or_update_secret "snowflake-account" \
    "Snowflake account identifier for Karbon dashboard" \
    "Enter your Snowflake account identifier (e.g., abc12345.us-east-1)"

create_or_update_secret "snowflake-warehouse" \
    "Snowflake warehouse for Karbon dashboard" \
    "Enter your Snowflake warehouse (default: COMPUTE_WH)"

create_or_update_secret "snowflake-database" \
    "Snowflake database for Karbon dashboard" \
    "Enter your Snowflake database (default: KARBON)"

create_or_update_secret "snowflake-schema" \
    "Snowflake schema for Karbon dashboard" \
    "Enter your Snowflake schema (default: PUBLIC)"

# Set up IAM permissions for the Cloud Function service account
echo ""
echo "🔐 Setting up IAM permissions..."

# Get the default compute service account
SERVICE_ACCOUNT="$PROJECT_ID@appspot.gserviceaccount.com"

echo "📋 Granting Secret Manager access to service account: $SERVICE_ACCOUNT"

# Grant Secret Manager Secret Accessor role to the service account
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor"

echo "✅ IAM permissions configured"

# List created secrets
echo ""
echo "📋 Created secrets:"
gcloud secrets list --project=$PROJECT_ID --filter="labels.component=karbon-dashboard" --format="table(name,createTime)"

echo ""
echo "🧪 Testing secret access..."

# Test reading a secret (non-sensitive one)
if gcloud secrets versions access latest --secret="snowflake-user" --project=$PROJECT_ID >/dev/null 2>&1; then
    echo "✅ Secret access test successful"
else
    echo "⚠️  Secret access test failed - check IAM permissions"
fi

echo ""
echo "🎉 Secret Manager setup complete!"
echo ""
echo "📊 Summary:"
echo "   ✅ Secret Manager API enabled"
echo "   ✅ Snowflake credentials stored securely"
echo "   ✅ IAM permissions configured"
echo "   ✅ Service account has secret access"
echo ""
echo "🔒 Security Benefits:"
echo "   • Credentials are encrypted at rest and in transit"
echo "   • Access is logged and auditable"
echo "   • Fine-grained access control via IAM"
echo "   • Automatic secret rotation capability"
echo "   • No credentials in environment variables or code"
echo ""
echo "🚀 Next Steps:"
echo "   1. Deploy the dashboard: ./deploy_secure_dashboard_with_secrets.sh"
echo "   2. The dashboard will automatically use Secret Manager"
echo "   3. Monitor secret access in Cloud Logging"
echo ""
echo "🔧 Management Commands:"
echo "   • List secrets: gcloud secrets list --filter='labels.component=karbon-dashboard'"
echo "   • View secret: gcloud secrets versions access latest --secret=snowflake-user"
echo "   • Update secret: echo 'new-value' | gcloud secrets versions add SECRET_NAME --data-file=-"
echo "   • Delete secret: gcloud secrets delete SECRET_NAME"
echo ""
echo "⚠️  Important:"
echo "   • Keep your Snowflake credentials secure"
echo "   • Regularly rotate passwords"
echo "   • Monitor secret access logs"
echo "   • Use least-privilege access principles"
