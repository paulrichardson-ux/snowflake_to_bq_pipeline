#!/bin/bash

# Quick Deployment Script for Unified Pipeline
# =============================================
# Run this from a machine with gcloud CLI installed

set -e

# Configuration - Update these as needed
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
DATASET_ID="unified_pipeline_data"
SERVICE_ACCOUNT_NAME="unified-pipeline-sa"

echo "🚀 Quick Deploy - Unified Pipeline"
echo "=================================="
echo "Project: $PROJECT_ID"
echo "Region:  $REGION"
echo "Dataset: $DATASET_ID"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI is not installed"
    echo "Please install it from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Set project
echo "→ Setting project..."
gcloud config set project $PROJECT_ID

# Check authentication
echo "→ Checking authentication..."
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "⚠️  No active authentication found"
    echo "Please run: gcloud auth login"
    exit 1
fi

# Create dataset
echo "→ Creating BigQuery dataset..."
bq mk --project_id=$PROJECT_ID --location=US --dataset $DATASET_ID 2>/dev/null || echo "  Dataset already exists"

# Deploy functions
echo "→ Deploying Cloud Functions..."
echo "  This may take 5-10 minutes..."

# Main function
gcloud functions deploy unified-snowflake-bq-pipeline \
    --gen2 \
    --region=$REGION \
    --runtime=python311 \
    --source=. \
    --entry-point=pipeline_handler \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
    --memory=2GB \
    --timeout=540s \
    --quiet

# Get URL
MAIN_URL=$(gcloud functions describe unified-snowflake-bq-pipeline \
    --region=$REGION --format='value(serviceConfig.uri)')

echo ""
echo "✅ Deployment Complete!"
echo "======================="
echo ""
echo "Function URL: $MAIN_URL"
echo ""
echo "Test with:"
echo "curl -X POST $MAIN_URL \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"pipeline\": \"client_dimension\", \"dry_run\": true}'"
echo ""
echo "See DEPLOYMENT_GUIDE.md for full setup including:"
echo "  • Service account creation"
echo "  • Secret Manager setup"
echo "  • Cloud Scheduler configuration"
echo "  • Monitoring setup"