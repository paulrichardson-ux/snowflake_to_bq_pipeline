#!/bin/bash

# Deploy paginated time entry sync with proper error handling and Pub/Sub integration
set -e

PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
FUNCTION_NAME="sync_daily_incremental_paginated"
PUBSUB_TOPIC="time-entry-sync-trigger"
DATASET_ID="karbon_bq_dataset"

echo "🚀 Deploying paginated time entry sync system..."

# 1. Create Pub/Sub topic if it doesn't exist
echo "📡 Creating Pub/Sub topic: $PUBSUB_TOPIC"
gcloud pubsub topics create $PUBSUB_TOPIC --project=$PROJECT_ID || echo "Topic already exists"

# 2. Deploy the main paginated Cloud Function
echo "☁️ Deploying paginated sync Cloud Function..."
cd "snowflake_bq_sync_daily Time Details"

gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=sync_daily_incremental \
    --trigger-http \
    --timeout=540s \
    --memory=2Gi \
    --max-instances=1 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
    --service-account=scheduler-invoker-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --no-allow-unauthenticated \
    --project=$PROJECT_ID

# 3. Deploy Pub/Sub triggered function for chaining
echo "🔗 Deploying Pub/Sub chain function..."
gcloud functions deploy "${FUNCTION_NAME}_chain" \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=sync_daily_incremental \
    --trigger-topic=$PUBSUB_TOPIC \
    --timeout=540s \
    --memory=2Gi \
    --max-instances=1 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
    --service-account=scheduler-invoker-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --project=$PROJECT_ID

# 4. Deploy manual sync function
echo "🔧 Deploying manual sync function..."
gcloud functions deploy "${FUNCTION_NAME}_manual" \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=sync_manual_chunk \
    --trigger-http \
    --timeout=540s \
    --memory=2Gi \
    --max-instances=10 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
    --service-account=scheduler-invoker-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --no-allow-unauthenticated \
    --project=$PROJECT_ID

cd ..

# 5. Update Cloud Scheduler to use new paginated function
echo "⏰ Updating Cloud Scheduler job..."
FUNCTION_URL="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"

gcloud scheduler jobs update http daily-snowflake-incremental-sync \
    --location=$REGION \
    --uri=$FUNCTION_URL \
    --http-method=POST \
    --oidc-service-account-email=scheduler-invoker-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --oidc-token-audience=$FUNCTION_URL \
    --project=$PROJECT_ID

# 6. Grant Pub/Sub permissions
echo "🔐 Setting up Pub/Sub permissions..."
gcloud pubsub topics add-iam-policy-binding $PUBSUB_TOPIC \
    --member="serviceAccount:scheduler-invoker-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher" \
    --project=$PROJECT_ID

echo "✅ Paginated sync deployment complete!"
echo ""
echo "📊 New Functions Deployed:"
echo "   • Main: https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
echo "   • Chain: https://$REGION-$PROJECT_ID.cloudfunctions.net/${FUNCTION_NAME}_chain"
echo "   • Manual: https://$REGION-$PROJECT_ID.cloudfunctions.net/${FUNCTION_NAME}_manual"
echo ""
echo "🔄 How it works:"
echo "   1. Scheduler triggers main function daily at 08:30 CAT"
echo "   2. Each execution processes 7 days of data (instead of 180 days)"
echo "   3. After completion, triggers next chunk via Pub/Sub"
echo "   4. Process continues until all data is synced"
echo "   5. State is tracked in BigQuery table for resumability"
echo ""
echo "🛠️ Manual sync usage:"
echo "   curl -X POST '$FUNCTION_URL' \\"
echo "        -H 'Authorization: Bearer \$(gcloud auth print-identity-token)' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"start_date\": \"2025-08-20\", \"end_date\": \"2025-08-26\"}'"
echo ""
echo "📈 Monitor progress:"
echo "   SELECT * FROM \`$PROJECT_ID.$DATASET_ID.time_entry_sync_state\` ORDER BY created_at DESC"
