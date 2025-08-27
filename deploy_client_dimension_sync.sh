#!/bin/bash

# Deploy CLIENT_DIMENSION sync Cloud Function
FUNCTION_NAME="client-dimension-sync-daily"
REGION="us-central1"
PROJECT_ID="red-octane-444308-f4"
DATASET_ID="karbon_data"

echo "Deploying CLIENT_DIMENSION sync function..."

gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --region=$REGION \
  --runtime=python311 \
  --source=./client_dimension_sync_daily \
  --entry-point=sync_client_dimension_full \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
  --memory=1024MB \
  --timeout=540s \
  --max-instances=1

if [ $? -eq 0 ]; then
    echo "✅ CLIENT_DIMENSION sync function deployed successfully!"
    echo "Function URL: https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
else
    echo "❌ Deployment failed!"
    exit 1
fi 