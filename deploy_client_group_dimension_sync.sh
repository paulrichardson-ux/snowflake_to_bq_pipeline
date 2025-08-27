#!/bin/bash

# Deploy Client Group Dimension Daily Sync Cloud Function

echo "Deploying Client Group Dimension Daily Sync Cloud Function..."

cd client_group_dimension_sync_daily

gcloud functions deploy client-group-dimension-sync-daily \
    --source=. \
    --entry-point=sync_client_group_dimension_full \
    --runtime=python311 \
    --trigger-http \
    --memory=1024MB \
    --timeout=540s \
    --region=us-central1 \
    --allow-unauthenticated

echo "Client Group Dimension Daily Sync Function deployed successfully!"
echo "Function URL: https://us-central1-red-octane-444308-f4.cloudfunctions.net/client-group-dimension-sync-daily"

cd . 