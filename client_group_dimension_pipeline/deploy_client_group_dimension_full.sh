#!/bin/bash

# Deploy Client Group Dimension Full Sync Cloud Function

echo "Deploying Client Group Dimension Full Sync Cloud Function..."

cd client_group_dimension_sync_full

gcloud functions deploy client-group-dimension-sync-full \
    --source=. \
    --entry-point=sync_full_client_group_dimension \
    --runtime=python311 \
    --trigger-http \
    --memory=1024MB \
    --timeout=540s \
    --region=us-central1 \
    --allow-unauthenticated

echo "Client Group Dimension Full Sync Function deployed successfully!"
echo "Function URL: https://us-central1-red-octane-444308-f4.cloudfunctions.net/client-group-dimension-sync-full"

cd .. 