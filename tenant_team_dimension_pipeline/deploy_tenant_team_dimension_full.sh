#!/bin/bash

# Deploy Tenant Team Dimension Full Sync Cloud Function

echo "Deploying Tenant Team Dimension Full Sync Cloud Function..."

cd tenant_team_dimension_sync_full

gcloud functions deploy tenant-team-dimension-sync-full \
    --source=. \
    --entry-point=sync_full_tenant_team_dimension \
    --runtime=python311 \
    --trigger-http \
    --memory=1024MB \
    --timeout=540s \
    --region=us-central1 \
    --allow-unauthenticated

echo "Tenant Team Dimension Full Sync Function deployed successfully!"
echo "Function URL: https://us-central1-red-octane-444308-f4.cloudfunctions.net/tenant-team-dimension-sync-full"

cd .. 