#!/bin/bash

# Deploy Tenant Team Dimension Daily Sync Cloud Function

echo "Deploying Tenant Team Dimension Daily Sync Cloud Function..."

cd tenant_team_dimension_sync_daily

gcloud functions deploy tenant-team-dimension-sync-daily \
    --source=. \
    --entry-point=sync_tenant_team_dimension_full \
    --runtime=python311 \
    --trigger-http \
    --memory=1024MB \
    --timeout=540s \
    --region=us-central1 \
    --allow-unauthenticated

echo "Tenant Team Dimension Daily Sync Function deployed successfully!"
echo "Function URL: https://us-central1-red-octane-444308-f4.cloudfunctions.net/tenant-team-dimension-sync-daily"

cd . 