# 📚 Unified Pipeline - Complete Deployment Guide

## 🎯 Overview
This guide provides step-by-step instructions for deploying the Unified Snowflake to BigQuery Pipeline using CLI commands.

## 📋 Prerequisites

1. **Google Cloud CLI (`gcloud`)** installed and authenticated
2. **BigQuery CLI (`bq`)** installed
3. **Python 3.11+** with pip
4. **Snowflake credentials** ready to be stored in Secret Manager

## 🚀 Deployment Steps

### Step 1: Initial Setup

```bash
# Set your project ID
export PROJECT_ID="red-octane-444308-f4"
export REGION="us-central1"
export DATASET_ID="unified_pipeline_data"

# Set the project
gcloud config set project $PROJECT_ID

# Authenticate if needed
gcloud auth login
gcloud auth application-default login
```

### Step 2: Enable Required APIs

```bash
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com \
    cloudscheduler.googleapis.com \
    logging.googleapis.com \
    monitoring.googleapis.com \
    clouderrorreporting.googleapis.com \
    artifactregistry.googleapis.com \
    run.googleapis.com \
    eventarc.googleapis.com \
    pubsub.googleapis.com
```

### Step 3: Create BigQuery Dataset

#### Option A: Using `bq` CLI
```bash
# Create the dataset
bq mk \
    --project_id=$PROJECT_ID \
    --location=US \
    --description="Unified pipeline data storage" \
    --dataset $DATASET_ID

# Verify dataset creation
bq ls --project_id=$PROJECT_ID
```

#### Option B: Using Python Script
```bash
cd /workspace/snowflake_bq_unified_pipeline
python setup_bigquery_dataset.py
```

### Step 4: Create Service Account

```bash
# Create service account
gcloud iam service-accounts create unified-pipeline-sa \
    --display-name="Unified Pipeline Service Account" \
    --description="Service account for unified Snowflake to BigQuery pipeline"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# Grant Secret Manager permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"

# Grant Logging permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/logging.logWriter"

# Grant Monitoring permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/monitoring.metricWriter"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/clouderrorreporting.writer"
```

### Step 5: Set Up Secrets in Secret Manager

```bash
# Create Snowflake credentials secrets
echo -n "your_snowflake_user" | gcloud secrets create SNOWFLAKE_USER --data-file=-
echo -n "your_snowflake_password" | gcloud secrets create SNOWFLAKE_PASSWORD --data-file=-
echo -n "your_account.region" | gcloud secrets create SNOWFLAKE_ACCOUNT --data-file=-
echo -n "COMPUTE_WH" | gcloud secrets create SNOWFLAKE_WAREHOUSE --data-file=-
echo -n "KARBON" | gcloud secrets create SNOWFLAKE_DATABASE --data-file=-
echo -n "PUBLIC" | gcloud secrets create SNOWFLAKE_SCHEMA --data-file=-

# Optional: Add Slack webhook for alerts
echo -n "https://hooks.slack.com/services/YOUR/WEBHOOK/URL" | \
    gcloud secrets create SLACK_WEBHOOK_URL --data-file=-

# Verify secrets
gcloud secrets list
```

### Step 6: Deploy Cloud Functions

```bash
cd /workspace/snowflake_bq_unified_pipeline

# Deploy main pipeline function
gcloud functions deploy unified-snowflake-bq-pipeline \
    --gen2 \
    --region=$REGION \
    --runtime=python311 \
    --source=. \
    --entry-point=pipeline_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
    --memory=2GB \
    --timeout=540s \
    --max-instances=10 \
    --min-instances=0 \
    --concurrency=1

# Deploy batch pipeline function
gcloud functions deploy unified-snowflake-bq-pipeline-batch \
    --gen2 \
    --region=$REGION \
    --runtime=python311 \
    --source=. \
    --entry-point=batch_pipeline_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
    --memory=4GB \
    --timeout=540s \
    --max-instances=5 \
    --min-instances=0 \
    --concurrency=1

# Deploy status monitoring function
gcloud functions deploy unified-snowflake-bq-pipeline-status \
    --gen2 \
    --region=$REGION \
    --runtime=python311 \
    --source=. \
    --entry-point=pipeline_status_handler \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=unified-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,BQ_DATASET=$DATASET_ID" \
    --memory=512MB \
    --timeout=60s \
    --max-instances=5
```

### Step 7: Get Function URLs

```bash
# Get the function URLs
export MAIN_URL=$(gcloud functions describe unified-snowflake-bq-pipeline \
    --region=$REGION --format='value(serviceConfig.uri)')

export BATCH_URL=$(gcloud functions describe unified-snowflake-bq-pipeline-batch \
    --region=$REGION --format='value(serviceConfig.uri)')

export STATUS_URL=$(gcloud functions describe unified-snowflake-bq-pipeline-status \
    --region=$REGION --format='value(serviceConfig.uri)')

# Display URLs
echo "Main Pipeline URL: $MAIN_URL"
echo "Batch Pipeline URL: $BATCH_URL"
echo "Status URL: $STATUS_URL"
```

### Step 8: Create Cloud Scheduler Jobs

```bash
# Create scheduler for client dimension (daily at 8 AM)
gcloud scheduler jobs create http unified-client-dimension-daily \
    --location=$REGION \
    --schedule="0 8 * * *" \
    --uri="$MAIN_URL" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"pipeline": "client_dimension"}' \
    --time-zone="America/Los_Angeles" \
    --description="Daily sync of CLIENT_DIMENSION table" \
    --attempt-deadline=540s \
    --max-retry-attempts=3

# Create scheduler for user dimension (daily at 8 AM)
gcloud scheduler jobs create http unified-user-dimension-daily \
    --location=$REGION \
    --schedule="0 8 * * *" \
    --uri="$MAIN_URL" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"pipeline": "user_dimension"}' \
    --time-zone="America/Los_Angeles" \
    --description="Daily sync of USER_DIMENSION table" \
    --attempt-deadline=540s \
    --max-retry-attempts=3

# Create scheduler for work item details (daily at 6:30 AM)
gcloud scheduler jobs create http unified-work-item-details-daily \
    --location=$REGION \
    --schedule="30 6 * * *" \
    --uri="$MAIN_URL" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"pipeline": "work_item_details"}' \
    --time-zone="America/Los_Angeles" \
    --description="Daily incremental sync of WORK_ITEM_DETAILS" \
    --attempt-deadline=540s \
    --max-retry-attempts=3

# Create batch job for all dimensions (daily at 9 AM)
gcloud scheduler jobs create http unified-all-dimensions-daily \
    --location=$REGION \
    --schedule="0 9 * * *" \
    --uri="$BATCH_URL" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{
        "pipelines": [
            "client_dimension",
            "client_group_dimension",
            "tenant_team_dimension",
            "tenant_team_member_dimension",
            "user_dimension"
        ],
        "parallel": true
    }' \
    --time-zone="America/Los_Angeles" \
    --description="Batch sync of all dimension tables" \
    --attempt-deadline=540s \
    --max-retry-attempts=2

# List all scheduler jobs
gcloud scheduler jobs list --location=$REGION
```

### Step 9: Test the Deployment

```bash
# Test a single pipeline (dry run)
curl -X POST $MAIN_URL \
  -H 'Content-Type: application/json' \
  -d '{"pipeline": "client_dimension", "dry_run": true}'

# Run a full sync for client_dimension
curl -X POST $MAIN_URL \
  -H 'Content-Type: application/json' \
  -d '{"pipeline": "client_dimension"}'

# Check pipeline status
curl $STATUS_URL

# Run multiple pipelines in parallel
curl -X POST $BATCH_URL \
  -H 'Content-Type: application/json' \
  -d '{
    "pipelines": ["client_dimension", "user_dimension"],
    "parallel": true
  }'

# Manually trigger a scheduler job
gcloud scheduler jobs run unified-client-dimension-daily --location=$REGION
```

### Step 10: Monitor the Pipeline

```bash
# View function logs
gcloud functions logs read unified-snowflake-bq-pipeline \
    --region=$REGION \
    --limit=50

# View specific pipeline logs
gcloud logging read "resource.type=cloud_function \
    AND resource.labels.function_name=unified-snowflake-bq-pipeline \
    AND jsonPayload.pipeline=client_dimension" \
    --limit=20 \
    --format=json

# Check BigQuery tables
bq ls --dataset_id=$DATASET_ID --project_id=$PROJECT_ID

# Query a synced table
bq query --use_legacy_sql=false \
    "SELECT COUNT(*) as total_rows FROM \`$PROJECT_ID.$DATASET_ID.CLIENT_DIMENSION\`"

# View scheduler job status
gcloud scheduler jobs describe unified-client-dimension-daily \
    --location=$REGION
```

## 🔧 Troubleshooting

### Check Function Status
```bash
gcloud functions describe unified-snowflake-bq-pipeline \
    --region=$REGION \
    --format="value(state)"
```

### Check Service Account Permissions
```bash
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:unified-pipeline-sa"
```

### Test Secret Access
```bash
gcloud secrets versions access latest --secret=SNOWFLAKE_USER
```

### Force Redeploy
```bash
# Add a timestamp to force redeployment
gcloud functions deploy unified-snowflake-bq-pipeline \
    --region=$REGION \
    --update-env-vars="DEPLOY_TIME=$(date +%s)" \
    [... other parameters ...]
```

## 📊 Validation

### Run Migration Validation
```bash
cd /workspace/snowflake_bq_unified_pipeline
python validate_migration.py --backup
```

### Check Data Freshness
```bash
bq query --use_legacy_sql=false "
SELECT 
    table_name,
    TIMESTAMP_MILLIS(creation_time) as created,
    TIMESTAMP_MILLIS(last_modified_time) as last_modified,
    row_count,
    ROUND(size_bytes/1024/1024, 2) as size_mb
FROM \`$PROJECT_ID.$DATASET_ID.__TABLES__\`
ORDER BY last_modified_time DESC
"
```

## 🎯 Success Criteria

✅ All Cloud Functions deployed and active
✅ BigQuery dataset created with proper permissions
✅ Secrets stored in Secret Manager
✅ Scheduler jobs created and enabled
✅ Test pipeline runs successfully
✅ Data appears in BigQuery tables
✅ Logs show successful execution
✅ Status endpoint returns healthy

## 📚 Additional Resources

- [Cloud Functions Documentation](https://cloud.google.com/functions/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
- [Secret Manager Documentation](https://cloud.google.com/secret-manager/docs)

## 🆘 Support

For issues:
1. Check Cloud Function logs
2. Verify all secrets are created
3. Ensure service account has proper permissions
4. Review this deployment guide
5. Check the README.md for additional information