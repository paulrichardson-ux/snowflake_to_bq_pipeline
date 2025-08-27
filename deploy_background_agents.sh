#!/bin/bash

# Deploy Background Agents for Snowflake to BigQuery Pipeline
# This script deploys various background monitoring and automation agents

set -e

PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
SERVICE_ACCOUNT="karbon-bq-sync@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🚀 Deploying Background Agents for Pipeline Monitoring"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Service Account: $SERVICE_ACCOUNT"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo "📋 Checking prerequisites..."
if ! command_exists gcloud; then
    echo "❌ gcloud CLI not found. Please install Google Cloud SDK."
    exit 1
fi

if ! command_exists python3; then
    echo "❌ python3 not found. Please install Python 3.11+."
    exit 1
fi

echo "✅ Prerequisites check passed"
echo ""

# Set project
echo "🔧 Setting up Google Cloud project..."
gcloud config set project $PROJECT_ID
echo ""

# Deploy Data Quality Agent as Cloud Function
echo "📊 Deploying Data Quality Agent..."
cat > data_quality_requirements.txt << EOF
google-cloud-bigquery>=3.11.4
google-cloud-secret-manager>=2.16.4
scikit-learn>=1.3.0
pandas>=2.0.3
numpy>=1.24.3
requests>=2.31.0
EOF

gcloud functions deploy data-quality-agent \
    --runtime python311 \
    --trigger-http \
    --allow-unauthenticated \
    --memory 1GB \
    --timeout 540s \
    --service-account $SERVICE_ACCOUNT \
    --region $REGION \
    --source . \
    --entry-point main \
    --env-vars-file <(echo "PROJECT_ID=$PROJECT_ID") \
    --requirements-file data_quality_requirements.txt

echo "✅ Data Quality Agent deployed"
echo ""

# Deploy Anomaly Detection Agent as Cloud Function  
echo "🤖 Deploying Anomaly Detection Agent..."
cat > anomaly_detection_requirements.txt << EOF
google-cloud-bigquery>=3.11.4
google-cloud-secret-manager>=2.16.4
scikit-learn>=1.3.0
pandas>=2.0.3
numpy>=1.24.3
requests>=2.31.0
EOF

gcloud functions deploy anomaly-detection-agent \
    --runtime python311 \
    --trigger-http \
    --allow-unauthenticated \
    --memory 2GB \
    --timeout 540s \
    --service-account $SERVICE_ACCOUNT \
    --region $REGION \
    --source . \
    --entry-point main \
    --env-vars-file <(echo "PROJECT_ID=$PROJECT_ID") \
    --requirements-file anomaly_detection_requirements.txt

echo "✅ Anomaly Detection Agent deployed"
echo ""

# Deploy Slack Integration Agent
echo "📱 Deploying Slack Integration Agent..."
cat > slack_integration_requirements.txt << EOF
google-cloud-bigquery>=3.11.4
google-cloud-secret-manager>=2.16.4
requests>=2.31.0
flask>=2.3.2
EOF

gcloud functions deploy slack-integration-agent \
    --runtime python311 \
    --trigger-http \
    --allow-unauthenticated \
    --memory 512MB \
    --timeout 300s \
    --service-account $SERVICE_ACCOUNT \
    --region $REGION \
    --source . \
    --entry-point main \
    --env-vars-file <(echo "PROJECT_ID=$PROJECT_ID") \
    --requirements-file slack_integration_requirements.txt

echo "✅ Slack Integration Agent deployed"
echo ""

# Create Cloud Schedulers for Background Agents
echo "⏰ Setting up Cloud Schedulers for Background Agents..."

# Data Quality Agent - Every 6 hours
gcloud scheduler jobs create http data-quality-agent-scheduler \
    --location=$REGION \
    --schedule="0 */6 * * *" \
    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/data-quality-agent" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"action": "run_quality_checks"}' \
    --time-zone="UTC" \
    --description="Run data quality checks every 6 hours"

echo "✅ Data Quality Agent scheduler created"

# Anomaly Detection Agent - Every 8 hours
gcloud scheduler jobs create http anomaly-detection-agent-scheduler \
    --location=$REGION \
    --schedule="0 */8 * * *" \
    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/anomaly-detection-agent" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"action": "run_anomaly_detection"}' \
    --time-zone="UTC" \
    --description="Run anomaly detection every 8 hours"

echo "✅ Anomaly Detection Agent scheduler created"

# Slack Daily Report - Every day at 09:00 UTC (11:00 CAT)
gcloud scheduler jobs create http slack-daily-report-scheduler \
    --location=$REGION \
    --schedule="0 9 * * *" \
    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/slack-integration-agent" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"action": "send_daily_report"}' \
    --time-zone="UTC" \
    --description="Send daily Slack status report at 11:00 CAT"

echo "✅ Slack Daily Report scheduler created"
echo ""

# Set up monitoring alerts
echo "📢 Setting up monitoring alerts..."

# Create alert policy for function failures
cat > alert_policy.json << EOF
{
  "displayName": "Background Agents Failure Alert",
  "combiner": "OR",
  "conditions": [
    {
      "displayName": "Cloud Function execution failures",
      "conditionThreshold": {
        "filter": "resource.type=\"cloud_function\" AND resource.labels.function_name=~\".*-agent.*\"",
        "comparison": "COMPARISON_GREATER_THAN",
        "thresholdValue": 2,
        "duration": "300s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE",
            "crossSeriesReducer": "REDUCE_SUM",
            "groupByFields": ["resource.labels.function_name"]
          }
        ]
      }
    }
  ],
  "alertStrategy": {
    "autoClose": "86400s"
  },
  "enabled": true
}
EOF

gcloud alpha monitoring policies create --policy-from-file=alert_policy.json

echo "✅ Monitoring alerts configured"
echo ""

# Create IAM bindings for agents
echo "🔐 Setting up IAM permissions..."

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.jobUser"

# Grant Secret Manager permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor"

# Grant Cloud Functions permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/cloudfunctions.invoker"

echo "✅ IAM permissions configured"
echo ""

# Test deployments
echo "🧪 Testing deployed agents..."

echo "Testing Data Quality Agent..."
curl -X POST "https://$REGION-$PROJECT_ID.cloudfunctions.net/data-quality-agent" \
    -H "Content-Type: application/json" \
    -d '{"action": "health_check"}' \
    --max-time 30 || echo "⚠️ Data Quality Agent test failed"

echo "Testing Anomaly Detection Agent..."
curl -X POST "https://$REGION-$PROJECT_ID.cloudfunctions.net/anomaly-detection-agent" \
    -H "Content-Type: application/json" \
    -d '{"action": "health_check"}' \
    --max-time 30 || echo "⚠️ Anomaly Detection Agent test failed"

echo "Testing Slack Integration Agent..."
curl -X POST "https://$REGION-$PROJECT_ID.cloudfunctions.net/slack-integration-agent" \
    -H "Content-Type: application/json" \
    -d '{"action": "health_check"}' \
    --max-time 30 || echo "⚠️ Slack Integration Agent test failed"

echo ""

# Display deployment summary
echo "🎉 Background Agents Deployment Complete!"
echo ""
echo "📊 Deployed Agents:"
echo "   • Data Quality Agent: https://$REGION-$PROJECT_ID.cloudfunctions.net/data-quality-agent"
echo "   • Anomaly Detection Agent: https://$REGION-$PROJECT_ID.cloudfunctions.net/anomaly-detection-agent" 
echo "   • Slack Integration Agent: https://$REGION-$PROJECT_ID.cloudfunctions.net/slack-integration-agent"
echo ""
echo "⏰ Scheduled Jobs:"
echo "   • Data Quality Checks: Every 6 hours"
echo "   • Anomaly Detection: Every 8 hours"
echo "   • Slack Daily Reports: Daily at 11:00 CAT"
echo ""
echo "🔗 Integration URLs:"
echo "   • GitHub Actions Workflow: .github/workflows/deploy-pipeline.yml"
echo "   • BigQuery Console: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo "   • Cloud Functions Console: https://console.cloud.google.com/functions/list?project=$PROJECT_ID"
echo ""
echo "📋 Next Steps:"
echo "   1. Set up Slack integration (see slack_integration_agent.py for instructions)"
echo "   2. Configure GitHub secrets for CI/CD pipeline"
echo "   3. Test all agents manually to ensure proper operation"
echo "   4. Monitor agent logs for the first 24 hours"
echo ""
echo "✅ Your pipeline now has comprehensive background monitoring!"

# Cleanup temporary files
rm -f data_quality_requirements.txt
rm -f anomaly_detection_requirements.txt  
rm -f slack_integration_requirements.txt
rm -f alert_policy.json

echo "🧹 Cleanup completed"
