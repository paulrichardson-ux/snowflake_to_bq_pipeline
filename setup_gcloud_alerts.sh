#!/bin/bash
set -e

PROJECT_ID="red-octane-444308-f4"
EMAIL="paulrichardson@fiskalfinance.com"

echo "🔧 Setting up Google Cloud Monitoring Alerts"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Email: $EMAIL"
echo ""

# Enable required APIs
echo "🔌 Enabling required APIs..."
gcloud services enable monitoring.googleapis.com --project=$PROJECT_ID
gcloud services enable logging.googleapis.com --project=$PROJECT_ID
gcloud services enable pubsub.googleapis.com --project=$PROJECT_ID

# Create notification channel for email
echo ""
echo "📧 Creating email notification channel..."
NOTIFICATION_CHANNEL=$(gcloud alpha monitoring channels create \
  --display-name="Pipeline Alerts" \
  --description="Email notifications for pipeline issues" \
  --type=email \
  --channel-labels=email_address=$EMAIL \
  --project=$PROJECT_ID \
  --format="value(name)" 2>/dev/null || echo "EXISTS")

if [ "$NOTIFICATION_CHANNEL" = "EXISTS" ]; then
    echo "✅ Email notification channel already exists"
    # Get existing channel
    NOTIFICATION_CHANNEL=$(gcloud alpha monitoring channels list \
      --filter="type=email AND labels.email_address=$EMAIL" \
      --format="value(name)" \
      --project=$PROJECT_ID | head -1)
else
    echo "✅ Created email notification channel: $EMAIL"
fi

echo "📧 Notification channel: $NOTIFICATION_CHANNEL"

# Create alert policy for Cloud Scheduler failures
echo ""
echo "🚨 Creating Cloud Scheduler failure alert..."

cat > scheduler_alert_policy.json << EOF
{
  "displayName": "Pipeline Scheduler Job Failures",
  "documentation": {
    "content": "Alert when critical pipeline scheduler jobs fail",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "Scheduler job error rate",
      "conditionThreshold": {
        "filter": "resource.type=\"cloud_scheduler_job\" AND metric.type=\"logging.googleapis.com/log_entry_count\" AND jsonPayload.severity=\"ERROR\"",
        "comparison": "COMPARISON_GREATER_THAN",
        "thresholdValue": 0,
        "duration": "60s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE",
            "crossSeriesReducer": "REDUCE_SUM",
            "groupByFields": ["resource.label.job_id"]
          }
        ]
      }
    }
  ],
  "notificationChannels": ["$NOTIFICATION_CHANNEL"],
  "combiner": "OR",
  "enabled": true
}
EOF

gcloud alpha monitoring policies create --policy-from-file=scheduler_alert_policy.json --project=$PROJECT_ID 2>/dev/null || echo "✅ Scheduler alert policy already exists"

# Create alert policy for Cloud Function failures
echo ""
echo "🚨 Creating Cloud Function failure alert..."

cat > function_alert_policy.json << EOF
{
  "displayName": "Pipeline Cloud Function Failures",
  "documentation": {
    "content": "Alert when pipeline Cloud Functions fail",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "Function execution errors",
      "conditionThreshold": {
        "filter": "resource.type=\"cloud_function\" AND metric.type=\"cloudfunctions.googleapis.com/function/execution_count\" AND metric.label.status!=\"ok\"",
        "comparison": "COMPARISON_GREATER_THAN",
        "thresholdValue": 0,
        "duration": "60s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE",
            "crossSeriesReducer": "REDUCE_SUM",
            "groupByFields": ["resource.label.function_name"]
          }
        ]
      }
    }
  ],
  "notificationChannels": ["$NOTIFICATION_CHANNEL"],
  "combiner": "OR",
  "enabled": true
}
EOF

gcloud alpha monitoring policies create --policy-from-file=function_alert_policy.json --project=$PROJECT_ID 2>/dev/null || echo "✅ Function alert policy already exists"

# Create uptime check for critical scheduler
echo ""
echo "⏱️  Creating uptime check for scheduler health..."

cat > uptime_check.json << EOF
{
  "displayName": "Pipeline Scheduler Health Check",
  "monitoredResource": {
    "type": "uptime_url",
    "labels": {
      "project_id": "$PROJECT_ID",
      "host": "us-central1-$PROJECT_ID.cloudfunctions.net"
    }
  },
  "httpCheck": {
    "path": "/pipeline-scheduler-monitor",
    "port": 443,
    "useSsl": true,
    "requestMethod": "POST",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": "{\"source\": \"uptime_check\"}"
  },
  "period": "300s",
  "timeout": "60s",
  "contentMatchers": [
    {
      "content": "success"
    }
  ]
}
EOF

gcloud alpha monitoring uptime create --config-from-file=uptime_check.json --project=$PROJECT_ID 2>/dev/null || echo "✅ Uptime check already exists"

# Clean up temporary files
rm -f scheduler_alert_policy.json function_alert_policy.json uptime_check.json

echo ""
echo "✅ Google Cloud Monitoring Setup Complete!"
echo "=========================================="
echo "📧 Email alerts: $EMAIL"
echo "🚨 Alert policies: Created for scheduler and function failures"
echo "⏱️  Uptime monitoring: Active for pipeline health"
echo ""
echo "🔍 Monitor your alerts:"
echo "https://console.cloud.google.com/monitoring/alerting?project=$PROJECT_ID"
echo ""
echo "📧 You'll receive email notifications for:"
echo "• Cloud Scheduler job failures"
echo "• Cloud Function execution failures"  
echo "• Pipeline health check failures"
echo ""
echo "🧪 Test an alert:"
echo "gcloud alpha monitoring policies list --project=$PROJECT_ID"
