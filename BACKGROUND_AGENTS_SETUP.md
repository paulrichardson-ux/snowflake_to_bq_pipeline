# 🤖 Background Agents Setup Guide

This guide explains how to connect various background agents to your Snowflake to BigQuery pipeline for enhanced monitoring, automation, and intelligence.

## 🎯 Overview

Your pipeline now supports multiple types of background agents:

| Agent Type | Purpose | Frequency | Key Features |
|------------|---------|-----------|--------------|
| **CI/CD Agent** | Automated deployments | On code changes | GitHub Actions, auto-deploy, validation |
| **Data Quality Agent** | Data validation | Every 6 hours | Freshness, row counts, null checks, duplicates |
| **AI Anomaly Agent** | Pattern detection | Every 8 hours | Machine learning, anomaly scoring, alerts |
| **Slack Integration** | Team notifications | Real-time + daily | Rich messages, interactive commands |
| **Existing Monitor** | Scheduler health | Every 4 hours | Auto-fix, email alerts, status reports |

## 🚀 Quick Start

### 1. Deploy All Background Agents
```bash
# Run the comprehensive deployment script
./deploy_background_agents.sh
```

This will deploy:
- ✅ Data Quality Agent (Cloud Function)
- ✅ AI Anomaly Detection Agent (Cloud Function) 
- ✅ Slack Integration Agent (Cloud Function)
- ✅ Cloud Schedulers for automation
- ✅ IAM permissions and monitoring alerts

### 2. Set Up GitHub Actions (CI/CD Agent)
```bash
# The workflow file is already created at:
# .github/workflows/deploy-pipeline.yml

# Add these secrets to your GitHub repository:
# Settings > Secrets and variables > Actions > New repository secret
```

**Required GitHub Secrets:**
- `GCP_SA_KEY`: Your service account JSON key

### 3. Configure Slack Integration (Optional)
```bash
# Follow the setup instructions in slack_integration_agent.py
# or run the setup helper:
python3 -c "from slack_integration_agent import setup_slack_integration; setup_slack_integration()"
```

## 📊 Agent Details

### 1. Data Quality Agent
**Purpose**: Continuous data quality monitoring
**Location**: `data_quality_agent.py`
**Endpoint**: `https://us-central1-red-octane-444308-f4.cloudfunctions.net/data-quality-agent`

**Features:**
- ✅ Data freshness checks (< 25 hours)
- ✅ Row count validation (minimum thresholds)
- ✅ Null percentage monitoring (< 10% for key columns)
- ✅ Duplicate detection (< 5% threshold)
- ✅ Automatic email alerts on issues

**Manual Trigger:**
```bash
curl -X POST "https://us-central1-red-octane-444308-f4.cloudfunctions.net/data-quality-agent" \
  -H "Content-Type: application/json" \
  -d '{"action": "run_quality_checks"}'
```

### 2. AI Anomaly Detection Agent
**Purpose**: Machine learning-based pattern detection
**Location**: `anomaly_detection_agent.py`
**Endpoint**: `https://us-central1-red-octane-444308-f4.cloudfunctions.net/anomaly-detection-agent`

**Features:**
- 🤖 Isolation Forest ML models
- 📈 Volume anomaly detection (unusual record counts)
- ⏰ Timing anomaly detection (unexpected sync patterns)
- 🔍 Quality anomaly detection (data quality deviations)
- 📊 30-day historical training data
- 🚨 Severity-based alerting (HIGH/MEDIUM)

**Manual Trigger:**
```bash
curl -X POST "https://us-central1-red-octane-444308-f4.cloudfunctions.net/anomaly-detection-agent" \
  -H "Content-Type: application/json" \
  -d '{"action": "run_anomaly_detection"}'
```

### 3. Slack Integration Agent
**Purpose**: Real-time team notifications and interactive control
**Location**: `slack_integration_agent.py`
**Endpoint**: `https://us-central1-red-octane-444308-f4.cloudfunctions.net/slack-integration-agent`

**Features:**
- 📱 Rich Slack messages with buttons and formatting
- 📊 Daily status reports (11:00 AM CAT)
- 🚨 Real-time alert notifications
- 💬 Interactive slash commands (`/pipeline status`)
- ✅ Success notifications for completed syncs

**Slash Commands:**
- `/pipeline status` - Get current status
- `/pipeline health` - Detailed health check
- `/pipeline help` - Show available commands

### 4. GitHub Actions CI/CD Agent
**Purpose**: Automated deployment and testing
**Location**: `.github/workflows/deploy-pipeline.yml`
**Trigger**: Push to main branch or manual dispatch

**Features:**
- 🚀 Automatic deployment on code changes
- ✅ Validation testing of deployed functions
- 📧 Success/failure notifications
- 🔄 Integration with existing monitoring system

## 🔧 Configuration

### Environment Variables
All agents use these environment variables:
```bash
PROJECT_ID="red-octane-444308-f4"
REGION="us-central1"
SERVICE_ACCOUNT="karbon-bq-sync@red-octane-444308-f4.iam.gserviceaccount.com"
```

### Required Secrets (Google Secret Manager)
- `PIPELINE_MONITOR_EMAIL_USERNAME` ✅ (Already configured)
- `PIPELINE_MONITOR_EMAIL_PASSWORD` ✅ (Already configured)
- `SLACK_WEBHOOK_URL` (Optional - for Slack integration)
- `SLACK_BOT_TOKEN` (Optional - for Slack commands)

### Create Slack Secrets (Optional)
```bash
# Store Slack webhook URL
echo "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK" | \
  gcloud secrets create SLACK_WEBHOOK_URL --data-file=-

# Store Slack bot token  
echo "xoxb-your-slack-bot-token" | \
  gcloud secrets create SLACK_BOT_TOKEN --data-file=-
```

## 📋 Monitoring and Management

### View Agent Status
```bash
# Check Cloud Functions
gcloud functions list --filter="name:*agent*"

# Check Cloud Schedulers
gcloud scheduler jobs list --location=us-central1 --filter="name:*agent*"

# View recent logs
gcloud functions logs read data-quality-agent --limit=50
```

### Manual Testing
```bash
# Test Data Quality Agent
curl -X POST "https://us-central1-red-octane-444308-f4.cloudfunctions.net/data-quality-agent" \
  -H "Content-Type: application/json" \
  -d '{"action": "health_check"}'

# Test Anomaly Detection Agent
curl -X POST "https://us-central1-red-octane-444308-f4.cloudfunctions.net/anomaly-detection-agent" \
  -H "Content-Type: application/json" \
  -d '{"action": "health_check"}'

# Test Slack Integration Agent
curl -X POST "https://us-central1-red-octane-444308-f4.cloudfunctions.net/slack-integration-agent" \
  -H "Content-Type: application/json" \
  -d '{"action": "health_check"}'
```

### Update Agents
```bash
# Redeploy specific agent
gcloud functions deploy data-quality-agent \
  --source . \
  --runtime python311 \
  --trigger-http

# Update all agents
./deploy_background_agents.sh
```

## 🔄 Integration with Existing System

The background agents integrate seamlessly with your existing monitoring:

### Current System (Already Active) ✅
- **Pipeline Scheduler Monitor**: Every 4 hours
- **Email Notifications**: Daily reports + alerts
- **Auto-fix Capabilities**: Paused scheduler recovery

### New Background Agents 🆕
- **Data Quality Agent**: Every 6 hours
- **AI Anomaly Detection**: Every 8 hours  
- **Slack Integration**: Real-time + daily
- **GitHub Actions**: On code changes

### Combined Coverage
```mermaid
graph TB
    subgraph "Existing Monitoring ✅"
        A[Scheduler Monitor<br/>Every 4h]
        B[Email Alerts<br/>Real-time]
        C[Auto-fix<br/>Immediate]
    end
    
    subgraph "New Background Agents 🆕"
        D[Data Quality<br/>Every 6h]
        E[AI Anomaly<br/>Every 8h]
        F[Slack Bot<br/>Real-time]
        G[GitHub Actions<br/>On changes]
    end
    
    subgraph "Your Pipeline"
        H[Snowflake → BigQuery]
    end
    
    A --> H
    B --> H
    C --> H
    D --> H
    E --> H
    F --> H
    G --> H
```

## 📊 Expected Notifications

After setup, you'll receive:

### Email Notifications ✅ (Already Active)
- Daily status reports (11:00 AM CAT)
- Critical scheduler alerts
- Auto-fix confirmations

### New Notifications 🆕
- **Data Quality Issues**: When data freshness/quality problems detected
- **AI Anomaly Alerts**: When ML models detect unusual patterns
- **Slack Messages**: Daily reports + real-time alerts in Slack
- **GitHub Notifications**: Deployment success/failure

## 🚨 Troubleshooting

### Common Issues

#### 1. Agent Deployment Fails
```bash
# Check service account permissions
gcloud projects get-iam-policy red-octane-444308-f4 \
  --flatten="bindings[].members" \
  --filter="bindings.members:karbon-bq-sync@red-octane-444308-f4.iam.gserviceaccount.com"

# Redeploy with verbose logging
gcloud functions deploy data-quality-agent --verbosity=debug
```

#### 2. Slack Integration Not Working
```bash
# Verify secrets exist
gcloud secrets list --filter="name:SLACK_"

# Test webhook URL
curl -X POST "YOUR_SLACK_WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -d '{"text": "Test message"}'
```

#### 3. GitHub Actions Failing
- Verify `GCP_SA_KEY` secret is correctly set
- Check service account has necessary permissions
- Review workflow logs in GitHub Actions tab

#### 4. No Email Alerts
```bash
# Check existing email configuration
gcloud secrets versions access latest --secret="PIPELINE_MONITOR_EMAIL_PASSWORD"
```

### Getting Help

1. **Check Logs**: `gcloud functions logs read FUNCTION_NAME --limit=50`
2. **Review Documentation**: Each agent has detailed comments in its code
3. **Test Manually**: Use the curl commands above to test each agent
4. **Monitor Schedulers**: Check Cloud Scheduler console for job execution

## 🎉 Success Indicators

After successful setup, you should see:

✅ **Cloud Functions**: 3 new functions deployed  
✅ **Cloud Schedulers**: 3 new scheduled jobs running  
✅ **GitHub Actions**: Workflow file committed and ready  
✅ **Monitoring**: Enhanced coverage across all pipeline components  
✅ **Notifications**: Multi-channel alerting (email + Slack)  
✅ **Intelligence**: AI-powered anomaly detection active  

Your pipeline now has **comprehensive background agent coverage** providing:
- 🔄 **Automated deployments** (GitHub Actions)
- 📊 **Continuous data quality monitoring** (Every 6 hours)
- 🤖 **AI-powered anomaly detection** (Every 8 hours)
- 📱 **Real-time team notifications** (Slack integration)
- ✅ **Existing scheduler monitoring** (Every 4 hours)

The agents work together to provide **24/7 intelligent monitoring** of your Snowflake to BigQuery pipeline! 🚀
