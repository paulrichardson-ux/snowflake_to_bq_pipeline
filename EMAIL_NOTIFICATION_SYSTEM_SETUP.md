# Email Notification System Setup

**Date**: August 14, 2025  
**Purpose**: Enhanced pipeline monitoring with daily email status reports  
**Recipient**: paulrichardson@fiskalfinance.com  
**Schedule**: Daily at 09:00 UTC (11:00 CAT)

## Overview

The pipeline monitoring system has been enhanced to send automated email notifications for:
- ✅ **Daily Status Reports** - Sent every day at 11:00 AM CAT
- 🚨 **Critical Alerts** - Sent immediately when issues are detected
- 🔧 **Auto-Fix Notifications** - Sent when schedulers are automatically resumed

## Schedule Changes Made

### 1. Budget vs Actual Sync ✅
**Changed from**: 02:00 UTC (04:00 CAT)  
**Changed to**: 06:30 UTC (08:30 CAT)

```bash
# Updated schedule
gcloud scheduler jobs update http work-item-budget-vs-actual-daily-sync \
  --location=us-central1 \
  --schedule="30 6 * * *" \
  --time-zone="UTC"
```

### 2. Monitoring Schedule
- **Health Checks**: Every 4 hours with auto-fix
- **Daily Reports**: 09:00 UTC (11:00 CAT)

## Email Notification Types

### 1. Daily Status Report (Healthy) ✅
**Subject**: `[Karbon Pipeline Monitor] Daily Status - All Systems Healthy`  
**Frequency**: Daily at 11:00 AM CAT  
**Content**:
- Status of all critical schedulers
- Next check time
- Confirmation that all systems are running

### 2. Critical Alert (Issues Found) 🚨
**Subject**: `[Karbon Pipeline Monitor] 🚨 URGENT: Pipeline Scheduler Issues Detected`  
**Frequency**: Immediate when issues detected  
**Content**:
- HTML formatted alert with color coding
- Detailed breakdown of issues (paused, missing, stale schedulers)
- Recommended actions
- Auto-fix status

### 3. System Failure Alert 💥
**Subject**: `[Karbon Pipeline Monitor] 🚨 CRITICAL: Monitor System Failure`  
**Frequency**: When monitor itself fails  
**Content**:
- Critical system failure notification
- Request for immediate manual intervention

## Setup Instructions

### Step 1: Configure Email Credentials
```bash
# Run the interactive setup script
./setup_email_notifications.sh
```

**Manual Setup Alternative**:
1. Create Gmail App Password:
   - Go to [Google Account Settings](https://myaccount.google.com/)
   - Security > 2-Step Verification > App passwords
   - Generate password for "Mail"

2. Create secrets:
```bash
echo 'your-email@gmail.com' | gcloud secrets create PIPELINE_MONITOR_EMAIL_USERNAME --data-file=-
echo 'your-16-char-app-password' | gcloud secrets create PIPELINE_MONITOR_EMAIL_PASSWORD --data-file=-
```

3. Grant service account access:
```bash
gcloud secrets add-iam-policy-binding PIPELINE_MONITOR_EMAIL_USERNAME \
  --member='serviceAccount:karbon-bq-sync@red-octane-444308-f4.iam.gserviceaccount.com' \
  --role='roles/secretmanager.secretAccessor'

gcloud secrets add-iam-policy-binding PIPELINE_MONITOR_EMAIL_PASSWORD \
  --member='serviceAccount:karbon-bq-sync@red-octane-444308-f4.iam.gserviceaccount.com' \
  --role='roles/secretmanager.secretAccessor'
```

### Step 2: Deploy Enhanced Monitor
```bash
# Deploy the enhanced monitoring system
./deploy_scheduler_monitor.sh
```

### Step 3: Test Email Notifications
```bash
# Test daily report email
python3 pipeline_scheduler_monitor.py --daily-report

# Test health check with auto-fix
python3 pipeline_scheduler_monitor.py --auto-fix
```

## Deployed Components

### 1. Enhanced Monitor Script ✅
**File**: `pipeline_scheduler_monitor.py`
**Features**:
- Email notification support
- HTML formatted alerts
- Daily status reports
- Auto-fix capability
- Comprehensive health checks

### 2. Cloud Function ✅
**Name**: `pipeline-scheduler-monitor`
**URL**: `https://us-central1-red-octane-444308-f4.cloudfunctions.net/pipeline-scheduler-monitor`
**Features**:
- HTTP trigger support
- Environment variable configuration
- JSON request parsing
- Email credential access

### 3. Cloud Schedulers ✅

#### Health Check Monitor
- **Name**: `pipeline-scheduler-monitor`
- **Schedule**: Every 4 hours
- **Features**: Auto-fix enabled, immediate alerts

#### Daily Status Report
- **Name**: `pipeline-daily-status-report`
- **Schedule**: 09:00 UTC (11:00 CAT)
- **Features**: Daily email reports, comprehensive status

## Email Configuration

### SMTP Settings
```python
EMAIL_CONFIG = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": "karbon-pipeline-monitor@red-octane-444308-f4.iam.gserviceaccount.com",
    "recipient_email": "paulrichardson@fiskalfinance.com",
    "subject_prefix": "[Karbon Pipeline Monitor]"
}
```

### Security
- ✅ Credentials stored in Google Secret Manager
- ✅ Service account access controls
- ✅ TLS encryption for SMTP
- ✅ App-specific password authentication

## Monitoring Coverage

### Critical Schedulers Monitored
1. `work-item-budget-vs-actual-daily-sync` ⭐ (Now at 08:30 CAT)
2. `sync-work-item-details-daily`
3. `time-details-daily-sync`
4. `client-dimension-daily-sync`
5. `user-dimension-daily-sync`
6. `tenant-team-dimension-daily-sync`
7. `tenant-team-member-dimension-daily-sync`
8. `client-group-dimension-daily-sync`

### Health Checks
- ✅ **Paused Schedulers**: Auto-resume capability
- ✅ **Missing Schedulers**: Detection and alerting
- ✅ **Stale Schedulers**: 25+ hour threshold detection
- ✅ **Failed Schedulers**: Error state detection

## Manual Commands

### Test Email Functionality
```bash
# Test daily status report
python3 pipeline_scheduler_monitor.py --daily-report

# Test health check without email
python3 pipeline_scheduler_monitor.py --no-email

# Test with auto-fix
python3 pipeline_scheduler_monitor.py --auto-fix
```

### Trigger via HTTP
```bash
# Health check with auto-fix
curl -X POST "https://us-central1-red-octane-444308-f4.cloudfunctions.net/pipeline-scheduler-monitor" \
  -H "Content-Type: application/json" \
  -d '{"auto_fix": true}'

# Daily status report
curl -X POST "https://us-central1-red-octane-444308-f4.cloudfunctions.net/pipeline-scheduler-monitor" \
  -H "Content-Type: application/json" \
  -d '{"daily_report": true}'
```

### Check Scheduler Status
```bash
# List all schedulers
gcloud scheduler jobs list --location=us-central1

# Check specific scheduler
gcloud scheduler jobs describe work-item-budget-vs-actual-daily-sync --location=us-central1
```

## Expected Email Schedule

### Daily Emails (11:00 AM CAT)
- **Monday-Sunday**: Daily status report
- **Content**: Health status of all schedulers
- **Action Required**: None (unless issues reported)

### Alert Emails (As Needed)
- **Trigger**: Scheduler issues detected
- **Response Time**: Within 4 hours (next health check)
- **Auto-Fix**: Paused schedulers automatically resumed
- **Action Required**: Review and address any remaining issues

## Troubleshooting

### Email Not Received
1. Check spam/junk folder
2. Verify Gmail app password is correct
3. Check Secret Manager credentials
4. Review Cloud Function logs

### Monitor Not Running
1. Check Cloud Scheduler status
2. Verify Cloud Function deployment
3. Check service account permissions
4. Review function execution logs

### Auto-Fix Not Working
1. Verify service account has scheduler admin permissions
2. Check Cloud Function timeout settings
3. Review error logs for specific failures

## Files Created/Modified

### New Files ✅
- `setup_email_notifications.sh` - Interactive email setup
- `EMAIL_NOTIFICATION_SYSTEM_SETUP.md` - This documentation

### Enhanced Files ✅
- `pipeline_scheduler_monitor.py` - Added email notifications
- `deploy_scheduler_monitor.sh` - Added daily report scheduler

### Configuration Changes ✅
- Updated `work-item-budget-vs-actual-daily-sync` schedule to 08:30 CAT
- Created email credential secrets in Secret Manager
- Deployed enhanced monitoring Cloud Function

## Status: READY FOR DEPLOYMENT ✅

The enhanced email notification system is ready for deployment:
- ✅ **Email notifications configured**
- ✅ **Daily reports scheduled for 11:00 AM CAT**
- ✅ **Budget sync moved to 08:30 CAT**
- ✅ **Auto-fix capability enabled**
- ✅ **Comprehensive monitoring coverage**

**Next Steps**:
1. Run `./setup_email_notifications.sh` to configure email credentials
2. Run `./deploy_scheduler_monitor.sh` to deploy the enhanced system
3. Test with `python3 pipeline_scheduler_monitor.py --daily-report`
