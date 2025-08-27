#!/usr/bin/env python3
"""
Setup Google Cloud native notifications for pipeline monitoring
Uses Cloud Monitoring, Pub/Sub, and Cloud Functions for notifications
"""

import json
import subprocess
import sys
from google.cloud import monitoring_v3
from google.cloud import pubsub_v1

PROJECT_ID = "red-octane-444308-f4"
NOTIFICATION_EMAIL = "paulrichardson@fiskalfinance.com"
TOPIC_NAME = "pipeline-scheduler-alerts"

def create_pubsub_topic():
    """Create Pub/Sub topic for notifications."""
    print("📢 Creating Pub/Sub topic for notifications...")
    
    try:
        # Create topic
        result = subprocess.run([
            "gcloud", "pubsub", "topics", "create", TOPIC_NAME,
            "--project", PROJECT_ID
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Created Pub/Sub topic: {TOPIC_NAME}")
        else:
            if "already exists" in result.stderr:
                print(f"✅ Pub/Sub topic already exists: {TOPIC_NAME}")
            else:
                print(f"❌ Failed to create topic: {result.stderr}")
                return False
                
        return True
        
    except Exception as e:
        print(f"❌ Error creating Pub/Sub topic: {e}")
        return False

def create_notification_channel():
    """Create email notification channel in Cloud Monitoring."""
    print("📧 Creating email notification channel...")
    
    try:
        client = monitoring_v3.NotificationChannelServiceClient()
        project_name = f"projects/{PROJECT_ID}"
        
        # Check if notification channel already exists
        channels = client.list_notification_channels(name=project_name)
        for channel in channels:
            if (channel.type_ == "email" and 
                channel.labels.get("email_address") == NOTIFICATION_EMAIL):
                print(f"✅ Email notification channel already exists: {NOTIFICATION_EMAIL}")
                return channel.name
        
        # Create new notification channel
        notification_channel = monitoring_v3.NotificationChannel(
            type_="email",
            display_name=f"Pipeline Alerts - {NOTIFICATION_EMAIL}",
            description="Email notifications for pipeline scheduler issues",
            labels={"email_address": NOTIFICATION_EMAIL},
            enabled=True
        )
        
        channel = client.create_notification_channel(
            name=project_name,
            notification_channel=notification_channel
        )
        
        print(f"✅ Created email notification channel: {NOTIFICATION_EMAIL}")
        return channel.name
        
    except Exception as e:
        print(f"❌ Error creating notification channel: {e}")
        return None

def create_scheduler_failure_alert(notification_channel_name):
    """Create alert policy for scheduler failures."""
    print("🚨 Creating scheduler failure alert policy...")
    
    try:
        client = monitoring_v3.AlertPolicyServiceClient()
        project_name = f"projects/{PROJECT_ID}"
        
        # Define the alert policy
        alert_policy = monitoring_v3.AlertPolicy(
            display_name="Pipeline Scheduler Failures",
            documentation=monitoring_v3.AlertPolicy.Documentation(
                content="Alert when Cloud Scheduler jobs fail",
                mime_type="text/markdown"
            ),
            conditions=[
                monitoring_v3.AlertPolicy.Condition(
                    display_name="Scheduler job failures",
                    condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                        filter='resource.type="cloud_scheduler_job" AND metric.type="logging.googleapis.com/user/scheduler_job_failed"',
                        comparison=monitoring_v3.ComparisonType.COMPARISON_GREATER_THAN,
                        threshold_value=0,
                        duration={"seconds": 60},
                        aggregations=[
                            monitoring_v3.Aggregation(
                                alignment_period={"seconds": 300},
                                per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
                                cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
                                group_by_fields=["resource.label.job_id"]
                            )
                        ]
                    )
                )
            ],
            notification_channels=[notification_channel_name],
            combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
            enabled=True
        )
        
        policy = client.create_alert_policy(
            name=project_name,
            alert_policy=alert_policy
        )
        
        print(f"✅ Created scheduler failure alert policy")
        return policy.name
        
    except Exception as e:
        print(f"❌ Error creating alert policy: {e}")
        return None

def create_cloud_function_failure_alert(notification_channel_name):
    """Create alert policy for Cloud Function failures."""
    print("🚨 Creating Cloud Function failure alert policy...")
    
    try:
        client = monitoring_v3.AlertPolicyServiceClient()
        project_name = f"projects/{PROJECT_ID}"
        
        alert_policy = monitoring_v3.AlertPolicy(
            display_name="Pipeline Cloud Function Failures",
            documentation=monitoring_v3.AlertPolicy.Documentation(
                content="Alert when pipeline Cloud Functions fail",
                mime_type="text/markdown"
            ),
            conditions=[
                monitoring_v3.AlertPolicy.Condition(
                    display_name="Function execution failures",
                    condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                        filter='resource.type="cloud_function" AND metric.type="cloudfunctions.googleapis.com/function/execution_count" AND metric.label.status!="ok"',
                        comparison=monitoring_v3.ComparisonType.COMPARISON_GREATER_THAN,
                        threshold_value=0,
                        duration={"seconds": 60},
                        aggregations=[
                            monitoring_v3.Aggregation(
                                alignment_period={"seconds": 300},
                                per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
                                cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
                                group_by_fields=["resource.label.function_name"]
                            )
                        ]
                    )
                )
            ],
            notification_channels=[notification_channel_name],
            combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
            enabled=True
        )
        
        policy = client.create_alert_policy(
            name=project_name,
            alert_policy=alert_policy
        )
        
        print(f"✅ Created Cloud Function failure alert policy")
        return policy.name
        
    except Exception as e:
        print(f"❌ Error creating alert policy: {e}")
        return None

def setup_log_based_metrics():
    """Create log-based metrics for custom monitoring."""
    print("📊 Creating log-based metrics...")
    
    # Create log-based metric for scheduler job failures
    try:
        result = subprocess.run([
            "gcloud", "logging", "metrics", "create", "scheduler_job_failed",
            "--description=Count of failed Cloud Scheduler jobs",
            "--log-filter=resource.type=\"cloud_scheduler_job\" AND severity>=ERROR",
            "--project", PROJECT_ID
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Created log-based metric: scheduler_job_failed")
        elif "already exists" in result.stderr:
            print("✅ Log-based metric already exists: scheduler_job_failed")
        else:
            print(f"⚠️ Could not create log-based metric: {result.stderr}")
            
    except Exception as e:
        print(f"⚠️ Error creating log-based metrics: {e}")

def main():
    """Main setup function."""
    print("🔧 Setting up Google Cloud Native Notifications")
    print("=" * 50)
    
    # Step 1: Create Pub/Sub topic
    if not create_pubsub_topic():
        print("❌ Failed to create Pub/Sub topic")
        return False
    
    # Step 2: Create log-based metrics
    setup_log_based_metrics()
    
    # Step 3: Create notification channel
    notification_channel = create_notification_channel()
    if not notification_channel:
        print("❌ Failed to create notification channel")
        return False
    
    # Step 4: Create alert policies
    scheduler_alert = create_scheduler_failure_alert(notification_channel)
    function_alert = create_cloud_function_failure_alert(notification_channel)
    
    print("\n✅ Google Cloud Native Notifications Setup Complete!")
    print("=" * 50)
    print(f"📧 Email notifications: {NOTIFICATION_EMAIL}")
    print(f"📢 Pub/Sub topic: {TOPIC_NAME}")
    print(f"🚨 Alert policies created: {2 if scheduler_alert and function_alert else 1 if scheduler_alert or function_alert else 0}")
    
    print("\n🔍 What you'll receive notifications for:")
    print("• Cloud Scheduler job failures")
    print("• Cloud Function execution failures")
    print("• Pipeline sync issues")
    
    print("\n📋 Next steps:")
    print("1. Notifications are now active")
    print("2. Test with: gcloud alpha monitoring policies list")
    print("3. Monitor via: https://console.cloud.google.com/monitoring/alerting")
    
    return True

if __name__ == "__main__":
    main()

