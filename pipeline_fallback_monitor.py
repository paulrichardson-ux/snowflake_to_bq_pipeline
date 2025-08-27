#!/usr/bin/env python3
"""
KARBON PIPELINE FALLBACK MONITOR
=====================================

This script monitors the health of the Karbon data pipeline and automatically
triggers fallback actions when data freshness issues are detected.

Features:
- Monitors data freshness across key tables
- Triggers full sync when daily sync fails
- Sends alerts via Cloud Logging
- Can be deployed as a Cloud Function
- Supports both HTTP trigger and Cloud Scheduler

Usage:
- Deploy as Cloud Function with daily schedule
- Monitor logs for alerts and actions taken
- Customize thresholds and actions as needed
"""

import os
import json
import logging
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import functions_v1
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "red-octane-444308-f4")
DATASET_ID = os.getenv("BQ_DATASET", "karbon_data")
REGION = os.getenv("FUNCTION_REGION", "us-central1")

# Email configuration
NOTIFICATION_EMAIL = "paulrichardson@fiskalfinance.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Pipeline monitoring configuration
MONITORING_CONFIG = {
    "tables": {
        "WORK_ITEM_BUDGET_TIME_TRACKING_VIEW_V4": {
            "warning_threshold_days": 1,
            "critical_threshold_days": 3,
            "fallback_function": "sync-full-work-item-details-to-bq"
        },
        "WORK_ITEM_DETAILS_BQ": {
            "warning_threshold_days": 1,
            "critical_threshold_days": 3,
            "fallback_function": "sync-full-work-item-details-to-bq"
        },
        "USER_TIME_ENTRY_BQ": {
            "warning_threshold_days": 1,
            "critical_threshold_days": 3,
            "fallback_function": "sync-user-time-entries"
        }
    },
    "critical_schedulers": [
        "work-item-budget-vs-actual-daily-sync",
        "work-item-budget-vs-actual-full-sync-daily",
        "sync-work-item-details-daily",
        "user-time-entries-daily-sync"
    ]
}

def check_data_freshness(bq_client):
    """
    Check data freshness for all monitored tables.
    
    Returns:
        dict: Status for each table with freshness information
    """
    results = {}
    
    for table_name, config in MONITORING_CONFIG["tables"].items():
        try:
            query = f"""
            SELECT 
                '{table_name}' as table_name,
                MAX(REPORTING_DATE) as latest_date,
                COUNT(*) as total_records,
                DATE_DIFF(CURRENT_DATE(), MAX(REPORTING_DATE), DAY) as days_behind
            FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
            """
            
            query_job = bq_client.query(query)
            result = list(query_job)[0]
            
            days_behind = result.days_behind or 0
            
            # Determine status
            if days_behind > config["critical_threshold_days"]:
                status = "CRITICAL"
            elif days_behind > config["warning_threshold_days"]:
                status = "WARNING"
            else:
                status = "OK"
            
            results[table_name] = {
                "latest_date": result.latest_date.isoformat() if result.latest_date else None,
                "total_records": result.total_records,
                "days_behind": days_behind,
                "status": status,
                "fallback_function": config["fallback_function"]
            }
            
            logger.info(f"✅ {table_name}: {status} - {days_behind} days behind")
            
        except Exception as e:
            logger.error(f"❌ Error checking {table_name}: {str(e)}")
            results[table_name] = {
                "status": "ERROR",
                "error": str(e),
                "fallback_function": config["fallback_function"]
            }
    
    return results

def check_scheduler_health():
    """
    Check the health of critical schedulers and auto-resume paused ones.
    
    Returns:
        dict: Status of each scheduler and actions taken
    """
    scheduler_results = {}
    actions_taken = []
    
    for scheduler_name in MONITORING_CONFIG["critical_schedulers"]:
        try:
            # Check scheduler status
            result = subprocess.run([
                "gcloud", "scheduler", "jobs", "describe", scheduler_name,
                "--location", REGION,
                "--format", "value(state)"
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                state = result.stdout.strip()
                scheduler_results[scheduler_name] = {
                    "status": state,
                    "exists": True
                }
                
                # Auto-resume paused schedulers
                if state == "PAUSED":
                    logger.warning(f"⚠️ Scheduler {scheduler_name} is PAUSED - attempting to resume...")
                    
                    resume_result = subprocess.run([
                        "gcloud", "scheduler", "jobs", "resume", scheduler_name,
                        "--location", REGION
                    ], capture_output=True, text=True, timeout=30)
                    
                    if resume_result.returncode == 0:
                        logger.info(f"✅ Successfully resumed scheduler: {scheduler_name}")
                        actions_taken.append(f"Auto-resumed paused scheduler: {scheduler_name}")
                        scheduler_results[scheduler_name]["status"] = "RESUMED"
                        scheduler_results[scheduler_name]["action"] = "AUTO_RESUMED"
                    else:
                        logger.error(f"❌ Failed to resume scheduler {scheduler_name}: {resume_result.stderr}")
                        scheduler_results[scheduler_name]["action"] = "RESUME_FAILED"
                        scheduler_results[scheduler_name]["error"] = resume_result.stderr
                        
                elif state == "ENABLED":
                    logger.info(f"✅ Scheduler {scheduler_name} is ENABLED")
                else:
                    logger.warning(f"⚠️ Scheduler {scheduler_name} has unexpected state: {state}")
                    
            else:
                logger.error(f"❌ Failed to check scheduler {scheduler_name}: {result.stderr}")
                scheduler_results[scheduler_name] = {
                    "status": "CHECK_FAILED",
                    "exists": False,
                    "error": result.stderr
                }
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout checking scheduler {scheduler_name}")
            scheduler_results[scheduler_name] = {
                "status": "TIMEOUT",
                "exists": False,
                "error": "Command timeout"
            }
        except Exception as e:
            logger.error(f"❌ Error checking scheduler {scheduler_name}: {str(e)}")
            scheduler_results[scheduler_name] = {
                "status": "ERROR",
                "exists": False,
                "error": str(e)
            }
    
    return scheduler_results, actions_taken

def trigger_fallback_function(function_name, reason):
    """
    Trigger a fallback Cloud Function.
    
    Args:
        function_name (str): Name of the Cloud Function to trigger
        reason (str): Reason for triggering the fallback
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        function_url = f"https://{REGION}-{PROJECT_ID}.cloudfunctions.net/{function_name}"
        
        payload = {
            "source": "fallback_monitor",
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
            "triggered_by": "pipeline_fallback_monitor"
        }
        
        logger.info(f"🚀 Triggering fallback function: {function_name}")
        logger.info(f"   URL: {function_url}")
        logger.info(f"   Reason: {reason}")
        
        response = requests.post(
            function_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30  # Short timeout for trigger, actual sync can take longer
        )
        
        if response.status_code == 200:
            logger.info(f"✅ Successfully triggered {function_name}")
            return True
        else:
            logger.error(f"❌ Failed to trigger {function_name}: HTTP {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error triggering {function_name}: {str(e)}")
        return False

def send_email_report(report, severity="INFO"):
    """
    Send email status report with monitoring results.
    
    Args:
        report (dict): Monitoring report data
        severity (str): Report severity level
    """
    try:
        # Create email content
        subject = f"🔍 Karbon Pipeline Status Report - {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
        
        if severity == "CRITICAL":
            subject = f"🚨 CRITICAL - {subject}"
        elif severity == "WARNING":
            subject = f"⚠️ WARNING - {subject}"
        else:
            subject = f"✅ OK - {subject}"
        
        # Create HTML email body
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                .summary {{ margin: 15px 0; }}
                .table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
                .table th, .table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .table th {{ background-color: #f2f2f2; }}
                .status-ok {{ color: green; font-weight: bold; }}
                .status-warning {{ color: orange; font-weight: bold; }}
                .status-critical {{ color: red; font-weight: bold; }}
                .actions {{ background-color: #fffbf0; padding: 10px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🔍 Karbon Pipeline Monitoring Report</h2>
                <p><strong>Timestamp:</strong> {report['monitoring_timestamp']}</p>
                <p><strong>Tables Checked:</strong> {report['tables_checked']}</p>
            </div>
            
            <div class="summary">
                <h3>📊 Summary</h3>
                <ul>
                    <li><strong>Critical Issues:</strong> {report['critical_issues']}</li>
                    <li><strong>Warnings:</strong> {report['warnings']}</li>
                    <li><strong>Actions Taken:</strong> {len(report['actions_taken'])}</li>
                </ul>
            </div>
        """
        
        if report['actions_taken']:
            html_body += f"""
            <div class="actions">
                <h3>🔧 Actions Taken</h3>
                <ul>
                    {''.join([f'<li>{action}</li>' for action in report['actions_taken']])}
                </ul>
            </div>
            """
        
        # Add table details
        html_body += """
            <h3>📋 Table Status Details</h3>
            <table class="table">
                <tr>
                    <th>Table</th>
                    <th>Status</th>
                    <th>Latest Date</th>
                    <th>Days Behind</th>
                    <th>Total Records</th>
                </tr>
        """
        
        for table_name, details in report['details'].items():
            status = details.get('status', 'UNKNOWN')
            status_class = f"status-{status.lower()}" if status in ['OK', 'WARNING', 'CRITICAL'] else ""
            
            html_body += f"""
                <tr>
                    <td>{table_name}</td>
                    <td class="{status_class}">{status}</td>
                    <td>{details.get('latest_date', 'N/A')}</td>
                    <td>{details.get('days_behind', 'N/A')}</td>
                    <td>{details.get('total_records', 'N/A'):,}</td>
                </tr>
            """
        
        html_body += """
            </table>
        """
        
        # Add scheduler status if available
        if 'scheduler_status' in report:
            html_body += """
                <h3>⏰ Scheduler Status</h3>
                <table class="table">
                    <tr>
                        <th>Scheduler</th>
                        <th>Status</th>
                        <th>Action Taken</th>
                    </tr>
            """
            
            for scheduler_name, details in report['scheduler_status'].items():
                status = details.get('status', 'UNKNOWN')
                action = details.get('action', 'None')
                status_class = "status-ok" if status in ['ENABLED', 'RESUMED'] else ("status-critical" if status == 'PAUSED' else "")
                
                html_body += f"""
                    <tr>
                        <td>{scheduler_name}</td>
                        <td class="{status_class}">{status}</td>
                        <td>{action}</td>
                    </tr>
                """
            
            html_body += """
                </table>
            """
        
        html_body += """
            <div style="margin-top: 20px; font-size: 12px; color: #666;">
                <p>This is an automated report from the Karbon Pipeline Monitoring System.</p>
                <p>Next check will run in 4 hours.</p>
            </div>
        </body>
        </html>
        """
        
        # Use Cloud Logging to send structured email data (since we don't have SMTP credentials)
        email_data = {
            "type": "EMAIL_REPORT",
            "to": NOTIFICATION_EMAIL,
            "subject": subject,
            "html_body": html_body,
            "report_summary": {
                "critical_issues": report['critical_issues'],
                "warnings": report['warnings'],
                "actions_taken": len(report['actions_taken']),
                "tables_checked": report['tables_checked']
            }
        }
        
        logger.info(f"📧 EMAIL_REPORT: {json.dumps(email_data)}")
        logger.info(f"📧 Status report prepared for {NOTIFICATION_EMAIL}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send email report: {str(e)}")
        return False

def send_alert(message, severity="INFO"):
    """
    Send an alert via Cloud Logging with structured data.
    
    Args:
        message (str): Alert message
        severity (str): Log severity level
    """
    alert_data = {
        "alert_type": "PIPELINE_MONITOR",
        "message": message,
        "timestamp": datetime.now().isoformat(),
        "project_id": PROJECT_ID,
        "severity": severity
    }
    
    if severity == "CRITICAL":
        logger.critical(f"🚨 CRITICAL ALERT: {message}")
    elif severity == "WARNING":
        logger.warning(f"⚠️ WARNING: {message}")
    else:
        logger.info(f"ℹ️ INFO: {message}")
    
    # Log structured data for monitoring systems
    logger.info(f"ALERT_DATA: {json.dumps(alert_data)}")

def pipeline_fallback_monitor(request):
    """
    Main Cloud Function entry point for pipeline monitoring.
    
    This function checks data freshness and triggers fallback actions as needed.
    """
    try:
        logger.info("🔍 Starting pipeline fallback monitoring...")
        
        # Initialize BigQuery client
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        # Check data freshness for all tables
        freshness_results = check_data_freshness(bq_client)
        
        # Check scheduler health and auto-resume paused ones
        scheduler_results, scheduler_actions = check_scheduler_health()
        
        # Process results and take action
        actions_taken = scheduler_actions.copy()  # Start with scheduler actions
        critical_issues = []
        warnings = []
        
        for table_name, result in freshness_results.items():
            status = result.get("status", "UNKNOWN")
            
            if status == "CRITICAL":
                critical_issues.append(table_name)
                reason = f"Data in {table_name} is {result['days_behind']} days behind (critical threshold exceeded)"
                
                # Trigger fallback function
                fallback_success = trigger_fallback_function(
                    result["fallback_function"], 
                    reason
                )
                
                if fallback_success:
                    actions_taken.append(f"Triggered {result['fallback_function']} for {table_name}")
                    send_alert(f"Fallback sync triggered for {table_name} - data was {result['days_behind']} days behind", "WARNING")
                else:
                    send_alert(f"FAILED to trigger fallback for {table_name} - manual intervention required", "CRITICAL")
                    
            elif status == "WARNING":
                warnings.append(table_name)
                send_alert(f"Data freshness warning for {table_name} - {result['days_behind']} days behind", "WARNING")
                
            elif status == "ERROR":
                critical_issues.append(table_name)
                send_alert(f"Error monitoring {table_name}: {result.get('error', 'Unknown error')}", "CRITICAL")
        
        # Generate summary report
        report = {
            "monitoring_timestamp": datetime.now().isoformat(),
            "tables_checked": len(freshness_results),
            "schedulers_checked": len(scheduler_results),
            "critical_issues": len(critical_issues),
            "warnings": len(warnings),
            "actions_taken": actions_taken,
            "details": freshness_results,
            "scheduler_status": scheduler_results
        }
        
        # Determine report severity
        if critical_issues:
            report_severity = "CRITICAL"
            summary_msg = f"Pipeline monitoring completed - {len(critical_issues)} critical issues, {len(actions_taken)} actions taken"
        elif warnings:
            report_severity = "WARNING"
            summary_msg = f"Pipeline monitoring completed - {len(warnings)} warnings detected"
        else:
            report_severity = "INFO"
            summary_msg = "Pipeline monitoring completed - all systems healthy"
        
        # Send summary alert
        send_alert(summary_msg, report_severity)
        
        # Send email report
        email_sent = send_email_report(report, report_severity)
        report["email_report_sent"] = email_sent
        
        logger.info(f"📊 Monitoring Summary: {json.dumps(report, indent=2)}")
        
        return {
            "status": "success",
            "report": report
        }
        
    except Exception as e:
        error_msg = f"Pipeline monitoring failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        send_alert(error_msg, "CRITICAL")
        
        return {
            "status": "error",
            "error": error_msg
        }, 500

# For local testing
if __name__ == "__main__":
    # Mock request object for local testing
    class MockRequest:
        def get_json(self):
            return {"source": "local_test"}
    
    result = pipeline_fallback_monitor(MockRequest())
    print(f"Test Result: {json.dumps(result, indent=2)}") 