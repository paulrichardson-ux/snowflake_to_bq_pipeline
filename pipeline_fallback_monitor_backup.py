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
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import functions_v1

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "red-octane-444308-f4")
DATASET_ID = os.getenv("BQ_DATASET", "karbon_data")
REGION = os.getenv("FUNCTION_REGION", "us-central1")

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
    }
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
        
        # Process results and take action
        actions_taken = []
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
            "critical_issues": len(critical_issues),
            "warnings": len(warnings),
            "actions_taken": actions_taken,
            "details": freshness_results
        }
        
        # Send summary alert
        if critical_issues:
            send_alert(f"Pipeline monitoring completed - {len(critical_issues)} critical issues, {len(actions_taken)} actions taken", "CRITICAL")
        elif warnings:
            send_alert(f"Pipeline monitoring completed - {len(warnings)} warnings detected", "WARNING")
        else:
            send_alert("Pipeline monitoring completed - all systems healthy", "INFO")
        
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