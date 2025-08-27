#!/usr/bin/env python3
"""
Data Quality Monitoring Agent
Runs continuous data quality checks and alerts on anomalies
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import secretmanager
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityAgent:
    def __init__(self, project_id="red-octane-444308-f4"):
        self.project_id = project_id
        self.bq_client = bigquery.Client(project=project_id)
        self.secret_client = secretmanager.SecretManagerServiceClient()
        
        # Data quality thresholds
        self.quality_checks = {
            "WORK_ITEM_BUDGET_VS_ACTUAL_BQ": {
                "max_age_hours": 25,
                "min_row_count": 1000,
                "null_percentage_threshold": 10,
                "duplicate_threshold": 5
            },
            "WORK_ITEM_DETAILS_BQ": {
                "max_age_hours": 25,
                "min_row_count": 500,
                "null_percentage_threshold": 5,
                "duplicate_threshold": 2
            },
            "USER_TIME_ENTRY_BQ": {
                "max_age_hours": 25,
                "min_row_count": 2000,
                "null_percentage_threshold": 15,
                "duplicate_threshold": 10
            }
        }

    def check_data_freshness(self, table_name):
        """Check if data is fresh enough"""
        query = f"""
        SELECT 
            MAX(DATETIME(sync_timestamp)) as last_sync,
            DATETIME_DIFF(CURRENT_DATETIME(), MAX(DATETIME(sync_timestamp)), HOUR) as hours_since_sync
        FROM `{self.project_id}.karbon_data.{table_name}`
        WHERE sync_timestamp IS NOT NULL
        """
        
        try:
            result = list(self.bq_client.query(query))
            if result:
                row = result[0]
                hours_since_sync = row.hours_since_sync
                threshold = self.quality_checks[table_name]["max_age_hours"]
                
                return {
                    "table": table_name,
                    "last_sync": str(row.last_sync) if row.last_sync else "Never",
                    "hours_since_sync": hours_since_sync,
                    "threshold_hours": threshold,
                    "is_fresh": hours_since_sync < threshold if hours_since_sync else False,
                    "status": "✅ Fresh" if (hours_since_sync and hours_since_sync < threshold) else "🚨 Stale"
                }
        except Exception as e:
            logger.error(f"Error checking freshness for {table_name}: {e}")
            return {
                "table": table_name,
                "error": str(e),
                "status": "❌ Error"
            }

    def check_row_count(self, table_name):
        """Check if table has minimum expected rows"""
        query = f"""
        SELECT COUNT(*) as row_count
        FROM `{self.project_id}.karbon_data.{table_name}`
        """
        
        try:
            result = list(self.bq_client.query(query))
            if result:
                row_count = result[0].row_count
                min_expected = self.quality_checks[table_name]["min_row_count"]
                
                return {
                    "table": table_name,
                    "row_count": row_count,
                    "min_expected": min_expected,
                    "meets_threshold": row_count >= min_expected,
                    "status": "✅ Good" if row_count >= min_expected else "🚨 Low Count"
                }
        except Exception as e:
            logger.error(f"Error checking row count for {table_name}: {e}")
            return {
                "table": table_name,
                "error": str(e),
                "status": "❌ Error"
            }

    def check_null_percentage(self, table_name):
        """Check percentage of null values in key columns"""
        # Key columns to check for each table
        key_columns = {
            "WORK_ITEM_BUDGET_VS_ACTUAL_BQ": ["WORK_ITEM_ID", "USER_NAME", "BUDGETED_MINUTES"],
            "WORK_ITEM_DETAILS_BQ": ["WORK_ITEM_ID", "WORK_TITLE", "CLIENT_NAME"],
            "USER_TIME_ENTRY_BQ": ["USER_ID", "WORK_ITEM_ID", "TIME_IN_MINUTES"]
        }
        
        columns = key_columns.get(table_name, [])
        if not columns:
            return {"table": table_name, "status": "⚠️ No key columns defined"}
        
        null_checks = []
        for column in columns:
            query = f"""
            SELECT 
                '{column}' as column_name,
                COUNTIF({column} IS NULL) as null_count,
                COUNT(*) as total_count,
                ROUND(COUNTIF({column} IS NULL) * 100.0 / COUNT(*), 2) as null_percentage
            FROM `{self.project_id}.karbon_data.{table_name}`
            """
            
            try:
                result = list(self.bq_client.query(query))
                if result:
                    row = result[0]
                    threshold = self.quality_checks[table_name]["null_percentage_threshold"]
                    
                    null_checks.append({
                        "column": column,
                        "null_percentage": float(row.null_percentage),
                        "threshold": threshold,
                        "acceptable": float(row.null_percentage) <= threshold
                    })
            except Exception as e:
                logger.error(f"Error checking nulls for {table_name}.{column}: {e}")
                null_checks.append({
                    "column": column,
                    "error": str(e)
                })
        
        overall_status = all(check.get("acceptable", False) for check in null_checks if "error" not in check)
        
        return {
            "table": table_name,
            "null_checks": null_checks,
            "status": "✅ Good" if overall_status else "🚨 High Nulls"
        }

    def check_duplicates(self, table_name):
        """Check for duplicate records"""
        # Define primary key columns for each table
        primary_keys = {
            "WORK_ITEM_BUDGET_VS_ACTUAL_BQ": ["WORK_ITEM_ID", "USER_NAME", "sync_reporting_date"],
            "WORK_ITEM_DETAILS_BQ": ["WORK_ITEM_ID"],
            "USER_TIME_ENTRY_BQ": ["USER_ID", "WORK_ITEM_ID", "TIME_ENTRY_DATE"]
        }
        
        pk_columns = primary_keys.get(table_name, [])
        if not pk_columns:
            return {"table": table_name, "status": "⚠️ No primary key defined"}
        
        pk_list = ", ".join(pk_columns)
        query = f"""
        WITH duplicates AS (
            SELECT {pk_list}, COUNT(*) as duplicate_count
            FROM `{self.project_id}.karbon_data.{table_name}`
            GROUP BY {pk_list}
            HAVING COUNT(*) > 1
        )
        SELECT 
            COUNT(*) as duplicate_groups,
            SUM(duplicate_count - 1) as extra_records
        FROM duplicates
        """
        
        try:
            result = list(self.bq_client.query(query))
            if result:
                row = result[0]
                threshold = self.quality_checks[table_name]["duplicate_threshold"]
                duplicate_percentage = (row.extra_records or 0) * 100.0 / max(1, self.check_row_count(table_name).get("row_count", 1))
                
                return {
                    "table": table_name,
                    "duplicate_groups": row.duplicate_groups or 0,
                    "extra_records": row.extra_records or 0,
                    "duplicate_percentage": round(duplicate_percentage, 2),
                    "threshold": threshold,
                    "acceptable": duplicate_percentage <= threshold,
                    "status": "✅ Good" if duplicate_percentage <= threshold else "🚨 High Duplicates"
                }
        except Exception as e:
            logger.error(f"Error checking duplicates for {table_name}: {e}")
            return {
                "table": table_name,
                "error": str(e),
                "status": "❌ Error"
            }

    def run_all_checks(self):
        """Run all data quality checks"""
        results = {
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }
        
        for table_name in self.quality_checks.keys():
            logger.info(f"Checking data quality for {table_name}...")
            
            table_results = {
                "freshness": self.check_data_freshness(table_name),
                "row_count": self.check_row_count(table_name),
                "null_percentage": self.check_null_percentage(table_name),
                "duplicates": self.check_duplicates(table_name)
            }
            
            # Determine overall table status
            statuses = [check["status"] for check in table_results.values()]
            if any("❌" in status or "🚨" in status for status in statuses):
                overall_status = "🚨 Issues Found"
            elif any("⚠️" in status for status in statuses):
                overall_status = "⚠️ Warnings"
            else:
                overall_status = "✅ Healthy"
            
            table_results["overall_status"] = overall_status
            results["tables"][table_name] = table_results
        
        return results

    def send_alert_email(self, results):
        """Send email alert for data quality issues"""
        # Check if any issues found
        has_issues = any(
            table_data["overall_status"] in ["🚨 Issues Found", "⚠️ Warnings"]
            for table_data in results["tables"].values()
        )
        
        if not has_issues:
            logger.info("No data quality issues found, skipping email alert")
            return
        
        # Prepare email content
        subject = "[Karbon Pipeline] 🚨 Data Quality Issues Detected"
        
        html_content = f"""
        <html>
        <body>
        <h2>🚨 Data Quality Alert</h2>
        <p><strong>Timestamp:</strong> {results['timestamp']}</p>
        
        <h3>Table Status Summary:</h3>
        <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Table</th><th>Status</th><th>Issues</th></tr>
        """
        
        for table_name, table_data in results["tables"].items():
            status = table_data["overall_status"]
            issues = []
            
            for check_type, check_data in table_data.items():
                if check_type == "overall_status":
                    continue
                if "🚨" in check_data.get("status", "") or "❌" in check_data.get("status", ""):
                    issues.append(f"{check_type}: {check_data['status']}")
            
            issues_text = "<br>".join(issues) if issues else "None"
            html_content += f"<tr><td>{table_name}</td><td>{status}</td><td>{issues_text}</td></tr>"
        
        html_content += """
        </table>
        
        <h3>Recommended Actions:</h3>
        <ul>
        <li>Check pipeline scheduler status</li>
        <li>Verify Snowflake connectivity</li>
        <li>Review recent Cloud Function logs</li>
        <li>Consider running manual sync if needed</li>
        </ul>
        
        <p>This is an automated alert from the Karbon Data Quality Agent.</p>
        </body>
        </html>
        """
        
        # Send email (reuse existing email infrastructure)
        try:
            # Get email credentials from Secret Manager
            email_username = self.get_secret("PIPELINE_MONITOR_EMAIL_USERNAME")
            email_password = self.get_secret("PIPELINE_MONITOR_EMAIL_PASSWORD")
            
            if email_username and email_password:
                self.send_email(
                    subject=subject,
                    html_content=html_content,
                    recipient="paulrichardson@fiskalfinance.com",
                    username=email_username,
                    password=email_password
                )
                logger.info("Data quality alert email sent successfully")
            else:
                logger.error("Email credentials not found in Secret Manager")
                
        except Exception as e:
            logger.error(f"Failed to send data quality alert email: {e}")

    def get_secret(self, secret_id):
        """Get secret from Google Secret Manager"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"
            response = self.secret_client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Error accessing secret {secret_id}: {e}")
            return None

    def send_email(self, subject, html_content, recipient, username, password):
        """Send email using SMTP"""
        msg = MimeMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = username
        msg["To"] = recipient
        
        html_part = MimeText(html_content, "html")
        msg.attach(html_part)
        
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)

def main():
    """Main function for running as background agent"""
    agent = DataQualityAgent()
    
    while True:
        try:
            logger.info("Starting data quality check cycle...")
            results = agent.run_all_checks()
            
            # Log results
            for table_name, table_data in results["tables"].items():
                status = table_data["overall_status"]
                logger.info(f"{table_name}: {status}")
            
            # Send alerts if needed
            agent.send_alert_email(results)
            
            # Save results to file for debugging
            with open(f"/tmp/data_quality_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info("Data quality check cycle completed")
            
        except Exception as e:
            logger.error(f"Error in data quality check cycle: {e}")
        
        # Wait 6 hours before next check
        logger.info("Waiting 6 hours for next check cycle...")
        time.sleep(6 * 60 * 60)  # 6 hours

if __name__ == "__main__":
    main()
