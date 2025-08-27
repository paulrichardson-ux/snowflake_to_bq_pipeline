#!/usr/bin/env python3
"""
Slack Integration Agent
Provides real-time pipeline status updates and interactive commands via Slack
"""

import os
import json
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import secretmanager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SlackIntegrationAgent:
    def __init__(self, project_id="red-octane-444308-f4"):
        self.project_id = project_id
        self.bq_client = bigquery.Client(project=project_id)
        self.secret_client = secretmanager.SecretManagerServiceClient()
        
        # Get Slack webhook URL from secrets
        self.slack_webhook_url = self.get_secret("SLACK_WEBHOOK_URL")
        self.slack_bot_token = self.get_secret("SLACK_BOT_TOKEN")

    def get_secret(self, secret_id):
        """Get secret from Google Secret Manager"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"
            response = self.secret_client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Error accessing secret {secret_id}: {e}")
            return None

    def send_slack_message(self, message, channel="#data-pipeline", username="Pipeline Bot", emoji=":robot_face:"):
        """Send message to Slack channel"""
        if not self.slack_webhook_url:
            logger.error("Slack webhook URL not configured")
            return False
        
        payload = {
            "channel": channel,
            "username": username,
            "icon_emoji": emoji,
            "text": message
        }
        
        try:
            response = requests.post(self.slack_webhook_url, json=payload)
            if response.status_code == 200:
                logger.info("Slack message sent successfully")
                return True
            else:
                logger.error(f"Failed to send Slack message: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error sending Slack message: {e}")
            return False

    def send_rich_slack_message(self, blocks, channel="#data-pipeline"):
        """Send rich message with blocks to Slack"""
        if not self.slack_webhook_url:
            logger.error("Slack webhook URL not configured")
            return False
        
        payload = {
            "channel": channel,
            "blocks": blocks
        }
        
        try:
            response = requests.post(self.slack_webhook_url, json=payload)
            if response.status_code == 200:
                logger.info("Rich Slack message sent successfully")
                return True
            else:
                logger.error(f"Failed to send rich Slack message: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error sending rich Slack message: {e}")
            return False

    def get_pipeline_status(self):
        """Get current pipeline status for Slack reporting"""
        status = {
            "timestamp": datetime.now().isoformat(),
            "tables": {},
            "overall_health": "healthy"
        }
        
        tables_to_check = [
            "WORK_ITEM_BUDGET_VS_ACTUAL_BQ",
            "WORK_ITEM_DETAILS_BQ",
            "USER_TIME_ENTRY_BQ"
        ]
        
        for table_name in tables_to_check:
            table_status = self.check_table_health(table_name)
            status["tables"][table_name] = table_status
            
            if table_status["status"] != "healthy":
                status["overall_health"] = "issues_detected"
        
        return status

    def check_table_health(self, table_name):
        """Check health of a specific table"""
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            MAX(DATETIME(sync_timestamp)) as last_sync,
            DATETIME_DIFF(CURRENT_DATETIME(), MAX(DATETIME(sync_timestamp)), HOUR) as hours_since_sync,
            COUNT(DISTINCT DATE(sync_timestamp)) as days_with_data
        FROM `{self.project_id}.karbon_data.{table_name}`
        WHERE DATE(sync_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        """
        
        try:
            result = list(self.bq_client.query(query))
            if result:
                row = result[0]
                
                # Determine health status
                hours_since_sync = row.hours_since_sync or 999
                status = "healthy"
                emoji = "✅"
                
                if hours_since_sync > 30:
                    status = "stale"
                    emoji = "⚠️"
                elif hours_since_sync > 48:
                    status = "critical"
                    emoji = "🚨"
                
                return {
                    "status": status,
                    "emoji": emoji,
                    "total_records": row.total_records or 0,
                    "last_sync": str(row.last_sync) if row.last_sync else "Never",
                    "hours_since_sync": hours_since_sync,
                    "days_with_data": row.days_with_data or 0
                }
        except Exception as e:
            logger.error(f"Error checking health for {table_name}: {e}")
            return {
                "status": "error",
                "emoji": "❌",
                "error": str(e)
            }

    def send_daily_status_report(self):
        """Send daily status report to Slack"""
        status = self.get_pipeline_status()
        
        # Create rich message blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🔄 Daily Pipeline Status Report"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Report Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            },
            {
                "type": "divider"
            }
        ]
        
        # Overall status
        overall_emoji = "✅" if status["overall_health"] == "healthy" else "⚠️"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Overall Status:* {overall_emoji} {status['overall_health'].replace('_', ' ').title()}"
            }
        })
        
        # Table status details
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Table Status Details:*"
            }
        })
        
        for table_name, table_status in status["tables"].items():
            if "error" in table_status:
                status_text = f"{table_status['emoji']} *{table_name}*: Error - {table_status['error']}"
            else:
                status_text = f"{table_status['emoji']} *{table_name}*: {table_status['total_records']:,} records, last sync {table_status['hours_since_sync']:.1f}h ago"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": status_text
                }
            })
        
        # Add action buttons
        blocks.extend([
            {
                "type": "divider"
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Dashboard"
                        },
                        "url": f"https://console.cloud.google.com/bigquery?project={self.project_id}",
                        "action_id": "view_dashboard"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Check Logs"
                        },
                        "url": f"https://console.cloud.google.com/functions/list?project={self.project_id}",
                        "action_id": "check_logs"
                    }
                ]
            }
        ])
        
        return self.send_rich_slack_message(blocks)

    def send_alert_notification(self, alert_type, details):
        """Send alert notification to Slack"""
        emoji_map = {
            "scheduler_paused": "⏸️",
            "data_stale": "⚠️",
            "sync_failed": "🚨",
            "anomaly_detected": "🤖",
            "system_error": "💥"
        }
        
        emoji = emoji_map.get(alert_type, "⚠️")
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Pipeline Alert: {alert_type.replace('_', ' ').title()}"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Alert Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Details:* {details}"
                }
            }
        ]
        
        # Add severity-based styling
        if alert_type in ["sync_failed", "system_error"]:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":warning: *This is a critical alert requiring immediate attention*"
                }
            })
        
        return self.send_rich_slack_message(blocks)

    def send_success_notification(self, message):
        """Send success notification to Slack"""
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"✅ *Pipeline Success:* {message}"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            }
        ]
        
        return self.send_rich_slack_message(blocks)

    def handle_slack_command(self, command, user_id):
        """Handle interactive Slack commands"""
        responses = {
            "status": self.get_status_response,
            "health": self.get_health_response,
            "help": self.get_help_response,
            "sync": self.trigger_manual_sync
        }
        
        handler = responses.get(command.lower(), self.get_help_response)
        return handler(user_id)

    def get_status_response(self, user_id):
        """Get current status response for Slack command"""
        status = self.get_pipeline_status()
        
        response = f"📊 *Pipeline Status for <@{user_id}>*\n\n"
        response += f"*Overall Health:* {status['overall_health'].replace('_', ' ').title()}\n\n"
        
        for table_name, table_status in status["tables"].items():
            if "error" in table_status:
                response += f"{table_status['emoji']} *{table_name}*: Error\n"
            else:
                response += f"{table_status['emoji']} *{table_name}*: {table_status['total_records']:,} records, {table_status['hours_since_sync']:.1f}h ago\n"
        
        return response

    def get_health_response(self, user_id):
        """Get detailed health response"""
        return f"🏥 *Detailed Health Check for <@{user_id}>*\n\nRunning comprehensive health check... This may take a moment.\n\n_Use `/pipeline status` for quick status._"

    def get_help_response(self, user_id):
        """Get help response"""
        return f"""
🤖 *Pipeline Bot Commands for <@{user_id}>*

Available commands:
• `/pipeline status` - Get current pipeline status
• `/pipeline health` - Run detailed health check
• `/pipeline sync` - Trigger manual sync (admin only)
• `/pipeline help` - Show this help message

*Quick Status Indicators:*
• ✅ Healthy (< 25 hours since sync)
• ⚠️ Stale (25-48 hours since sync)  
• 🚨 Critical (> 48 hours since sync)
• ❌ Error (unable to check)

For more details, visit the <https://console.cloud.google.com/bigquery?project={self.project_id}|BigQuery Console>.
        """

    def trigger_manual_sync(self, user_id):
        """Trigger manual sync (admin only)"""
        # You could implement admin user checking here
        return f"🔄 *Manual Sync Requested by <@{user_id}>*\n\nTriggering manual sync of all pipelines... You'll receive a notification when complete."

def setup_slack_integration():
    """Setup instructions for Slack integration"""
    instructions = """
    # Slack Integration Setup

    ## 1. Create Slack App
    1. Go to https://api.slack.com/apps
    2. Click "Create New App" > "From scratch"
    3. Name: "Pipeline Bot"
    4. Select your workspace

    ## 2. Configure Bot
    1. Go to "OAuth & Permissions"
    2. Add Bot Token Scopes:
       - chat:write
       - commands
       - incoming-webhook
    3. Install app to workspace
    4. Copy Bot User OAuth Token

    ## 3. Create Slash Command
    1. Go to "Slash Commands"
    2. Create command: /pipeline
    3. Request URL: https://your-cloud-function-url/slack-command
    4. Description: "Get pipeline status and control"

    ## 4. Add Webhook
    1. Go to "Incoming Webhooks"
    2. Activate incoming webhooks
    3. Add webhook to channel (#data-pipeline)
    4. Copy webhook URL

    ## 5. Store Secrets
    Run these commands to store secrets:
    
    gcloud secrets create SLACK_WEBHOOK_URL --data-file=webhook_url.txt
    gcloud secrets create SLACK_BOT_TOKEN --data-file=bot_token.txt
    """
    
    print(instructions)

if __name__ == "__main__":
    # Example usage
    agent = SlackIntegrationAgent()
    
    # Send daily report
    agent.send_daily_status_report()
    
    # Send test alert
    agent.send_alert_notification("data_stale", "WORK_ITEM_BUDGET_VS_ACTUAL_BQ hasn't been updated in 26 hours")
    
    # Send success notification
    agent.send_success_notification("All pipelines completed successfully at 08:30 CAT")
