"""
Monitoring and Observability Module
===================================

This module provides comprehensive monitoring, logging, and alerting capabilities:
- Structured logging with context
- Metrics collection and reporting
- Alert notifications via Slack/email
- Performance tracking
- Error aggregation
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import requests
from google.cloud import logging as cloud_logging
from google.cloud import monitoring_v3
from google.cloud import error_reporting
import threading
from collections import defaultdict

# Initialize Google Cloud clients
try:
    cloud_logging_client = cloud_logging.Client()
    cloud_logging_client.setup_logging()
    error_client = error_reporting.Client()
    monitoring_client = monitoring_v3.MetricServiceClient()
except Exception as e:
    print(f"Warning: Could not initialize Google Cloud monitoring clients: {e}")
    cloud_logging_client = None
    error_client = None
    monitoring_client = None


@dataclass
class PipelineMetrics:
    """Data class for pipeline metrics"""
    pipeline_name: str
    start_time: float
    end_time: float
    status: str
    rows_processed: int
    duration_seconds: float
    batches_processed: int
    errors_count: int
    retry_count: int


class StructuredLogger:
    """Structured logger with context and Google Cloud integration"""
    
    def __init__(self, name: str):
        """Initialize the structured logger"""
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Add console handler with structured format
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Context for structured logging
        self.context = {}
    
    def set_context(self, **kwargs):
        """Set context variables for all subsequent logs"""
        self.context.update(kwargs)
    
    def clear_context(self):
        """Clear the context"""
        self.context = {}
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Internal method to log with structure"""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": level,
            "logger": self.name,
            "message": message,
            "context": self.context,
            **kwargs
        }
        
        # Log to console
        getattr(self.logger, level.lower())(json.dumps(log_entry))
        
        # Log to Google Cloud Logging if available
        if cloud_logging_client:
            try:
                cloud_logger = cloud_logging_client.logger(self.name)
                cloud_logger.log_struct(log_entry, severity=level)
            except Exception as e:
                self.logger.error(f"Failed to log to Cloud Logging: {e}")
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self._log_structured("DEBUG", message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self._log_structured("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self._log_structured("WARNING", message, **kwargs)
    
    def error(self, message: str, exception: Exception = None, **kwargs):
        """Log error message with optional exception"""
        if exception:
            kwargs["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": None  # Could add traceback if needed
            }
            
            # Report to Google Cloud Error Reporting
            if error_client:
                try:
                    error_client.report_exception()
                except Exception as e:
                    self.logger.error(f"Failed to report error: {e}")
        
        self._log_structured("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self._log_structured("CRITICAL", message, **kwargs)


class MetricsCollector:
    """Collects and reports pipeline metrics"""
    
    def __init__(self):
        """Initialize the metrics collector"""
        self.metrics = defaultdict(list)
        self.active_pipelines = {}
        self.lock = threading.Lock()
        
        # Metric aggregation intervals
        self.aggregation_interval = 60  # seconds
        self.last_aggregation = time.time()
    
    def start_pipeline_run(self, pipeline_name: str):
        """Record the start of a pipeline run"""
        with self.lock:
            self.active_pipelines[pipeline_name] = {
                "start_time": time.time(),
                "batches_processed": 0,
                "rows_processed": 0,
                "errors_count": 0,
                "retry_count": 0
            }
    
    def record_batch_processed(self, pipeline_name: str, rows: int):
        """Record a processed batch"""
        with self.lock:
            if pipeline_name in self.active_pipelines:
                self.active_pipelines[pipeline_name]["batches_processed"] += 1
                self.active_pipelines[pipeline_name]["rows_processed"] += rows
    
    def record_error(self, pipeline_name: str):
        """Record an error occurrence"""
        with self.lock:
            if pipeline_name in self.active_pipelines:
                self.active_pipelines[pipeline_name]["errors_count"] += 1
    
    def record_retry(self, pipeline_name: str):
        """Record a retry attempt"""
        with self.lock:
            if pipeline_name in self.active_pipelines:
                self.active_pipelines[pipeline_name]["retry_count"] += 1
    
    def end_pipeline_run(self, pipeline_name: str, status: str, total_rows: int, duration: float):
        """Record the end of a pipeline run"""
        with self.lock:
            if pipeline_name in self.active_pipelines:
                pipeline_data = self.active_pipelines.pop(pipeline_name)
                
                metric = PipelineMetrics(
                    pipeline_name=pipeline_name,
                    start_time=pipeline_data["start_time"],
                    end_time=time.time(),
                    status=status,
                    rows_processed=total_rows,
                    duration_seconds=duration,
                    batches_processed=pipeline_data["batches_processed"],
                    errors_count=pipeline_data["errors_count"],
                    retry_count=pipeline_data["retry_count"]
                )
                
                self.metrics[pipeline_name].append(metric)
                
                # Send to monitoring system
                self._send_metrics_to_monitoring(metric)
                
                # Check if aggregation is needed
                if time.time() - self.last_aggregation > self.aggregation_interval:
                    self._aggregate_metrics()
    
    def _send_metrics_to_monitoring(self, metric: PipelineMetrics):
        """Send metrics to Google Cloud Monitoring"""
        if not monitoring_client:
            return
        
        try:
            project_name = f"projects/{os.getenv('GOOGLE_CLOUD_PROJECT', 'red-octane-444308-f4')}"
            
            # Create time series data
            series = monitoring_v3.TimeSeries()
            series.metric.type = f"custom.googleapis.com/pipeline/{metric.pipeline_name}/duration"
            series.resource.type = "global"
            
            now = time.time()
            seconds = int(now)
            nanos = int((now - seconds) * 10 ** 9)
            interval = monitoring_v3.TimeInterval(
                {"end_time": {"seconds": seconds, "nanos": nanos}}
            )
            point = monitoring_v3.Point(
                {"interval": interval, "value": {"double_value": metric.duration_seconds}}
            )
            series.points = [point]
            
            # Write the time series data
            monitoring_client.create_time_series(
                name=project_name,
                time_series=[series]
            )
        except Exception as e:
            logging.error(f"Failed to send metrics to monitoring: {e}")
    
    def _aggregate_metrics(self):
        """Aggregate and report metrics"""
        with self.lock:
            for pipeline_name, metrics_list in self.metrics.items():
                if metrics_list:
                    # Calculate aggregates
                    total_runs = len(metrics_list)
                    successful_runs = sum(1 for m in metrics_list if m.status == "success")
                    failed_runs = sum(1 for m in metrics_list if m.status == "error")
                    avg_duration = sum(m.duration_seconds for m in metrics_list) / total_runs
                    total_rows = sum(m.rows_processed for m in metrics_list)
                    
                    # Log aggregate metrics
                    logger = StructuredLogger("metrics_aggregator")
                    logger.info(
                        f"Pipeline metrics for {pipeline_name}",
                        total_runs=total_runs,
                        successful_runs=successful_runs,
                        failed_runs=failed_runs,
                        avg_duration_seconds=round(avg_duration, 2),
                        total_rows_processed=total_rows
                    )
            
            # Clear old metrics
            self.metrics.clear()
            self.last_aggregation = time.time()
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get a summary of all metrics"""
        with self.lock:
            summary = {}
            
            for pipeline_name, metrics_list in self.metrics.items():
                if metrics_list:
                    summary[pipeline_name] = {
                        "total_runs": len(metrics_list),
                        "successful_runs": sum(1 for m in metrics_list if m.status == "success"),
                        "failed_runs": sum(1 for m in metrics_list if m.status == "error"),
                        "total_rows_processed": sum(m.rows_processed for m in metrics_list),
                        "avg_duration_seconds": sum(m.duration_seconds for m in metrics_list) / len(metrics_list)
                    }
            
            return summary


class AlertManager:
    """Manages alerts and notifications"""
    
    def __init__(self):
        """Initialize the alert manager"""
        self.logger = StructuredLogger("alert_manager")
        from ..core.credentials_manager import credentials_manager
        self.credentials_manager = credentials_manager
    
    def send_alert(self, severity: str, title: str, message: str, details: Dict[str, Any] = None):
        """
        Send an alert notification
        
        Args:
            severity: Alert severity (info, warning, error, critical)
            title: Alert title
            message: Alert message
            details: Additional details
        """
        alert_data = {
            "severity": severity,
            "title": title,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "details": details or {}
        }
        
        # Log the alert
        self.logger.warning(f"Alert: {title}", alert=alert_data)
        
        # Send to Slack if configured
        try:
            self._send_slack_alert(alert_data)
        except Exception as e:
            self.logger.error(f"Failed to send Slack alert: {e}")
    
    def _send_slack_alert(self, alert_data: Dict[str, Any]):
        """Send alert to Slack"""
        try:
            webhook_url = self.credentials_manager.get_secret("SLACK_WEBHOOK_URL")
        except:
            # Slack webhook not configured
            return
        
        # Format message for Slack
        color_map = {
            "info": "#36a64f",
            "warning": "#ff9900",
            "error": "#ff0000",
            "critical": "#990000"
        }
        
        slack_message = {
            "attachments": [{
                "color": color_map.get(alert_data["severity"], "#808080"),
                "title": alert_data["title"],
                "text": alert_data["message"],
                "fields": [
                    {
                        "title": key.replace("_", " ").title(),
                        "value": str(value),
                        "short": True
                    }
                    for key, value in alert_data["details"].items()
                ],
                "footer": "Pipeline Monitor",
                "ts": int(time.time())
            }]
        }
        
        response = requests.post(webhook_url, json=slack_message)
        response.raise_for_status()
    
    def send_pipeline_failure_alert(self, pipeline_name: str, error: Exception, context: Dict[str, Any] = None):
        """Send a pipeline failure alert"""
        self.send_alert(
            severity="error",
            title=f"Pipeline Failed: {pipeline_name}",
            message=str(error),
            details={
                "pipeline": pipeline_name,
                "error_type": type(error).__name__,
                **(context or {})
            }
        )
    
    def send_validation_warning(self, pipeline_name: str, validation_results: Dict[str, Any]):
        """Send a data validation warning"""
        if validation_results.get("status") == "warning":
            self.send_alert(
                severity="warning",
                title=f"Data Validation Warning: {pipeline_name}",
                message=validation_results.get("message", "Validation threshold exceeded"),
                details=validation_results
            )


# Global instances
metrics_collector = MetricsCollector()
alert_manager = AlertManager()