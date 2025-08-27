#!/usr/bin/env python3
"""
AI-Powered Anomaly Detection Agent
Uses machine learning to detect unusual patterns in data pipeline behavior
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import logging
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetectionAgent:
    def __init__(self, project_id="red-octane-444308-f4"):
        self.project_id = project_id
        self.bq_client = bigquery.Client(project=project_id)
        
        # Initialize ML models
        self.models = {
            'volume_anomaly': IsolationForest(contamination=0.1, random_state=42),
            'timing_anomaly': IsolationForest(contamination=0.1, random_state=42),
            'quality_anomaly': IsolationForest(contamination=0.1, random_state=42)
        }
        
        self.scalers = {
            'volume': StandardScaler(),
            'timing': StandardScaler(),
            'quality': StandardScaler()
        }

    def collect_pipeline_metrics(self, days_back=30):
        """Collect historical pipeline metrics for training and anomaly detection"""
        
        # Query to get daily pipeline metrics
        query = f"""
        WITH daily_metrics AS (
            SELECT 
                DATE(sync_timestamp) as sync_date,
                'WORK_ITEM_BUDGET_VS_ACTUAL' as table_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
                COUNT(DISTINCT USER_NAME) as unique_users,
                AVG(BUDGETED_MINUTES) as avg_budgeted_minutes,
                STDDEV(BUDGETED_MINUTES) as stddev_budgeted_minutes,
                COUNTIF(BUDGETED_MINUTES IS NULL) * 100.0 / COUNT(*) as null_percentage,
                MIN(sync_timestamp) as first_sync,
                MAX(sync_timestamp) as last_sync,
                DATETIME_DIFF(MAX(sync_timestamp), MIN(sync_timestamp), MINUTE) as sync_duration_minutes
            FROM `{self.project_id}.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ`
            WHERE DATE(sync_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
            GROUP BY DATE(sync_timestamp)
            
            UNION ALL
            
            SELECT 
                DATE(sync_timestamp) as sync_date,
                'WORK_ITEM_DETAILS' as table_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
                COUNT(DISTINCT CLIENT_NAME) as unique_clients,
                0 as avg_budgeted_minutes,  -- Not applicable
                0 as stddev_budgeted_minutes,  -- Not applicable
                COUNTIF(WORK_TITLE IS NULL) * 100.0 / COUNT(*) as null_percentage,
                MIN(sync_timestamp) as first_sync,
                MAX(sync_timestamp) as last_sync,
                DATETIME_DIFF(MAX(sync_timestamp), MIN(sync_timestamp), MINUTE) as sync_duration_minutes
            FROM `{self.project_id}.karbon_data.WORK_ITEM_DETAILS_BQ`
            WHERE DATE(sync_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
            GROUP BY DATE(sync_timestamp)
        )
        SELECT * FROM daily_metrics
        ORDER BY sync_date DESC, table_name
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Collected {len(df)} daily metric records")
            return df
        except Exception as e:
            logger.error(f"Error collecting pipeline metrics: {e}")
            return pd.DataFrame()

    def prepare_features(self, df):
        """Prepare features for anomaly detection"""
        if df.empty:
            return {}, {}
        
        # Group by table for separate analysis
        features = {}
        
        for table_name in df['table_name'].unique():
            table_df = df[df['table_name'] == table_name].copy()
            
            # Volume features
            volume_features = table_df[['record_count', 'unique_work_items', 'unique_users']].fillna(0)
            
            # Timing features (extract hour from sync times)
            table_df['sync_hour'] = pd.to_datetime(table_df['first_sync']).dt.hour
            table_df['sync_day_of_week'] = pd.to_datetime(table_df['sync_date']).dt.dayofweek
            timing_features = table_df[['sync_duration_minutes', 'sync_hour', 'sync_day_of_week']].fillna(0)
            
            # Quality features
            quality_features = table_df[['null_percentage', 'avg_budgeted_minutes', 'stddev_budgeted_minutes']].fillna(0)
            
            features[table_name] = {
                'volume': volume_features,
                'timing': timing_features,
                'quality': quality_features,
                'dates': table_df['sync_date'].tolist()
            }
        
        return features, df

    def train_models(self, features):
        """Train anomaly detection models on historical data"""
        trained_models = {}
        
        for table_name, table_features in features.items():
            trained_models[table_name] = {}
            
            for feature_type in ['volume', 'timing', 'quality']:
                feature_data = table_features[feature_type]
                
                if len(feature_data) < 10:  # Need minimum data for training
                    logger.warning(f"Insufficient data for {table_name} {feature_type} model")
                    continue
                
                # Scale features
                scaler = StandardScaler()
                scaled_features = scaler.fit_transform(feature_data)
                
                # Train isolation forest
                model = IsolationForest(contamination=0.1, random_state=42)
                model.fit(scaled_features)
                
                trained_models[table_name][feature_type] = {
                    'model': model,
                    'scaler': scaler,
                    'feature_names': feature_data.columns.tolist()
                }
                
                logger.info(f"Trained {feature_type} anomaly model for {table_name}")
        
        return trained_models

    def detect_anomalies(self, current_metrics, trained_models):
        """Detect anomalies in current metrics using trained models"""
        anomalies = []
        
        for table_name, table_metrics in current_metrics.items():
            if table_name not in trained_models:
                continue
            
            table_models = trained_models[table_name]
            
            for feature_type in ['volume', 'timing', 'quality']:
                if feature_type not in table_models:
                    continue
                
                model_info = table_models[feature_type]
                model = model_info['model']
                scaler = model_info['scaler']
                feature_names = model_info['feature_names']
                
                # Prepare current data
                current_features = table_metrics[feature_type]
                
                if current_features.empty:
                    continue
                
                # Get the latest record
                latest_record = current_features.iloc[-1:][feature_names]
                
                try:
                    # Scale and predict
                    scaled_record = scaler.transform(latest_record)
                    anomaly_score = model.decision_function(scaled_record)[0]
                    is_anomaly = model.predict(scaled_record)[0] == -1
                    
                    if is_anomaly:
                        anomalies.append({
                            'table': table_name,
                            'feature_type': feature_type,
                            'anomaly_score': float(anomaly_score),
                            'values': latest_record.to_dict('records')[0],
                            'timestamp': datetime.now().isoformat(),
                            'severity': 'HIGH' if anomaly_score < -0.5 else 'MEDIUM'
                        })
                        
                        logger.warning(f"Anomaly detected in {table_name} {feature_type}: score={anomaly_score:.3f}")
                
                except Exception as e:
                    logger.error(f"Error detecting anomalies for {table_name} {feature_type}: {e}")
        
        return anomalies

    def get_current_metrics(self):
        """Get current day metrics for anomaly detection"""
        query = f"""
        SELECT 
            CURRENT_DATE() as sync_date,
            'WORK_ITEM_BUDGET_VS_ACTUAL' as table_name,
            COUNT(*) as record_count,
            COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
            COUNT(DISTINCT USER_NAME) as unique_users,
            AVG(BUDGETED_MINUTES) as avg_budgeted_minutes,
            STDDEV(BUDGETED_MINUTES) as stddev_budgeted_minutes,
            COUNTIF(BUDGETED_MINUTES IS NULL) * 100.0 / COUNT(*) as null_percentage,
            MIN(sync_timestamp) as first_sync,
            MAX(sync_timestamp) as last_sync,
            DATETIME_DIFF(MAX(sync_timestamp), MIN(sync_timestamp), MINUTE) as sync_duration_minutes,
            EXTRACT(HOUR FROM MIN(sync_timestamp)) as sync_hour,
            EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) - 1 as sync_day_of_week
        FROM `{self.project_id}.karbon_data.WORK_ITEM_BUDGET_VS_ACTUAL_BQ`
        WHERE DATE(sync_timestamp) = CURRENT_DATE()
        
        UNION ALL
        
        SELECT 
            CURRENT_DATE() as sync_date,
            'WORK_ITEM_DETAILS' as table_name,
            COUNT(*) as record_count,
            COUNT(DISTINCT WORK_ITEM_ID) as unique_work_items,
            COUNT(DISTINCT CLIENT_NAME) as unique_clients,
            0 as avg_budgeted_minutes,
            0 as stddev_budgeted_minutes,
            COUNTIF(WORK_TITLE IS NULL) * 100.0 / COUNT(*) as null_percentage,
            MIN(sync_timestamp) as first_sync,
            MAX(sync_timestamp) as last_sync,
            DATETIME_DIFF(MAX(sync_timestamp), MIN(sync_timestamp), MINUTE) as sync_duration_minutes,
            EXTRACT(HOUR FROM MIN(sync_timestamp)) as sync_hour,
            EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) - 1 as sync_day_of_week
        FROM `{self.project_id}.karbon_data.WORK_ITEM_DETAILS_BQ`
        WHERE DATE(sync_timestamp) = CURRENT_DATE()
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            features, _ = self.prepare_features(df)
            return features
        except Exception as e:
            logger.error(f"Error getting current metrics: {e}")
            return {}

    def send_anomaly_alert(self, anomalies):
        """Send email alert for detected anomalies"""
        if not anomalies:
            return
        
        subject = f"[Karbon Pipeline] 🤖 AI Anomaly Detection Alert - {len(anomalies)} Issues Found"
        
        html_content = f"""
        <html>
        <body>
        <h2>🤖 AI Anomaly Detection Alert</h2>
        <p><strong>Detection Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        <p><strong>Anomalies Detected:</strong> {len(anomalies)}</p>
        
        <h3>Anomaly Details:</h3>
        <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Table</th><th>Feature Type</th><th>Severity</th><th>Score</th><th>Values</th></tr>
        """
        
        for anomaly in anomalies:
            values_str = ", ".join([f"{k}: {v}" for k, v in anomaly['values'].items()])
            severity_color = "#ff4444" if anomaly['severity'] == 'HIGH' else "#ff8800"
            
            html_content += f"""
            <tr>
                <td>{anomaly['table']}</td>
                <td>{anomaly['feature_type']}</td>
                <td style="color: {severity_color}; font-weight: bold;">{anomaly['severity']}</td>
                <td>{anomaly['anomaly_score']:.3f}</td>
                <td style="font-size: 0.9em;">{values_str}</td>
            </tr>
            """
        
        html_content += """
        </table>
        
        <h3>What This Means:</h3>
        <ul>
        <li><strong>Volume Anomalies:</strong> Unusual record counts or data distribution</li>
        <li><strong>Timing Anomalies:</strong> Unexpected sync times or durations</li>
        <li><strong>Quality Anomalies:</strong> Unusual data quality patterns</li>
        </ul>
        
        <h3>Recommended Actions:</h3>
        <ul>
        <li>Review recent pipeline changes</li>
        <li>Check source system (Karbon/Snowflake) for issues</li>
        <li>Verify Cloud Function logs for errors</li>
        <li>Consider manual data validation</li>
        </ul>
        
        <p><em>This alert was generated by the AI Anomaly Detection Agent using machine learning models trained on 30 days of historical pipeline data.</em></p>
        </body>
        </html>
        """
        
        # Send email using existing infrastructure
        try:
            # This would integrate with your existing email system
            logger.info("Anomaly alert would be sent via email")
            # You can integrate with your existing email notification system here
        except Exception as e:
            logger.error(f"Failed to send anomaly alert: {e}")

    def run_detection_cycle(self):
        """Run a complete anomaly detection cycle"""
        try:
            logger.info("Starting anomaly detection cycle...")
            
            # Collect historical data
            historical_df = self.collect_pipeline_metrics(days_back=30)
            
            if historical_df.empty:
                logger.warning("No historical data available for training")
                return
            
            # Prepare features and train models
            features, _ = self.prepare_features(historical_df)
            trained_models = self.train_models(features)
            
            # Get current metrics
            current_metrics = self.get_current_metrics()
            
            if not current_metrics:
                logger.warning("No current metrics available")
                return
            
            # Detect anomalies
            anomalies = self.detect_anomalies(current_metrics, trained_models)
            
            # Log results
            if anomalies:
                logger.warning(f"Detected {len(anomalies)} anomalies")
                for anomaly in anomalies:
                    logger.warning(f"  - {anomaly['table']} {anomaly['feature_type']}: {anomaly['severity']} (score: {anomaly['anomaly_score']:.3f})")
                
                # Send alerts
                self.send_anomaly_alert(anomalies)
            else:
                logger.info("No anomalies detected - all systems appear normal")
            
            # Save results for analysis
            results = {
                'timestamp': datetime.now().isoformat(),
                'anomalies': anomalies,
                'models_trained': len(trained_models),
                'tables_analyzed': list(current_metrics.keys())
            }
            
            with open(f"/tmp/anomaly_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info("Anomaly detection cycle completed")
            return results
            
        except Exception as e:
            logger.error(f"Error in anomaly detection cycle: {e}")
            return None

def main():
    """Main function for running as background agent"""
    agent = AnomalyDetectionAgent()
    
    # Run detection every 8 hours
    import time
    while True:
        try:
            results = agent.run_detection_cycle()
            logger.info("Waiting 8 hours for next detection cycle...")
            time.sleep(8 * 60 * 60)  # 8 hours
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(60 * 60)  # Wait 1 hour on error

if __name__ == "__main__":
    main()
