"""
Base Pipeline Class with Common Functionality
=============================================

This module provides the base class for all pipeline implementations with:
- Automatic schema detection and mapping
- Efficient batch processing
- Error handling and retry logic
- Monitoring and logging
- Data validation
"""

import os
import json
import uuid
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Any, Optional, Tuple
from abc import ABC, abstractmethod
import yaml
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import numpy as np

from .credentials_manager import connection_pool
from .monitoring import MetricsCollector, StructuredLogger

# Configure logging
logger = StructuredLogger(__name__)
metrics = MetricsCollector()


class PipelineBase(ABC):
    """Base class for all pipeline implementations"""
    
    # Snowflake to BigQuery type mapping
    SF_TO_BQ_TYPE_MAP = {
        "VARCHAR": "STRING",
        "CHAR": "STRING",
        "TEXT": "STRING",
        "STRING": "STRING",
        "NUMBER": "NUMERIC",
        "DECIMAL": "NUMERIC",
        "INT": "INT64",
        "INTEGER": "INT64",
        "BIGINT": "INT64",
        "SMALLINT": "INT64",
        "FLOAT": "FLOAT64",
        "DOUBLE": "FLOAT64",
        "BOOLEAN": "BOOL",
        "DATE": "DATE",
        "TIMESTAMP_NTZ": "TIMESTAMP",
        "TIMESTAMP_LTZ": "TIMESTAMP",
        "TIMESTAMP_TZ": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "DATETIME": "DATETIME",
        "TIME": "TIME",
        "VARIANT": "JSON",
        "OBJECT": "JSON",
        "ARRAY": "JSON",
    }
    
    def __init__(self, pipeline_name: str, config_path: str = None):
        """
        Initialize the pipeline
        
        Args:
            pipeline_name: Name of the pipeline (must match config key)
            config_path: Path to configuration file
        """
        self.pipeline_name = pipeline_name
        self.config = self._load_config(config_path)
        self.pipeline_config = self.config['pipelines'][pipeline_name]
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client()
        self.project_id = self.config['project']['gcp_project_id']
        self.dataset_id = self.config['project']['bigquery_dataset']
        
        # Set up table references
        self.source_table = self.pipeline_config['source_table']
        self.target_table_id = f"{self.project_id}.{self.dataset_id}.{self.pipeline_config['target_table']}"
        
        # Performance settings
        self.batch_size = self.pipeline_config.get('batch_size', 5000)
        self.max_parallel_batches = self.config['performance'].get('max_parallel_batches', 5)
        
        # Initialize metrics
        self.start_time = None
        self.total_rows_processed = 0
        self.errors_encountered = []
    
    def _load_config(self, config_path: str = None) -> Dict:
        """Load configuration from YAML file"""
        if not config_path:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config',
                'pipeline_config.yaml'
            )
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_snowflake_schema(self) -> List[Tuple[str, str]]:
        """Get schema information from Snowflake"""
        with connection_pool.get_connection() as conn:
            cursor = conn.cursor()
            try:
                query = f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{self.source_table.upper()}'
                      AND table_schema = '{self.config["snowflake"]["schema"].upper()}'
                    ORDER BY ordinal_position
                """
                cursor.execute(query)
                return cursor.fetchall()
            finally:
                cursor.close()
    
    def create_bigquery_schema(self, sf_columns: List[Tuple[str, str]]) -> List[bigquery.SchemaField]:
        """Convert Snowflake schema to BigQuery schema"""
        bq_schema = []
        
        for col_name, data_type in sf_columns:
            # Extract base type (remove precision/scale)
            base_type = data_type.split("(")[0].upper()
            bq_type = self.SF_TO_BQ_TYPE_MAP.get(base_type, "STRING")
            
            # All fields nullable by default for flexibility
            bq_schema.append(bigquery.SchemaField(col_name, bq_type, mode="NULLABLE"))
        
        return bq_schema
    
    def ensure_target_table_exists(self) -> bigquery.Table:
        """Ensure the target BigQuery table exists with proper schema"""
        try:
            table = self.bq_client.get_table(self.target_table_id)
            logger.info(f"Target table {self.target_table_id} exists")
            return table
        except NotFound:
            logger.info(f"Target table {self.target_table_id} not found, creating...")
            
            # Get schema from Snowflake
            sf_columns = self.get_snowflake_schema()
            bq_schema = self.create_bigquery_schema(sf_columns)
            
            # Create table
            table = bigquery.Table(self.target_table_id, schema=bq_schema)
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {self.target_table_id}")
            
            return table
    
    def fetch_batch(self, offset: int, limit: int) -> Tuple[List[str], List[tuple]]:
        """
        Fetch a batch of data from Snowflake
        
        Args:
            offset: Starting offset
            limit: Number of rows to fetch
            
        Returns:
            Tuple of (column_names, rows)
        """
        with connection_pool.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Build query based on sync type
                if self.pipeline_config['sync_type'] == 'incremental':
                    query = self._build_incremental_query(offset, limit)
                else:
                    query = self._build_full_query(offset, limit)
                
                logger.info(f"Fetching batch: offset={offset}, limit={limit}")
                cursor.execute(query)
                
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                return columns, rows
            finally:
                cursor.close()
    
    def _build_full_query(self, offset: int, limit: int) -> str:
        """Build query for full sync"""
        primary_key = self.pipeline_config.get('primary_key', '*')
        
        return f"""
            SELECT *
            FROM {self.config['snowflake']['schema']}.{self.source_table}
            ORDER BY {primary_key}
            LIMIT {limit} OFFSET {offset}
        """
    
    def _build_incremental_query(self, offset: int, limit: int) -> str:
        """Build query for incremental sync"""
        incremental_column = self.pipeline_config.get('incremental_column', 'LAST_MODIFIED_TIME')
        lookback_days = self.pipeline_config.get('lookback_days', 7)
        cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        primary_key = self.pipeline_config.get('primary_key', incremental_column)
        
        return f"""
            SELECT *
            FROM {self.config['snowflake']['schema']}.{self.source_table}
            WHERE {incremental_column} >= '{cutoff_date}'
            ORDER BY {primary_key}
            LIMIT {limit} OFFSET {offset}
        """
    
    def convert_row_to_dict(self, columns: List[str], row: tuple) -> Dict[str, Any]:
        """Convert a Snowflake row to a dictionary suitable for BigQuery"""
        row_dict = {}
        
        for idx, col_name in enumerate(columns):
            value = row[idx]
            
            # Handle data type conversions
            if value is None:
                row_dict[col_name] = None
            elif isinstance(value, (datetime, pd.Timestamp)):
                row_dict[col_name] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[col_name] = str(value)
            elif isinstance(value, (dict, list)):
                row_dict[col_name] = json.dumps(value)
            else:
                row_dict[col_name] = value
        
        return row_dict
    
    def load_batch_to_bigquery(self, rows: List[Dict[str, Any]], table_id: str) -> int:
        """
        Load a batch of rows to BigQuery
        
        Args:
            rows: List of row dictionaries
            table_id: Target table ID
            
        Returns:
            Number of rows loaded
        """
        if not rows:
            return 0
        
        errors = self.bq_client.insert_rows_json(table_id, rows)
        
        if errors:
            logger.error(f"Errors loading batch to {table_id}: {errors[:5]}")  # Log first 5 errors
            self.errors_encountered.extend(errors[:5])
            raise RuntimeError(f"Failed to load batch: {len(errors)} errors")
        
        logger.info(f"Loaded {len(rows)} rows to {table_id}")
        return len(rows)
    
    def validate_sync(self) -> Dict[str, Any]:
        """Validate the sync by comparing row counts and sample data"""
        validation_results = {
            "status": "success",
            "source_count": 0,
            "target_count": 0,
            "difference": 0,
            "difference_percent": 0,
            "sample_validation": True
        }
        
        try:
            # Get source count
            with connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {self.config['snowflake']['schema']}.{self.source_table}")
                validation_results["source_count"] = cursor.fetchone()[0]
                cursor.close()
            
            # Get target count
            query = f"SELECT COUNT(*) as count FROM `{self.target_table_id}`"
            result = self.bq_client.query(query).result()
            validation_results["target_count"] = list(result)[0].count
            
            # Calculate difference
            diff = abs(validation_results["source_count"] - validation_results["target_count"])
            validation_results["difference"] = diff
            
            if validation_results["source_count"] > 0:
                diff_percent = (diff / validation_results["source_count"]) * 100
                validation_results["difference_percent"] = round(diff_percent, 2)
                
                # Check if difference exceeds threshold
                threshold = self.config['data_quality'].get('validation_threshold_percent', 5)
                if diff_percent > threshold:
                    validation_results["status"] = "warning"
                    validation_results["message"] = f"Row count difference ({diff_percent}%) exceeds threshold ({threshold}%)"
            
            logger.info(f"Validation results: {validation_results}")
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            validation_results["status"] = "error"
            validation_results["message"] = str(e)
        
        return validation_results
    
    def run_full_sync(self) -> Dict[str, Any]:
        """Run a full synchronization"""
        self.start_time = time.time()
        metrics.start_pipeline_run(self.pipeline_name)
        
        try:
            logger.info(f"Starting full sync for {self.pipeline_name}")
            
            # Ensure target table exists
            target_table = self.ensure_target_table_exists()
            
            # Create temporary table for atomic replacement
            temp_table_name = f"temp_{self.pipeline_config['target_table']}_{uuid.uuid4().hex[:8]}"
            temp_table_id = f"{self.project_id}.{self.dataset_id}.{temp_table_name}"
            
            # Create temp table with same schema
            temp_table = bigquery.Table(temp_table_id, schema=target_table.schema)
            temp_table.expires = datetime.now(timezone.utc) + timedelta(
                hours=self.config['performance']['temp_table_expiration_hours']
            )
            self.bq_client.create_table(temp_table)
            logger.info(f"Created temporary table {temp_table_id}")
            
            # Process data in batches
            offset = 0
            total_rows = 0
            
            while True:
                # Fetch batch from Snowflake
                columns, rows = self.fetch_batch(offset, self.batch_size)
                
                if not rows:
                    break
                
                # Convert to BigQuery format
                bq_rows = [self.convert_row_to_dict(columns, row) for row in rows]
                
                # Load to temp table
                loaded = self.load_batch_to_bigquery(bq_rows, temp_table_id)
                total_rows += loaded
                
                offset += len(rows)
                metrics.record_batch_processed(self.pipeline_name, len(rows))
                
                logger.info(f"Progress: {total_rows} rows processed")
            
            self.total_rows_processed = total_rows
            
            # Replace target table with temp table (atomic operation)
            if total_rows > 0:
                self._replace_table(temp_table_id, self.target_table_id)
                logger.info(f"Replaced {self.target_table_id} with {total_rows} rows")
            
            # Clean up temp table
            self.bq_client.delete_table(temp_table_id, not_found_ok=True)
            
            # Validate sync
            validation_results = self.validate_sync()
            
            # Record metrics
            duration = time.time() - self.start_time
            metrics.end_pipeline_run(self.pipeline_name, "success", total_rows, duration)
            
            return {
                "status": "success",
                "pipeline": self.pipeline_name,
                "rows_processed": total_rows,
                "duration_seconds": round(duration, 2),
                "validation": validation_results
            }
            
        except Exception as e:
            logger.error(f"Full sync failed for {self.pipeline_name}: {e}")
            duration = time.time() - self.start_time if self.start_time else 0
            metrics.end_pipeline_run(self.pipeline_name, "error", self.total_rows_processed, duration)
            
            # Clean up temp table if it exists
            if 'temp_table_id' in locals():
                self.bq_client.delete_table(temp_table_id, not_found_ok=True)
            
            raise
    
    def run_incremental_sync(self) -> Dict[str, Any]:
        """Run an incremental synchronization"""
        self.start_time = time.time()
        metrics.start_pipeline_run(self.pipeline_name)
        
        try:
            logger.info(f"Starting incremental sync for {self.pipeline_name}")
            
            # Ensure target table exists
            self.ensure_target_table_exists()
            
            # Process data in batches
            offset = 0
            total_rows = 0
            
            while True:
                # Fetch batch from Snowflake
                columns, rows = self.fetch_batch(offset, self.batch_size)
                
                if not rows:
                    break
                
                # Convert to BigQuery format
                bq_rows = [self.convert_row_to_dict(columns, row) for row in rows]
                
                # Use MERGE for incremental updates
                if bq_rows:
                    self._merge_batch(bq_rows, columns)
                    total_rows += len(bq_rows)
                
                offset += len(rows)
                metrics.record_batch_processed(self.pipeline_name, len(rows))
                
                logger.info(f"Progress: {total_rows} rows processed")
            
            self.total_rows_processed = total_rows
            
            # Run deduplication if enabled
            if self.config['data_quality'].get('enable_deduplication', True):
                self._deduplicate_table()
            
            # Validate sync
            validation_results = self.validate_sync()
            
            # Record metrics
            duration = time.time() - self.start_time
            metrics.end_pipeline_run(self.pipeline_name, "success", total_rows, duration)
            
            return {
                "status": "success",
                "pipeline": self.pipeline_name,
                "sync_type": "incremental",
                "rows_processed": total_rows,
                "duration_seconds": round(duration, 2),
                "validation": validation_results
            }
            
        except Exception as e:
            logger.error(f"Incremental sync failed for {self.pipeline_name}: {e}")
            duration = time.time() - self.start_time if self.start_time else 0
            metrics.end_pipeline_run(self.pipeline_name, "error", self.total_rows_processed, duration)
            raise
    
    def _replace_table(self, source_table_id: str, target_table_id: str):
        """Atomically replace target table with source table"""
        query = f"""
            CREATE OR REPLACE TABLE `{target_table_id}` AS
            SELECT * FROM `{source_table_id}`
        """
        
        job = self.bq_client.query(query)
        job.result()  # Wait for completion
        
        if job.errors:
            raise RuntimeError(f"Table replacement failed: {job.errors}")
    
    def _merge_batch(self, rows: List[Dict[str, Any]], columns: List[str]):
        """Merge a batch of rows using MERGE statement"""
        # Create temp table for batch
        temp_table_name = f"temp_merge_{uuid.uuid4().hex[:8]}"
        temp_table_id = f"{self.project_id}.{self.dataset_id}.{temp_table_name}"
        
        # Load batch to temp table
        self.load_batch_to_bigquery(rows, temp_table_id)
        
        # Build MERGE statement
        primary_key = self.pipeline_config['primary_key']
        
        update_cols = [f"T.{col} = S.{col}" for col in columns if col != primary_key]
        insert_cols = columns
        
        merge_query = f"""
            MERGE `{self.target_table_id}` T
            USING `{temp_table_id}` S
            ON T.{primary_key} = S.{primary_key}
            WHEN MATCHED THEN
                UPDATE SET {', '.join(update_cols)}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(insert_cols)})
                VALUES ({', '.join([f'S.{col}' for col in insert_cols])})
        """
        
        job = self.bq_client.query(merge_query)
        job.result()
        
        # Clean up temp table
        self.bq_client.delete_table(temp_table_id, not_found_ok=True)
        
        logger.info(f"Merged {len(rows)} rows using {primary_key} as key")
    
    def _deduplicate_table(self):
        """Remove duplicates from the target table"""
        primary_key = self.pipeline_config['primary_key']
        
        dedup_query = f"""
            CREATE OR REPLACE TABLE `{self.target_table_id}` AS
            SELECT * EXCEPT(row_num)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {primary_key}) as row_num
                FROM `{self.target_table_id}`
            )
            WHERE row_num = 1
        """
        
        job = self.bq_client.query(dedup_query)
        job.result()
        
        logger.info(f"Deduplication completed for {self.target_table_id}")
    
    def run(self) -> Dict[str, Any]:
        """Run the pipeline based on configured sync type"""
        sync_type = self.pipeline_config.get('sync_type', 'full')
        
        if sync_type == 'incremental':
            return self.run_incremental_sync()
        else:
            return self.run_full_sync()