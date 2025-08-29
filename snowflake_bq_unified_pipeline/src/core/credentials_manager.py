"""
Secure Credentials Manager with Caching and Connection Pooling
==============================================================

This module provides secure credential management with:
- Single fetch and caching of credentials
- Connection pooling for Snowflake
- Automatic retry logic
- Secure credential rotation support
"""

import os
import json
import time
from typing import Dict, Any, Optional
from functools import lru_cache
from contextlib import contextmanager
import threading
from google.cloud import secretmanager
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError, OperationalError
import logging

# Configure structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CredentialsManager:
    """Manages credentials with caching and security best practices"""
    
    _instance = None
    _lock = threading.Lock()
    _credentials_cache = {}
    _cache_timestamp = 0
    _cache_ttl = 3600  # 1 hour cache TTL
    
    def __new__(cls):
        """Singleton pattern to ensure single instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the credentials manager"""
        if not hasattr(self, 'initialized'):
            self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "red-octane-444308-f4")
            self.secret_client = secretmanager.SecretManagerServiceClient()
            self.initialized = True
    
    @lru_cache(maxsize=32)
    def _access_secret(self, secret_id: str) -> str:
        """Access a secret from Secret Manager with caching"""
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"
        try:
            response = self.secret_client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Error accessing secret {secret_id}: {e}")
            raise
    
    def get_snowflake_credentials(self, force_refresh: bool = False) -> Dict[str, str]:
        """
        Get Snowflake credentials with caching
        
        Args:
            force_refresh: Force refresh of cached credentials
            
        Returns:
            Dictionary of Snowflake credentials
        """
        current_time = time.time()
        
        # Check if cache is valid
        if (not force_refresh and 
            self._credentials_cache and 
            (current_time - self._cache_timestamp) < self._cache_ttl):
            logger.info("Using cached Snowflake credentials")
            return self._credentials_cache
        
        # Fetch fresh credentials
        logger.info("Fetching fresh Snowflake credentials from Secret Manager")
        
        with self._lock:
            # Double-check after acquiring lock
            if (not force_refresh and 
                self._credentials_cache and 
                (current_time - self._cache_timestamp) < self._cache_ttl):
                return self._credentials_cache
            
            # Fetch all secrets in parallel (conceptually - GCP client handles this)
            credentials = {
                "user": self._access_secret("SNOWFLAKE_USER"),
                "password": self._access_secret("SNOWFLAKE_PASSWORD"),
                "account": self._access_secret("SNOWFLAKE_ACCOUNT"),
                "warehouse": self._access_secret("SNOWFLAKE_WAREHOUSE"),
                "database": self._access_secret("SNOWFLAKE_DATABASE"),
                "schema": self._access_secret("SNOWFLAKE_SCHEMA"),
            }
            
            # Optional role if it exists
            try:
                credentials["role"] = self._access_secret("SNOWFLAKE_ROLE")
            except:
                logger.info("No SNOWFLAKE_ROLE secret found, using default")
            
            self._credentials_cache = credentials
            self._cache_timestamp = current_time
            
        return credentials
    
    def get_secret(self, secret_id: str) -> str:
        """Get a generic secret from Secret Manager"""
        return self._access_secret(secret_id)
    
    def clear_cache(self):
        """Clear the credentials cache"""
        with self._lock:
            self._credentials_cache = {}
            self._cache_timestamp = 0
            self._access_secret.cache_clear()
        logger.info("Credentials cache cleared")


class SnowflakeConnectionPool:
    """Manages a pool of Snowflake connections for efficiency"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the connection pool"""
        if not hasattr(self, 'initialized'):
            self.pool = []
            self.max_connections = 3
            self.active_connections = 0
            self.credentials_manager = CredentialsManager()
            self.initialized = True
    
    def _create_connection(self) -> SnowflakeConnection:
        """Create a new Snowflake connection"""
        credentials = self.credentials_manager.get_snowflake_credentials()
        
        try:
            conn = snowflake.connector.connect(
                user=credentials["user"],
                password=credentials["password"],
                account=credentials["account"],
                warehouse=credentials["warehouse"],
                database=credentials["database"],
                schema=credentials["schema"],
                role=credentials.get("role"),
                login_timeout=60,
                network_timeout=300,
                autocommit=True,
                session_parameters={
                    'QUERY_TAG': 'unified_pipeline',
                    'STATEMENT_TIMEOUT_IN_SECONDS': 600,
                }
            )
            logger.info("Created new Snowflake connection")
            return conn
        except Exception as e:
            logger.error(f"Failed to create Snowflake connection: {e}")
            raise
    
    @contextmanager
    def get_connection(self, retry_attempts: int = 3):
        """
        Get a connection from the pool with automatic retry
        
        Args:
            retry_attempts: Number of retry attempts for failed connections
            
        Yields:
            SnowflakeConnection object
        """
        conn = None
        attempt = 0
        
        while attempt < retry_attempts:
            try:
                # Try to get a connection from the pool
                with self._lock:
                    if self.pool:
                        conn = self.pool.pop()
                        logger.info(f"Retrieved connection from pool (pool size: {len(self.pool)})")
                    elif self.active_connections < self.max_connections:
                        conn = self._create_connection()
                        self.active_connections += 1
                    else:
                        # Wait for a connection to be available
                        logger.warning("Connection pool exhausted, creating new connection")
                        conn = self._create_connection()
                
                # Test the connection
                if conn and not conn.is_closed():
                    conn.cursor().execute("SELECT 1")
                    break
                elif conn:
                    conn.close()
                    conn = None
                    
            except (DatabaseError, OperationalError) as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                conn = None
                attempt += 1
                if attempt < retry_attempts:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        if not conn:
            raise ConnectionError(f"Failed to get connection after {retry_attempts} attempts")
        
        try:
            yield conn
        finally:
            # Return connection to pool
            with self._lock:
                if len(self.pool) < self.max_connections and not conn.is_closed():
                    self.pool.append(conn)
                    logger.info(f"Returned connection to pool (pool size: {len(self.pool)})")
                else:
                    try:
                        conn.close()
                        self.active_connections -= 1
                        logger.info("Closed excess connection")
                    except:
                        pass
    
    def close_all(self):
        """Close all connections in the pool"""
        with self._lock:
            for conn in self.pool:
                try:
                    conn.close()
                except:
                    pass
            self.pool = []
            self.active_connections = 0
        logger.info("Closed all connections in pool")


# Global instances for easy access
credentials_manager = CredentialsManager()
connection_pool = SnowflakeConnectionPool()