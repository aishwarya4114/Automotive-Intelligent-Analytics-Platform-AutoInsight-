"""
Snowflake database connection for MCP server
"""

import snowflake.connector
import os
from typing import Optional
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class SnowflakeConnection:
    """Singleton Snowflake connection manager"""
    
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SnowflakeConnection, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.config = {
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'AUTOMOTIVE_AI'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            }
            self._validate_config()
    
    def _validate_config(self):
        """Validate required environment variables"""
        required = ['user', 'password', 'account']
        missing = [k for k in required if not self.config.get(k)]
        if missing:
            raise ValueError(f"Missing required Snowflake config: {', '.join(missing)}")
    
    def get_connection(self):

        """Get or create Snowflake connection"""

        try:

            # Check if connection is None OR if the connection is dead

            if self._connection is None or not self._is_connection_alive():

                logger.info("Creating new Snowflake connection...")

                # CRITICAL: Re-initialize connection with clean config

                self._connection = snowflake.connector.connect(

                    user=self.config['user'],

                    password=self.config['password'],

                    account=self.config['account'],

                    warehouse=self.config['warehouse'],

                    database=self.config['database'],

                    schema=self.config['schema'],

                    role=self.config['role']

                    # NO INSECURE_MODE=TRUE HERE - rely on PYTHONHTTPSVERIFY=0

                )

                logger.info(f"✓ Connected to {self.config['database']}.{self.config['schema']}")

            return self._connection

        except Exception as e:

            logger.error(f"Failed to connect to Snowflake: {e}")

            # If connection fails, force cleanup before re-raising

            self._connection = None 

            raise
 
    
    def _is_connection_alive(self) -> bool:
        """Check if connection is still alive"""
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except:
            return False
    
    def close(self):
        """Close Snowflake connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("✓ Closed Snowflake connection")

@contextmanager
def get_cursor():
    """Context manager for Snowflake cursor"""
    conn = SnowflakeConnection().get_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)
    try:
        yield cursor
    finally:
        cursor.close()

def get_snowflake_connection():
    """Get Snowflake connection (for backward compatibility)"""
    return SnowflakeConnection().get_connection()

def execute_query(query: str, params: Optional[tuple] = None):
    """Execute a query and return results as list of dicts"""
    with get_cursor() as cursor:
        cursor.execute(query, params or ())
        return cursor.fetchall()

def execute_query_single(query: str, params: Optional[tuple] = None):
    """Execute a query and return single result"""
    with get_cursor() as cursor:
        cursor.execute(query, params or ())
        return cursor.fetchone()