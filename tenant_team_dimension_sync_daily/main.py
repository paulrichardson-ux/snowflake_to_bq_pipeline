import functions_framework
from google.cloud import bigquery
from google.cloud import secretmanager
import snowflake.connector
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_snowflake_creds():
    # Load individual Snowflake secrets from Secret Manager
    client = secretmanager.SecretManagerServiceClient()
    project_id = "red-octane-444308-f4"
    
    def access_secret(secret_id):
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    
    return {
        'user': access_secret('SNOWFLAKE_USER'),
        'password': access_secret('SNOWFLAKE_PASSWORD'),
        'account': access_secret('SNOWFLAKE_ACCOUNT'),
        'warehouse': access_secret('SNOWFLAKE_WAREHOUSE'),
        'database': access_secret('SNOWFLAKE_DATABASE'),
        'schema': access_secret('SNOWFLAKE_SCHEMA')
    }

def get_bq_schema_from_snowflake(sf_creds):
    """
    Auto-detect BigQuery schema from Snowflake DIMN_TENANT_TEAM table
    """
    conn = snowflake.connector.connect(**sf_creds)
    cursor = conn.cursor()
    
    # Get column information from Snowflake
    cursor.execute("DESCRIBE TABLE DIMN_TENANT_TEAM")
    columns = cursor.fetchall()
    
    schema = []
    for col in columns:
        col_name = col[0]  # Column name
        col_type = col[1].upper()  # Data type
        
        # Map Snowflake types to BigQuery types
        if 'VARCHAR' in col_type or 'TEXT' in col_type or 'STRING' in col_type:
            bq_type = "STRING"
        elif 'NUMBER' in col_type or 'DECIMAL' in col_type or 'NUMERIC' in col_type:
            bq_type = "NUMERIC"
        elif 'INTEGER' in col_type or 'INT' in col_type:
            bq_type = "INTEGER"
        elif 'FLOAT' in col_type or 'DOUBLE' in col_type:
            bq_type = "FLOAT"
        elif 'BOOLEAN' in col_type:
            bq_type = "BOOLEAN"
        elif 'DATE' in col_type:
            bq_type = "DATE"
        elif 'TIMESTAMP' in col_type or 'DATETIME' in col_type:
            bq_type = "TIMESTAMP"
        else:
            bq_type = "STRING"  # Default to STRING for unknown types
        
        schema.append(bigquery.SchemaField(col_name, bq_type, mode="NULLABLE"))
    
    cursor.close()
    conn.close()
    
    return schema

@functions_framework.http
def sync_tenant_team_dimension_full(request):
    """
    Cloud Function to sync DIMN_TENANT_TEAM from Snowflake to BigQuery
    Performs full table replacement (truncate and insert) - same as CLIENT_DIMENSION approach
    """
    try:
        logger.info("Starting TENANT_TEAM dimension daily sync (full replacement)")
        
        # Get request data
        request_json = request.get_json(silent=True) or {}
        source = request_json.get('source', 'unknown')
        logger.info(f"Sync triggered by: {source}")
        
        # Initialize clients
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()
        
        # BigQuery configuration
        project_id = "red-octane-444308-f4"
        dataset_id = "karbon_data"
        table_id = "TENANT_TEAM_DIMENSION"
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        temp_table_id = f"{project_id}.{dataset_id}.{table_id}_temp"
        
        logger.info(f"Target table: {full_table_id}")
        
        # Auto-detect schema from Snowflake
        schema = get_bq_schema_from_snowflake(sf_creds)
        logger.info(f"Detected schema with {len(schema)} columns")
        
        # Create/update BigQuery table with detected schema
        table = bigquery.Table(full_table_id, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        logger.info(f"BigQuery table ready: {full_table_id}")
        
        # Create temporary table for staging
        temp_table = bigquery.Table(temp_table_id, schema=schema)
        temp_table = bq_client.create_table(temp_table, exists_ok=True)
        logger.info(f"Temporary table created: {temp_table_id}")
        
        # Connect to Snowflake and fetch data
        logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(**sf_creds)
        cursor = conn.cursor()
        
        # Query all tenant team data from Snowflake
        query = "SELECT * FROM DIMN_TENANT_TEAM ORDER BY TENANT_TEAM_NAME"
        
        logger.info("Executing Snowflake query...")
        cursor.execute(query)
        
        # Get column names for mapping
        column_names = [desc[0] for desc in cursor.description]
        logger.info(f"Columns: {column_names}")
        
        # Fetch all results
        rows = cursor.fetchall()
        logger.info(f"Fetched {len(rows)} tenant team records from Snowflake")
        
        if rows:
            # Convert to list of dictionaries for BigQuery
            tenant_team_data = []
            for row in rows:
                record = {}
                for i, col_name in enumerate(column_names):
                    record[col_name] = row[i]
                tenant_team_data.append(record)
            
            # Insert data into temporary table in batches
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(tenant_team_data), batch_size):
                batch = tenant_team_data[i:i + batch_size]
                errors = bq_client.insert_rows_json(temp_table, batch)
                
                if errors:
                    logger.error(f"Batch insert errors: {errors}")
                    raise Exception(f"Failed to insert batch: {errors}")
                
                total_inserted += len(batch)
                logger.info(f"Inserted batch: {len(batch)} records (Total: {total_inserted})")
            
            logger.info(f"All data loaded into temporary table: {total_inserted} records")
            
            # Replace main table with temporary table data
            logger.info("Replacing main table with new data...")
            
            # Truncate main table
            truncate_query = f"TRUNCATE TABLE `{full_table_id}`"
            bq_client.query(truncate_query).result()
            logger.info("Main table truncated")
            
            # Insert from temp table to main table
            insert_query = f"""
            INSERT INTO `{full_table_id}`
            SELECT * FROM `{temp_table_id}`
            """
            job = bq_client.query(insert_query)
            job.result()
            logger.info("Data copied from temporary to main table")
            
            # Clean up temporary table
            bq_client.delete_table(temp_table_id)
            logger.info("Temporary table cleaned up")
            
            # Verify final count
            count_query = f"SELECT COUNT(*) as count FROM `{full_table_id}`"
            result = list(bq_client.query(count_query).result())
            final_count = result[0].count
            
            logger.info(f"Sync completed successfully!")
            logger.info(f"Final tenant team count in BigQuery: {final_count}")
            
            # Close Snowflake connection
            cursor.close()
            conn.close()
            
            return {
                'status': 'success',
                'message': f'TENANT_TEAM dimension sync completed',
                'records_synced': final_count,
                'source': source
            }, 200
            
        else:
            logger.warning("No tenant team data found in Snowflake")
            cursor.close()
            conn.close()
            
            return {
                'status': 'warning',
                'message': 'No tenant team data found in Snowflake',
                'records_synced': 0,
                'source': source
            }, 200
            
    except Exception as e:
        logger.error(f"Error in tenant team dimension sync: {str(e)}")
        
        # Clean up temporary table if it exists
        try:
            bq_client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        
        return {
            'status': 'error',
            'message': f'Tenant team dimension sync failed: {str(e)}',
            'source': source
        }, 500 