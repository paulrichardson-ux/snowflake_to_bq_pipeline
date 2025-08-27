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

def get_bq_schema_for_dimn_client_group():
    """
    Define BigQuery schema for DIMN_CLIENT_GROUP table
    Based on the CSV structure: CLIENT_GROUP_ID,CLIENT_GROUP_NAME,CLIENT_ID,CLIENT,CLIENT_GROUP_MEMBER_TYPE,ACCOUNT_ID,ACCOUNT_NAME
    """
    return [
        bigquery.SchemaField("CLIENT_GROUP_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CLIENT_GROUP_NAME", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CLIENT_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CLIENT", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CLIENT_GROUP_MEMBER_TYPE", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ACCOUNT_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ACCOUNT_NAME", "STRING", mode="NULLABLE"),
    ]

@functions_framework.http
def sync_full_client_group_dimension(request):
    """
    Cloud Function to sync DIMN_CLIENT_GROUP from Snowflake to BigQuery
    Performs full table replacement (truncate and insert)
    """
    try:
        logger.info("Starting CLIENT_GROUP dimension full sync")
        
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
        table_id = "CLIENT_GROUP_DIMENSION"
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        temp_table_id = f"{project_id}.{dataset_id}.{table_id}_temp"
        
        logger.info(f"Target table: {full_table_id}")
        
        # Create/update BigQuery table with proper schema
        schema = get_bq_schema_for_dimn_client_group()
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
        
        # Query all client group data from Snowflake
        query = """
        SELECT 
            CLIENT_GROUP_ID,
            CLIENT_GROUP_NAME,
            CLIENT_ID,
            CLIENT,
            CLIENT_GROUP_MEMBER_TYPE,
            ACCOUNT_ID,
            ACCOUNT_NAME
        FROM DIMN_CLIENT_GROUP
        ORDER BY CLIENT_GROUP_NAME, CLIENT
        """
        
        logger.info("Executing Snowflake query...")
        cursor.execute(query)
        
        # Fetch all results
        rows = cursor.fetchall()
        logger.info(f"Fetched {len(rows)} client group records from Snowflake")
        
        if rows:
            # Convert to list of dictionaries for BigQuery
            client_group_data = []
            for row in rows:
                client_group_record = {
                    'CLIENT_GROUP_ID': row[0],
                    'CLIENT_GROUP_NAME': row[1],
                    'CLIENT_ID': row[2],
                    'CLIENT': row[3],
                    'CLIENT_GROUP_MEMBER_TYPE': row[4],
                    'ACCOUNT_ID': row[5],
                    'ACCOUNT_NAME': row[6]
                }
                client_group_data.append(client_group_record)
            
            # Insert data into temporary table in batches
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(client_group_data), batch_size):
                batch = client_group_data[i:i + batch_size]
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
            logger.info(f"Final client group count in BigQuery: {final_count}")
            
            # Close Snowflake connection
            cursor.close()
            conn.close()
            
            return {
                'status': 'success',
                'message': f'CLIENT_GROUP dimension sync completed',
                'records_synced': final_count,
                'source': source
            }, 200
            
        else:
            logger.warning("No client group data found in Snowflake")
            cursor.close()
            conn.close()
            
            return {
                'status': 'warning',
                'message': 'No client group data found in Snowflake',
                'records_synced': 0,
                'source': source
            }, 200
            
    except Exception as e:
        logger.error(f"Error in client group dimension sync: {str(e)}")
        
        # Clean up temporary table if it exists
        try:
            bq_client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        
        return {
            'status': 'error',
            'message': f'Client group dimension sync failed: {str(e)}',
            'source': source
        }, 500 