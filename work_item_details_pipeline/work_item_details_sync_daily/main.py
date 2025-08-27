import os
import json
import uuid
import datetime
import time
from decimal import Decimal
from google.cloud import secretmanager, bigquery
from google.api_core.exceptions import NotFound
import snowflake.connector
from functools import wraps

def get_snowflake_creds():
    # Load individual Snowflake secrets from Secret Manager
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    client = secretmanager.SecretManagerServiceClient()
    def access_secret(secret_id):
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        try:
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"Error accessing secret {secret_id}: {e}")
            raise
    return {
        "user": access_secret("SNOWFLAKE_USER"),
        "password": access_secret("SNOWFLAKE_PASSWORD"),
        "account": access_secret("SNOWFLAKE_ACCOUNT"),
        "warehouse": access_secret("SNOWFLAKE_WAREHOUSE"),
        "database": access_secret("SNOWFLAKE_DATABASE"),
        "schema": access_secret("SNOWFLAKE_SCHEMA"),
    }

def get_snowflake_connection_with_timeouts(sf_creds):
    """Create Snowflake connection with optimized timeout settings"""
    return snowflake.connector.connect(
        user=sf_creds["user"],
        password=sf_creds["password"],
        account=sf_creds["account"],
        warehouse=sf_creds["warehouse"],
        database=sf_creds["database"],
        schema=sf_creds["schema"],
        role=sf_creds.get("role"),
        # Optimized connection settings
        login_timeout=60,
        network_timeout=300,
        socket_timeout=300,
        client_session_keep_alive=True,
        client_session_keep_alive_heartbeat_frequency=900
    )

def retry_on_timeout(max_retries=3, delay=30):
    """Retry decorator for handling timeouts"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (snowflake.connector.errors.OperationalError, 
                        TimeoutError, Exception) as e:
                    if attempt == max_retries - 1:
                        print(f"Final attempt failed: {e}")
                        raise
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def get_bq_schema_from_snowflake(sf_creds):
    """Connects to Snowflake and derives a BigQuery schema from WORK_ITEM_DETAILS."""
    conn = get_snowflake_connection_with_timeouts(sf_creds)
    cs = conn.cursor()
    try:
        # Get column names and types from Snowflake for work_item_details table
        cs.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'WORK_ITEM_DETAILS'
              AND table_schema = '{sf_creds["schema"].upper()}'
            ORDER BY ordinal_position
        """)
        columns = cs.fetchall()
    finally:
        cs.close()
        conn.close()

    # Map Snowflake types to BigQuery types (consistent with main.py)
    sf_to_bq = {
        "VARCHAR": "STRING", "CHAR": "STRING", "TEXT": "STRING", "STRING": "STRING",
        "NUMBER": "NUMERIC", # Using NUMERIC for potential Decimals
        "DECIMAL": "NUMERIC",
        "INT": "INT64",
        "FLOAT": "FLOAT64",
        "BOOLEAN": "BOOL",
        "DATE": "DATE",
        "TIMESTAMP_NTZ": "TIMESTAMP", "TIMESTAMP_LTZ": "TIMESTAMP", "TIMESTAMP_TZ": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "DATETIME": "DATETIME",
    }
    bq_schema = []
    for name, data_type in columns:
        base_type = data_type.split("(")[0].upper()
        bq_type = sf_to_bq.get(base_type, "STRING") # Default to STRING
        # Make required fields nullable for temp table flexibility if needed, but stick to original for now
        bq_schema.append(bigquery.SchemaField(name, bq_type, mode="NULLABLE")) # Use NULLABLE for temp table

    return bq_schema

def sync_daily_incremental(request):
    # Entry point for daily incremental sync Cloud Function for Work Item Details
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    target_table_id = f"{project_id}.{dataset_id}.WORK_ITEM_DETAILS_BQ"

    # OPTIMIZED: Narrower date range for daily sync efficiency
    # Old: ±90 days = 180-day window (916K+ rows)
    # New: -7 to +30 days = 37-day window (much smaller)
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=7)   # Last 7 days for recent changes
    end_date = today + datetime.timedelta(days=30)    # Next 30 days for upcoming work
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    
    print(f"OPTIMIZATION: Reduced date range from 180 days to 37 days for daily sync")

    if not project_id or not dataset_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT and BQ_DATASET environment variables must be set.")
    print(f"Starting daily incremental sync for {target_table_id}")
    print(f"Date range: {start_date_str} to {end_date_str}")

    try:
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()

        # Define temp table details first
        temp_table_name = f"temp_work_item_details_daily_{uuid.uuid4().hex}"
        temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

        # Get schema and prepare temporary table
        print(f"Preparing temporary table: {temp_table_id}")
        try:
            target_table = bq_client.get_table(target_table_id)
            bq_schema = target_table.schema
            # Make schema fields nullable for temp table definition
            temp_schema_repr = [field.to_api_repr() for field in bq_schema]
            for field in temp_schema_repr:
                field['mode'] = 'NULLABLE'
            temp_bq_schema = [bigquery.SchemaField.from_api_repr(field) for field in temp_schema_repr]
            print(f"Target table {target_table_id} found. Using its schema.")

        except NotFound:
            print(f"Target table {target_table_id} not found. Deriving schema from Snowflake WORK_ITEM_DETAILS.")
            bq_schema = get_bq_schema_from_snowflake(sf_creds)
            if not bq_schema:
                 raise RuntimeError("Could not determine schema for BigQuery table.")

            # --- START NEW CODE ---
            print(f"Creating target table {target_table_id} with derived schema...")
            target_table_obj = bigquery.Table(target_table_id, schema=bq_schema)
            # You might want to add partitioning/clustering here if needed for the main table
            # target_table_obj.time_partitioning = bigquery.TimePartitioning(field="REPORTING_DATE")
            bq_client.create_table(target_table_obj)
            print(f"Target table {target_table_id} created successfully.")
            # --- END NEW CODE ---

            # Make schema fields nullable for temp table definition (using the same derived schema)
            temp_schema_repr = [field.to_api_repr() for field in bq_schema]
            for field in temp_schema_repr:
                field['mode'] = 'NULLABLE'
            temp_bq_schema = [bigquery.SchemaField.from_api_repr(field) for field in temp_schema_repr]

        # Use temp_bq_schema (all nullable) for the temporary table
        temp_table = bigquery.Table(temp_table_id, schema=temp_bq_schema)
        temp_table.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2) # Increased expiration
        bq_client.create_table(temp_table)
        print(f"Temporary table {temp_table_id} created.")

        # 1. Fetch data from Snowflake in batches and load into temp table
        print("Establishing optimized Snowflake connection with timeout settings...")
        conn = get_snowflake_connection_with_timeouts(sf_creds)
        cs = conn.cursor()
        offset = 0
        # CRITICAL FIX: Increase batch size from 20 to 5000 (250x improvement)
        # This reduces 45,804 batches to ~183 batches (99.6% reduction in DB calls)
        batch_size = 5000  # Match full sync performance
        print(f"PERFORMANCE FIX: Using optimized batch size of {batch_size} (was 20)")
        total_rows_fetched = 0
        sf_cols = None # Get column names once

        try:
            batch_number = 0
            sync_start_time = time.time()
            
            while True:
                batch_start_time = time.time()
                batch_number += 1
                
                query = f"""
                    SELECT *
                    FROM {sf_creds["schema"]}.WORK_ITEM_DETAILS
                    WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
                    ORDER BY WORK_ITEM_ID, REPORTING_DATE
                    LIMIT {batch_size} OFFSET {offset}
                """
                print(f"BATCH {batch_number}: Fetching from Snowflake: offset={offset}, batch_size={batch_size}")
                
                # Execute with retry logic
                @retry_on_timeout(max_retries=3, delay=10)
                def execute_snowflake_query():
                    cs.execute(query)
                    return cs.fetchall()
                
                sf_rows = execute_snowflake_query()

                if not sf_cols:
                     sf_cols = [col[0] for col in cs.description] # Get column names on first fetch

                if not sf_rows:
                    print("No more rows found in Snowflake for the date range.")
                    break # Exit loop when no more rows are fetched

                total_rows_fetched += len(sf_rows)
                fetch_duration = time.time() - batch_start_time
                
                # Performance logging
                print(f"BATCH {batch_number}: Fetched {len(sf_rows)} rows in {fetch_duration:.2f}s. Total: {total_rows_fetched}")
                print(f"PERF_METRIC: Batch:{batch_number} | Size:{len(sf_rows)} | FetchRate:{len(sf_rows)/fetch_duration:.1f} rows/s")

                # Prepare and load this batch into the single temp table
                transform_start_time = time.time()
                bq_rows_to_load = []
                for row in sf_rows:
                    row_dict = {}
                    for idx, col_name in enumerate(sf_cols):
                        value = row[idx]
                        if isinstance(value, (datetime.date, datetime.datetime)):
                            row_dict[col_name] = value.isoformat()
                        elif isinstance(value, Decimal):
                            row_dict[col_name] = str(value)
                        else:
                            row_dict[col_name] = value
                    bq_rows_to_load.append(row_dict)

                transform_duration = time.time() - transform_start_time
                print(f"BATCH {batch_number}: Data transformation completed in {transform_duration:.2f}s")

                # Load to BigQuery with retry
                load_start_time = time.time()
                print(f"BATCH {batch_number}: Loading {len(bq_rows_to_load)} rows to BigQuery...")
                
                @retry_on_timeout(max_retries=3, delay=5)
                def load_to_bigquery():
                    return bq_client.insert_rows_json(temp_table_id, bq_rows_to_load)
                
                errors = load_to_bigquery()
                if errors:
                    print(f"Errors loading batch into temp table {temp_table_id}: {errors}")
                    raise RuntimeError(f"Failed to load batch data into {temp_table_id}")

                load_duration = time.time() - load_start_time
                batch_total_duration = time.time() - batch_start_time
                
                # Comprehensive performance metrics
                print(f"BATCH {batch_number}: BigQuery load completed in {load_duration:.2f}s")
                print(f"BATCH {batch_number}: Total batch time: {batch_total_duration:.2f}s | Overall rate: {len(sf_rows)/batch_total_duration:.1f} rows/s")
                
                # Progress tracking
                elapsed_time = time.time() - sync_start_time
                estimated_total_time = (elapsed_time / total_rows_fetched) * (total_rows_fetched + batch_size * 10) if total_rows_fetched > 0 else 0
                print(f"PROGRESS: {total_rows_fetched} rows processed in {elapsed_time:.1f}s | Est. completion: {estimated_total_time:.1f}s")

                offset += batch_size # Prepare for the next batch

        finally:
            cs.close()
            conn.close()

        # Check if *any* rows were processed
        if total_rows_fetched == 0:
            print("No rows found in Snowflake for the entire date range.")
            # Clean up the empty temp table
            bq_client.delete_table(temp_table_id, not_found_ok=True)
            return "No relevant rows found.", 200

        # Now that all batches are loaded into the single temp table, execute the MERGE
        print(f"Merging data from {temp_table_id} into {target_table_id}...")
        merge_sql = f"""
            MERGE `{target_table_id}` T
            USING `{temp_table_id}` S
            ON T.WORK_ITEM_ID = S.WORK_ITEM_ID AND T.REPORTING_DATE = S.REPORTING_DATE
            WHEN MATCHED THEN
              UPDATE SET {', '.join([f'T.{field.name} = S.{field.name}' for field in bq_schema])}
            WHEN NOT MATCHED THEN
              INSERT ({', '.join([field.name for field in bq_schema])})
              VALUES ({', '.join([f'S.{field.name}' for field in bq_schema])})
        """
        print(f"Executing MERGE SQL... (first 200 chars: {merge_sql[:200]}...)")
        query_job = bq_client.query(merge_sql)
        query_job.result()  # Wait for the job to complete

        if query_job.errors:
             print(f"Errors during MERGE operation: {query_job.errors}")
             raise RuntimeError("MERGE operation failed.")

        print(f"MERGE completed. Rows affected: {query_job.num_dml_affected_rows}")

        # Delete temporary table *after* successful merge
        print(f"Deleting temporary table: {temp_table_id}")
        bq_client.delete_table(temp_table_id, not_found_ok=True)

        print("Work Item Details daily incremental sync completed successfully.")
        return "Work Item Details daily sync finished.", 200

    except Exception as e:
        print(f"Error during Work Item Details daily incremental sync: {e}")
        # Consider more robust error handling/logging here
        return f"Error: {e}", 500 