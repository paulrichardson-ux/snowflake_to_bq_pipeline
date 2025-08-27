import os
import json
import uuid
import datetime
from decimal import Decimal
from google.cloud import secretmanager, bigquery, pubsub_v1
from google.api_core.exceptions import NotFound
import snowflake.connector

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

def get_bq_schema_from_snowflake(sf_creds):
    """Connects to Snowflake and derives a BigQuery schema from USER_TIME_ENTRY_DETAIL."""
    conn = snowflake.connector.connect(
        user=sf_creds["user"],
        password=sf_creds["password"],
        account=sf_creds["account"],
        warehouse=sf_creds["warehouse"],
        database=sf_creds["database"],
        schema=sf_creds["schema"],
        role=sf_creds.get("role"),
    )
    cs = conn.cursor()
    try:
        # Get column names and types from Snowflake
        cs.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'USER_TIME_ENTRY_DETAIL'
              AND table_schema = '{sf_creds["schema"].upper()}'
            ORDER BY ordinal_position
        """)
        columns = cs.fetchall()
    finally:
        cs.close()
        conn.close()

    # Map Snowflake types to BigQuery types
    sf_to_bq = {
        "VARCHAR": "STRING", "CHAR": "STRING", "TEXT": "STRING", "STRING": "STRING",
        "NUMBER": "NUMERIC",
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
        bq_type = sf_to_bq.get(base_type, "STRING")
        bq_schema.append(bigquery.SchemaField(name, bq_type, mode="NULLABLE"))

    return bq_schema

def create_sync_state_table(bq_client, project_id, dataset_id):
    """Create a table to track sync progress"""
    state_table_id = f"{project_id}.{dataset_id}.time_entry_sync_state"
    
    schema = [
        bigquery.SchemaField("sync_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("start_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("end_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # PENDING, PROCESSING, COMPLETED, FAILED
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("rows_processed", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]
    
    try:
        bq_client.get_table(state_table_id)
        print(f"Sync state table already exists: {state_table_id}")
    except NotFound:
        table = bigquery.Table(state_table_id, schema=schema)
        table = bq_client.create_table(table)
        print(f"Created sync state table: {state_table_id}")

def get_next_date_chunk(bq_client, project_id, dataset_id, chunk_days=7):
    """Get the next date range that needs to be processed"""
    state_table_id = f"{project_id}.{dataset_id}.time_entry_sync_state"
    
    # Find the latest completed date range
    query = f"""
        SELECT MAX(end_date) as last_completed_date
        FROM `{state_table_id}`
        WHERE status = 'COMPLETED'
    """
    
    try:
        result = list(bq_client.query(query))
        if result and result[0].last_completed_date:
            last_date = result[0].last_completed_date
            start_date = last_date + datetime.timedelta(days=1)
        else:
            # First time running - start from 90 days ago
            today = datetime.date.today()
            start_date = today - datetime.timedelta(days=90)
    except Exception as e:
        print(f"Error querying sync state: {e}")
        # Fallback to default range
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=90)
    
    end_date = start_date + datetime.timedelta(days=chunk_days - 1)
    today = datetime.date.today()
    
    # Don't process beyond today + 90 days
    max_end_date = today + datetime.timedelta(days=90)
    if end_date > max_end_date:
        end_date = max_end_date
    
    # If start_date is already beyond our max range, we're done
    if start_date > max_end_date:
        return None, None
    
    return start_date, end_date

def create_sync_record(bq_client, project_id, dataset_id, sync_id, start_date, end_date):
    """Create a record to track this sync operation"""
    state_table_id = f"{project_id}.{dataset_id}.time_entry_sync_state"
    
    now = datetime.datetime.now(datetime.timezone.utc)
    row = {
        "sync_id": sync_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "status": "PROCESSING",
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }
    
    errors = bq_client.insert_rows_json(state_table_id, [row])
    if errors:
        raise RuntimeError(f"Failed to create sync record: {errors}")

def update_sync_record(bq_client, project_id, dataset_id, sync_id, status, rows_processed=None, error_message=None):
    """Update the sync record with completion status"""
    state_table_id = f"{project_id}.{dataset_id}.time_entry_sync_state"
    
    now = datetime.datetime.now(datetime.timezone.utc)
    update_fields = [
        f"status = '{status}'",
        f"updated_at = '{now.isoformat()}'"
    ]
    
    if rows_processed is not None:
        update_fields.append(f"rows_processed = {rows_processed}")
    
    if error_message:
        # Escape single quotes in error message
        escaped_error = error_message.replace("'", "\\'")
        update_fields.append(f"error_message = '{escaped_error}'")
    
    update_sql = f"""
        UPDATE `{state_table_id}`
        SET {', '.join(update_fields)}
        WHERE sync_id = '{sync_id}'
    """
    
    query_job = bq_client.query(update_sql)
    query_job.result()

def trigger_next_chunk(project_id, topic_name="time-entry-sync-trigger"):
    """Trigger the next chunk processing via Pub/Sub"""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        # Simple message to trigger next processing
        message_data = json.dumps({"trigger": "next_chunk"}).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        
        print(f"Triggered next chunk processing: message_id={future.result()}")
    except Exception as e:
        print(f"Failed to trigger next chunk: {e}")
        # Don't fail the whole operation if Pub/Sub fails

def sync_date_chunk(bq_client, sf_creds, project_id, dataset_id, start_date, end_date):
    """Sync a specific date range chunk"""
    target_table_id = f"{project_id}.{dataset_id}.USER_TIME_ENTRY_BQ"
    
    # Create temp table for this chunk
    temp_table_name = f"temp_chunk_sync_{uuid.uuid4().hex}"
    temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"
    
    try:
        target_table = bq_client.get_table(target_table_id)
        bq_schema = target_table.schema
        temp_schema_repr = [field.to_api_repr() for field in bq_schema]
        for field in temp_schema_repr:
            field['mode'] = 'NULLABLE'
        bq_schema = [bigquery.SchemaField.from_api_repr(field) for field in temp_schema_repr]
    except NotFound:
        print(f"Target table {target_table_id} not found. Deriving schema from Snowflake.")
        bq_schema = get_bq_schema_from_snowflake(sf_creds)
        if not bq_schema:
            raise RuntimeError("Could not determine schema for temporary table.")

    temp_table = bigquery.Table(temp_table_id, schema=bq_schema)
    temp_table.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
    bq_client.create_table(temp_table)
    print(f"Temporary table {temp_table_id} created for date range {start_date} to {end_date}")

    # Fetch data from Snowflake in smaller batches
    conn = snowflake.connector.connect(
        user=sf_creds["user"], password=sf_creds["password"], account=sf_creds["account"],
        warehouse=sf_creds["warehouse"], database=sf_creds["database"], schema=sf_creds["schema"],
        role=sf_creds.get("role")
    )
    cs = conn.cursor()
    offset = 0
    batch_size = 50  # Increased batch size since we're processing smaller date ranges
    total_rows_fetched = 0
    sf_cols = None

    try:
        while True:
            query = f"""
                SELECT *
                FROM {sf_creds["schema"]}.USER_TIME_ENTRY_DETAIL
                WHERE REPORTING_DATE BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
                ORDER BY TIME_ENTRY_ID, REPORTING_DATE
                LIMIT {batch_size} OFFSET {offset}
            """
            print(f"Fetching batch: offset={offset}, batch_size={batch_size}")
            cs.execute(query)
            sf_rows = cs.fetchall()

            if not sf_cols:
                sf_cols = [col[0] for col in cs.description]

            if not sf_rows:
                print("No more rows found in Snowflake for this date chunk.")
                break

            total_rows_fetched += len(sf_rows)
            print(f"Fetched {len(sf_rows)} rows in this batch. Total fetched: {total_rows_fetched}")

            # Prepare and load batch into temp table
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

            print(f"Loading batch of {len(bq_rows_to_load)} rows into {temp_table_id}...")
            errors = bq_client.insert_rows_json(temp_table_id, bq_rows_to_load)
            if errors:
                raise RuntimeError(f"Failed to load batch data into {temp_table_id}: {errors}")

            offset += batch_size

    finally:
        cs.close()
        conn.close()

    # Perform merge operation
    if total_rows_fetched > 0:
        print(f"Merging {total_rows_fetched} rows from {temp_table_id} into {target_table_id}...")
        merge_sql = f"""
            MERGE `{target_table_id}` T
            USING `{temp_table_id}` S
            ON T.TIME_ENTRY_ID = S.TIME_ENTRY_ID AND T.REPORTING_DATE = S.REPORTING_DATE
            WHEN MATCHED THEN
              UPDATE SET {', '.join([f'T.{field.name} = S.{field.name}' for field in bq_schema])}
            WHEN NOT MATCHED THEN
              INSERT ({', '.join([field.name for field in bq_schema])})
              VALUES ({', '.join([f'S.{field.name}' for field in bq_schema])})
        """
        query_job = bq_client.query(merge_sql)
        query_job.result()

        if query_job.errors:
            raise RuntimeError(f"MERGE operation failed: {query_job.errors}")

        print(f"MERGE completed. Rows affected: {query_job.num_dml_affected_rows}")

    # Clean up temp table
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    
    return total_rows_fetched

def sync_daily_incremental(request):
    """Entry point for chunked daily incremental sync Cloud Function"""
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")

    if not project_id or not dataset_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT and BQ_DATASET environment variables must be set.")

    try:
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()

        # Create sync state table if it doesn't exist
        create_sync_state_table(bq_client, project_id, dataset_id)

        # Get the next date chunk to process
        start_date, end_date = get_next_date_chunk(bq_client, project_id, dataset_id)
        
        if start_date is None:
            print("All date ranges have been processed. Sync is complete.")
            return "All chunks processed", 200

        print(f"Processing date chunk: {start_date} to {end_date}")
        
        # Create sync record
        sync_id = f"chunk_{start_date.isoformat()}_{end_date.isoformat()}_{uuid.uuid4().hex[:8]}"
        create_sync_record(bq_client, project_id, dataset_id, sync_id, start_date, end_date)

        # Process the chunk
        rows_processed = sync_date_chunk(bq_client, sf_creds, project_id, dataset_id, start_date, end_date)
        
        # Update sync record as completed
        update_sync_record(bq_client, project_id, dataset_id, sync_id, "COMPLETED", rows_processed)
        
        print(f"Chunk sync completed successfully. Processed {rows_processed} rows.")
        
        # Trigger next chunk processing
        trigger_next_chunk(project_id)
        
        return f"Chunk processed: {start_date} to {end_date}, {rows_processed} rows", 200

    except Exception as e:
        print(f"Error during chunked sync: {e}")
        
        # Update sync record as failed if we have sync_id
        try:
            if 'sync_id' in locals():
                update_sync_record(bq_client, project_id, dataset_id, sync_id, "FAILED", error_message=str(e))
        except:
            pass  # Don't fail on error logging
            
        return f"Error: {e}", 500

def sync_manual_chunk(request):
    """Manual endpoint to sync a specific date range"""
    try:
        request_json = request.get_json(silent=True)
        if not request_json or 'start_date' not in request_json or 'end_date' not in request_json:
            return "Missing start_date or end_date in request body", 400
            
        start_date = datetime.datetime.strptime(request_json['start_date'], '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(request_json['end_date'], '%Y-%m-%d').date()
        
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
        dataset_id = os.getenv("BQ_DATASET")
        
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()
        
        rows_processed = sync_date_chunk(bq_client, sf_creds, project_id, dataset_id, start_date, end_date)
        
        return f"Manual sync completed: {start_date} to {end_date}, {rows_processed} rows", 200
        
    except Exception as e:
        print(f"Error in manual sync: {e}")
        return f"Error: {e}", 500
