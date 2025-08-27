import os
import json
import uuid
import datetime
from decimal import Decimal
from google.cloud import secretmanager, bigquery
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
    """Connects to Snowflake and derives a BigQuery schema from WORK_ITEM_BUDGET_VS_ACTUAL."""
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
        # Get column names and types from Snowflake for work_item_budget_vs_actual table
        cs.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'WORK_ITEM_BUDGET_VS_ACTUAL'
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

def create_tracking_table_if_not_exists(bq_client, project_id, dataset_id):
    """Create the sync tracking table if it doesn't exist."""
    tracking_table_id = f"{project_id}.{dataset_id}.work_item_budget_vs_actual_sync_tracker"
    try:
        bq_client.get_table(tracking_table_id)
        print(f"Tracking table {tracking_table_id} already exists.")
    except NotFound:
        schema = [
            bigquery.SchemaField("unique_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("work_item_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("reporting_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("sync_type", "STRING", mode="REQUIRED"),  # 'FULL' or 'INCREMENTAL'
        ]
        table = bigquery.Table(tracking_table_id, schema=schema)
        bq_client.create_table(table)
        print(f"Created tracking table: {tracking_table_id}")

def sync_daily_incremental(request):
    # Entry point for daily incremental sync Cloud Function for Work Item Budget vs Actual
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    target_table_id = f"{project_id}.{dataset_id}.WORK_ITEM_BUDGET_VS_ACTUAL_BQ"
    tracking_table_id = f"{project_id}.{dataset_id}.work_item_budget_vs_actual_sync_tracker"

    # OPTIMIZATION 1: Reduce date range to prevent timeout
    # Use a smaller window for daily sync to reduce data volume
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=30)  # Reduced from 90 to 30 days
    end_date = today + datetime.timedelta(days=30)    # Reduced from 90 to 30 days
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    
    print(f"OPTIMIZATION: Reduced date window to ±30 days to prevent timeout")

    if not project_id or not dataset_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT and BQ_DATASET environment variables must be set.")
    print(f"Starting daily incremental sync for {target_table_id}")
    print(f"Date range: {start_date_str} to {end_date_str}")

    try:
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()
        
        # Create tracking table if it doesn't exist
        create_tracking_table_if_not_exists(bq_client, project_id, dataset_id)

        # Define temp table details first
        temp_table_name = f"temp_work_item_budget_vs_actual_daily_{uuid.uuid4().hex}"
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
            print(f"Target table {target_table_id} not found. Deriving schema from Snowflake WORK_ITEM_BUDGET_VS_ACTUAL.")
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
        conn = snowflake.connector.connect(
            user=sf_creds["user"], password=sf_creds["password"], account=sf_creds["account"],
            warehouse=sf_creds["warehouse"], database=sf_creds["database"], schema=sf_creds["schema"],
            role=sf_creds.get("role")
        )
        cs = conn.cursor()
        offset = 0
        batch_size = 1000  # OPTIMIZATION 2: Increase batch size for better performance
        total_rows_fetched = 0
        sf_cols = None # Get column names once
        max_processing_time = 750  # OPTIMIZATION 3: Time limit in seconds (leave buffer for cleanup)
        start_time = datetime.datetime.now()

        try:
            while True:
                # OPTIMIZATION 4: Check time limit to prevent timeout
                elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
                if elapsed_time > max_processing_time:
                    print(f"⚠️  Time limit reached ({elapsed_time:.1f}s). Stopping to prevent timeout.")
                    print(f"Processed {total_rows_fetched} rows so far. Will resume on next run.")
                    break
                
                query = f"""
                    SELECT *
                    FROM {sf_creds["schema"]}.WORK_ITEM_BUDGET_VS_ACTUAL
                    WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
                    ORDER BY WORK_ITEM_ID, REPORTING_DATE
                    LIMIT {batch_size} OFFSET {offset}
                """
                print(f"Fetching batch from Snowflake WORK_ITEM_BUDGET_VS_ACTUAL: offset={offset}, batch_size={batch_size} (elapsed: {elapsed_time:.1f}s)")
                cs.execute(query)
                sf_rows = cs.fetchall()

                if not sf_cols:
                     sf_cols = [col[0] for col in cs.description] # Get column names on first fetch

                if not sf_rows:
                    print("No more rows found in Snowflake for the date range.")
                    break # Exit loop when no more rows are fetched

                total_rows_fetched += len(sf_rows)
                print(f"Fetched {len(sf_rows)} rows in this batch. Total fetched: {total_rows_fetched}")

                # Prepare and load this batch into the single temp table
                bq_rows_to_load = []
                tracking_rows = []
                current_timestamp = datetime.datetime.utcnow().isoformat()
                current_date = datetime.date.today().isoformat()  # Current sync date
                
                for row in sf_rows:
                    row_dict = {}
                    for idx, col_name in enumerate(sf_cols):
                        value = row[idx]
                        if isinstance(value, (datetime.date, datetime.datetime)):
                            row_dict[col_name] = value.isoformat()
                        elif isinstance(value, Decimal):
                            row_dict[col_name] = str(value)
                        elif col_name == "REPORTING_DATE" and (value is None or str(value).lower() in ['none', 'null', '']):
                            # Set NULL reporting dates to current sync date
                            row_dict[col_name] = current_date
                            print(f"Setting NULL REPORTING_DATE to sync date: {current_date}")
                        else:
                            row_dict[col_name] = value
                    bq_rows_to_load.append(row_dict)
                    
                    # Prepare tracking row - now that we set NULL dates to sync date, we can track all rows
                    work_item_id_idx = sf_cols.index("WORK_ITEM_ID")
                    work_item_id_value = row[work_item_id_idx]
                    
                    # Create tracking record for all rows with valid work_item_id
                    # Use current_date since we're setting NULL reporting dates to sync date
                    if work_item_id_value is not None:
                        tracking_rows.append({
                            "unique_id": str(uuid.uuid4()),
                            "work_item_id": str(work_item_id_value),
                            "reporting_date": current_date,  # Use sync date
                            "sync_timestamp": current_timestamp,
                            "sync_type": "INCREMENTAL"
                        })

                # OPTIMIZATION 5: Use load_table_from_json for better performance with large batches
                print(f"Loading batch of {len(bq_rows_to_load)} rows into {temp_table_id}...")
                
                if len(bq_rows_to_load) > 100:  # Use load job for large batches
                    job_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        schema=temp_bq_schema
                    )
                    # Convert to NDJSON format for load job
                    import io
                    ndjson_data = '\n'.join([json.dumps(row) for row in bq_rows_to_load])
                    job = bq_client.load_table_from_file(
                        io.StringIO(ndjson_data), 
                        temp_table_id, 
                        job_config=job_config
                    )
                    job.result()  # Wait for job to complete
                    if job.errors:
                        print(f"Load job errors: {job.errors}")
                        raise RuntimeError(f"Failed to load batch data into {temp_table_id}")
                else:
                    # Use streaming insert for small batches
                    errors = bq_client.insert_rows_json(temp_table_id, bq_rows_to_load)
                    if errors:
                        print(f"Errors loading batch into temp table {temp_table_id}: {errors}")
                        raise RuntimeError(f"Failed to load batch data into {temp_table_id}")

                                # Load tracking data only if we have valid rows
                if tracking_rows:
                    print(f"Loading {len(tracking_rows)} tracking records...")
                    tracking_errors = bq_client.insert_rows_json(tracking_table_id, tracking_rows)
                    if tracking_errors:
                        print(f"Errors loading tracking data: {tracking_errors}")
                        # Don't fail the entire process for tracking errors, just log
                else:
                    print(f"No valid tracking records to load for this batch (all dates were NULL/invalid).")

                offset += batch_size # Prepare for the next batch

        finally:
            cs.close()
            conn.close()

        # Check if *any* rows were processed
        if total_rows_fetched == 0:
            print("No rows found in Snowflake for the entire date range.")
            
            # NEW: Even if Snowflake has no data, we should delete stale BigQuery records for this date range
            print(f"Checking for stale records in BigQuery to delete for date range {start_date_str} to {end_date_str}...")
            delete_stale_sql = f"""
                DELETE FROM `{target_table_id}` 
                WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
            """
            print(f"Executing DELETE SQL for stale records when Snowflake has no data...")
            delete_stale_query_job = bq_client.query(delete_stale_sql)
            delete_stale_query_job.result()  # Wait for the job to complete
            
            if delete_stale_query_job.errors:
                print(f"Errors during stale data DELETE operation: {delete_stale_query_job.errors}")
            else:
                deleted_count = delete_stale_query_job.num_dml_affected_rows or 0
                print(f"Stale data DELETE completed. Rows deleted: {deleted_count}")
                
            # Clean up the empty temp table
            bq_client.delete_table(temp_table_id, not_found_ok=True)
            return f"No source rows found, but cleaned up {deleted_count if 'deleted_count' in locals() else 0} stale BigQuery records.", 200

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

        # NEW: Delete records that no longer exist in Snowflake for this date range
        print(f"Checking for records to delete from BigQuery that no longer exist in Snowflake...")
        delete_sql = f"""
            DELETE FROM `{target_table_id}` 
            WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND CONCAT(WORK_ITEM_ID, '|', CAST(REPORTING_DATE AS STRING)) NOT IN (
                SELECT CONCAT(WORK_ITEM_ID, '|', CAST(REPORTING_DATE AS STRING))
                FROM `{temp_table_id}`
              )
        """
        print(f"Executing DELETE SQL for records no longer in source...")
        delete_query_job = bq_client.query(delete_sql)
        delete_query_job.result()  # Wait for the job to complete
        
        if delete_query_job.errors:
            print(f"Errors during DELETE operation: {delete_query_job.errors}")
            # Don't fail the entire process for delete errors, just log
        else:
            print(f"DELETE completed. Rows deleted: {delete_query_job.num_dml_affected_rows}")

        # Delete temporary table *after* successful merge and delete operations
        print(f"Deleting temporary table: {temp_table_id}")
        bq_client.delete_table(temp_table_id, not_found_ok=True)

        print("Work Item Budget vs Actual daily incremental sync completed successfully.")
        return "Work Item Budget vs Actual daily sync finished.", 200

    except Exception as e:
        print(f"Error during Work Item Budget vs Actual daily incremental sync: {e}")
        # Consider more robust error handling/logging here
        return f"Error: {e}", 500 