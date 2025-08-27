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

def cleanup_duplicate_time_entries(bq_client, project_id, dataset_id):
    """
    Removes duplicate rows from the USER_TIME_ENTRY_BQ table based on
    REPORTING_DATE and TIME_ENTRY_ID, keeping only one row per group (the latest based on ingestion timestamp).
    Requires 'row_uuid' (STRING) and 'bq_ingestion_timestamp' (TIMESTAMP) columns to exist.
    """
    target_table_id = f"{project_id}.{dataset_id}.USER_TIME_ENTRY_BQ"
    print(f"Starting cleanup of duplicates in {target_table_id}")

    # Use the actual column names added to the table
    unique_identifier_column = "row_uuid" # Assuming this is the name of the unique ID column added
    ordering_column = "bq_ingestion_timestamp" # Assuming this is the name of the timestamp column added

    cleanup_sql = f"""
    DELETE FROM `{target_table_id}`
    WHERE {unique_identifier_column} IN (
        SELECT {unique_identifier_column}
        FROM (
            SELECT
                {unique_identifier_column},
                ROW_NUMBER() OVER (
                    PARTITION BY REPORTING_DATE, TIME_ENTRY_ID
                    ORDER BY {ordering_column} DESC -- Keep the latest row based on ingestion timestamp
                ) as rn
            FROM `{target_table_id}`
        )
        WHERE rn > 1
    );
    """

    print(f"Executing duplicate cleanup SQL for {target_table_id} using '{unique_identifier_column}' and ordering by '{ordering_column}' DESC...")
    try:
        query_job = bq_client.query(cleanup_sql)
        query_job.result()  # Wait for the job to complete

        if query_job.errors:
            print(f"Errors during duplicate cleanup: {query_job.errors}")
            # Decide if this should raise an error or just log
            raise RuntimeError(f"Duplicate cleanup failed for {target_table_id}")
        else:
            print(f"Duplicate cleanup completed. Rows deleted: {query_job.num_dml_affected_rows}")

    except Exception as e:
        print(f"Error executing duplicate cleanup query: {e}")
        raise # Re-raise the exception to indicate failure

def sync_daily_incremental(request):
    # Entry point for daily incremental sync Cloud Function
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    target_table_id = f"{project_id}.{dataset_id}.USER_TIME_ENTRY_BQ"

    # Define the date range: today +/- 90 days
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=90)
    end_date = today + datetime.timedelta(days=90)
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    if not project_id or not dataset_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT and BQ_DATASET environment variables must be set.")
    print(f"Starting daily incremental sync for {target_table_id}")
    print(f"Date range: {start_date_str} to {end_date_str}")

    try:
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()

        # Define temp table details first
        temp_table_name = f"temp_daily_sync_{uuid.uuid4().hex}"
        temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

        # Get schema and create the single temporary table *before* the loop
        print(f"Preparing temporary table: {temp_table_id}")
        try:
            target_table = bq_client.get_table(target_table_id)
            bq_schema = target_table.schema
            print(f"Using schema from existing target table: {target_table_id}")

            # Ensure the new columns are present in the schema object for the MERGE list
            schema_field_names = {field.name for field in bq_schema}
            if "row_uuid" not in schema_field_names:
                 raise RuntimeError(f"Target table {target_table_id} is missing required column 'row_uuid'")
            if "bq_ingestion_timestamp" not in schema_field_names:
                 raise RuntimeError(f"Target table {target_table_id} is missing required column 'bq_ingestion_timestamp'")

            # Filter out the new columns from the main update/insert list derived from Snowflake
            update_insert_schema = [field for field in bq_schema if field.name not in ("row_uuid", "bq_ingestion_timestamp")]

        except NotFound:
             # This path should ideally not happen if the table is pre-created with the right schema
             print(f"ERROR: Target table {target_table_id} not found. Cannot proceed without schema including new columns.")
             raise # Or handle schema creation including new columns explicitly here

        temp_table = bigquery.Table(temp_table_id, schema=bq_schema)
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
        batch_size = 20 # Use a smaller batch size if memory is still a concern, but 20 is fine
        total_rows_fetched = 0
        sf_cols = None # Get column names once

        try:
            while True:
                query = f"""
                    SELECT *
                    FROM {sf_creds["schema"]}.USER_TIME_ENTRY_DETAIL
                    WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
                    ORDER BY TIME_ENTRY_ID, REPORTING_DATE -- Add ordering for consistent batching
                    LIMIT {batch_size} OFFSET {offset}
                """
                print(f"Fetching batch from Snowflake: offset={offset}, batch_size={batch_size}")
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
                    print(f"Errors loading batch into temp table {temp_table_id}: {errors}")
                    # Decide on error handling: break, continue, log, raise?
                    # For now, let's raise an error to stop the process if a batch fails.
                    raise RuntimeError(f"Failed to load batch data into {temp_table_id}")

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

        # Construct SET clause excluding new columns (timestamp updated separately)
        set_clause = ', '.join([f'T.{field.name} = S.{field.name}' for field in update_insert_schema])
        # Construct INSERT column list excluding new columns
        insert_cols = ', '.join([field.name for field in update_insert_schema])
        # Construct VALUES list excluding new columns
        values_cols = ', '.join([f'S.{field.name}' for field in update_insert_schema])

        merge_sql = f"""
            MERGE `{target_table_id}` T
            USING `{temp_table_id}` S
            ON T.TIME_ENTRY_ID = S.TIME_ENTRY_ID AND T.REPORTING_DATE = S.REPORTING_DATE
            WHEN MATCHED THEN
              -- Update existing columns AND the ingestion timestamp
              UPDATE SET {set_clause}, T.bq_ingestion_timestamp = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              -- Insert new row with all columns, generating UUID and setting timestamp
              INSERT ({insert_cols}, row_uuid, bq_ingestion_timestamp)
              VALUES ({values_cols}, GENERATE_UUID(), CURRENT_TIMESTAMP())
        """
        print(f"Executing MERGE SQL including updates/inserts for row_uuid and bq_ingestion_timestamp...")
        query_job = bq_client.query(merge_sql)
        query_job.result()  # Wait for the job to complete

        if query_job.errors:
             print(f"Errors during MERGE operation: {query_job.errors}")
             raise RuntimeError("MERGE operation failed.")

        print(f"MERGE completed. Rows affected: {query_job.num_dml_affected_rows}")

        # Call cleanup function after successful merge
        try:
            cleanup_duplicate_time_entries(bq_client, project_id, dataset_id)
        except Exception as cleanup_error:
            # Log the cleanup error but potentially allow the process to continue,
            # depending on desired behavior. Here, we'll log and re-raise
            # to ensure the overall function status reflects the cleanup failure.
            print(f"Duplicate cleanup failed after MERGE: {cleanup_error}")
            # Optionally, decide if failure here is critical enough to stop everything
            # For now, re-raising to make the function fail if cleanup fails.
            raise cleanup_error

        # Delete temporary table *after* successful merge and cleanup
        print(f"Deleting temporary table: {temp_table_id}")
        bq_client.delete_table(temp_table_id, not_found_ok=True)

        print("Daily incremental sync completed successfully.")
        return "Daily sync finished.", 200

    except Exception as e:
        print(f"Error during daily incremental sync: {e}")
        # Consider more robust error handling/logging here
        return f"Error: {e}", 500 