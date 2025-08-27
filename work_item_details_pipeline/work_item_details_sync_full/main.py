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
    """Connects to Snowflake and derives a BigQuery schema from WORK_ITEM_DETAILS."""
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

    # Map Snowflake types to BigQuery types
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
        bq_schema.append(bigquery.SchemaField(name, bq_type, mode="NULLABLE")) # Start as NULLABLE for flexibility

    return bq_schema

def sync_full_work_item_details(request):
    """Cloud Function for FULL sync of WORK_ITEM_DETAILS from Snowflake to BigQuery."""
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    target_table_id = f"{project_id}.{dataset_id}.WORK_ITEM_DETAILS_BQ"

    if not project_id or not dataset_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT and BQ_DATASET environment variables must be set.")
    print(f"Starting FULL sync for {target_table_id}")

    try:
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()

        # Define temp table details first
        temp_table_name = f"temp_work_item_details_full_{uuid.uuid4().hex}"
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

            print(f"Creating target table {target_table_id} with derived schema...")
            target_table_obj = bigquery.Table(target_table_id, schema=bq_schema)
            # Consider adding partitioning/clustering here if needed for the main table
            # target_table_obj.time_partitioning = bigquery.TimePartitioning(field="REPORTING_DATE")
            bq_client.create_table(target_table_obj)
            print(f"Target table {target_table_id} created successfully.")

            # Make schema fields nullable for temp table definition (using the same derived schema)
            temp_schema_repr = [field.to_api_repr() for field in bq_schema]
            for field in temp_schema_repr:
                field['mode'] = 'NULLABLE'
            temp_bq_schema = [bigquery.SchemaField.from_api_repr(field) for field in temp_schema_repr]

        # Use temp_bq_schema (all nullable) for the temporary table
        temp_table = bigquery.Table(temp_table_id, schema=temp_bq_schema)
        temp_table.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
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
        batch_size = 5000 # Larger batch size for full sync
        total_rows_fetched = 0
        sf_cols = None # Get column names once

        try:
            while True:
                query = f"""
                    SELECT *
                    FROM {sf_creds["schema"]}.WORK_ITEM_DETAILS
                    ORDER BY WORK_ITEM_ID, REPORTING_DATE -- Order for consistency
                    LIMIT {batch_size} OFFSET {offset}
                """
                print(f"Fetching FULL batch from Snowflake WORK_ITEM_DETAILS: offset={offset}, batch_size={batch_size}")
                cs.execute(query)
                sf_rows = cs.fetchall()

                if not sf_cols:
                     sf_cols = [col[0] for col in cs.description] # Get column names on first fetch

                rows_in_batch = len(sf_rows)
                if rows_in_batch == 0:
                    print("No more rows found in Snowflake.")
                    break # Exit loop when no more rows are fetched

                total_rows_fetched += rows_in_batch
                print(f"Fetched {rows_in_batch} rows in this batch. Total fetched: {total_rows_fetched}")

                # Prepare and load this batch into the single temp table
                bq_rows_to_load = []
                for row in sf_rows:
                    row_dict = {}
                    for idx, col_name in enumerate(sf_cols):
                        value = row[idx]
                        # Handle data type conversions for JSON compatibility
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
                    raise RuntimeError(f"Failed to load batch data into {temp_table_id}")

                offset += rows_in_batch # Prepare for the next batch

        finally:
            cs.close()
            conn.close()

        # Check if *any* rows were processed
        if total_rows_fetched == 0:
            print("No rows found in Snowflake WORK_ITEM_DETAILS table.")
            # Clean up the empty temp table
            bq_client.delete_table(temp_table_id, not_found_ok=True)
            return "No rows found in source table.", 200

        # Now that all batches are loaded into the single temp table, execute the MERGE
        print(f"Merging data from {temp_table_id} into {target_table_id}...")
        # Use the schema from the target table (or derived schema) for the MERGE columns
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

        print(f"MERGE completed. Rows affected/inserted/updated: {query_job.num_dml_affected_rows}")

        # Delete temporary table *after* successful merge
        print(f"Deleting temporary table: {temp_table_id}")
        bq_client.delete_table(temp_table_id, not_found_ok=True)

        print("Work Item Details FULL sync completed successfully.")
        return f"Full sync finished. Total rows processed: {total_rows_fetched}", 200

    except Exception as e:
        print(f"Error during Work Item Details FULL sync: {e}")
        # Clean up temp table on error if it exists
        try:
            bq_client.delete_table(temp_table_id, not_found_ok=True)
            print(f"Cleaned up temporary table {temp_table_id} after error.")
        except Exception as cleanup_e:
            print(f"Error cleaning up temp table {temp_table_id} after error: {cleanup_e}")
        return f"Error: {e}", 500 