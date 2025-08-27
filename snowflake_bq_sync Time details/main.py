import os
import json
import uuid
from google.cloud import secretmanager, bigquery
from google.api_core.exceptions import NotFound
import snowflake.connector
import datetime
from decimal import Decimal

def get_snowflake_creds():
    # Load individual Snowflake secrets from Secret Manager
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    print(f"DEBUG: Using project ID: {project_id}")
    client = secretmanager.SecretManagerServiceClient()
    def access_secret(secret_id):
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        print(f"DEBUG: Accessing secret: {name}")
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    return {
        "user": access_secret("SNOWFLAKE_USER"),
        "password": access_secret("SNOWFLAKE_PASSWORD"),
        "account": access_secret("SNOWFLAKE_ACCOUNT"),
        "warehouse": access_secret("SNOWFLAKE_WAREHOUSE"),
        "database": access_secret("SNOWFLAKE_DATABASE"),
        "schema": access_secret("SNOWFLAKE_SCHEMA"),
    }

def create_bq_tables_if_not_exist(client, project, dataset, sf_creds):
    # Ensure dataset exists before creating tables
    dataset_id = f"{project}.{dataset}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset_obj = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset_obj)
    # Create time_sync_tracker if it doesn't exist
    time_sync_table = f"{project}.{dataset}.time_sync_tracker"
    try:
        client.get_table(time_sync_table)
    except NotFound:
        schema = [
            bigquery.SchemaField("unique_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("time_entry_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("reporting_date", "DATE", mode="REQUIRED"),
        ]
        table = bigquery.Table(time_sync_table, schema=schema)
        client.create_table(table)
    # Create USER_TIME_ENTRY_BQ if it doesn't exist
    user_table = f"{project}.{dataset}.USER_TIME_ENTRY_BQ"
    try:
        client.get_table(user_table)
    except NotFound:
        # Fetch Snowflake schema for USER_TIME_ENTRY_DETAIL
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
            cs.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{sf_creds.get("table_name","USER_TIME_ENTRY_DETAIL").upper()}'
                  AND table_schema = '{sf_creds["schema"].upper()}'
                ORDER BY ordinal_position
            """)
            columns = cs.fetchall()
        finally:
            cs.close()
            conn.close()
        # Map Snowflake types to BigQuery types
        sf_to_bq = {
            "VARCHAR": "STRING",
            "CHAR": "STRING",
            "TEXT": "STRING",
            "STRING": "STRING",
            "NUMBER": "FLOAT64",
            "DECIMAL": "NUMERIC",
            "INT": "INT64",
            "FLOAT": "FLOAT64",
            "BOOLEAN": "BOOL",
            "DATE": "DATE",
            "TIMESTAMP_NTZ": "TIMESTAMP",
            "TIMESTAMP_LTZ": "TIMESTAMP",
            "TIMESTAMP_TZ": "TIMESTAMP",
            "TIMESTAMP": "TIMESTAMP",
            "DATETIME": "DATETIME",
        }
        schema = []
        for name, data_type in columns:
            base = data_type.split("(")[0].upper()
            bq_type = sf_to_bq.get(base, "STRING")
            schema.append(bigquery.SchemaField(name, bq_type))
        table = bigquery.Table(user_table, schema=schema)
        client.create_table(table)

def fetch_next_batch(sf_creds, offset, batch_size=10):
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
        cs.execute(f"""
            SELECT *
            FROM {sf_creds["schema"]}.USER_TIME_ENTRY_DETAIL
            ORDER BY TIME_ENTRY_ID
            LIMIT {batch_size} OFFSET {offset}
        """)
        rows = cs.fetchall()
        cols = [col[0] for col in cs.description]
    finally:
        cs.close()
        conn.close()
    return cols, rows

def insert_rows_json(client, table_id, rows):
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"Failed to insert rows into {table_id}: {errors}")

def main(request, context=None):
    """Processes one batch of Snowflake data based on provided offset."""
    # context is unused, but added to satisfy potential runtime signature mismatch
    request_json = request.get_json(silent=True) or {}
    # Get offset from request, default to 0
    offset = int(request_json.get("offset", 0))

    project = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset = os.getenv("BQ_DATASET")

    if not project or not dataset:
        return {"status": "error", "message": "Missing GOOGLE_CLOUD_PROJECT or BQ_DATASET env vars"}, 500

    # Load Snowflake credentials from individual secrets in Secret Manager
    sf_creds = get_snowflake_creds()
    bq_client = bigquery.Client()

    # Ensure tables exist in BigQuery (run once if needed, less critical per-batch)
    # Consider moving this to a separate setup function or accepting it might run multiple times initially
    create_bq_tables_if_not_exist(bq_client, project, dataset, sf_creds)

    # Fetch next batch from Snowflake using the provided offset
    print(f"Processing batch starting at offset: {offset}")
    cols, rows = fetch_next_batch(sf_creds, offset)

    rows_processed = len(rows)

    if not rows:
        print("No new rows found at this offset.")
        return {"status": "complete", "rows_processed": 0, "next_offset": offset}

    print(f"Fetched {rows_processed} rows from Snowflake.")

    # Prepare and insert into USER_TIME_ENTRY_BQ
    user_table_id = f"{project}.{dataset}.USER_TIME_ENTRY_BQ"
    # Convert date/datetime/Decimal objects to strings for JSON serialization
    bq_rows = []
    for row in rows:
        row_dict = {}
        for idx, col in enumerate(cols):
            value = row[idx]
            if isinstance(value, (datetime.date, datetime.datetime)):
                row_dict[col] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[col] = str(value)
            else:
                row_dict[col] = value
        bq_rows.append(row_dict)
    insert_rows_json(bq_client, user_table_id, bq_rows)

    # Prepare and insert into time_sync_tracker
    tracker_table_id = f"{project}.{dataset}.time_sync_tracker"
    tracker_rows = []
    idx_id = cols.index("TIME_ENTRY_ID")
    idx_date = cols.index("REPORTING_DATE")
    for row in rows:
        reporting_date_value = row[idx_date]
        if isinstance(reporting_date_value, (datetime.date, datetime.datetime)):
            reporting_date_str = reporting_date_value.isoformat()
        else:
            reporting_date_str = str(reporting_date_value)
        tracker_rows.append({
            "unique_id": str(uuid.uuid4()),
            "time_entry_id": row[idx_id],
            "reporting_date": reporting_date_str
        })
    insert_rows_json(bq_client, tracker_table_id, tracker_rows)

    print(f"Successfully processed {rows_processed} rows.")
    # Indicate success and provide next offset for workflow loop
    # If rows_processed < batch_size, we might be at the end.
    status = "complete" if rows_processed < 10 else "more_available"
    return {"status": status, "rows_processed": rows_processed, "next_offset": offset + rows_processed} 