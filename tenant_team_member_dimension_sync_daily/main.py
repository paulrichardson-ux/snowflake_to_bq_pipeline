import os
import json
import uuid
import datetime
from decimal import Decimal
from google.cloud import secretmanager, bigquery
from google.api_core.exceptions import NotFound
import snowflake.connector
import functions_framework

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
    """Connects to Snowflake and derives a BigQuery schema from DIMN_TENANT_TEAM_MEMBER."""
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
            WHERE table_name = 'DIMN_TENANT_TEAM_MEMBER'
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
        bq_schema.append(bigquery.SchemaField(name, bq_type, mode="NULLABLE"))

    return bq_schema

@functions_framework.http
def sync_tenant_team_member_dimension_full(request):
    """Entry point for TENANT_TEAM_MEMBER_DIMENSION full sync Cloud Function - replaces table to exactly mirror Snowflake"""
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    target_table_id = f"{project_id}.{dataset_id}.TENANT_TEAM_MEMBER_DIMENSION"

    if not project_id or not dataset_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT and BQ_DATASET environment variables must be set.")
    print(f"Starting TENANT_TEAM_MEMBER_DIMENSION full sync for {target_table_id}")

    try:
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()

        # Define temp table details first
        temp_table_name = f"temp_tenant_team_member_dimension_sync_{uuid.uuid4().hex}"
        temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

        # Get schema and create the single temporary table
        print(f"Preparing temporary table: {temp_table_id}")
        try:
            target_table = bq_client.get_table(target_table_id)
            bq_schema = target_table.schema
            print(f"Using existing target table schema")
        except NotFound:
            print(f"Target table {target_table_id} not found. Deriving schema from Snowflake.")
            bq_schema = get_bq_schema_from_snowflake(sf_creds)
            if not bq_schema:
                 raise RuntimeError("Could not determine schema for temporary table.")
            
            # Create target table if it doesn't exist
            print(f"Creating target table {target_table_id}")
            target_table = bigquery.Table(target_table_id, schema=bq_schema)
            bq_client.create_table(target_table)

        temp_table = bigquery.Table(temp_table_id, schema=bq_schema)
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
        batch_size = 1000  # Reasonable batch size for tenant team member dimension data
        total_rows_fetched = 0
        sf_cols = None

        try:
            while True:
                query = f"""
                    SELECT *
                    FROM {sf_creds["schema"]}.DIMN_TENANT_TEAM_MEMBER
                    ORDER BY TENANT_TEAM_MEMBER_ID
                    LIMIT {batch_size} OFFSET {offset}
                """
                print(f"Fetching batch from Snowflake DIMN_TENANT_TEAM_MEMBER: offset={offset}, batch_size={batch_size}")
                cs.execute(query)
                sf_rows = cs.fetchall()

                if not sf_cols:
                     sf_cols = [col[0] for col in cs.description]

                if not sf_rows:
                    print("No more rows found in Snowflake DIMN_TENANT_TEAM_MEMBER.")
                    break

                total_rows_fetched += len(sf_rows)
                print(f"Fetched {len(sf_rows)} rows in this batch. Total fetched: {total_rows_fetched}")

                # Prepare and load this batch into the temp table
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
                    raise RuntimeError(f"Failed to load batch data into {temp_table_id}")

                offset += batch_size

        finally:
            cs.close()
            conn.close()

        # Check if any rows were processed
        if total_rows_fetched == 0:
            print("No rows found in Snowflake DIMN_TENANT_TEAM_MEMBER table.")
            bq_client.delete_table(temp_table_id, not_found_ok=True)
            return "No rows found in source table.", 200

        # Replace the entire target table with temp table data (truncate and load)
        print(f"Replacing {target_table_id} with {total_rows_fetched} rows from Snowflake...")
        
        # Truncate target table first
        truncate_sql = f"TRUNCATE TABLE `{target_table_id}`"
        print("Truncating target table...")
        query_job = bq_client.query(truncate_sql)
        query_job.result()

        # Insert all data from temp table
        insert_sql = f"""
            INSERT INTO `{target_table_id}`
            SELECT * FROM `{temp_table_id}`
        """
        print("Inserting new data...")
        query_job = bq_client.query(insert_sql)
        query_job.result()

        if query_job.errors:
             print(f"Errors during INSERT operation: {query_job.errors}")
             raise RuntimeError("INSERT operation failed.")

        print(f"✅ Successfully replaced {target_table_id} with {total_rows_fetched} rows.")

        # Clean up temporary table
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        print(f"Temporary table {temp_table_id} deleted.")

        return f"Successfully synced {total_rows_fetched} rows to TENANT_TEAM_MEMBER_DIMENSION", 200

    except Exception as e:
        print(f"❌ Error in TENANT_TEAM_MEMBER_DIMENSION sync: {e}")
        # Clean up temp table on error
        try:
            if 'temp_table_id' in locals():
                bq_client.delete_table(temp_table_id, not_found_ok=True)
        except Exception as cleanup_error:
            print(f"Error during cleanup: {cleanup_error}")
        raise e 