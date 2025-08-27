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

def get_progressive_date_range():
    """
    PROGRESSIVE SYNC STRATEGY: Sync different date ranges on different days
    This prevents timeout by processing smaller chunks daily
    """
    today = datetime.date.today()
    day_of_week = today.weekday()  # 0 = Monday, 6 = Sunday
    
    # Define 7 different date ranges for each day of the week
    date_ranges = {
        0: (-30, -15),   # Monday: 30-15 days ago
        1: (-15, -1),    # Tuesday: 15-1 days ago  
        2: (0, 15),      # Wednesday: today to 15 days ahead
        3: (15, 30),     # Thursday: 15-30 days ahead
        4: (-45, -30),   # Friday: 45-30 days ago
        5: (30, 45),     # Saturday: 30-45 days ahead
        6: (-60, -45),   # Sunday: 60-45 days ago (older data cleanup)
    }
    
    start_offset, end_offset = date_ranges[day_of_week]
    start_date = today + datetime.timedelta(days=start_offset)
    end_date = today + datetime.timedelta(days=end_offset)
    
    return start_date, end_date, day_of_week

def sync_daily_progressive(request):
    """
    PROGRESSIVE DAILY SYNC: Syncs different date ranges each day to prevent timeout
    This ensures full coverage over a week while keeping each day's processing light
    """
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    target_table_id = f"{project_id}.{dataset_id}.WORK_ITEM_BUDGET_VS_ACTUAL_BQ"
    tracking_table_id = f"{project_id}.{dataset_id}.work_item_budget_vs_actual_sync_tracker"

    # Get progressive date range based on day of week
    start_date, end_date, day_of_week = get_progressive_date_range()
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    if not project_id or not dataset_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT and BQ_DATASET environment variables must be set.")
    
    print(f"🗓️  PROGRESSIVE SYNC STRATEGY - {day_names[day_of_week]}")
    print(f"Starting progressive daily sync for {target_table_id}")
    print(f"Date range: {start_date_str} to {end_date_str} ({(end_date - start_date).days} days)")
    print(f"Strategy: Day {day_of_week + 1}/7 of weekly cycle")

    try:
        sf_creds = get_snowflake_creds()
        bq_client = bigquery.Client()
        
        # Create tracking table if it doesn't exist
        create_tracking_table_if_not_exists(bq_client, project_id, dataset_id)

        # Connect to Snowflake with timeout optimization
        conn = snowflake.connector.connect(
            user=sf_creds["user"], 
            password=sf_creds["password"], 
            account=sf_creds["account"],
            warehouse=sf_creds["warehouse"], 
            database=sf_creds["database"], 
            schema=sf_creds["schema"],
            role=sf_creds.get("role"),
            client_session_keep_alive=True,  # Keep connection alive
            network_timeout=300  # 5 minute network timeout
        )
        
        cs = conn.cursor()
        
        # OPTIMIZED: Single query with larger batch for the smaller date range
        query = f"""
            SELECT *
            FROM {sf_creds["schema"]}.WORK_ITEM_BUDGET_VS_ACTUAL
            WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
            ORDER BY WORK_ITEM_ID, REPORTING_DATE
        """
        
        print(f"🔍 Fetching all data for date range from Snowflake...")
        start_time = datetime.datetime.now()
        
        cs.execute(query)
        sf_rows = cs.fetchall()
        sf_cols = [col[0] for col in cs.description]
        
        fetch_time = (datetime.datetime.now() - start_time).total_seconds()
        print(f"✅ Fetched {len(sf_rows)} rows in {fetch_time:.1f} seconds")
        
        cs.close()
        conn.close()
        
        if not sf_rows:
            print("No rows found in Snowflake for this date range.")
            # Clean up any stale BigQuery records for this date range
            delete_stale_sql = f"""
                DELETE FROM `{target_table_id}` 
                WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
            """
            print(f"Cleaning up stale records for date range...")
            delete_query_job = bq_client.query(delete_stale_sql)
            delete_query_job.result()
            deleted_count = delete_query_job.num_dml_affected_rows or 0
            print(f"Cleaned up {deleted_count} stale records")
            return f"Progressive sync completed. No source data, cleaned {deleted_count} stale records.", 200
        
        # Create temporary table for this batch
        temp_table_name = f"temp_budget_progressive_{uuid.uuid4().hex[:8]}"
        temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"
        
        # Get schema from target table
        try:
            target_table = bq_client.get_table(target_table_id)
            bq_schema = target_table.schema
        except NotFound:
            raise RuntimeError(f"Target table {target_table_id} not found. Run full sync first.")
        
        # Create temp table with nullable schema
        temp_schema_repr = [field.to_api_repr() for field in bq_schema]
        for field in temp_schema_repr:
            field['mode'] = 'NULLABLE'
        temp_bq_schema = [bigquery.SchemaField.from_api_repr(field) for field in temp_schema_repr]
        
        temp_table = bigquery.Table(temp_table_id, schema=temp_bq_schema)
        temp_table.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        bq_client.create_table(temp_table)
        print(f"📊 Created temporary table: {temp_table_id}")
        
        # Process and load data
        bq_rows_to_load = []
        tracking_rows = []
        current_timestamp = datetime.datetime.utcnow().isoformat()
        current_date = datetime.date.today().isoformat()
        
        print(f"🔄 Processing {len(sf_rows)} rows...")
        process_start = datetime.datetime.now()
        
        for row in sf_rows:
            row_dict = {}
            for idx, col_name in enumerate(sf_cols):
                value = row[idx]
                if isinstance(value, (datetime.date, datetime.datetime)):
                    row_dict[col_name] = value.isoformat()
                elif isinstance(value, Decimal):
                    row_dict[col_name] = str(value)
                elif col_name == "REPORTING_DATE" and (value is None or str(value).lower() in ['none', 'null', '']):
                    row_dict[col_name] = current_date
                else:
                    row_dict[col_name] = value
            bq_rows_to_load.append(row_dict)
            
            # Create tracking record
            work_item_id_idx = sf_cols.index("WORK_ITEM_ID")
            work_item_id_value = row[work_item_id_idx]
            
            if work_item_id_value is not None:
                tracking_rows.append({
                    "unique_id": str(uuid.uuid4()),
                    "work_item_id": str(work_item_id_value),
                    "reporting_date": current_date,
                    "sync_timestamp": current_timestamp,
                    "sync_type": "PROGRESSIVE"
                })
        
        process_time = (datetime.datetime.now() - process_start).total_seconds()
        print(f"✅ Processed rows in {process_time:.1f} seconds")
        
        # Load data using load job for better performance
        print(f"⬆️  Loading {len(bq_rows_to_load)} rows to BigQuery...")
        load_start = datetime.datetime.now()
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=temp_bq_schema
        )
        
        import io
        ndjson_data = '\n'.join([json.dumps(row) for row in bq_rows_to_load])
        job = bq_client.load_table_from_file(
            io.StringIO(ndjson_data), 
            temp_table_id, 
            job_config=job_config
        )
        job.result()
        
        if job.errors:
            raise RuntimeError(f"Load job failed: {job.errors}")
        
        load_time = (datetime.datetime.now() - load_start).total_seconds()
        print(f"✅ Loaded data in {load_time:.1f} seconds")
        
        # Merge data
        print(f"🔄 Merging data into target table...")
        merge_start = datetime.datetime.now()
        
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
        
        merge_job = bq_client.query(merge_sql)
        merge_job.result()
        
        if merge_job.errors:
            raise RuntimeError(f"Merge failed: {merge_job.errors}")
        
        merge_time = (datetime.datetime.now() - merge_start).total_seconds()
        rows_affected = merge_job.num_dml_affected_rows
        print(f"✅ Merged {rows_affected} rows in {merge_time:.1f} seconds")
        
        # Clean up stale records for this date range
        print(f"🧹 Cleaning up stale records...")
        delete_sql = f"""
            DELETE FROM `{target_table_id}` 
            WHERE REPORTING_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND CONCAT(WORK_ITEM_ID, '|', CAST(REPORTING_DATE AS STRING)) NOT IN (
                SELECT CONCAT(WORK_ITEM_ID, '|', CAST(REPORTING_DATE AS STRING))
                FROM `{temp_table_id}`
              )
        """
        
        delete_job = bq_client.query(delete_sql)
        delete_job.result()
        deleted_count = delete_job.num_dml_affected_rows or 0
        print(f"✅ Deleted {deleted_count} stale records")
        
        # Load tracking data
        if tracking_rows:
            print(f"📝 Loading {len(tracking_rows)} tracking records...")
            tracking_errors = bq_client.insert_rows_json(tracking_table_id, tracking_rows)
            if tracking_errors:
                print(f"⚠️  Tracking errors (non-fatal): {tracking_errors}")
        
        # Clean up temp table
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        
        total_time = (datetime.datetime.now() - start_time).total_seconds()
        print(f"🎉 Progressive sync completed successfully in {total_time:.1f} seconds")
        print(f"📊 Summary: {len(sf_rows)} source rows, {rows_affected} merged, {deleted_count} deleted")
        
        return f"Progressive sync completed. Processed {len(sf_rows)} rows in {total_time:.1f}s", 200

    except Exception as e:
        print(f"❌ Error during progressive sync: {e}")
        return f"Error: {e}", 500

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
            bigquery.SchemaField("sync_type", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(tracking_table_id, schema=schema)
        bq_client.create_table(table)
        print(f"Created tracking table: {tracking_table_id}")
