#!/usr/bin/env python3
"""
BigQuery Dataset Setup Script
==============================

This script creates and configures the BigQuery dataset for the unified pipeline.
Can be run locally with appropriate Google Cloud credentials.
"""

import os
import sys
from google.cloud import bigquery
from google.cloud.exceptions import Conflict
from datetime import datetime

# Configuration
PROJECT_ID = "red-octane-444308-f4"
DATASET_ID = "unified_pipeline_data"
LOCATION = "US"

def create_dataset():
    """Create the BigQuery dataset for unified pipeline"""
    
    print("🚀 BigQuery Dataset Setup")
    print("=" * 60)
    print(f"Project ID: {PROJECT_ID}")
    print(f"Dataset ID: {DATASET_ID}")
    print(f"Location:   {LOCATION}")
    print("=" * 60)
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=PROJECT_ID)
        
        # Create dataset reference
        dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        
        # Check if dataset exists
        try:
            dataset = client.get_dataset(dataset_ref)
            print(f"✅ Dataset {DATASET_ID} already exists")
            return dataset
        except:
            pass
        
        # Create new dataset
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        dataset.description = "Unified Snowflake to BigQuery pipeline data storage"
        
        # Set dataset labels
        dataset.labels = {
            "environment": "production",
            "pipeline": "unified",
            "created": datetime.now().strftime("%Y%m%d"),
            "managed_by": "unified_pipeline"
        }
        
        # Create the dataset
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"✅ Created dataset {PROJECT_ID}.{DATASET_ID}")
        
        # Set dataset access controls (optional)
        print("Setting dataset permissions...")
        
        # Get current access entries
        entries = list(dataset.access_entries)
        
        # Add service account access if needed
        service_account = f"unified-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com"
        
        # Grant WRITER role to service account
        entries.append(
            bigquery.AccessEntry(
                role="WRITER",
                entity_type="userByEmail",
                entity_id=service_account,
            )
        )
        
        dataset.access_entries = entries
        dataset = client.update_dataset(dataset, ["access_entries"])
        
        print(f"✅ Granted WRITER access to {service_account}")
        
        return dataset
        
    except Conflict:
        print(f"⚠️  Dataset {DATASET_ID} already exists")
        return client.get_dataset(dataset_ref)
    except Exception as e:
        print(f"❌ Error creating dataset: {e}")
        sys.exit(1)


def create_sample_tables(client, dataset):
    """Create sample table schemas for reference"""
    
    print("\n📊 Creating sample table schemas...")
    
    # Define sample schemas for key tables
    tables = {
        "CLIENT_DIMENSION": [
            bigquery.SchemaField("CLIENT_ID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("CLIENT", "STRING"),
            bigquery.SchemaField("DATE_CREATED", "TIMESTAMP"),
            bigquery.SchemaField("CLIENT_TYPE", "STRING"),
            bigquery.SchemaField("CLIENT_PRIMARY_EMAIL_ADDRESS", "STRING"),
            bigquery.SchemaField("LAST_SYNC_TIME", "TIMESTAMP"),
        ],
        "USER_DIMENSION": [
            bigquery.SchemaField("USER_ID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("USER_NAME", "STRING"),
            bigquery.SchemaField("USER_EMAIL", "STRING"),
            bigquery.SchemaField("DEPARTMENT", "STRING"),
            bigquery.SchemaField("IS_ACTIVE", "BOOLEAN"),
            bigquery.SchemaField("LAST_SYNC_TIME", "TIMESTAMP"),
        ],
        "PIPELINE_METADATA": [
            bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_run_time", "TIMESTAMP"),
            bigquery.SchemaField("last_run_status", "STRING"),
            bigquery.SchemaField("rows_processed", "INT64"),
            bigquery.SchemaField("duration_seconds", "FLOAT64"),
            bigquery.SchemaField("error_message", "STRING"),
        ]
    }
    
    for table_name, schema in tables.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        try:
            # Check if table exists
            client.get_table(table_id)
            print(f"  ⏭️  Table {table_name} already exists")
        except:
            # Create table
            table = bigquery.Table(table_id, schema=schema)
            table.description = f"Sample schema for {table_name}"
            
            table = client.create_table(table)
            print(f"  ✅ Created table {table_name}")


def create_views(client, dataset):
    """Create useful views for monitoring"""
    
    print("\n📈 Creating monitoring views...")
    
    # Pipeline status view
    view_id = f"{PROJECT_ID}.{DATASET_ID}.v_pipeline_status"
    view_query = f"""
    SELECT 
        pipeline_name,
        last_run_time,
        last_run_status,
        rows_processed,
        duration_seconds,
        CASE 
            WHEN last_run_status = 'success' THEN '✅'
            WHEN last_run_status = 'warning' THEN '⚠️'
            WHEN last_run_status = 'error' THEN '❌'
            ELSE '⏸️'
        END as status_icon,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_run_time, HOUR) as hours_since_last_run
    FROM `{PROJECT_ID}.{DATASET_ID}.PIPELINE_METADATA`
    ORDER BY pipeline_name
    """
    
    try:
        view = bigquery.Table(view_id)
        view.view_query = view_query
        view.description = "Pipeline status monitoring view"
        
        view = client.create_table(view)
        print(f"  ✅ Created view v_pipeline_status")
    except Conflict:
        print(f"  ⏭️  View v_pipeline_status already exists")
    except Exception as e:
        print(f"  ⚠️  Could not create view: {e}")


def main():
    """Main execution"""
    
    # Check for credentials
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and not os.getenv("GOOGLE_CLOUD_PROJECT"):
        print("\n⚠️  WARNING: Google Cloud credentials not detected")
        print("Please ensure you have either:")
        print("  1. GOOGLE_APPLICATION_CREDENTIALS environment variable set")
        print("  2. Application Default Credentials configured (gcloud auth application-default login)")
        print("")
        
        response = input("Do you want to continue? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)
    
    # Create dataset
    client = bigquery.Client(project=PROJECT_ID)
    dataset = create_dataset()
    
    # Create sample tables
    create_sample_tables(client, dataset)
    
    # Create views
    create_views(client, dataset)
    
    print("\n" + "=" * 60)
    print("✅ BigQuery Setup Complete!")
    print("=" * 60)
    print(f"\n📊 Dataset: {PROJECT_ID}:{DATASET_ID}")
    print(f"🔗 Console: https://console.cloud.google.com/bigquery?project={PROJECT_ID}&ws=!1m4!1m3!3m2!1s{PROJECT_ID}!2s{DATASET_ID}")
    print("\n📝 Next Steps:")
    print("  1. Update pipeline_config.yaml with the new dataset ID")
    print("  2. Deploy Cloud Functions using deploy.sh or manual deployment")
    print("  3. Set up Cloud Scheduler jobs")
    print("  4. Test the pipeline with test_pipeline.sh")


if __name__ == "__main__":
    main()