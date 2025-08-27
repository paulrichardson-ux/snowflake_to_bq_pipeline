#!/usr/bin/env python3
"""
Comprehensive Snowflake-BigQuery Deduplication Sync
====================================================

This script validates and cleans up data inconsistencies between Snowflake and BigQuery.
It removes BigQuery records that no longer exist in Snowflake source tables.

Key Functions:
- Validates work items exist in both Snowflake and BigQuery
- Removes orphaned BigQuery records not found in Snowflake
- Provides detailed reporting on cleanup actions
- Handles all core tables: WORK_ITEM_DETAILS, WORK_ITEM_BUDGET_VS_ACTUAL, USER_TIME_ENTRY

Usage:
- Deploy as Cloud Function for scheduled cleanup
- Run manually for ad-hoc validation
- Integrate with monitoring system
"""

import os
import sys
import json
from datetime import datetime, timedelta
from google.cloud import secretmanager, bigquery
import snowflake.connector

def get_snowflake_creds():
    """Get Snowflake credentials from Secret Manager"""
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "red-octane-444308-f4")
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

def get_snowflake_work_items(sf_conn, days_back=30):
    """Get all work item IDs from Snowflake within the specified date range"""
    cs = sf_conn.cursor()
    
    # Get work items from the last N days to avoid checking too much historical data
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    query = f"""
        SELECT DISTINCT WORK_ITEM_ID
        FROM WORK_ITEM_DETAILS 
        WHERE REPORTING_DATE >= '{cutoff_date}'
        ORDER BY WORK_ITEM_ID
    """
    
    print(f"🔍 Getting Snowflake work items from {cutoff_date} onwards...")
    cs.execute(query)
    work_items = [row[0] for row in cs.fetchall()]
    print(f"✅ Found {len(work_items)} unique work items in Snowflake")
    
    return set(work_items)

def get_bigquery_work_items(bq_client, project_id, dataset_id, days_back=30):
    """Get all work item IDs from BigQuery within the specified date range"""
    
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    query = f"""
        SELECT DISTINCT WORK_ITEM_ID
        FROM `{project_id}.{dataset_id}.WORK_ITEM_DETAILS_BQ`
        WHERE REPORTING_DATE >= '{cutoff_date}'
        ORDER BY WORK_ITEM_ID
    """
    
    print(f"🔍 Getting BigQuery work items from {cutoff_date} onwards...")
    query_job = bq_client.query(query)
    results = query_job.result()
    
    work_items = [row.WORK_ITEM_ID for row in results]
    print(f"✅ Found {len(work_items)} unique work items in BigQuery")
    
    return set(work_items)

def cleanup_orphaned_work_items(bq_client, project_id, dataset_id, orphaned_items, dry_run=True):
    """Remove work items from BigQuery that don't exist in Snowflake"""
    
    if not orphaned_items:
        print("✅ No orphaned work items to clean up")
        return {"cleaned_tables": [], "total_deleted": 0}
    
    print(f"🧹 {'DRY RUN: Would clean up' if dry_run else 'Cleaning up'} {len(orphaned_items)} orphaned work items...")
    
    # Tables to clean up
    tables_to_clean = [
        "WORK_ITEM_DETAILS_BQ",
        "WORK_ITEM_BUDGET_VS_ACTUAL_BQ",
        "USER_TIME_ENTRY_BQ"
    ]
    
    cleanup_results = {"cleaned_tables": [], "total_deleted": 0}
    
    # Convert set to comma-separated string for SQL IN clause
    orphaned_list = "', '".join(orphaned_items)
    
    for table_name in tables_to_clean:
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        # First, check how many records would be affected
        count_query = f"""
            SELECT COUNT(*) as record_count
            FROM `{table_id}`
            WHERE WORK_ITEM_ID IN ('{orphaned_list}')
        """
        
        try:
            count_job = bq_client.query(count_query)
            count_result = list(count_job.result())[0]
            record_count = count_result.record_count
            
            if record_count == 0:
                print(f"  📋 {table_name}: No records to clean")
                continue
            
            print(f"  📋 {table_name}: {'Would delete' if dry_run else 'Deleting'} {record_count} records")
            
            if not dry_run:
                # Execute the delete
                delete_query = f"""
                    DELETE FROM `{table_id}`
                    WHERE WORK_ITEM_ID IN ('{orphaned_list}')
                """
                
                delete_job = bq_client.query(delete_query)
                delete_job.result()  # Wait for completion
                
                if delete_job.errors:
                    print(f"  ❌ Errors deleting from {table_name}: {delete_job.errors}")
                else:
                    actual_deleted = delete_job.num_dml_affected_rows
                    print(f"  ✅ Successfully deleted {actual_deleted} records from {table_name}")
                    cleanup_results["cleaned_tables"].append({
                        "table": table_name,
                        "records_deleted": actual_deleted
                    })
                    cleanup_results["total_deleted"] += actual_deleted
            else:
                cleanup_results["cleaned_tables"].append({
                    "table": table_name,
                    "records_would_delete": record_count
                })
                cleanup_results["total_deleted"] += record_count
                
        except Exception as e:
            print(f"  ❌ Error processing {table_name}: {str(e)}")
    
    return cleanup_results

def validate_specific_work_item(sf_conn, bq_client, project_id, dataset_id, work_item_id):
    """Validate a specific work item across all tables"""
    
    print(f"\n🔍 Validating work item: {work_item_id}")
    print("=" * 50)
    
    validation_results = {
        "work_item_id": work_item_id,
        "snowflake": {},
        "bigquery": {},
        "status": "unknown"
    }
    
    # Check Snowflake
    sf_tables = {
        "WORK_ITEM_DETAILS": "SELECT COUNT(*) FROM WORK_ITEM_DETAILS WHERE WORK_ITEM_ID = %s",
        "WORK_ITEM_BUDGET_VS_ACTUAL": "SELECT COUNT(*) FROM WORK_ITEM_BUDGET_VS_ACTUAL WHERE WORK_ITEM_ID = %s"
    }
    
    cs = sf_conn.cursor()
    for table_name, query in sf_tables.items():
        try:
            cs.execute(query, (work_item_id,))
            count = cs.fetchone()[0]
            validation_results["snowflake"][table_name] = count
            print(f"  📊 Snowflake {table_name}: {count} records")
        except Exception as e:
            print(f"  ❌ Error checking Snowflake {table_name}: {str(e)}")
            validation_results["snowflake"][table_name] = "error"
    
    # Check BigQuery
    bq_tables = {
        "WORK_ITEM_DETAILS_BQ": f"SELECT COUNT(*) as count FROM `{project_id}.{dataset_id}.WORK_ITEM_DETAILS_BQ` WHERE WORK_ITEM_ID = '{work_item_id}'",
        "WORK_ITEM_BUDGET_VS_ACTUAL_BQ": f"SELECT COUNT(*) as count FROM `{project_id}.{dataset_id}.WORK_ITEM_BUDGET_VS_ACTUAL_BQ` WHERE WORK_ITEM_ID = '{work_item_id}'",
        "USER_TIME_ENTRY_BQ": f"SELECT COUNT(*) as count FROM `{project_id}.{dataset_id}.USER_TIME_ENTRY_BQ` WHERE WORK_ITEM_ID = '{work_item_id}'"
    }
    
    for table_name, query in bq_tables.items():
        try:
            query_job = bq_client.query(query)
            result = list(query_job.result())[0]
            count = result.count
            validation_results["bigquery"][table_name] = count
            print(f"  📊 BigQuery {table_name}: {count} records")
        except Exception as e:
            print(f"  ❌ Error checking BigQuery {table_name}: {str(e)}")
            validation_results["bigquery"][table_name] = "error"
    
    # Determine status
    sf_has_data = any(count > 0 for count in validation_results["snowflake"].values() if isinstance(count, int))
    bq_has_data = any(count > 0 for count in validation_results["bigquery"].values() if isinstance(count, int))
    
    if sf_has_data and bq_has_data:
        validation_results["status"] = "consistent"
        print(f"  ✅ Status: CONSISTENT - exists in both systems")
    elif not sf_has_data and bq_has_data:
        validation_results["status"] = "orphaned_in_bq"
        print(f"  ⚠️  Status: ORPHANED IN BIGQUERY - should be cleaned up")
    elif sf_has_data and not bq_has_data:
        validation_results["status"] = "missing_in_bq"
        print(f"  ⚠️  Status: MISSING IN BIGQUERY - needs sync")
    else:
        validation_results["status"] = "not_found"
        print(f"  ❌ Status: NOT FOUND in either system")
    
    return validation_results

def run_deduplication_sync(dry_run=True, days_back=30, specific_work_item=None):
    """Main deduplication sync function"""
    
    print("🚀 SNOWFLAKE-BIGQUERY DEDUPLICATION SYNC")
    print("=" * 60)
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE RUN'}")
    print(f"Date Range: Last {days_back} days")
    if specific_work_item:
        print(f"Specific Item: {specific_work_item}")
    print()
    
    try:
        # Initialize connections
        print("🔗 Initializing connections...")
        sf_creds = get_snowflake_creds()
        sf_conn = snowflake.connector.connect(**sf_creds)
        
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "red-octane-444308-f4")
        dataset_id = "karbon_data"
        bq_client = bigquery.Client(project=project_id)
        
        print("✅ Connections established")
        
        # If specific work item validation requested
        if specific_work_item:
            validation_result = validate_specific_work_item(
                sf_conn, bq_client, project_id, dataset_id, specific_work_item
            )
            sf_conn.close()
            return {"validation": validation_result}
        
        # Get work items from both systems
        sf_work_items = get_snowflake_work_items(sf_conn, days_back)
        bq_work_items = get_bigquery_work_items(bq_client, project_id, dataset_id, days_back)
        
        # Find orphaned items (in BigQuery but not in Snowflake)
        orphaned_items = bq_work_items - sf_work_items
        missing_items = sf_work_items - bq_work_items
        
        print(f"\n📊 ANALYSIS RESULTS:")
        print(f"  • Snowflake work items: {len(sf_work_items)}")
        print(f"  • BigQuery work items: {len(bq_work_items)}")
        print(f"  • Orphaned in BigQuery: {len(orphaned_items)}")
        print(f"  • Missing in BigQuery: {len(missing_items)}")
        
        # Show some examples
        if orphaned_items:
            print(f"\n⚠️  ORPHANED ITEMS (first 10):")
            for item in list(orphaned_items)[:10]:
                print(f"    • {item}")
            if len(orphaned_items) > 10:
                print(f"    • ... and {len(orphaned_items) - 10} more")
        
        if missing_items:
            print(f"\n⚠️  MISSING ITEMS (first 10):")
            for item in list(missing_items)[:10]:
                print(f"    • {item}")
            if len(missing_items) > 10:
                print(f"    • ... and {len(missing_items) - 10} more")
        
        # Clean up orphaned items
        cleanup_results = cleanup_orphaned_work_items(
            bq_client, project_id, dataset_id, orphaned_items, dry_run
        )
        
        sf_conn.close()
        
        # Summary
        print(f"\n🎯 DEDUPLICATION SUMMARY:")
        print(f"  • Mode: {'DRY RUN' if dry_run else 'LIVE RUN'}")
        print(f"  • Orphaned items found: {len(orphaned_items)}")
        print(f"  • Tables processed: {len(cleanup_results['cleaned_tables'])}")
        print(f"  • Total records {'would be deleted' if dry_run else 'deleted'}: {cleanup_results['total_deleted']}")
        
        return {
            "success": True,
            "orphaned_count": len(orphaned_items),
            "missing_count": len(missing_items),
            "cleanup_results": cleanup_results,
            "dry_run": dry_run
        }
        
    except Exception as e:
        print(f"❌ Error during deduplication sync: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

def deduplication_sync_cloud_function(request):
    """Cloud Function entry point for deduplication sync"""
    
    try:
        # Parse request parameters
        request_json = request.get_json(silent=True)
        request_args = request.args
        
        dry_run = True  # Default to dry run for safety
        days_back = 30
        specific_work_item = None
        
        if request_json:
            dry_run = request_json.get('dry_run', True)
            days_back = request_json.get('days_back', 30)
            specific_work_item = request_json.get('work_item_id')
        elif request_args:
            dry_run = request_args.get('dry_run', 'true').lower() == 'true'
            days_back = int(request_args.get('days_back', 30))
            specific_work_item = request_args.get('work_item_id')
        
        # Run the deduplication sync
        results = run_deduplication_sync(dry_run, days_back, specific_work_item)
        
        return json.dumps(results), 200
        
    except Exception as e:
        error_result = {"success": False, "error": str(e)}
        return json.dumps(error_result), 500

if __name__ == "__main__":
    # Command line interface
    import argparse
    
    parser = argparse.ArgumentParser(description='Snowflake-BigQuery Deduplication Sync')
    parser.add_argument('--live', action='store_true', help='Run in live mode (default: dry run)')
    parser.add_argument('--days', type=int, default=30, help='Number of days back to check (default: 30)')
    parser.add_argument('--work-item', type=str, help='Validate specific work item ID')
    
    args = parser.parse_args()
    
    dry_run = not args.live
    results = run_deduplication_sync(dry_run, args.days, args.work_item)
    
    if results.get("success"):
        print("\n✅ Deduplication sync completed successfully!")
    else:
        print("\n❌ Deduplication sync failed!")
        sys.exit(1)
