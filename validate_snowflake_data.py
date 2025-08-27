#!/usr/bin/env python3
"""
Validate work item JvqmhFJBFGP in Snowflake source tables
"""

import os
import sys
from google.cloud import secretmanager
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

def validate_work_item_in_snowflake(work_item_id="JvqmhFJBFGP"):
    """Validate if work item exists in Snowflake source tables"""
    
    try:
        print(f"🔗 Getting Snowflake credentials...")
        sf_creds = get_snowflake_creds()
        print("✅ Credentials retrieved successfully")
        
        print("🔌 Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=sf_creds['user'],
            password=sf_creds['password'],
            account=sf_creds['account'],
            warehouse=sf_creds['warehouse'],
            database=sf_creds['database'],
            schema=sf_creds['schema']
        )
        cs = conn.cursor()
        print("✅ Connected to Snowflake successfully")
        print(f"📂 Database: {sf_creds['database']}")
        print(f"📂 Schema: {sf_creds['schema']}")
        
        # Check WORK_ITEM_DETAILS table
        print(f"\n🔍 Checking WORK_ITEM_DETAILS for work item: {work_item_id}")
        cs.execute(f"""
            SELECT WORK_ITEM_ID, WORK_TITLE, PRIMARY_STATUS, SECONDARY_STATUS, 
                   CREATED_DATETIME, DUE_DATETIME, CLIENT, REPORTING_DATE
            FROM {sf_creds['schema']}.WORK_ITEM_DETAILS 
            WHERE WORK_ITEM_ID = '{work_item_id}'
            ORDER BY REPORTING_DATE DESC
            LIMIT 5
        """)
        
        work_item_rows = cs.fetchall()
        if work_item_rows:
            print(f"✅ Found {len(work_item_rows)} records in WORK_ITEM_DETAILS:")
            for i, row in enumerate(work_item_rows):
                print(f"  {i+1}. 📋 ID: {row[0]}")
                print(f"     📝 Title: {row[1]}")
                print(f"     📊 Status: {row[2]} / {row[3]}")
                print(f"     👤 Client: {row[6]}")
                print(f"     📅 Reporting Date: {row[7]}")
                print(f"     📅 Due Date: {row[5]}")
                print()
        else:
            print("❌ No records found in WORK_ITEM_DETAILS")
        
        # Check WORK_ITEM_BUDGET_VS_ACTUAL table
        print(f"🔍 Checking WORK_ITEM_BUDGET_VS_ACTUAL for work item: {work_item_id}")
        cs.execute(f"""
            SELECT WORK_ITEM_ID, WORK_TITLE, USER_NAME, BUDGETED_MINUTES, 
                   ACTUAL_MINUTES, REPORTING_DATE, CLIENT
            FROM {sf_creds['schema']}.WORK_ITEM_BUDGET_VS_ACTUAL 
            WHERE WORK_ITEM_ID = '{work_item_id}'
            ORDER BY REPORTING_DATE DESC
            LIMIT 10
        """)
        
        budget_rows = cs.fetchall()
        if budget_rows:
            print(f"✅ Found {len(budget_rows)} records in WORK_ITEM_BUDGET_VS_ACTUAL:")
            for i, row in enumerate(budget_rows):
                print(f"  {i+1}. 💰 ID: {row[0]}")
                print(f"     👤 User: {row[2]}")
                print(f"     💵 Budget: {row[3]} minutes")
                print(f"     ⏱️  Actual: {row[4]} minutes")
                print(f"     📅 Date: {row[5]}")
                print()
        else:
            print("❌ No records found in WORK_ITEM_BUDGET_VS_ACTUAL")
        
        # Check for any Next Innovations VAT201 work items in 2025
        print(f"\n🔍 Checking for similar Next Innovations VAT201 2025 work items...")
        cs.execute(f"""
            SELECT WORK_ITEM_ID, WORK_TITLE, PRIMARY_STATUS, CLIENT, REPORTING_DATE
            FROM {sf_creds['schema']}.WORK_ITEM_DETAILS 
            WHERE WORK_TITLE LIKE '%Next Innovations%VAT201%2025%'
               OR (CLIENT LIKE '%Next Innovations%' AND WORK_TITLE LIKE '%VAT201%' AND WORK_TITLE LIKE '%2025%')
            ORDER BY REPORTING_DATE DESC
            LIMIT 10
        """)
        
        similar_rows = cs.fetchall()
        if similar_rows:
            print(f"✅ Found {len(similar_rows)} similar Next Innovations VAT201 2025 work items:")
            for i, row in enumerate(similar_rows):
                print(f"  {i+1}. 📋 ID: {row[0]}")
                print(f"     📝 Title: {row[1]}")
                print(f"     📊 Status: {row[2]}")
                print(f"     📅 Date: {row[4]}")
                print()
        else:
            print("❌ No similar Next Innovations VAT201 2025 work items found")
        
        conn.close()
        print("✅ Snowflake validation complete!")
        
        return {
            "work_item_details_count": len(work_item_rows) if work_item_rows else 0,
            "budget_vs_actual_count": len(budget_rows) if budget_rows else 0,
            "similar_items_count": len(similar_rows) if similar_rows else 0
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    work_item_id = sys.argv[1] if len(sys.argv) > 1 else "JvqmhFJBFGP"
    print(f"🎯 Validating work item: {work_item_id}")
    print("=" * 60)
    
    results = validate_work_item_in_snowflake(work_item_id)
    
    if results:
        print("\n📊 SUMMARY:")
        print(f"  • Work Item Details records: {results['work_item_details_count']}")
        print(f"  • Budget vs Actual records: {results['budget_vs_actual_count']}")
        print(f"  • Similar items found: {results['similar_items_count']}")
    else:
        print("\n❌ Validation failed - check error messages above")
