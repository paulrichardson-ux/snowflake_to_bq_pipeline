#!/usr/bin/env python3
"""
Python-based Deployment Script for Unified Pipeline
===================================================

This script can deploy the pipeline using Python libraries instead of gcloud CLI.
Requires: google-cloud-bigquery, google-cloud-functions, google-cloud-scheduler
"""

import os
import sys
import json
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime

try:
    from google.cloud import bigquery
    from google.cloud import functions_v2
    from google.cloud import scheduler_v1
    from google.cloud import secretmanager
    from google.api_core import exceptions
except ImportError:
    print("❌ Required libraries not installed")
    print("Please run: pip install google-cloud-bigquery google-cloud-functions google-cloud-scheduler google-cloud-secret-manager")
    sys.exit(1)

# Configuration
PROJECT_ID = "red-octane-444308-f4"
REGION = "us-central1"
LOCATION = "US"
DATASET_ID = "unified_pipeline_data"
SERVICE_ACCOUNT = f"unified-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com"


class UnifiedPipelineDeployer:
    """Deploys the Unified Pipeline using Python APIs"""
    
    def __init__(self):
        self.project_id = PROJECT_ID
        self.region = REGION
        self.dataset_id = DATASET_ID
        
        # Initialize clients
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.functions_client = functions_v2.FunctionServiceClient()
        self.scheduler_client = scheduler_v1.CloudSchedulerClient()
        self.secrets_client = secretmanager.SecretManagerServiceClient()
        
        print("🚀 Unified Pipeline Python Deployment")
        print("=" * 60)
        print(f"Project:  {PROJECT_ID}")
        print(f"Region:   {REGION}")
        print(f"Dataset:  {DATASET_ID}")
        print("=" * 60)
    
    def create_dataset(self):
        """Create BigQuery dataset"""
        print("\n📊 Creating BigQuery Dataset...")
        
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        
        try:
            dataset = self.bq_client.get_dataset(dataset_ref)
            print(f"  ✅ Dataset {self.dataset_id} already exists")
            return dataset
        except exceptions.NotFound:
            # Create new dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = LOCATION
            dataset.description = "Unified Snowflake to BigQuery pipeline data"
            
            dataset = self.bq_client.create_dataset(dataset, timeout=30)
            print(f"  ✅ Created dataset {self.dataset_id}")
            return dataset
    
    def check_secrets(self):
        """Check if required secrets exist"""
        print("\n🔐 Checking Secret Manager...")
        
        required_secrets = [
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA"
        ]
        
        missing_secrets = []
        
        for secret_name in required_secrets:
            secret_path = f"projects/{self.project_id}/secrets/{secret_name}"
            try:
                self.secrets_client.get_secret(request={"name": secret_path})
                print(f"  ✅ {secret_name} exists")
            except exceptions.NotFound:
                print(f"  ❌ {secret_name} missing")
                missing_secrets.append(secret_name)
        
        if missing_secrets:
            print("\n⚠️  Missing secrets detected!")
            print("Please create them using:")
            for secret in missing_secrets:
                print(f'  echo "your_value" | gcloud secrets create {secret} --data-file=-')
            return False
        
        return True
    
    def create_sample_tables(self):
        """Create sample tables with schemas"""
        print("\n📋 Creating Sample Tables...")
        
        # Pipeline metadata table for tracking
        table_id = f"{self.project_id}.{self.dataset_id}.pipeline_metadata"
        
        schema = [
            bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("start_time", "TIMESTAMP"),
            bigquery.SchemaField("end_time", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("rows_processed", "INT64"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]
        
        try:
            self.bq_client.get_table(table_id)
            print(f"  ⏭️  Table pipeline_metadata already exists")
        except exceptions.NotFound:
            table = bigquery.Table(table_id, schema=schema)
            table.description = "Pipeline execution metadata and history"
            
            table = self.bq_client.create_table(table)
            print(f"  ✅ Created table pipeline_metadata")
    
    def create_deployment_package(self):
        """Create a deployment package (zip file) for Cloud Functions"""
        print("\n📦 Creating deployment package...")
        
        # Create temporary zip file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            zip_path = tmp_file.name
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add all Python files
            base_path = Path(__file__).parent
            
            # Add main.py from src/
            src_main = base_path / 'src' / 'main.py'
            if src_main.exists():
                zipf.write(src_main, 'main.py')
            
            # Add core modules
            core_path = base_path / 'src' / 'core'
            if core_path.exists():
                for file in core_path.glob('*.py'):
                    zipf.write(file, f'core/{file.name}')
            
            # Add config
            config_file = base_path / 'config' / 'pipeline_config.yaml'
            if config_file.exists():
                zipf.write(config_file, 'config/pipeline_config.yaml')
            
            # Add requirements.txt
            req_file = base_path / 'requirements.txt'
            if req_file.exists():
                zipf.write(req_file, 'requirements.txt')
        
        print(f"  ✅ Created deployment package: {zip_path}")
        return zip_path
    
    def deploy_function_instructions(self):
        """Provide instructions for manual deployment"""
        print("\n🚀 Cloud Function Deployment Instructions")
        print("=" * 60)
        
        print("\nSince Cloud Functions v2 API requires additional setup,")
        print("please deploy using the gcloud CLI with these commands:\n")
        
        print("# Deploy main pipeline function")
        print(f"""gcloud functions deploy unified-snowflake-bq-pipeline \\
    --gen2 \\
    --region={REGION} \\
    --runtime=python311 \\
    --source=. \\
    --entry-point=pipeline_handler \\
    --trigger-http \\
    --allow-unauthenticated \\
    --service-account={SERVICE_ACCOUNT} \\
    --set-env-vars="GOOGLE_CLOUD_PROJECT={PROJECT_ID},BQ_DATASET={DATASET_ID}" \\
    --memory=2GB \\
    --timeout=540s
""")
        
        print("\n# Deploy batch pipeline function")
        print(f"""gcloud functions deploy unified-snowflake-bq-pipeline-batch \\
    --gen2 \\
    --region={REGION} \\
    --runtime=python311 \\
    --source=. \\
    --entry-point=batch_pipeline_handler \\
    --trigger-http \\
    --allow-unauthenticated \\
    --service-account={SERVICE_ACCOUNT} \\
    --set-env-vars="GOOGLE_CLOUD_PROJECT={PROJECT_ID},BQ_DATASET={DATASET_ID}" \\
    --memory=4GB \\
    --timeout=540s
""")
        
        print("\n# Deploy status function")
        print(f"""gcloud functions deploy unified-snowflake-bq-pipeline-status \\
    --gen2 \\
    --region={REGION} \\
    --runtime=python311 \\
    --source=. \\
    --entry-point=pipeline_status_handler \\
    --trigger-http \\
    --allow-unauthenticated \\
    --service-account={SERVICE_ACCOUNT} \\
    --set-env-vars="GOOGLE_CLOUD_PROJECT={PROJECT_ID},BQ_DATASET={DATASET_ID}" \\
    --memory=512MB \\
    --timeout=60s
""")
    
    def create_test_script(self):
        """Create a test script for validation"""
        print("\n🧪 Creating Test Script...")
        
        test_script = """#!/bin/bash
# Test script for Unified Pipeline

FUNCTION_URL="https://unified-snowflake-bq-pipeline-XXXXX.cloudfunctions.net"

echo "Testing pipeline..."
curl -X POST $FUNCTION_URL \\
  -H 'Content-Type: application/json' \\
  -d '{"pipeline": "client_dimension", "dry_run": true}'
"""
        
        script_path = Path(__file__).parent / 'test_unified_pipeline.sh'
        script_path.write_text(test_script)
        script_path.chmod(0o755)
        
        print(f"  ✅ Created test script: {script_path}")
    
    def run(self):
        """Run the deployment process"""
        
        # Step 1: Create dataset
        self.create_dataset()
        
        # Step 2: Check secrets
        secrets_ok = self.check_secrets()
        if not secrets_ok:
            print("\n⚠️  Please create missing secrets before deploying functions")
        
        # Step 3: Create sample tables
        self.create_sample_tables()
        
        # Step 4: Create deployment package
        # zip_path = self.create_deployment_package()
        
        # Step 5: Provide deployment instructions
        self.deploy_function_instructions()
        
        # Step 6: Create test script
        self.create_test_script()
        
        print("\n" + "=" * 60)
        print("✅ Preparation Complete!")
        print("=" * 60)
        print(f"\n📊 BigQuery Dataset: {PROJECT_ID}:{DATASET_ID}")
        print(f"🔗 Console: https://console.cloud.google.com/bigquery?project={PROJECT_ID}")
        print("\n📝 Next Steps:")
        print("  1. Create any missing secrets (see above)")
        print("  2. Run the gcloud commands shown above to deploy functions")
        print("  3. Test using the generated test script")
        print("  4. Set up Cloud Scheduler jobs (see DEPLOYMENT_GUIDE.md)")
        print("\n📚 Full instructions in DEPLOYMENT_GUIDE.md")


def main():
    """Main entry point"""
    
    # Check for credentials
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and not os.getenv("GOOGLE_CLOUD_PROJECT"):
        print("\n⚠️  Google Cloud credentials not detected")
        print("Please ensure you have either:")
        print("  1. GOOGLE_APPLICATION_CREDENTIALS environment variable set")
        print("  2. Application Default Credentials (run: gcloud auth application-default login)")
        print("")
        
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)
    
    deployer = UnifiedPipelineDeployer()
    deployer.run()


if __name__ == "__main__":
    main()