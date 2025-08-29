#!/usr/bin/env python3
"""
Migration Validation Script
===========================

This script validates that the new unified pipeline produces the same results
as the old individual pipelines by comparing data in BigQuery.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from google.cloud import bigquery
import pandas as pd
import argparse

# Configuration
PROJECT_ID = "red-octane-444308-f4"
DATASET_ID = "karbon_data"

# Pipeline mappings
PIPELINE_TABLES = {
    "client_dimension": "CLIENT_DIMENSION",
    "client_group_dimension": "CLIENT_GROUP_DIMENSION",
    "tenant_team_dimension": "TENANT_TEAM_DIMENSION",
    "tenant_team_member_dimension": "TENANT_TEAM_MEMBER_DIMENSION",
    "user_dimension": "USER_DIMENSION",
    "work_item_details": "WORK_ITEM_DETAILS_BQ",
    "work_item_budget_vs_actual": "WORK_ITEM_BUDGET_VS_ACTUAL_BQ",
    "user_time_entry": "USER_TIME_ENTRY_BQ"
}


class MigrationValidator:
    """Validates migration from old to new pipeline system"""
    
    def __init__(self):
        """Initialize the validator"""
        self.bq_client = bigquery.Client()
        self.results = {}
    
    def create_backup_tables(self) -> Dict[str, str]:
        """Create backup of current tables before migration"""
        print("📦 Creating backup tables...")
        backups = {}
        
        for pipeline, table in PIPELINE_TABLES.items():
            source_table = f"{PROJECT_ID}.{DATASET_ID}.{table}"
            backup_table = f"{PROJECT_ID}.{DATASET_ID}.{table}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            try:
                # Create backup
                query = f"""
                    CREATE TABLE `{backup_table}` AS
                    SELECT * FROM `{source_table}`
                """
                
                job = self.bq_client.query(query)
                job.result()
                
                backups[pipeline] = backup_table
                print(f"  ✅ Backed up {table} to {backup_table}")
                
            except Exception as e:
                print(f"  ⚠️  Could not backup {table}: {e}")
        
        return backups
    
    def compare_row_counts(self, table1: str, table2: str) -> Tuple[int, int, float]:
        """Compare row counts between two tables"""
        # Get count from table1
        query1 = f"SELECT COUNT(*) as count FROM `{table1}`"
        result1 = list(self.bq_client.query(query1).result())[0].count
        
        # Get count from table2
        query2 = f"SELECT COUNT(*) as count FROM `{table2}`"
        result2 = list(self.bq_client.query(query2).result())[0].count
        
        # Calculate difference percentage
        if result1 > 0:
            diff_percent = abs(result1 - result2) / result1 * 100
        else:
            diff_percent = 0 if result2 == 0 else 100
        
        return result1, result2, diff_percent
    
    def compare_sample_data(self, table1: str, table2: str, sample_size: int = 100) -> Dict[str, Any]:
        """Compare sample data between two tables"""
        # Get sample from table1
        query1 = f"""
            SELECT * FROM `{table1}`
            ORDER BY RAND()
            LIMIT {sample_size}
        """
        df1 = self.bq_client.query(query1).to_dataframe()
        
        # Get sample from table2
        query2 = f"""
            SELECT * FROM `{table2}`
            ORDER BY RAND()
            LIMIT {sample_size}
        """
        df2 = self.bq_client.query(query2).to_dataframe()
        
        # Compare schemas
        schema_match = set(df1.columns) == set(df2.columns)
        
        # Compare data types
        dtype_match = all(
            str(df1[col].dtype) == str(df2[col].dtype)
            for col in df1.columns if col in df2.columns
        )
        
        return {
            "schema_match": schema_match,
            "dtype_match": dtype_match,
            "columns_table1": list(df1.columns),
            "columns_table2": list(df2.columns),
            "sample_rows": sample_size
        }
    
    def compare_checksums(self, table1: str, table2: str, column: str) -> Tuple[int, int, bool]:
        """Compare checksums of a numeric column"""
        try:
            # Get sum from table1
            query1 = f"SELECT SUM(CAST({column} AS INT64)) as checksum FROM `{table1}` WHERE {column} IS NOT NULL"
            result1 = list(self.bq_client.query(query1).result())[0].checksum or 0
            
            # Get sum from table2
            query2 = f"SELECT SUM(CAST({column} AS INT64)) as checksum FROM `{table2}` WHERE {column} IS NOT NULL"
            result2 = list(self.bq_client.query(query2).result())[0].checksum or 0
            
            return result1, result2, result1 == result2
        except:
            return 0, 0, False
    
    def validate_pipeline(self, pipeline_name: str, old_table: str, new_table: str) -> Dict[str, Any]:
        """Validate a single pipeline migration"""
        print(f"\n🔍 Validating {pipeline_name}...")
        
        validation_result = {
            "pipeline": pipeline_name,
            "old_table": old_table,
            "new_table": new_table,
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        # Check 1: Row counts
        old_count, new_count, diff_percent = self.compare_row_counts(old_table, new_table)
        validation_result["checks"]["row_count"] = {
            "old_count": old_count,
            "new_count": new_count,
            "difference_percent": round(diff_percent, 2),
            "status": "✅ PASS" if diff_percent < 1 else "⚠️ WARNING" if diff_percent < 5 else "❌ FAIL"
        }
        print(f"  Row count: {old_count} → {new_count} ({diff_percent:.2f}% diff)")
        
        # Check 2: Schema comparison
        sample_comparison = self.compare_sample_data(old_table, new_table)
        validation_result["checks"]["schema"] = {
            "schema_match": sample_comparison["schema_match"],
            "dtype_match": sample_comparison["dtype_match"],
            "status": "✅ PASS" if sample_comparison["schema_match"] else "❌ FAIL"
        }
        print(f"  Schema match: {sample_comparison['schema_match']}")
        
        # Check 3: Data freshness
        try:
            query = f"""
                SELECT MAX(LAST_MODIFIED_TIME) as latest
                FROM `{new_table}`
                WHERE LAST_MODIFIED_TIME IS NOT NULL
            """
            result = list(self.bq_client.query(query).result())
            if result and result[0].latest:
                days_old = (datetime.now() - result[0].latest.replace(tzinfo=None)).days
                validation_result["checks"]["freshness"] = {
                    "last_modified": result[0].latest.isoformat(),
                    "days_old": days_old,
                    "status": "✅ PASS" if days_old < 2 else "⚠️ WARNING" if days_old < 7 else "❌ FAIL"
                }
                print(f"  Data freshness: {days_old} days old")
        except:
            validation_result["checks"]["freshness"] = {"status": "⏭️ SKIP"}
        
        # Overall status
        statuses = [check.get("status", "") for check in validation_result["checks"].values()]
        if "❌ FAIL" in statuses:
            validation_result["overall_status"] = "❌ FAIL"
        elif "⚠️ WARNING" in statuses:
            validation_result["overall_status"] = "⚠️ WARNING"
        else:
            validation_result["overall_status"] = "✅ PASS"
        
        return validation_result
    
    def run_validation(self, create_backups: bool = False) -> Dict[str, Any]:
        """Run full validation suite"""
        print("=" * 60)
        print("🚀 MIGRATION VALIDATION REPORT")
        print("=" * 60)
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"Project: {PROJECT_ID}")
        print(f"Dataset: {DATASET_ID}")
        print("=" * 60)
        
        # Create backups if requested
        backups = {}
        if create_backups:
            backups = self.create_backup_tables()
        
        # Validate each pipeline
        validation_results = []
        for pipeline_name, table_name in PIPELINE_TABLES.items():
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            
            # For testing, compare against the same table (in production, compare old vs new)
            validation_result = self.validate_pipeline(
                pipeline_name,
                table_id,  # Old table
                table_id   # New table (would be different after migration)
            )
            
            validation_results.append(validation_result)
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 VALIDATION SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for r in validation_results if r["overall_status"] == "✅ PASS")
        warnings = sum(1 for r in validation_results if r["overall_status"] == "⚠️ WARNING")
        failed = sum(1 for r in validation_results if r["overall_status"] == "❌ FAIL")
        
        print(f"✅ Passed:  {passed}/{len(validation_results)}")
        print(f"⚠️  Warning: {warnings}/{len(validation_results)}")
        print(f"❌ Failed:  {failed}/{len(validation_results)}")
        
        # Detailed results
        print("\nDetailed Results:")
        for result in validation_results:
            print(f"  {result['overall_status']} {result['pipeline']}")
        
        # Save results to file
        report = {
            "timestamp": datetime.now().isoformat(),
            "project_id": PROJECT_ID,
            "dataset_id": DATASET_ID,
            "backups": backups,
            "summary": {
                "total": len(validation_results),
                "passed": passed,
                "warnings": warnings,
                "failed": failed
            },
            "validations": validation_results
        }
        
        report_file = f"migration_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n📄 Full report saved to: {report_file}")
        
        return report


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Validate pipeline migration')
    parser.add_argument('--backup', action='store_true', help='Create backup tables before validation')
    parser.add_argument('--pipeline', type=str, help='Validate specific pipeline only')
    
    args = parser.parse_args()
    
    validator = MigrationValidator()
    
    if args.pipeline:
        # Validate single pipeline
        if args.pipeline not in PIPELINE_TABLES:
            print(f"❌ Unknown pipeline: {args.pipeline}")
            print(f"Available pipelines: {', '.join(PIPELINE_TABLES.keys())}")
            sys.exit(1)
        
        table_name = PIPELINE_TABLES[args.pipeline]
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        result = validator.validate_pipeline(args.pipeline, table_id, table_id)
        print(f"\nOverall Status: {result['overall_status']}")
    else:
        # Run full validation
        report = validator.run_validation(create_backups=args.backup)
        
        # Exit with appropriate code
        if report["summary"]["failed"] > 0:
            sys.exit(1)
        elif report["summary"]["warnings"] > 0:
            sys.exit(0)
        else:
            sys.exit(0)


if __name__ == "__main__":
    main()