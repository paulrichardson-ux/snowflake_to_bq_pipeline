"""
Unified Pipeline Cloud Function Entry Point
===========================================

This module provides the main entry point for the unified pipeline system.
It handles HTTP requests and routes them to the appropriate pipeline.
"""

import os
import json
import traceback
from typing import Dict, Any
from flask import Request, jsonify
import functions_framework

from core.pipeline_base import PipelineBase
from core.monitoring import StructuredLogger, alert_manager
from core.credentials_manager import connection_pool

# Initialize logger
logger = StructuredLogger("unified_pipeline")


class UnifiedPipeline(PipelineBase):
    """Unified pipeline implementation that can handle any configured pipeline"""
    
    def __init__(self, pipeline_name: str):
        """Initialize the unified pipeline with dynamic configuration"""
        super().__init__(pipeline_name)
        logger.set_context(pipeline=pipeline_name)


@functions_framework.http
def pipeline_handler(request: Request):
    """
    Main Cloud Function entry point for unified pipeline
    
    Expected request format:
    {
        "pipeline": "client_dimension",  # Required: pipeline name from config
        "sync_type": "full",  # Optional: override sync type (full/incremental)
        "dry_run": false  # Optional: perform validation only
    }
    """
    
    # Set CORS headers for browser requests
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        # Parse request
        request_json = request.get_json(silent=True) or {}
        
        # Validate required parameters
        pipeline_name = request_json.get('pipeline')
        if not pipeline_name:
            return jsonify({
                "status": "error",
                "message": "Missing required parameter: 'pipeline'"
            }), 400, headers
        
        # Optional parameters
        sync_type_override = request_json.get('sync_type')
        dry_run = request_json.get('dry_run', False)
        
        logger.info(
            f"Processing pipeline request",
            pipeline=pipeline_name,
            sync_type=sync_type_override,
            dry_run=dry_run
        )
        
        # Initialize pipeline
        try:
            pipeline = UnifiedPipeline(pipeline_name)
        except KeyError:
            return jsonify({
                "status": "error",
                "message": f"Unknown pipeline: {pipeline_name}"
            }), 400, headers
        
        # Override sync type if specified
        if sync_type_override:
            pipeline.pipeline_config['sync_type'] = sync_type_override
        
        # Run pipeline
        if dry_run:
            # Validation only
            result = {
                "status": "success",
                "mode": "dry_run",
                "pipeline": pipeline_name,
                "validation": pipeline.validate_sync()
            }
        else:
            # Full execution
            result = pipeline.run()
        
        # Check for validation warnings
        if result.get('validation', {}).get('status') == 'warning':
            alert_manager.send_validation_warning(pipeline_name, result['validation'])
        
        logger.info(
            f"Pipeline completed successfully",
            pipeline=pipeline_name,
            rows_processed=result.get('rows_processed', 0),
            duration=result.get('duration_seconds', 0)
        )
        
        return jsonify(result), 200, headers
        
    except Exception as e:
        error_message = f"Pipeline execution failed: {str(e)}"
        logger.error(error_message, exception=e)
        
        # Send alert for pipeline failure
        alert_manager.send_pipeline_failure_alert(
            pipeline_name=pipeline_name if 'pipeline_name' in locals() else 'unknown',
            error=e,
            context=request_json if 'request_json' in locals() else {}
        )
        
        return jsonify({
            "status": "error",
            "message": error_message,
            "details": traceback.format_exc() if os.getenv('DEBUG') else None
        }), 500, headers
    
    finally:
        # Clean up connections
        try:
            connection_pool.close_all()
        except:
            pass


@functions_framework.http
def batch_pipeline_handler(request: Request):
    """
    Handle batch pipeline execution requests
    
    Expected request format:
    {
        "pipelines": ["client_dimension", "user_dimension"],  # List of pipelines to run
        "parallel": true  # Optional: run pipelines in parallel (default: false)
    }
    """
    
    # Set CORS headers
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        # Parse request
        request_json = request.get_json(silent=True) or {}
        pipelines = request_json.get('pipelines', [])
        parallel = request_json.get('parallel', False)
        
        if not pipelines:
            return jsonify({
                "status": "error",
                "message": "No pipelines specified"
            }), 400, headers
        
        results = []
        
        if parallel:
            # Parallel execution (using threading for simplicity)
            import concurrent.futures
            
            def run_pipeline(pipeline_name):
                try:
                    pipeline = UnifiedPipeline(pipeline_name)
                    return pipeline.run()
                except Exception as e:
                    return {
                        "status": "error",
                        "pipeline": pipeline_name,
                        "message": str(e)
                    }
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(run_pipeline, p): p for p in pipelines}
                
                for future in concurrent.futures.as_completed(futures):
                    pipeline_name = futures[future]
                    try:
                        result = future.result(timeout=600)  # 10 minute timeout
                        results.append(result)
                    except Exception as e:
                        results.append({
                            "status": "error",
                            "pipeline": pipeline_name,
                            "message": f"Execution failed: {str(e)}"
                        })
        else:
            # Sequential execution
            for pipeline_name in pipelines:
                try:
                    pipeline = UnifiedPipeline(pipeline_name)
                    result = pipeline.run()
                    results.append(result)
                except Exception as e:
                    results.append({
                        "status": "error",
                        "pipeline": pipeline_name,
                        "message": str(e)
                    })
        
        # Aggregate results
        total_success = sum(1 for r in results if r.get('status') == 'success')
        total_error = sum(1 for r in results if r.get('status') == 'error')
        
        return jsonify({
            "status": "completed",
            "summary": {
                "total": len(pipelines),
                "successful": total_success,
                "failed": total_error
            },
            "results": results
        }), 200, headers
        
    except Exception as e:
        error_message = f"Batch pipeline execution failed: {str(e)}"
        logger.error(error_message, exception=e)
        
        return jsonify({
            "status": "error",
            "message": error_message
        }), 500, headers


@functions_framework.http
def pipeline_status_handler(request: Request):
    """
    Get status and metrics for pipelines
    
    Expected request format:
    {
        "pipeline": "client_dimension"  # Optional: specific pipeline
    }
    """
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        from core.monitoring import metrics_collector
        
        # Get metrics summary
        metrics_summary = metrics_collector.get_metrics_summary()
        
        # Parse request for specific pipeline
        request_json = request.get_json(silent=True) or {}
        pipeline_name = request_json.get('pipeline')
        
        if pipeline_name:
            # Return metrics for specific pipeline
            pipeline_metrics = metrics_summary.get(pipeline_name, {})
            return jsonify({
                "status": "success",
                "pipeline": pipeline_name,
                "metrics": pipeline_metrics
            }), 200, headers
        else:
            # Return all metrics
            return jsonify({
                "status": "success",
                "metrics": metrics_summary
            }), 200, headers
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500, headers


# Health check endpoint
@functions_framework.http
def health_check(request: Request):
    """Simple health check endpoint"""
    return jsonify({"status": "healthy", "service": "unified-pipeline"}), 200