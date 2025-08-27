import functions_framework
import subprocess
import sys
import os
import json

@functions_framework.http
def monitor_schedulers(request):
    """HTTP Cloud Function to monitor scheduler health."""
    try:
        # Parse request data
        request_json = request.get_json(silent=True) or {}
        
        # Build command arguments
        cmd_args = [sys.executable, 'pipeline_scheduler_monitor.py']
        
        # Add flags based on request
        if request_json.get('auto_fix', False):
            cmd_args.append('--auto-fix')
        
        if request_json.get('daily_report', False):
            cmd_args.append('--daily-report')
        
        # Set environment variables
        env = os.environ.copy()
        env['NOTIFICATION_EMAIL'] = os.getenv('NOTIFICATION_EMAIL', 'paulrichardson@fiskalfinance.com')
        
        # Run the monitoring script
        result = subprocess.run(
            cmd_args, 
            capture_output=True, 
            text=True, 
            cwd=os.path.dirname(__file__),
            env=env
        )
        
        output = result.stdout
        if result.stderr:
            output += f"\nSTDERR:\n{result.stderr}"
        
        if result.returncode == 0:
            return {"status": "success", "message": "Monitor completed successfully", "output": output}, 200
        else:
            return {"status": "warning", "message": "Issues detected or handled", "output": output}, 200
            
    except Exception as e:
        return {"status": "error", "message": f"Monitor failed: {str(e)}"}, 500
