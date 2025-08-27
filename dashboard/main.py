import functions_framework
from flask import Flask, render_template_string, jsonify, request, redirect, session
from google.cloud import functions_v1
from google.cloud import scheduler_v1
from google.cloud import logging
from google.cloud import bigquery
from google.cloud import secretmanager
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token
import snowflake.connector
import pandas as pd
import numpy as np
import json
import datetime
import pytz
from typing import Dict, List, Any, Optional, Tuple
import os
import base64
import secrets

app = Flask(__name__)

# Configuration
ALLOWED_DOMAIN = "fiskalfinance.com"
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '')  # Will be set via environment variable

# Set a proper secret key for Flask sessions
SECRET_KEY = os.environ.get('SECRET_KEY')
if not SECRET_KEY:
    # Generate a secure secret key if not provided
    SECRET_KEY = base64.b64encode(secrets.token_bytes(32)).decode()
    print(f"Generated new secret key: {SECRET_KEY}")

# Ensure the secret key is set before any session operations
app.secret_key = SECRET_KEY

# Verify the secret key is properly set
print(f"Flask app configured with secret key length: {len(app.secret_key)}")
print(f"Flask secret key set: {bool(app.secret_key)}")

# Test session functionality
with app.app_context():
    try:
        from flask import session
        # This will fail if secret key isn't working
        test_session = session
        print("Session functionality verified")
    except Exception as e:
        print(f"Session test failed: {e}")

# Authentication functions
def verify_google_token(token):
    """Verify Google ID token and check domain"""
    try:
        print(f"Verifying token with Client ID: {GOOGLE_CLIENT_ID}")
        
        # Verify the token
        idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)
        print(f"Token verified successfully. User info: {idinfo}")
        
        # Check if the email domain matches
        email = idinfo.get('email', '')
        domain = email.split('@')[-1] if '@' in email else ''
        
        print(f"User email: {email}, domain: {domain}, allowed domain: {ALLOWED_DOMAIN}")
        
        if domain.lower() == ALLOWED_DOMAIN.lower():
            return {
                'email': email,
                'name': idinfo.get('name', ''),
                'picture': idinfo.get('picture', ''),
                'verified': True
            }
        else:
            return {'verified': False, 'error': f'Access denied. Only {ALLOWED_DOMAIN} users are allowed. Your domain: {domain}'}
            
    except ValueError as e:
        print(f"Token verification failed: {str(e)}")
        return {'verified': False, 'error': f'Invalid token: {str(e)}'}

def require_auth(f):
    """Decorator to require authentication"""
    def decorated_function(*args, **kwargs):
        # Check if user is authenticated
        if 'user' not in session or not session['user'].get('verified'):
            return redirect('/login')
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function

# Login page HTML
LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Karbon Pipeline Dashboard - Login</title>
    <script src="https://accounts.google.com/gsi/client" async defer></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }
        
        .login-container {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
            padding: 50px;
            text-align: center;
            max-width: 500px;
            width: 100%;
        }
        
        .logo {
            font-size: 3em;
            margin-bottom: 20px;
        }
        
        h1 {
            color: #2c3e50;
            margin-bottom: 10px;
            font-weight: 300;
        }
        
        .subtitle {
            color: #666;
            margin-bottom: 40px;
            font-size: 1.1em;
        }
        
        .domain-restriction {
            background: #e3f2fd;
            border: 1px solid #2196f3;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 30px;
            color: #1565c0;
        }
        
        .domain-restriction strong {
            color: #0d47a1;
        }
        
        .google-signin-container {
            margin: 30px 0;
        }
        
        .error-message {
            background: #ffebee;
            border: 1px solid #f44336;
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 20px;
            color: #c62828;
        }
        
        .loading {
            display: none;
            color: #666;
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="logo">🚀</div>
        <h1>Karbon Pipeline Dashboard</h1>
        <div class="subtitle">Secure access for Fiskal Finance team</div>
        
        <div class="domain-restriction">
            <strong>🔒 Restricted Access</strong><br>
            Only users with <strong>@fiskalfinance.com</strong> email addresses can access this dashboard.
        </div>
        
        <div id="error-message" class="error-message" style="display: none;"></div>
        
        <div class="google-signin-container">
            <div id="g_id_onload"
                 data-client_id="{{ client_id }}"
                 data-callback="handleCredentialResponse"
                 data-auto_prompt="false"
                 data-itp_support="true">
            </div>
            <div class="g_id_signin"
                 data-type="standard"
                 data-size="large"
                 data-theme="outline"
                 data-text="sign_in_with"
                 data-shape="rectangular"
                 data-logo_alignment="left"
                 data-width="300">
            </div>
        </div>
        
        <div class="loading" id="loading">
            Verifying your credentials...
        </div>
    </div>
    
    <script>
        function handleCredentialResponse(response) {
            console.log('Credential response received:', response);
            document.getElementById('loading').style.display = 'block';
            document.getElementById('error-message').style.display = 'none';
            
            // Send the token to our backend for verification
            fetch(window.location.origin + '/karbon-pipeline-dashboard/auth/verify', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    credential: response.credential
                })
            })
            .then(response => {
                console.log('Backend response status:', response.status);
                return response.json();
            })
            .then(data => {
                console.log('Backend response data:', data);
                document.getElementById('loading').style.display = 'none';
                
                if (data.success) {
                    // Store user info in localStorage for now
                    if (data.user) {
                        localStorage.setItem('dashboard_user', JSON.stringify(data.user));
                    }
                    window.location.href = window.location.origin + '/karbon-pipeline-dashboard';
                } else {
                    document.getElementById('error-message').textContent = data.error || 'Authentication failed';
                    document.getElementById('error-message').style.display = 'block';
                }
            })
            .catch(error => {
                console.error('Authentication error:', error);
                document.getElementById('loading').style.display = 'none';
                document.getElementById('error-message').textContent = 'Network error. Please try again. Check console for details.';
                document.getElementById('error-message').style.display = 'block';
            });
        }
        
        // Initialize Google Sign-In
        window.onload = function () {
            console.log('Initializing Google Sign-In with client ID: {{ client_id }}');
            try {
                google.accounts.id.initialize({
                    client_id: "{{ client_id }}",
                    callback: handleCredentialResponse,
                    auto_select: false,
                    cancel_on_tap_outside: false
                });
                console.log('Google Sign-In initialized successfully');
            } catch (error) {
                console.error('Error initializing Google Sign-In:', error);
                document.getElementById('error-message').textContent = 'Failed to initialize Google Sign-In. Please refresh the page.';
                document.getElementById('error-message').style.display = 'block';
            }
        }
    </script>
</body>
</html>
"""

# Data Comparison HTML template
COMPARISON_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BigQuery vs Snowflake Comparison</title>
    <style>
        /* CSS styles for comparison page */
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }
        .container { max-width: 1600px; margin: 0 auto; background: rgba(255, 255, 255, 0.95); border-radius: 20px; box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1); overflow: hidden; }
        .header { background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%); color: white; padding: 30px; text-align: center; }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; font-weight: 300; }
        .nav-tabs { display: flex; background: #f8f9fa; border-bottom: 1px solid #dee2e6; }
        .nav-tab { flex: 1; padding: 15px 20px; text-align: center; background: none; border: none; cursor: pointer; font-size: 1em; color: #666; text-decoration: none; transition: all 0.3s ease; }
        .nav-tab:hover { background: #e9ecef; color: #2c3e50; }
        .nav-tab.active { background: #2c3e50; color: white; }
        .loading { text-align: center; padding: 50px; font-size: 1.2em; color: #666; }
        .error { text-align: center; padding: 50px; color: #721c24; background: #f8d7da; border-radius: 10px; margin: 20px; }
        .summary-card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; }
        .summary-card h3 { margin-top: 0; color: #495057; border-bottom: 2px solid #007bff; padding-bottom: 10px; }
        .export-btn { background: #28a745; color: white; border: none; padding: 10px 20px; margin: 5px; border-radius: 5px; cursor: pointer; font-size: 14px; transition: background-color 0.3s; }
        .export-btn:hover { background: #218838; }
        .result-row.match { background-color: #f8fff8; }
        .result-row.discrepancy { background-color: #fff8f8; }
        .result-row:hover { background-color: #e9ecef; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 BigQuery vs Snowflake Comparison</h1>
            <div>Work Item Budget & Time Tracking Data Validation</div>
        </div>
        
        <div class="nav-tabs">
            <a href="/karbon-pipeline-dashboard" class="nav-tab">🚀 Pipeline Dashboard</a>
            <a href="/karbon-pipeline-dashboard/comparison" class="nav-tab active">📊 Data Comparison</a>
            <button onclick="showDateAnalysis()" class="nav-tab" id="date-analysis-tab">📅 Date Analysis</button>
        </div>
        
        <div id="comparison-content">
            <div class="loading">Loading comparison data...</div>
        </div>
    </div>
    
    <script>
        async function loadComparison() {
            try {
                const response = await fetch('/karbon-pipeline-dashboard/api/comparison');
                const data = await response.json();
                
                if (data.error) {
                    document.getElementById('comparison-content').innerHTML = 
                        '<div class="error"><h3>Error Loading Comparison</h3><p>' + data.error + '</p></div>';
                    return;
                }
                
                // Display detailed client-level comparison results
                const summary = data.summary || {};
                const results = data.comparison_results || [];
                const discrepancies = data.discrepancies || [];
                
                let html = '<div style="padding: 20px;">';
                
                // Summary Section
                html += '<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px;">';
                html += '<div class="summary-card"><h3>Client Comparison Summary</h3>' +
                    '<p>BigQuery Clients: ' + (summary.total_bq_clients || 0) + '</p>' +
                    '<p>Snowflake Clients: ' + (summary.total_sf_clients || 0) + '</p>' +
                    '<p>Common Clients: ' + (summary.common_clients || 0) + '</p>' +
                    '<p>Matching Clients: ' + (summary.matching_clients || 0) + '</p>' +
                    '<p>Discrepancies: ' + (summary.discrepancy_count || 0) + '</p>' +
                    '<p><strong>Match Rate: ' + ((summary.match_percentage || 0).toFixed(1)) + '%</strong></p></div>';
                
                html += '<div class="summary-card"><h3>Total Hours Summary</h3>' +
                    '<p>BQ Total Budgeted: ' + ((summary.total_bq_budgeted_hours || 0).toLocaleString()) + ' hours</p>' +
                    '<p>SF Total Budgeted: ' + ((summary.total_sf_budgeted_hours || 0).toLocaleString()) + ' hours</p>' +
                    '<p>BQ Total Logged: ' + ((summary.total_bq_logged_hours || 0).toLocaleString()) + ' hours</p>' +
                    '<p>SF Total Logged: ' + ((summary.total_sf_logged_hours || 0).toLocaleString()) + ' hours</p>' +
                    '<p><strong>Budgeted Diff: ' + ((summary.budgeted_hours_difference || 0).toFixed(1)) + ' hours</strong></p>' +
                    '<p><strong>Logged Diff: ' + ((summary.logged_hours_difference || 0).toFixed(1)) + ' hours</strong></p></div>';
                html += '</div>';
                
                // Export Buttons
                html += '<div style="margin: 20px 0; text-align: center;">';
                html += '<div style="margin-bottom: 15px;"><strong>Summary Data Exports:</strong></div>';
                html += '<button onclick="exportData(\\'all\\', \\'csv\\')" class="export-btn">Export All Data (CSV)</button> ';
                html += '<button onclick="exportData(\\'discrepancies\\', \\'csv\\')" class="export-btn">Export Discrepancies (CSV)</button> ';
                html += '<button onclick="exportData(\\'all\\', \\'json\\')" class="export-btn">Export All Data (JSON)</button>';
                html += '<div style="margin: 15px 0;"><strong>Detailed Raw Data Exports (Client × User):</strong></div>';
                html += '<button onclick="exportDetailedData(\\'both\\', \\'csv\\')" class="export-btn">Export Detailed BQ + SF (CSV)</button> ';
                html += '<button onclick="exportDetailedData(\\'bigquery\\', \\'csv\\')" class="export-btn">Export BigQuery Detail (CSV)</button> ';
                html += '<button onclick="exportDetailedData(\\'snowflake\\', \\'csv\\')" class="export-btn">Export Snowflake Detail (CSV)</button> ';
                html += '<button onclick="exportDetailedData(\\'both\\', \\'json\\')" class="export-btn">Export Detailed Raw (JSON)</button>';
                html += '</div>';
                
                // Enhanced Filter Controls
                html += '<div style="margin: 20px 0; padding: 20px; background: #f8f9fa; border-radius: 8px; border: 1px solid #dee2e6;">';
                html += '<h4 style="margin-top: 0; color: #495057;">🔍 Data Filters</h4>';
                
                // Row 1: Match Status and Budget Filters
                html += '<div style="display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 15px; align-items: center;">';
                html += '<label style="display: flex; align-items: center; gap: 8px;">Match Status: ';
                html += '<select onchange="applyFilters()" id="match-filter" style="padding: 6px 10px; border: 1px solid #ced4da; border-radius: 4px;">';
                html += '<option value="all">All Clients</option>';
                html += '<option value="match">Perfect Matches Only</option>';
                html += '<option value="discrepancy">Discrepancies Only</option>';
                html += '</select></label>';
                
                html += '<label style="display: flex; align-items: center; gap: 8px;">Min Budget Hours: ';
                html += '<input type="number" onchange="applyFilters()" id="budget-filter" placeholder="0" min="0" step="0.5" style="padding: 6px 10px; border: 1px solid #ced4da; border-radius: 4px; width: 100px;"></label>';
                
                html += '<label style="display: flex; align-items: center; gap: 8px;">Min Logged Hours: ';
                html += '<input type="number" onchange="applyFilters()" id="logged-filter" placeholder="0" min="0" step="0.5" style="padding: 6px 10px; border: 1px solid #ced4da; border-radius: 4px; width: 100px;"></label>';
                html += '</div>';
                
                // Row 2: Client Search and Variance Filters
                html += '<div style="display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 15px; align-items: center;">';
                html += '<label style="display: flex; align-items: center; gap: 8px;">Client Search: ';
                html += '<input type="text" onkeyup="applyFilters()" id="client-search" placeholder="Search client names..." style="padding: 6px 10px; border: 1px solid #ced4da; border-radius: 4px; width: 200px;"></label>';
                
                html += '<label style="display: flex; align-items: center; gap: 8px;">Min Variance (abs): ';
                html += '<input type="number" onchange="applyFilters()" id="variance-filter" placeholder="0" min="0" step="0.1" style="padding: 6px 10px; border: 1px solid #ced4da; border-radius: 4px; width: 100px;"></label>';
                
                html += '<label style="display: flex; align-items: center; gap: 8px;">Sort by: ';
                html += '<select onchange="applySorting(this.value)" id="sort-filter" style="padding: 6px 10px; border: 1px solid #ced4da; border-radius: 4px;">';
                html += '<option value="budget-desc">Budget Hours (High to Low)</option>';
                html += '<option value="budget-asc">Budget Hours (Low to High)</option>';
                html += '<option value="logged-desc">Logged Hours (High to Low)</option>';
                html += '<option value="logged-asc">Logged Hours (Low to High)</option>';
                html += '<option value="variance-desc">Variance (High to Low)</option>';
                html += '<option value="variance-asc">Variance (Low to High)</option>';
                html += '<option value="client-asc">Client Name (A-Z)</option>';
                html += '<option value="client-desc">Client Name (Z-A)</option>';
                html += '</select></label>';
                html += '</div>';
                
                // Row 3: Quick Filter Buttons and Reset
                html += '<div style="display: flex; flex-wrap: wrap; gap: 10px; align-items: center;">';
                html += '<strong>Quick Filters:</strong>';
                html += '<button onclick="setQuickFilter(\\'large-clients\\')" class="export-btn" style="font-size: 12px; padding: 4px 8px;">Large Clients (>100h)</button>';
                html += '<button onclick="setQuickFilter(\\'high-variance\\')" class="export-btn" style="font-size: 12px; padding: 4px 8px;">High Variance (>10h)</button>';
                html += '<button onclick="setQuickFilter(\\'over-budget\\')" class="export-btn" style="font-size: 12px; padding: 4px 8px;">Over Budget</button>';
                html += '<button onclick="setQuickFilter(\\'under-budget\\')" class="export-btn" style="font-size: 12px; padding: 4px 8px;">Under Budget</button>';
                html += '<button onclick="resetFilters()" class="export-btn" style="font-size: 12px; padding: 4px 8px; background: #6c757d;">Reset All</button>';
                html += '</div>';
                
                // Results Summary
                html += '<div id="filter-summary" style="margin-top: 15px; padding: 10px; background: #e9ecef; border-radius: 4px; font-size: 14px;"></div>';
                html += '</div>';
                
                // Detailed Results Table
                html += '<div id="results-container">';
                html += buildResultsTable(results);
                html += '</div>';
                
                html += '</div>';
                
                document.getElementById('comparison-content').innerHTML = html;
                
                // Store data globally for export functions
                window.comparisonData = data;
                
                // Initialize filters after data is loaded
                setTimeout(function() {
                    if (document.getElementById('filter-summary')) {
                        applyFilters();
                    }
                }, 100);
            } catch (error) {
                document.getElementById('comparison-content').innerHTML = 
                    '<div class="error"><h3>Connection Error</h3><p>Unable to load comparison data: ' + error.message + '</p></div>';
            }
        }
        
        // Build detailed results table
        function buildResultsTable(results) {
            if (!results || results.length === 0) {
                return '<p>No comparison data available.</p>';
            }
            
            let html = '<div style="overflow-x: auto; margin-top: 20px;">';
            html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;" id="results-table">';
            html += '<thead><tr style="background: #343a40; color: white;">';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">Client</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">BQ Budget (h)</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">SF Budget (h)</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">Budget Diff</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">BQ Logged (h)</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">SF Logged (h)</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">Logged Diff</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">BQ Work Items</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">SF Work Items</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">BQ Users</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">SF Users</th>';
            html += '<th style="padding: 8px; border: 1px solid #ddd;">Match Status</th>';
            html += '</tr></thead><tbody>';
            
            results.forEach(function(row) {
                const budgetDiff = (row.bq_total_budgeted_hours - row.sf_total_budgeted_hours).toFixed(2);
                const loggedDiff = (row.bq_total_hours_logged - row.sf_total_hours_logged).toFixed(2);
                const matchClass = row.overall_match ? 'match' : 'discrepancy';
                const matchText = row.overall_match ? '✓ Match' : '⚠ Discrepancy';
                
                html += '<tr class="result-row ' + matchClass + '" data-match="' + row.overall_match + '" data-budget="' + row.bq_total_budgeted_hours + '" data-logged="' + row.bq_total_hours_logged + '" data-variance="' + budgetDiff + '" data-client="' + (row.client || '') + '">';
                html += '<td style="padding: 6px; border: 1px solid #ddd; font-weight: bold;">' + (row.client || 'N/A') + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (row.bq_total_budgeted_hours || 0).toFixed(2) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (row.sf_total_budgeted_hours || 0).toFixed(2) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right; ' + (Math.abs(budgetDiff) > 0.1 ? 'color: red; font-weight: bold;' : '') + '">' + budgetDiff + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (row.bq_total_hours_logged || 0).toFixed(2) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (row.sf_total_hours_logged || 0).toFixed(2) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right; ' + (Math.abs(loggedDiff) > 0.1 ? 'color: red; font-weight: bold;' : '') + '">' + loggedDiff + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.bq_work_item_count || 0) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.sf_work_item_count || 0) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.bq_user_count || 0) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.sf_user_count || 0) + '</td>';
                html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center; ' + (row.overall_match ? 'color: green;' : 'color: orange;') + '">' + matchText + '</td>';
                html += '</tr>';
            });
            
            html += '</tbody></table></div>';
            return html;
        }
        
        // Enhanced filtering system
        let allRows = [];
        let filteredRows = [];
        
        // Apply all filters
        function applyFilters() {
            const matchFilter = document.getElementById('match-filter').value;
            const budgetFilter = parseFloat(document.getElementById('budget-filter').value) || 0;
            const loggedFilter = parseFloat(document.getElementById('logged-filter').value) || 0;
            const clientSearch = document.getElementById('client-search').value.toLowerCase().trim();
            const varianceFilter = parseFloat(document.getElementById('variance-filter').value) || 0;
            
            const rows = document.querySelectorAll('.result-row');
            let visibleCount = 0;
            let totalBudget = 0;
            let totalLogged = 0;
            let matchCount = 0;
            
            rows.forEach(function(row) {
                const isMatch = row.dataset.match === 'true';
                const budget = parseFloat(row.dataset.budget) || 0;
                const logged = parseFloat(row.dataset.logged) || 0;
                const client = row.dataset.client ? row.dataset.client.toLowerCase() : '';
                const variance = Math.abs(parseFloat(row.dataset.variance) || 0);
                
                let show = true;
                
                // Apply filters
                if (matchFilter === 'match' && !isMatch) show = false;
                if (matchFilter === 'discrepancy' && isMatch) show = false;
                if (budget < budgetFilter) show = false;
                if (logged < loggedFilter) show = false;
                if (clientSearch && !client.includes(clientSearch)) show = false;
                if (variance < varianceFilter) show = false;
                
                row.style.display = show ? '' : 'none';
                
                if (show) {
                    visibleCount++;
                    totalBudget += budget;
                    totalLogged += logged;
                    if (isMatch) matchCount++;
                }
            });
            
            // Update summary
            updateFilterSummary(visibleCount, totalBudget, totalLogged, matchCount);
        }
        
        // Update filter summary
        function updateFilterSummary(visibleCount, totalBudget, totalLogged, matchCount) {
            const summary = document.getElementById('filter-summary');
            const matchRate = visibleCount > 0 ? ((matchCount / visibleCount) * 100).toFixed(1) : 0;
            
            summary.innerHTML = 
                '<strong>Showing ' + visibleCount + ' clients</strong> | ' +
                'Total Budget: ' + totalBudget.toFixed(1) + 'h | ' +
                'Total Logged: ' + totalLogged.toFixed(1) + 'h | ' +
                'Match Rate: ' + matchRate + '% | ' +
                'Variance: ' + (totalBudget - totalLogged).toFixed(1) + 'h';
        }
        
        // Apply sorting
        function applySorting(sortType) {
            const tbody = document.querySelector('#results-table tbody');
            const rows = Array.from(tbody.querySelectorAll('.result-row'));
            
            rows.sort(function(a, b) {
                let aVal, bVal;
                
                switch(sortType) {
                    case 'budget-desc':
                        return parseFloat(b.dataset.budget) - parseFloat(a.dataset.budget);
                    case 'budget-asc':
                        return parseFloat(a.dataset.budget) - parseFloat(b.dataset.budget);
                    case 'logged-desc':
                        return parseFloat(b.dataset.logged) - parseFloat(a.dataset.logged);
                    case 'logged-asc':
                        return parseFloat(a.dataset.logged) - parseFloat(b.dataset.logged);
                    case 'variance-desc':
                        return Math.abs(parseFloat(b.dataset.variance)) - Math.abs(parseFloat(a.dataset.variance));
                    case 'variance-asc':
                        return Math.abs(parseFloat(a.dataset.variance)) - Math.abs(parseFloat(b.dataset.variance));
                    case 'client-asc':
                        return (a.dataset.client || '').localeCompare(b.dataset.client || '');
                    case 'client-desc':
                        return (b.dataset.client || '').localeCompare(a.dataset.client || '');
                    default:
                        return 0;
                }
            });
            
            // Re-append sorted rows
            rows.forEach(function(row) {
                tbody.appendChild(row);
            });
        }
        
        // Quick filter presets
        function setQuickFilter(filterType) {
            // Reset all filters first
            resetFilters();
            
            switch(filterType) {
                case 'large-clients':
                    document.getElementById('budget-filter').value = '100';
                    break;
                case 'high-variance':
                    document.getElementById('variance-filter').value = '10';
                    break;
                case 'over-budget':
                    // Show clients where logged > budget (negative variance)
                    document.getElementById('variance-filter').value = '0.1';
                    // This would need additional logic to filter by positive/negative variance
                    break;
                case 'under-budget':
                    // Show clients where budget > logged (positive variance)
                    document.getElementById('variance-filter').value = '0.1';
                    break;
            }
            
            applyFilters();
        }
        
        // Reset all filters
        function resetFilters() {
            document.getElementById('match-filter').value = 'all';
            document.getElementById('budget-filter').value = '';
            document.getElementById('logged-filter').value = '';
            document.getElementById('client-search').value = '';
            document.getElementById('variance-filter').value = '';
            document.getElementById('sort-filter').value = 'budget-desc';
            
            applyFilters();
        }
        
        // Legacy functions for backward compatibility
        function filterResults(filterType) {
            document.getElementById('match-filter').value = filterType;
            applyFilters();
        }
        
        function filterByBudget(minBudget) {
            document.getElementById('budget-filter').value = minBudget;
            applyFilters();
        }
        
        // Export data functionality
        function exportData(type, format) {
            if (!window.comparisonData) {
                alert('No data available to export');
                return;
            }
            
            let dataToExport;
            let filename;
            
            if (type === 'all') {
                dataToExport = window.comparisonData.comparison_results || [];
                filename = 'client_comparison_all';
            } else if (type === 'discrepancies') {
                dataToExport = window.comparisonData.discrepancies || [];
                filename = 'client_comparison_discrepancies';
            }
            
            if (format === 'csv') {
                exportToCSV(dataToExport, filename);
            } else if (format === 'json') {
                exportToJSON(dataToExport, filename);
            }
        }
        
        // Export to CSV
        function exportToCSV(data, filename) {
            if (!data || data.length === 0) {
                alert('No data to export');
                return;
            }
            
            const headers = [
                'Client', 'BQ_Total_Budgeted_Hours', 'SF_Total_Budgeted_Hours', 'Budget_Difference',
                'BQ_Total_Hours_Logged', 'SF_Total_Hours_Logged', 'Logged_Difference',
                'BQ_Work_Item_Count', 'SF_Work_Item_Count', 'BQ_User_Count', 'SF_User_Count',
                'Budget_Match', 'Hours_Match', 'Variance_Match', 'Overall_Match'
            ];
            
            let csv = headers.join(',') + '\\n';
            
            data.forEach(function(row) {
                const csvRow = [
                    '"' + (row.client || '') + '"',
                    row.bq_total_budgeted_hours || 0,
                    row.sf_total_budgeted_hours || 0,
                    (row.bq_total_budgeted_hours - row.sf_total_budgeted_hours).toFixed(2),
                    row.bq_total_hours_logged || 0,
                    row.sf_total_hours_logged || 0,
                    (row.bq_total_hours_logged - row.sf_total_hours_logged).toFixed(2),
                    row.bq_work_item_count || 0,
                    row.sf_work_item_count || 0,
                    row.bq_user_count || 0,
                    row.sf_user_count || 0,
                    row.budget_match ? 'TRUE' : 'FALSE',
                    row.hours_match ? 'TRUE' : 'FALSE',
                    row.variance_match ? 'TRUE' : 'FALSE',
                    row.overall_match ? 'TRUE' : 'FALSE'
                ];
                csv += csvRow.join(',') + '\\n';
            });
            
            downloadFile(csv, filename + '.csv', 'text/csv');
        }
        
        // Export to JSON
        function exportToJSON(data, filename) {
            const jsonStr = JSON.stringify(data, null, 2);
            downloadFile(jsonStr, filename + '.json', 'application/json');
        }
        
        // Export detailed data functionality
        async function exportDetailedData(source, format) {
            try {
                // Show loading indicator
                const loadingMsg = document.createElement('div');
                loadingMsg.id = 'export-loading';
                loadingMsg.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.2); z-index: 1000;';
                loadingMsg.innerHTML = '<div style="text-align: center;"><div style="margin-bottom: 10px;">🔄 Fetching detailed data...</div><div style="font-size: 0.9em; color: #666;">This may take a moment for large datasets</div></div>';
                document.body.appendChild(loadingMsg);
                
                console.log('Fetching detailed data for source:', source);
                const response = await fetch('/karbon-pipeline-dashboard/api/detailed-data?source=' + source);
                const data = await response.json();
                
                // Remove loading indicator
                document.body.removeChild(loadingMsg);
                
                if (data.error) {
                    alert('Error fetching detailed data: ' + data.error);
                    return;
                }
                
                let filename = 'detailed_budget_tracking_' + source + '_' + new Date().toISOString().split('T')[0];
                
                if (format === 'csv') {
                    exportDetailedToCSV(data, source, filename);
                } else if (format === 'json') {
                    exportDetailedToJSON(data, filename);
                }
                
            } catch (error) {
                // Remove loading indicator if it exists
                const loadingMsg = document.getElementById('export-loading');
                if (loadingMsg) document.body.removeChild(loadingMsg);
                
                console.error('Error exporting detailed data:', error);
                alert('Error exporting detailed data: ' + error.message);
            }
        }
        
        // Export detailed data to CSV
        function exportDetailedToCSV(data, source, filename) {
            let csvContent = '';
            
            if (source === 'both' && data.bigquery_data && data.snowflake_data) {
                // Combined export with source identifier
                const headers = ['Data_Source', 'CLIENT', 'USER_NAME', 'WORK_ITEM_ID', 'WORK_TITLE', 
                               'Budgeted_Hours', 'Hours_Logged_Actual', 'Budget_Variance_Hours', 
                               'Budget_Utilization_Percentage', 'REPORTING_DATE'];
                csvContent = headers.join(',') + '\\n';
                
                // Add BigQuery data
                data.bigquery_data.forEach(function(row) {
                    const csvRow = [
                        '"BigQuery"',
                        '"' + (row.CLIENT || '') + '"',
                        '"' + (row.USER_NAME || '') + '"',
                        '"' + (row.WORK_ITEM_ID || '') + '"',
                        '"' + (row.WORK_TITLE || '').replace(/"/g, '""') + '"',
                        (row.budgeted_hours || 0).toFixed(2),
                        (row.hours_logged_actual || 0).toFixed(2),
                        (row.budget_variance_hours || 0).toFixed(2),
                        (row.budget_utilization_percentage || 0).toFixed(2),
                        '"' + (row.REPORTING_DATE || '') + '"'
                    ];
                    csvContent += csvRow.join(',') + '\\n';
                });
                
                // Add Snowflake data
                data.snowflake_data.forEach(function(row) {
                    const csvRow = [
                        '"Snowflake"',
                        '"' + (row.CLIENT || '') + '"',
                        '"' + (row.USER_NAME || '') + '"',
                        '"' + (row.WORK_ITEM_ID || '') + '"',
                        '"' + (row.WORK_TITLE || '').replace(/"/g, '""') + '"',
                        (row.budgeted_hours || 0).toFixed(2),
                        (row.hours_logged_actual || 0).toFixed(2),
                        (row.budget_variance_hours || 0).toFixed(2),
                        (row.budget_utilization_percentage || 0).toFixed(2),
                        '"' + (row.REPORTING_DATE || '') + '"'
                    ];
                    csvContent += csvRow.join(',') + '\\n';
                });
                
            } else {
                // Single source export
                const headers = ['CLIENT', 'USER_NAME', 'WORK_ITEM_ID', 'WORK_TITLE', 
                               'Budgeted_Hours', 'Hours_Logged_Actual', 'Budget_Variance_Hours', 
                               'Budget_Utilization_Percentage', 'REPORTING_DATE'];
                csvContent = headers.join(',') + '\\n';
                
                const sourceData = source === 'bigquery' ? data.bigquery_data : data.snowflake_data;
                if (sourceData) {
                    sourceData.forEach(function(row) {
                        const csvRow = [
                            '"' + (row.CLIENT || '') + '"',
                            '"' + (row.USER_NAME || '') + '"',
                            '"' + (row.WORK_ITEM_ID || '') + '"',
                            '"' + (row.WORK_TITLE || '').replace(/"/g, '""') + '"',
                            (row.budgeted_hours || 0).toFixed(2),
                            (row.hours_logged_actual || 0).toFixed(2),
                            (row.budget_variance_hours || 0).toFixed(2),
                            (row.budget_utilization_percentage || 0).toFixed(2),
                            '"' + (row.REPORTING_DATE || '') + '"'
                        ];
                        csvContent += csvRow.join(',') + '\\n';
                    });
                }
            }
            
            downloadFile(csvContent, filename + '.csv', 'text/csv');
        }
        
        // Export detailed data to JSON
        function exportDetailedToJSON(data, filename) {
            const jsonStr = JSON.stringify(data, null, 2);
            downloadFile(jsonStr, filename + '.json', 'application/json');
        }
        
        // Download file helper
        function downloadFile(content, filename, contentType) {
            const blob = new Blob([content], { type: contentType });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            a.click();
            window.URL.revokeObjectURL(url);
        }
        
        // Initialize with comparison view
        loadComparison();
        
        // Date analysis functionality
        async function showDateAnalysis() {
            // Update tab styling
            document.querySelectorAll('.nav-tab').forEach(tab => tab.classList.remove('active'));
            document.getElementById('date-analysis-tab').classList.add('active');
            
            // Show loading
            document.getElementById('comparison-content').innerHTML = 
                '<div class="loading">Loading date analysis...</div>';
                
            try {
                const response = await fetch('/karbon-pipeline-dashboard/api/date-analysis');
                const data = await response.json();
                
                if (data.error) {
                    document.getElementById('comparison-content').innerHTML = 
                        '<div class="error"><h3>Error Loading Date Analysis</h3><p>' + data.error + '</p></div>';
                    return;
                }
                
                displayDateAnalysis(data);
                
            } catch (error) {
                document.getElementById('comparison-content').innerHTML = 
                    '<div class="error"><h3>Connection Error</h3><p>Unable to load date analysis: ' + error.message + '</p></div>';
            }
        }
        
        function displayDateAnalysis(data) {
            let html = '<div style="padding: 20px;">';
            
            // Header
            html += '<h2>📅 Date & Timing Analysis</h2>';
            html += '<p style="margin-bottom: 30px; color: #666;">Understanding when hours are recognized in BigQuery vs Snowflake</p>';
            
            // Summary section
            if (data.summary && data.summary.potential_issues.length > 0) {
                html += '<div class="summary-card" style="margin-bottom: 30px; border-left: 4px solid #f39c12;">';
                html += '<h3>🚨 Potential Issues Detected</h3>';
                data.summary.potential_issues.forEach(function(issue) {
                    html += '<div style="margin: 10px 0; padding: 10px; background: #fff3cd; border-radius: 5px;">';
                    html += '<strong>' + issue.type + ':</strong> ' + issue.description;
                    if (issue.impact) {
                        html += '<br><em>Impact: ' + issue.impact + '</em>';
                    }
                    if (issue.affected_clients) {
                        html += '<br><em>Affected clients: ' + issue.affected_clients + '</em>';
                    }
                    html += '</div>';
                });
                html += '</div>';
            }
            
            // BigQuery date patterns
            if (data.bigquery_date_patterns && data.bigquery_date_patterns.length > 0) {
                html += '<div class="summary-card" style="margin-bottom: 30px;">';
                html += '<h3>📊 BigQuery Reporting Date Patterns</h3>';
                html += '<p>Shows when time entries are being reported vs when they were actually logged</p>';
                html += '<div style="overflow-x: auto; margin-top: 15px;">';
                html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
                html += '<thead><tr style="background: #343a40; color: white;">';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Reporting Date</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Earliest Entry</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Latest Entry</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Avg Lag (Days)</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Max Lag (Days)</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Total Hours</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Records</th>';
                html += '</tr></thead><tbody>';
                
                data.bigquery_date_patterns.forEach(function(row) {
                    const avgLag = parseFloat(row.avg_reporting_lag_days || 0);
                    const maxLag = parseFloat(row.max_reporting_lag_days || 0);
                    const lagStyle = avgLag > 1 ? 'background: #ffebee;' : '';
                    
                    html += '<tr style="' + lagStyle + '">';
                    html += '<td style="padding: 6px; border: 1px solid #ddd;">' + (row.REPORTING_DATE || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd;">' + (row.earliest_time_entry || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd;">' + (row.latest_time_entry || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + avgLag.toFixed(1) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + maxLag + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (parseFloat(row.total_hours || 0)).toFixed(2) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.total_records || 0) + '</td>';
                    html += '</tr>';
                });
                
                html += '</tbody></table></div></div>';
            }
            
            // Filter impact analysis
            if (data.comparison_filter_impact && data.comparison_filter_impact.length > 0) {
                html += '<div class="summary-card" style="margin-bottom: 30px;">';
                html += '<h3>🔍 Comparison Filter Impact</h3>';
                html += '<p>Hours excluded by current comparison filters (may explain discrepancies)</p>';
                html += '<div style="overflow-x: auto; margin-top: 15px;">';
                html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
                html += '<thead><tr style="background: #343a40; color: white;">';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Client</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">All Hours</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Filtered Hours</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Hours Excluded</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Latest Data Date</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Latest Time Entry</th>';
                html += '</tr></thead><tbody>';
                
                data.comparison_filter_impact.forEach(function(row) {
                    const hoursExcluded = parseFloat(row.hours_excluded || 0);
                    const excludedStyle = hoursExcluded > 10 ? 'background: #ffebee;' : '';
                    
                    html += '<tr style="' + excludedStyle + '">';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; font-weight: bold;">' + (row.CLIENT || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (parseFloat(row.all_hours || 0)).toFixed(2) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (parseFloat(row.filtered_hours || 0)).toFixed(2) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right; font-weight: bold; color: red;">' + hoursExcluded.toFixed(2) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd;">' + (row.latest_all_data_date || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd;">' + (row.latest_actual_time_entry || 'N/A') + '</td>';
                    html += '</tr>';
                });
                
                html += '</tbody></table></div></div>';
            }
            
            // Snowflake date patterns
            if (data.snowflake_date_patterns && data.snowflake_date_patterns.length > 0) {
                html += '<div class="summary-card" style="margin-bottom: 30px;">';
                html += '<h3>❄️ Snowflake Date Patterns</h3>';
                html += '<div style="overflow-x: auto; margin-top: 15px;">';
                html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
                html += '<thead><tr style="background: #343a40; color: white;">';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Reporting Date</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Records</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Total Hours</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Clients</th>';
                html += '<th style="padding: 8px; border: 1px solid #ddd;">Users</th>';
                html += '</tr></thead><tbody>';
                
                data.snowflake_date_patterns.forEach(function(row) {
                    html += '<tr>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd;">' + (row.REPORTING_DATE || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.RECORD_COUNT || 0) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: right;">' + (parseFloat(row.TOTAL_HOURS || 0)).toFixed(2) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.UNIQUE_CLIENTS || 0) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #ddd; text-align: center;">' + (row.UNIQUE_USERS || 0) + '</td>';
                    html += '</tr>';
                });
                
                html += '</tbody></table></div></div>';
            }
            
            // Back to comparison button
            html += '<div style="text-align: center; margin-top: 30px;">';
            html += '<button onclick="loadComparison(); document.querySelectorAll(\\'.nav-tab\\').forEach(tab => tab.classList.remove(\\'active\\')); document.querySelector(\\'.nav-tab[href*=\\'comparison\\']\\').classList.add(\\'active\\');" class="export-btn">← Back to Data Comparison</button>';
            html += '</div>';
            
            html += '</div>';
            
            document.getElementById('comparison-content').innerHTML = html;
        }
        
        loadComparison();
    </script>
</body>
</html>
"""

# Dashboard HTML template with modern UI
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Karbon Pipeline Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 300;
        }
        
        .header .subtitle {
            font-size: 1.1em;
            opacity: 0.8;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
        }
        
        .stat-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.08);
            text-align: center;
            transition: transform 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-number {
            font-size: 3em;
            font-weight: bold;
            margin-bottom: 10px;
        }
        
        .stat-label {
            color: #666;
            font-size: 1.1em;
        }
        
        .functions-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            padding: 30px;
        }
        
        .function-card {
            background: white;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.08);
            overflow: hidden;
            transition: transform 0.3s ease;
        }
        
        .function-card:hover {
            transform: translateY(-3px);
        }
        
        .function-header {
            padding: 20px;
            border-bottom: 1px solid #eee;
        }
        
        .function-name {
            font-size: 1.3em;
            font-weight: 600;
            margin-bottom: 5px;
            color: #2c3e50;
        }
        
        .function-type {
            color: #666;
            font-size: 0.9em;
        }
        
        .function-body {
            padding: 20px;
        }
        
        .status-indicator {
            display: inline-flex;
            align-items: center;
            padding: 8px 15px;
            border-radius: 25px;
            font-size: 0.9em;
            font-weight: 500;
            margin-bottom: 15px;
        }
        
        .status-active {
            background: #d4edda;
            color: #155724;
        }
        
        .status-running {
            background: #cce5ff;
            color: #0066cc;
            animation: pulse 2s infinite;
        }
        
        .status-paused {
            background: #f8d7da;
            color: #721c24;
        }
        
        .status-error {
            background: #f5c6cb;
            color: #721c24;
        }
        
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.7; }
            100% { opacity: 1; }
        }
        
        .function-details {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin-top: 15px;
        }
        
        .detail-item {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 8px;
        }
        
        .detail-label {
            font-size: 0.8em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .detail-value {
            font-weight: 600;
            margin-top: 3px;
            color: #2c3e50;
        }
        
        .refresh-btn {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 15px 25px;
            border-radius: 50px;
            font-size: 1em;
            cursor: pointer;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
            transition: transform 0.3s ease;
        }
        
        .refresh-btn:hover {
            transform: scale(1.05);
        }
        
        .last-updated {
            text-align: center;
            padding: 20px;
            color: #666;
            background: #f8f9fa;
            border-top: 1px solid #eee;
        }
        
        .loading {
            text-align: center;
            padding: 50px;
            font-size: 1.2em;
            color: #666;
        }
        
        .error {
            text-align: center;
            padding: 50px;
            color: #721c24;
            background: #f8d7da;
            border-radius: 10px;
            margin: 20px;
        }
        
        .nav-tabs {
            display: flex;
            background: #f8f9fa;
            border-bottom: 1px solid #dee2e6;
        }
        
        .nav-tab {
            flex: 1;
            padding: 15px 20px;
            text-align: center;
            background: none;
            border: none;
            cursor: pointer;
            font-size: 1em;
            color: #666;
            text-decoration: none;
            transition: all 0.3s ease;
        }
        
        .nav-tab:hover {
            background: #e9ecef;
            color: #2c3e50;
        }
        
        .nav-tab.active {
            background: #2c3e50;
            color: white;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <div>
                    <h1>🚀 Karbon Pipeline Dashboard</h1>
                    <div class="subtitle">Real-time monitoring of Snowflake → BigQuery data pipeline</div>
                </div>
                <div style="text-align: right;">
                    <div id="user-info" style="margin-bottom: 10px; opacity: 0.9;"></div>
                    <a href="/karbon-pipeline-dashboard/logout" style="color: rgba(255,255,255,0.8); text-decoration: none; font-size: 0.9em;">🚪 Logout</a>
                </div>
            </div>
        </div>
        
        <div class="nav-tabs">
            <a href="/karbon-pipeline-dashboard" class="nav-tab active">🚀 Pipeline Dashboard</a>
            <a href="/karbon-pipeline-dashboard/comparison" class="nav-tab">📊 Data Comparison</a>
        </div>
        
        <div id="dashboard-content">
            <div class="loading">Loading pipeline status...</div>
        </div>
        
        <div class="last-updated" id="last-updated">
            Last updated: Loading...
        </div>
    </div>
    
    <button class="refresh-btn" onclick="refreshDashboard()">🔄 Refresh</button>
    
    <script>
        async function loadDashboard() {
            try {
                const response = await fetch('/karbon-pipeline-dashboard/api/status');
                const data = await response.json();
                
                if (data.error) {
                    document.getElementById('dashboard-content').innerHTML = `
                        <div class="error">
                            <h3>Error Loading Dashboard</h3>
                            <p>${data.error}</p>
                        </div>
                    `;
                    return;
                }
                
                renderDashboard(data);
                document.getElementById('last-updated').textContent = 
                    `Last updated: ${new Date().toLocaleString()}`;
                    
            } catch (error) {
                document.getElementById('dashboard-content').innerHTML = `
                    <div class="error">
                        <h3>Connection Error</h3>
                        <p>Unable to load dashboard data: ${error.message}</p>
                    </div>
                `;
            }
        }
        
        function renderDashboard(data) {
            const statsHtml = `
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-number" style="color: #28a745;">${data.stats.total_functions}</div>
                        <div class="stat-label">Total Functions</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number" style="color: #17a2b8;">${data.stats.active_functions}</div>
                        <div class="stat-label">Active Functions</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number" style="color: #ffc107;">${data.stats.total_schedulers}</div>
                        <div class="stat-label">Schedulers</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number" style="color: #dc3545;">${data.stats.failed_functions}</div>
                        <div class="stat-label">Issues</div>
                    </div>
                </div>
            `;
            
            const functionsHtml = data.functions.map(func => `
                <div class="function-card">
                    <div class="function-header">
                        <div class="function-name">${func.name}</div>
                        <div class="function-type">${func.type}</div>
                    </div>
                    <div class="function-body">
                        <div class="status-indicator status-${func.status.toLowerCase()}">
                            ${getStatusIcon(func.status)} ${func.status.toUpperCase()}
                        </div>
                        <div class="function-details">
                            <div class="detail-item">
                                <div class="detail-label">Schedule</div>
                                <div class="detail-value">${func.schedule || 'HTTP Trigger'}</div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Last Run</div>
                                <div class="detail-value">${func.last_run || 'Never'}</div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Next Run</div>
                                <div class="detail-value">${func.next_run || 'On Demand'}</div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Runtime</div>
                                <div class="detail-value">${func.runtime || 'N/A'}</div>
                            </div>
                        </div>
                    </div>
                </div>
            `).join('');
            
            document.getElementById('dashboard-content').innerHTML = statsHtml + 
                '<div class="functions-grid">' + functionsHtml + '</div>';
        }
        
        function getStatusIcon(status) {
            switch(status.toLowerCase()) {
                case 'active': return '✅';
                case 'running': return '🔄';
                case 'paused': return '⏸️';
                case 'error': return '❌';
                case 'enabled': return '✅';
                default: return '❓';
            }
        }
        
        function refreshDashboard() {
            document.getElementById('dashboard-content').innerHTML = 
                '<div class="loading">Refreshing pipeline status...</div>';
            loadDashboard();
        }
        
        // Auto-refresh every 30 seconds
        setInterval(loadDashboard, 30000);
        
        // Display user info
        function displayUserInfo() {
            // Check localStorage for user info
            const userInfo = localStorage.getItem('dashboard_user');
            if (userInfo) {
                try {
                    const user = JSON.parse(userInfo);
                    if (user.verified) {
                        document.getElementById('user-info').innerHTML = 
                            `Welcome, ${user.name || user.email}`;
                        return;
                    }
                } catch (e) {
                    console.error('Error parsing user info:', e);
                }
            }
            
            // If no valid user info, redirect to login
            window.location.href = '/karbon-pipeline-dashboard/login';
        }
        
        // Load dashboard on page load
        displayUserInfo();
        loadDashboard();
    </script>
</body>
</html>
"""

class PipelineDashboard:
    def __init__(self):
        self.project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'red-octane-444308-f4')
        self.region = 'us-central1'
        
    def get_functions_status(self) -> List[Dict[str, Any]]:
        """Get status of all Cloud Functions"""
        try:
            client = functions_v1.CloudFunctionsServiceClient()
            location = f"projects/{self.project_id}/locations/{self.region}"
            
            functions = []
            
            # List all functions
            for function in client.list_functions(parent=location):
                func_data = {
                    'name': function.name.split('/')[-1],
                    'type': 'Cloud Function',
                    'status': 'Active' if function.status == functions_v1.CloudFunction.Status.ACTIVE else 'Error',
                    'runtime': function.runtime,
                    'last_run': 'Unknown',
                    'next_run': 'On Demand',
                    'schedule': None
                }
                
                # Determine function category
                if any(keyword in func_data['name'].lower() for keyword in ['sync', 'dimension', 'budget', 'monitor']):
                    functions.append(func_data)
                    
            return functions
            
        except Exception as e:
            print(f"Error getting functions: {e}")
            return []
    
    def get_schedulers_status(self) -> List[Dict[str, Any]]:
        """Get status of all Cloud Scheduler jobs"""
        try:
            client = scheduler_v1.CloudSchedulerClient()
            location = f"projects/{self.project_id}/locations/{self.region}"
            
            schedulers = []
            
            # List all scheduler jobs
            for job in client.list_jobs(parent=location):
                # Parse last run time
                last_run = 'Never'
                next_run = 'Unknown'
                
                if hasattr(job, 'last_attempt_time') and job.last_attempt_time:
                    last_run = job.last_attempt_time.strftime('%Y-%m-%d %H:%M UTC')
                
                # Calculate next run (simplified)
                if job.schedule:
                    next_run = 'Per Schedule'
                
                scheduler_data = {
                    'name': job.name.split('/')[-1],
                    'type': 'Scheduler',
                    'status': 'Enabled' if job.state == scheduler_v1.Job.State.ENABLED else 'Paused',
                    'schedule': job.schedule,
                    'last_run': last_run,
                    'next_run': next_run,
                    'runtime': 'Scheduler'
                }
                
                # Filter pipeline-related schedulers
                if any(keyword in scheduler_data['name'].lower() for keyword in 
                      ['sync', 'dimension', 'budget', 'monitor', 'pipeline', 'work-item']):
                    schedulers.append(scheduler_data)
                    
            return schedulers
            
        except Exception as e:
            print(f"Error getting schedulers: {e}")
            return []
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive dashboard data"""
        try:
            functions = self.get_functions_status()
            schedulers = self.get_schedulers_status()
            
            all_items = functions + schedulers
            
            # Calculate statistics
            stats = {
                'total_functions': len(functions),
                'active_functions': len([f for f in functions if f['status'] == 'Active']),
                'total_schedulers': len(schedulers),
                'failed_functions': len([item for item in all_items if item['status'] in ['Error', 'Paused']])
            }
            
            return {
                'functions': all_items,
                'stats': stats,
                'timestamp': datetime.datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            return {'error': f'Failed to load dashboard data: {str(e)}'}

class SecretManager:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()
        
    def get_secret(self, secret_name: str, version: str = "latest") -> str:
        """Retrieve a secret from Google Cloud Secret Manager"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/{version}"
            response = self.client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"Error retrieving secret {secret_name}: {e}")
            return None
            
    def get_snowflake_config(self) -> Dict[str, str]:
        """Get all Snowflake configuration from secrets"""
        config = {}
        # Try uppercase secret names first (your current format), then lowercase as fallback
        secrets_map = {
            'user': ['SNOWFLAKE_USER', 'snowflake-user'],
            'password': ['SNOWFLAKE_PASSWORD', 'snowflake-password'], 
            'account': ['SNOWFLAKE_ACCOUNT', 'snowflake-account'],
            'warehouse': ['SNOWFLAKE_WAREHOUSE', 'snowflake-warehouse'],
            'database': ['SNOWFLAKE_DATABASE', 'snowflake-database'],
            'schema': ['SNOWFLAKE_SCHEMA', 'snowflake-schema']
        }
        
        for key, secret_names in secrets_map.items():
            value = None
            # Try each secret name until we find one that works
            for secret_name in secret_names:
                value = self.get_secret(secret_name)
                if value and value.strip():
                    # Clean up any trailing % characters
                    value = value.strip().rstrip('%')
                    config[key] = value
                    break
            
            # If no secret found, try environment variables as fallback
            if not value:
                env_key = f'SNOWFLAKE_{key.upper()}'
                config[key] = os.environ.get(env_key, '')
                
        # Set defaults for optional configs if not found
        config['warehouse'] = config.get('warehouse') or 'COMPUTE_WH'
        config['database'] = config.get('database') or 'KPI_DATABASE'  # Use your actual database name
        config['schema'] = config.get('schema') or 'SECURE_VIEWS'  # Use your actual schema name
        
        return config

class DataComparison:
    def __init__(self):
        self.project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'red-octane-444308-f4')
        self.secret_manager = SecretManager(self.project_id)
        self.bq_client = None
        self.snowflake_conn = None
        self.snowflake_config = None
        
    def get_bq_client(self):
        """Initialize BigQuery client"""
        if not self.bq_client:
            self.bq_client = bigquery.Client(project=self.project_id)
        return self.bq_client
        
    def get_snowflake_connection(self):
        """Initialize Snowflake connection using Secret Manager"""
        if not self.snowflake_conn:
            try:
                # Get Snowflake configuration from Secret Manager
                if not self.snowflake_config:
                    self.snowflake_config = self.secret_manager.get_snowflake_config()
                
                # Validate required credentials
                required_fields = ['user', 'password', 'account']
                missing_fields = [field for field in required_fields if not self.snowflake_config.get(field)]
                
                if missing_fields:
                    print(f"Missing required Snowflake credentials: {missing_fields}")
                    return None
                
                print(f"Connecting to Snowflake with:")
                print(f"  User: {self.snowflake_config['user']}")
                print(f"  Account: {self.snowflake_config['account']}")
                print(f"  Warehouse: {self.snowflake_config['warehouse']}")
                print(f"  Database: {self.snowflake_config['database']}")
                print(f"  Schema: {self.snowflake_config['schema']}")
                
                self.snowflake_conn = snowflake.connector.connect(
                    user=self.snowflake_config['user'],
                    password=self.snowflake_config['password'],
                    account=self.snowflake_config['account'],
                    warehouse=self.snowflake_config['warehouse'],
                    database=self.snowflake_config['database'],
                    schema=self.snowflake_config['schema']
                )
                
                print("Successfully connected to Snowflake")
                
            except Exception as e:
                print(f"Error connecting to Snowflake: {e}")
                return None
        return self.snowflake_conn
        
    def query_bigquery_view(self, limit: int = 1000) -> pd.DataFrame:
        """Query the BigQuery view - client level aggregation"""
        try:
            print("Initializing BigQuery client...")
            client = self.get_bq_client()
            print("BigQuery client initialized successfully")
            query = f"""
            SELECT 
                CLIENT,
                SUM(individual_budgeted_hours) as total_budgeted_hours,
                SUM(individual_hours_logged_actual) as total_hours_logged_actual,
                SUM(individual_budget_variance_hours) as total_budget_variance_hours,
                COUNT(DISTINCT WORK_ITEM_ID) as work_item_count,
                COUNT(DISTINCT budget_user_name) as user_count,
                AVG(individual_budget_utilization_percentage) as avg_budget_utilization_percentage,
                MAX(REPORTING_DATE) as reporting_date
            FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            WHERE REPORTING_DATE = (
                SELECT MAX(REPORTING_DATE) 
                FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            )
            AND individual_budgeted_hours > 0
            AND budget_user_name IS NOT NULL
            AND CLIENT IS NOT NULL
            GROUP BY CLIENT
            ORDER BY total_budgeted_hours DESC
            LIMIT {limit}
            """
            
            print("Executing BigQuery query...")
            query_job = client.query(query)
            print("Query job created, waiting for results...")
            df = query_job.to_dataframe()
            print(f"BigQuery query completed. Returned {len(df)} rows")
            return df
            
        except Exception as e:
            print(f"Error querying BigQuery: {e}")
            return pd.DataFrame()
            
    def query_bigquery_detailed(self, limit: int = 5000) -> pd.DataFrame:
        """Query detailed BigQuery data by client and user"""
        try:
            print("Querying detailed BigQuery data...")
            client = self.get_bq_client()
            query = f"""
            SELECT 
                CLIENT,
                budget_user_name as USER_NAME,
                WORK_ITEM_ID,
                WORK_TITLE,
                individual_budgeted_hours as budgeted_hours,
                individual_hours_logged_actual as hours_logged_actual,
                individual_budget_variance_hours as budget_variance_hours,
                individual_budget_utilization_percentage as budget_utilization_percentage,
                REPORTING_DATE
            FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            WHERE REPORTING_DATE = (
                SELECT MAX(REPORTING_DATE) 
                FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            )
            AND individual_budgeted_hours > 0
            AND budget_user_name IS NOT NULL
            AND CLIENT IS NOT NULL
            ORDER BY CLIENT, budget_user_name, individual_budgeted_hours DESC
            LIMIT {limit}
            """
            
            query_job = client.query(query)
            df = query_job.to_dataframe()
            print(f"BigQuery detailed query completed. Returned {len(df)} rows")
            return df
            
        except Exception as e:
            print(f"Error querying detailed BigQuery data: {e}")
            return pd.DataFrame()
            
    def query_snowflake_data(self, limit: int = 1000) -> pd.DataFrame:
        """Query equivalent data from Snowflake"""
        try:
            conn = self.get_snowflake_connection()
            if not conn:
                return pd.DataFrame()
                
            # Client-level aggregation from raw Snowflake data
            # Aggregate budgeted and actual time per client directly from raw tables
            query = f"""
            SELECT 
                wi.CLIENT,
                SUM(wib.BUDGETED_MINUTES / 60.0) as total_budgeted_hours,
                SUM(wib.ACTUAL_MINUTES / 60.0) as total_hours_logged_actual,
                SUM((wib.BUDGETED_MINUTES - wib.ACTUAL_MINUTES) / 60.0) as total_budget_variance_hours,
                COUNT(DISTINCT wib.WORK_ITEM_ID) as work_item_count,
                COUNT(DISTINCT wib.USER_NAME) as user_count,
                AVG(CASE 
                    WHEN wib.BUDGETED_MINUTES > 0 
                    THEN (wib.ACTUAL_MINUTES / wib.BUDGETED_MINUTES) * 100
                    ELSE NULL 
                END) as avg_budget_utilization_percentage,
                MAX(wi.REPORTING_DATE) as reporting_date
            FROM {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_BUDGET_VS_ACTUAL wib
            LEFT JOIN {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_DETAILS wi 
                ON wib.WORK_ITEM_ID = wi.WORK_ITEM_ID
            WHERE wib.USER_NAME IS NOT NULL
                AND wib.BUDGETED_MINUTES > 0
                AND wi.CLIENT IS NOT NULL
                AND wi.REPORTING_DATE = (
                    SELECT MAX(REPORTING_DATE) 
                    FROM {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_DETAILS
                )
            GROUP BY wi.CLIENT
            ORDER BY total_budgeted_hours DESC
            LIMIT {limit}
            """
            
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            print(f"Snowflake query returned columns: {columns}")
            
            # Fetch data and create DataFrame
            data = cursor.fetchall()
            print(f"Snowflake query returned {len(data)} rows")
            df = pd.DataFrame(data, columns=columns)
            print(f"Snowflake DataFrame shape: {df.shape}")
            if not df.empty:
                print(f"Snowflake DataFrame columns: {list(df.columns)}")
            
            cursor.close()
            return df
            
        except Exception as e:
            print(f"Error querying Snowflake: {e}")
            return pd.DataFrame()
            
    def query_snowflake_detailed(self, limit: int = 5000) -> pd.DataFrame:
        """Query detailed Snowflake data by client and user"""
        try:
            conn = self.get_snowflake_connection()
            if not conn:
                return pd.DataFrame()
                
            # Detailed query for individual user budget/time tracking data
            query = f"""
            SELECT 
                wi.CLIENT,
                wib.USER_NAME,
                wib.WORK_ITEM_ID,
                wi.WORK_TITLE,
                (wib.BUDGETED_MINUTES / 60.0) as budgeted_hours,
                (wib.ACTUAL_MINUTES / 60.0) as hours_logged_actual,
                ((wib.BUDGETED_MINUTES - wib.ACTUAL_MINUTES) / 60.0) as budget_variance_hours,
                CASE 
                    WHEN wib.BUDGETED_MINUTES > 0 
                    THEN (wib.ACTUAL_MINUTES / wib.BUDGETED_MINUTES) * 100
                    ELSE NULL 
                END as budget_utilization_percentage,
                wi.REPORTING_DATE
            FROM {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_BUDGET_VS_ACTUAL wib
            LEFT JOIN {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_DETAILS wi 
                ON wib.WORK_ITEM_ID = wi.WORK_ITEM_ID
            WHERE wib.USER_NAME IS NOT NULL
                AND wib.BUDGETED_MINUTES > 0
                AND wi.CLIENT IS NOT NULL
                AND wi.REPORTING_DATE = (
                    SELECT MAX(REPORTING_DATE) 
                    FROM {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_DETAILS
                )
            ORDER BY wi.CLIENT, wib.USER_NAME, wib.BUDGETED_MINUTES DESC
            LIMIT {limit}
            """
            
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            print(f"Snowflake detailed query returned columns: {columns}")
            
            # Fetch data and create DataFrame
            data = cursor.fetchall()
            print(f"Snowflake detailed query returned {len(data)} rows")
            df = pd.DataFrame(data, columns=columns)
            
            cursor.close()
            return df
            
        except Exception as e:
            print(f"Error querying detailed Snowflake data: {e}")
            return pd.DataFrame()
            
    def analyze_date_differences(self) -> Dict[str, Any]:
        """Analyze date/timing differences between BQ and Snowflake for hours recognition"""
        try:
            print("Starting date difference analysis...")
            
            # Analysis 1: Check BigQuery reporting date patterns
            bq_client = self.get_bq_client()
            
            # Query to analyze BQ date patterns
            bq_date_query = f"""
            WITH date_analysis AS (
                SELECT 
                    REPORTING_DATE,
                    individual_first_time_entry,
                    individual_last_time_entry,
                    DATE_DIFF(REPORTING_DATE, individual_last_time_entry, DAY) as reporting_lag_days,
                    COUNT(*) as record_count,
                    SUM(individual_hours_logged_actual) as total_hours,
                    COUNT(DISTINCT CLIENT) as unique_clients,
                    COUNT(DISTINCT budget_user_name) as unique_users
                FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
                WHERE REPORTING_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    AND individual_hours_logged_actual > 0
                    AND individual_last_time_entry IS NOT NULL
                GROUP BY REPORTING_DATE, individual_first_time_entry, individual_last_time_entry
            )
            SELECT 
                'BigQuery_Date_Analysis' as source,
                REPORTING_DATE,
                MIN(individual_first_time_entry) as earliest_time_entry,
                MAX(individual_last_time_entry) as latest_time_entry,
                AVG(reporting_lag_days) as avg_reporting_lag_days,
                MAX(reporting_lag_days) as max_reporting_lag_days,
                SUM(record_count) as total_records,
                SUM(total_hours) as total_hours,
                SUM(unique_clients) as total_unique_clients,
                SUM(unique_users) as total_unique_users
            FROM date_analysis
            GROUP BY REPORTING_DATE
            ORDER BY REPORTING_DATE DESC
            LIMIT 15
            """
            
            print("Executing BigQuery date analysis...")
            bq_date_result = bq_client.query(bq_date_query).to_dataframe()
            
            # Analysis 2: Check current comparison filter impact
            bq_comparison_impact_query = f"""
            WITH all_data AS (
                SELECT 
                    CLIENT,
                    REPORTING_DATE,
                    individual_hours_logged_actual,
                    individual_budgeted_hours,
                    budget_user_name,
                    individual_last_time_entry
                FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
                WHERE individual_hours_logged_actual > 0
                    AND CLIENT IS NOT NULL
            ),
            filtered_data AS (
                SELECT 
                    CLIENT,
                    REPORTING_DATE,
                    individual_hours_logged_actual,
                    individual_budgeted_hours,
                    budget_user_name,
                    individual_last_time_entry
                FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
                WHERE REPORTING_DATE = (
                    SELECT MAX(REPORTING_DATE) 
                    FROM `{self.project_id}.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
                )
                AND individual_budgeted_hours > 0
                AND budget_user_name IS NOT NULL
                AND CLIENT IS NOT NULL
            )
            SELECT 
                'Comparison_Filter_Impact' as analysis_type,
                all_data.CLIENT,
                SUM(all_data.individual_hours_logged_actual) as all_hours,
                COALESCE(SUM(filtered_data.individual_hours_logged_actual), 0) as filtered_hours,
                SUM(all_data.individual_hours_logged_actual) - COALESCE(SUM(filtered_data.individual_hours_logged_actual), 0) as hours_excluded,
                COUNT(all_data.CLIENT) as all_records,
                COUNT(filtered_data.CLIENT) as filtered_records,
                MAX(all_data.REPORTING_DATE) as latest_all_data_date,
                MAX(filtered_data.REPORTING_DATE) as latest_filtered_date,
                MAX(all_data.individual_last_time_entry) as latest_actual_time_entry
            FROM all_data
            LEFT JOIN filtered_data ON all_data.CLIENT = filtered_data.CLIENT 
                AND all_data.REPORTING_DATE = filtered_data.REPORTING_DATE
                AND all_data.budget_user_name = filtered_data.budget_user_name
            GROUP BY all_data.CLIENT
            HAVING SUM(all_data.individual_hours_logged_actual) - COALESCE(SUM(filtered_data.individual_hours_logged_actual), 0) > 1
            ORDER BY hours_excluded DESC
            LIMIT 20
            """
            
            print("Executing comparison filter impact analysis...")
            bq_filter_impact = bq_client.query(bq_comparison_impact_query).to_dataframe()
            
            # Analysis 3: Snowflake date patterns (if connection available)
            sf_date_analysis = None
            try:
                sf_conn = self.get_snowflake_connection()
                if sf_conn:
                    sf_date_query = f"""
                    SELECT 
                        'Snowflake_Date_Analysis' as source,
                        wi.REPORTING_DATE,
                        COUNT(*) as record_count,
                        SUM(wib.ACTUAL_MINUTES / 60.0) as total_hours,
                        COUNT(DISTINCT wi.CLIENT) as unique_clients,
                        COUNT(DISTINCT wib.USER_NAME) as unique_users,
                        MIN(wi.REPORTING_DATE) as min_reporting_date,
                        MAX(wi.REPORTING_DATE) as max_reporting_date
                    FROM {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_BUDGET_VS_ACTUAL wib
                    LEFT JOIN {self.snowflake_config['database']}.{self.snowflake_config['schema']}.WORK_ITEM_DETAILS wi 
                        ON wib.WORK_ITEM_ID = wi.WORK_ITEM_ID
                    WHERE wib.ACTUAL_MINUTES > 0
                        AND wi.CLIENT IS NOT NULL
                        AND wi.REPORTING_DATE >= DATEADD(day, -30, CURRENT_DATE())
                    GROUP BY wi.REPORTING_DATE
                    ORDER BY wi.REPORTING_DATE DESC
                    LIMIT 15
                    """
                    
                    cursor = sf_conn.cursor()
                    cursor.execute(sf_date_query)
                    columns = [desc[0] for desc in cursor.description]
                    data = cursor.fetchall()
                    sf_date_analysis = pd.DataFrame(data, columns=columns)
                    cursor.close()
                    print(f"Snowflake date analysis completed: {len(sf_date_analysis)} rows")
            except Exception as e:
                print(f"Snowflake date analysis failed: {e}")
                sf_date_analysis = pd.DataFrame()
            
            # Compile results
            analysis_results = {
                'timestamp': datetime.datetime.now(pytz.UTC).isoformat(),
                'bigquery_date_patterns': bq_date_result.to_dict('records') if not bq_date_result.empty else [],
                'comparison_filter_impact': bq_filter_impact.to_dict('records') if not bq_filter_impact.empty else [],
                'snowflake_date_patterns': sf_date_analysis.to_dict('records') if sf_date_analysis is not None and not sf_date_analysis.empty else [],
                'summary': {
                    'bq_analysis_records': len(bq_date_result),
                    'filter_impact_clients': len(bq_filter_impact),
                    'sf_analysis_records': len(sf_date_analysis) if sf_date_analysis is not None else 0,
                    'potential_issues': []
                }
            }
            
            # Identify potential issues
            if not bq_filter_impact.empty:
                total_excluded_hours = bq_filter_impact['hours_excluded'].sum()
                if total_excluded_hours > 0:
                    analysis_results['summary']['potential_issues'].append({
                        'type': 'HOURS_EXCLUDED_BY_FILTER',
                        'description': f'{total_excluded_hours:.2f} hours excluded by comparison filters',
                        'affected_clients': len(bq_filter_impact)
                    })
            
            if not bq_date_result.empty:
                avg_lag = bq_date_result['avg_reporting_lag_days'].mean()
                max_lag = bq_date_result['max_reporting_lag_days'].max()
                if avg_lag > 1:
                    analysis_results['summary']['potential_issues'].append({
                        'type': 'REPORTING_LAG',
                        'description': f'Average {avg_lag:.1f} day lag between time entry and reporting (max: {max_lag} days)',
                        'impact': 'Time entries may appear in different reporting periods'
                    })
            
            return analysis_results
            
        except Exception as e:
            print(f"Date analysis failed: {e}")
            return {'error': f'Date analysis failed: {str(e)}'}

    def compare_data(self) -> Dict[str, Any]:
        """Compare BigQuery and Snowflake data"""
        try:
            print("Starting data comparison...")
            
            # Get data from both sources
            print("Querying BigQuery...")
            bq_data = self.query_bigquery_view()
            print(f"BigQuery returned {len(bq_data)} rows")
            
            print("Querying Snowflake...")
            sf_data = self.query_snowflake_data()
            print(f"Snowflake returned {len(sf_data)} rows")
            
            if bq_data.empty and sf_data.empty:
                print("No data from either source")
                return {'error': 'No data available from either source'}
            elif bq_data.empty:
                print("No data from BigQuery")
                return {'error': 'No data available from BigQuery'}
            elif sf_data.empty:
                print("No data from Snowflake")
                return {'error': 'No data available from Snowflake'}
                
            # Standardize column names - Snowflake returns uppercase, BigQuery returns mixed case
            sf_data_std = sf_data.copy()
            bq_data_std = bq_data.copy()
            
            # Standardize Snowflake columns to match BigQuery naming
            sf_column_mapping = {
                'TOTAL_BUDGETED_HOURS': 'total_budgeted_hours',
                'TOTAL_HOURS_LOGGED_ACTUAL': 'total_hours_logged_actual', 
                'TOTAL_BUDGET_VARIANCE_HOURS': 'total_budget_variance_hours',
                'WORK_ITEM_COUNT': 'work_item_count',
                'USER_COUNT': 'user_count',
                'AVG_BUDGET_UTILIZATION_PERCENTAGE': 'avg_budget_utilization_percentage',
                'REPORTING_DATE': 'reporting_date'
            }
            
            for old_col, new_col in sf_column_mapping.items():
                if old_col in sf_data_std.columns:
                    sf_data_std = sf_data_std.rename(columns={old_col: new_col})
            
            # Create comparison key using CLIENT
            bq_data_std['comparison_key'] = bq_data_std['CLIENT'].astype(str)
            sf_data_std['comparison_key'] = sf_data_std['CLIENT'].astype(str)
            
            # Find common records
            common_keys = set(bq_data_std['comparison_key']).intersection(set(sf_data_std['comparison_key']))
            
            # Filter to common records for comparison
            bq_common = bq_data_std[bq_data_std['comparison_key'].isin(common_keys)].set_index('comparison_key')
            sf_common = sf_data_std[sf_data_std['comparison_key'].isin(common_keys)].set_index('comparison_key')
            
            # Compare key metrics
            comparison_results = []
            discrepancies = []
            
            for key in common_keys:
                if key in bq_common.index and key in sf_common.index:
                    bq_row = bq_common.loc[key]
                    sf_row = sf_common.loc[key]
                    
                    # Compare numeric values with tolerance
                    tolerance = 0.01  # 1% tolerance for floating point comparisons
                    
                    # Handle potential Series values (in case of duplicates) by taking first value
                    def safe_float(value):
                        if hasattr(value, 'iloc'):  # It's a Series
                            val = value.iloc[0] if len(value) > 0 else 0
                            return float(val) if pd.notna(val) else 0.0
                        else:  # It's a scalar
                            try:
                                return float(value) if pd.notna(value) else 0.0
                            except (TypeError, ValueError):
                                return 0.0
                    
                    bq_budget = safe_float(bq_row['total_budgeted_hours'])
                    sf_budget = safe_float(sf_row['total_budgeted_hours'])
                    bq_hours = safe_float(bq_row['total_hours_logged_actual'])
                    sf_hours = safe_float(sf_row['total_hours_logged_actual'])
                    bq_variance = safe_float(bq_row['total_budget_variance_hours'])
                    sf_variance = safe_float(sf_row['total_budget_variance_hours'])
                    
                    budget_match = abs(bq_budget - sf_budget) <= tolerance
                    hours_match = abs(bq_hours - sf_hours) <= tolerance
                    variance_match = abs(bq_variance - sf_variance) <= tolerance
                    
                    # Helper function to safely get string values
                    def safe_str(value):
                        if hasattr(value, 'iloc'):  # It's a Series
                            return str(value.iloc[0]) if len(value) > 0 else ''
                        else:  # It's a scalar
                            return str(value) if pd.notna(value) else ''
                    
                    result = {
                        'client': safe_str(bq_row['CLIENT']),
                        'bq_total_budgeted_hours': bq_budget,
                        'sf_total_budgeted_hours': sf_budget,
                        'bq_total_hours_logged': bq_hours,
                        'sf_total_hours_logged': sf_hours,
                        'bq_total_variance': bq_variance,
                        'sf_total_variance': sf_variance,
                        'bq_work_item_count': safe_float(bq_row['work_item_count']),
                        'sf_work_item_count': safe_float(sf_row['work_item_count']),
                        'bq_user_count': safe_float(bq_row['user_count']),
                        'sf_user_count': safe_float(sf_row['user_count']),
                        'budget_match': budget_match,
                        'hours_match': hours_match,
                        'variance_match': variance_match,
                        'overall_match': budget_match and hours_match and variance_match
                    }
                    
                    comparison_results.append(result)
                    
                    if not result['overall_match']:
                        discrepancies.append(result)
            
            # Calculate summary statistics for client-level comparison
            total_records = len(comparison_results)
            matching_records = len([r for r in comparison_results if r['overall_match']])
            discrepancy_count = len(discrepancies)
            
            # Calculate total hours across all clients (handle Decimal types)
            def safe_sum(series):
                try:
                    return float(series.sum())
                except (TypeError, ValueError):
                    return float(sum(float(x) for x in series if pd.notna(x)))
                    
            total_bq_budgeted = safe_sum(bq_data_std['total_budgeted_hours'])
            total_sf_budgeted = safe_sum(sf_data_std['total_budgeted_hours'])
            total_bq_logged = safe_sum(bq_data_std['total_hours_logged_actual'])
            total_sf_logged = safe_sum(sf_data_std['total_hours_logged_actual'])
            
            summary = {
                'total_bq_clients': len(bq_data_std),
                'total_sf_clients': len(sf_data_std),
                'common_clients': len(common_keys),
                'matching_clients': matching_records,
                'discrepancy_count': discrepancy_count,
                'match_percentage': (matching_records / total_records * 100) if total_records > 0 else 0,
                'bq_only_clients': len(bq_data_std) - len(common_keys),
                'sf_only_clients': len(sf_data_std) - len(common_keys),
                'total_bq_budgeted_hours': total_bq_budgeted,
                'total_sf_budgeted_hours': total_sf_budgeted,
                'total_bq_logged_hours': total_bq_logged,
                'total_sf_logged_hours': total_sf_logged,
                'budgeted_hours_difference': total_bq_budgeted - total_sf_budgeted,
                'logged_hours_difference': total_bq_logged - total_sf_logged
            }
            
            return {
                'summary': summary,
                'comparison_results': comparison_results[:100],  # Limit for display
                'discrepancies': discrepancies[:50],  # Top 50 discrepancies
                'timestamp': datetime.datetime.now(pytz.UTC).isoformat()
            }
            
        except Exception as e:
            return {'error': f'Comparison failed: {str(e)}'}
            
    def close_connections(self):
        """Close database connections"""
        if self.snowflake_conn:
            self.snowflake_conn.close()
            self.snowflake_conn = None

dashboard = PipelineDashboard()
data_comparison = DataComparison()

@functions_framework.http
def pipeline_dashboard(request):
    """Main dashboard endpoint with authentication"""
    
    # Get the path from the request
    path = request.path
    
    # Handle different routes
    if path == '/login' or path.endswith('/login'):
        return render_template_string(LOGIN_HTML, client_id=GOOGLE_CLIENT_ID)
    
    elif path == '/auth/verify' or path.endswith('/auth/verify'):
        if request.method == 'POST':
            try:
                print(f"Auth verify request received. Headers: {dict(request.headers)}")
                data = request.get_json()
                print(f"Request data: {data}")
                
                if not data:
                    return jsonify({'success': False, 'error': 'No JSON data provided'})
                    
                token = data.get('credential')
                
                if not token:
                    return jsonify({'success': False, 'error': 'No credential provided in request'})
                
                print(f"Token received, length: {len(token)}")
                
                # Verify the token
                user_info = verify_google_token(token)
                print(f"Verification result: {user_info}")
                
                if user_info.get('verified'):
                    # Authentication successful - return success without using sessions for now
                    print(f"User authenticated successfully: {user_info['email']}")
                    # Return user info in response so frontend can handle it
                    return jsonify({
                        'success': True, 
                        'user': {
                            'email': user_info['email'],
                            'name': user_info['name'],
                            'verified': True
                        }
                    })
                else:
                    print(f"Authentication failed: {user_info.get('error')}")
                    return jsonify({'success': False, 'error': user_info.get('error', 'Authentication failed')})
                    
            except Exception as e:
                print(f"Exception in auth verification: {str(e)}")
                return jsonify({'success': False, 'error': f'Verification failed: {str(e)}'})
        else:
            return jsonify({'success': False, 'error': 'Method not allowed'})
    
    elif path == '/logout' or path.endswith('/logout'):
        # Clear any session data if it exists
        try:
            session.clear()
        except:
            pass
        # Return a page that clears localStorage and redirects
        logout_html = '''
        <script>
            localStorage.removeItem('dashboard_user');
            window.location.href = '/karbon-pipeline-dashboard/login';
        </script>
        <p>Logging out...</p>
        '''
        return logout_html
    
    elif path == '/api/user' or path.endswith('/api/user'):
        user = session.get('user', {})
        return jsonify({'user': user if user.get('verified') else None})
    
    elif path == '/api/comparison' or path.endswith('/api/comparison'):
        # Data comparison API endpoint
        try:
            print("API /api/comparison called")
            comparison_data = data_comparison.compare_data()
            return jsonify(comparison_data)
        except Exception as e:
            print(f"Exception in comparison API: {str(e)}")
            return jsonify({'error': f'Comparison failed: {str(e)}'})
    
    elif path == '/api/detailed-data' or path.endswith('/api/detailed-data'):
        # Detailed data export API endpoint
        try:
            print("API /api/detailed-data called")
            
            # Get query parameter for data source
            source = request.args.get('source', 'both')  # 'bigquery', 'snowflake', or 'both'
            
            result = {'timestamp': datetime.datetime.now(pytz.UTC).isoformat()}
            
            if source in ['bigquery', 'both']:
                print("Fetching detailed BigQuery data...")
                bq_detailed = data_comparison.query_bigquery_detailed()
                if not bq_detailed.empty:
                    # Convert to records for JSON serialization
                    result['bigquery_data'] = bq_detailed.to_dict('records')
                    result['bigquery_count'] = len(bq_detailed)
                    print(f"BigQuery detailed data: {len(bq_detailed)} records")
                else:
                    result['bigquery_data'] = []
                    result['bigquery_count'] = 0
            
            if source in ['snowflake', 'both']:
                print("Fetching detailed Snowflake data...")
                sf_detailed = data_comparison.query_snowflake_detailed()
                if not sf_detailed.empty:
                    # Convert to records for JSON serialization, handling Decimal types
                    records = []
                    for _, row in sf_detailed.iterrows():
                        record = {}
                        for col, val in row.items():
                            if pd.isna(val):
                                record[col] = None
                            elif isinstance(val, (int, float)):
                                record[col] = float(val)
                            else:
                                record[col] = str(val)
                        records.append(record)
                    result['snowflake_data'] = records
                    result['snowflake_count'] = len(sf_detailed)
                    print(f"Snowflake detailed data: {len(sf_detailed)} records")
                else:
                    result['snowflake_data'] = []
                    result['snowflake_count'] = 0
            
            return jsonify(result)
            
        except Exception as e:
            print(f"Exception in detailed data API: {str(e)}")
            return jsonify({'error': f'Failed to fetch detailed data: {str(e)}'})
    
    elif path == '/api/date-analysis' or path.endswith('/api/date-analysis'):
        # Date analysis API endpoint
        try:
            print("API /api/date-analysis called")
            analysis_data = data_comparison.analyze_date_differences()
            return jsonify(analysis_data)
        except Exception as e:
            print(f"Exception in date analysis API: {str(e)}")
            return jsonify({'error': f'Date analysis failed: {str(e)}'})
    
    elif path == '/comparison' or path.endswith('/comparison'):
        # Data comparison page
        return render_template_string(COMPARISON_HTML)
    
    elif path == '/api/test' or path.endswith('/api/test'):
        # Test endpoint to verify function execution
        print("Test endpoint called - function is working!")
        try:
            # Test BigQuery connection with the exact comparison query
            from google.cloud import bigquery
            import pandas as pd
            client = bigquery.Client(project='red-octane-444308-f4')
            
            # Test 1: Total count
            query1 = "SELECT COUNT(*) as count FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`"
            result1 = client.query(query1).result()
            total_count = list(result1)[0][0]
            
            # Test 2: Filtered count (exact query from comparison)
            query2 = """
            SELECT COUNT(*) as count
            FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            WHERE REPORTING_DATE = (
                SELECT MAX(REPORTING_DATE) 
                FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            )
            AND individual_budgeted_hours > 0
            AND budget_user_name IS NOT NULL
            """
            result2 = client.query(query2).result()
            filtered_count = list(result2)[0][0]
            
            # Test 3: Try the actual dataframe conversion
            query3 = """
            SELECT 
                WORK_ITEM_ID,
                WORK_TITLE,
                CLIENT,
                budget_user_name,
                individual_budgeted_hours as budgeted_hours,
                individual_hours_logged_actual as hours_logged_actual
            FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            WHERE REPORTING_DATE = (
                SELECT MAX(REPORTING_DATE) 
                FROM `red-octane-444308-f4.karbon_data.WORK_ITEM_INDIVIDUAL_BUDGET_TIME_TRACKING_VIEW_V5`
            )
            AND individual_budgeted_hours > 0
            AND budget_user_name IS NOT NULL
            LIMIT 5
            """
            query_job3 = client.query(query3)
            df = query_job3.to_dataframe()
            
            print(f"BigQuery tests: Total={total_count}, Filtered={filtered_count}, DataFrame rows={len(df)}")
            return jsonify({
                'message': 'Test successful', 
                'timestamp': datetime.datetime.now().isoformat(),
                'total_records': total_count,
                'filtered_records': filtered_count,
                'dataframe_rows': len(df),
                'sample_data': df.head(2).to_dict('records') if not df.empty else []
            })
        except Exception as e:
            print(f"BigQuery test failed: {e}")
            return jsonify({
                'message': 'Test successful', 
                'timestamp': datetime.datetime.now().isoformat(),
                'bigquery_error': str(e)
            })
    
    elif path == '/api/status' or path.endswith('/api/status'):
        # For now, allow API access since frontend handles auth via localStorage
        return jsonify(dashboard.get_dashboard_data())
    
    else:
        # Main dashboard route - for now, just serve the dashboard
        # Authentication will be checked on the frontend via localStorage
        return render_template_string(DASHBOARD_HTML)
