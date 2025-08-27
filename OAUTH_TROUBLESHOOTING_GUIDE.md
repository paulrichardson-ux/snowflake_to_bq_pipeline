# 🔧 OAuth Troubleshooting Guide

## Current Setup ✅
- **Client ID**: `215368045889-9kvil1h97hhq843drngse7m68fdc8j3b.apps.googleusercontent.com`
- **Name**: `Karbon_Automation`
- **JavaScript Origins**: `https://us-central1-red-octane-444308-f4.cloudfunctions.net`
- **Redirect URIs**: 
  - `https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard`
  - `https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard/login`
  - `https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard/auth/callback`

## 🔍 Debugging Steps

### Step 1: Test the Dashboard with Browser Console Open

1. **Open the dashboard**: https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard
2. **Open Developer Tools** (F12 or right-click → Inspect)
3. **Go to Console tab**
4. **Try to sign in** and watch for console messages

### Step 2: Check What Errors You See

The new version includes detailed logging. Look for these console messages:

```javascript
// Good signs:
"Initializing Google Sign-In with client ID: 215368045889-9kvil1h97hhq843drngse7m68fdc8j3b.apps.googleusercontent.com"
"Google Sign-In initialized successfully"
"Credential response received: [object]"

// Error signs:
"Error initializing Google Sign-In: [error]"
"Authentication error: [error]"
"Backend response status: [not 200]"
```

### Step 3: Check Function Logs

Run this command to see detailed backend logs:

```bash
gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard' --limit=10 --format="value(timestamp,severity,textPayload)" | grep -v "^$"
```

Look for these log messages:
- `"Verifying token with Client ID: ..."`
- `"Token verified successfully. User info: ..."`
- `"User email: ..., domain: ..., allowed domain: fiskalfinance.com"`

## 🚨 Common Issues & Solutions

### Issue 1: "Google Sign-In button not appearing"
**Cause**: Google Identity Services not loading
**Solution**: 
1. Check if you have ad blockers disabled
2. Ensure you're using HTTPS (not HTTP)
3. Check browser console for script loading errors

### Issue 2: "Authentication error" after clicking sign-in
**Cause**: OAuth configuration mismatch
**Solution**: 
1. Verify your OAuth Client ID matches exactly: `215368045889-9kvil1h97hhq843drngse7m68fdc8j3b.apps.googleusercontent.com`
2. Ensure redirect URIs are exactly as listed above
3. Check OAuth consent screen is configured

### Issue 3: "Access denied" for fiskalfinance.com users
**Cause**: Domain restriction not working properly
**Solution**:
1. Check function logs for the exact email and domain being detected
2. Ensure the user is signing in with their @fiskalfinance.com account (not personal Gmail)

### Issue 4: "Page not found" errors
**Cause**: Routing issues
**Solution**: Already fixed in latest deployment

## 📋 OAuth Consent Screen Requirements

Make sure your OAuth consent screen has:

1. **Application name**: Karbon Pipeline Dashboard
2. **User support email**: your-email@fiskalfinance.com  
3. **Authorized domains**: `fiskalfinance.com`
4. **Developer contact**: your-email@fiskalfinance.com
5. **Publishing status**: In production (or Testing with test users added)

## 🧪 Test Commands

```bash
# Test main dashboard (should redirect to login)
curl -I https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard

# Test login page (should return 200)
curl -I https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard/login

# Check recent function logs
gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard' --limit=5
```

## 🔄 If Still Not Working

1. **Clear browser cache** completely
2. **Try incognito/private browsing mode**
3. **Test with different @fiskalfinance.com accounts**
4. **Check if OAuth consent screen needs approval** (if app is in testing mode)

## 📞 Next Steps

Try accessing the dashboard now with the browser console open and let me know:

1. **What console messages you see**
2. **What error appears (if any)**  
3. **At what point it fails** (page load, sign-in click, after authentication)

The new version has extensive debugging that will help us identify exactly what's going wrong!

## 🎯 Quick Test

**Right now, try this:**
1. Open: https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard
2. Open browser console (F12)
3. Try to sign in
4. Share what messages appear in the console

This will help us pinpoint the exact issue! 🔍
