# 🔧 OAuth Configuration Fix Instructions

## Issue Identified
The OAuth redirect URIs in your Google Cloud Console don't match what the dashboard expects.

## 📋 Step 1: Update OAuth Configuration in Google Cloud Console

**Go to your OAuth Client configuration** (you already have it open):
https://console.cloud.google.com/apis/credentials/oauthclient/215368045889-9kvil1h97hhq843drngse7m68fdc8j3b.apps.googleusercontent.com?project=red-octane-444308-f4

### Update Authorized redirect URIs:
**Replace the current URIs with these exact ones:**

1. `https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard`
2. `https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard/auth/callback`
3. `https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard/login`

### Update Authorized JavaScript origins:
**Keep this as is:**
- `https://us-central1-red-octane-444308-f4.cloudfunctions.net`

## 📋 Step 2: Click "SAVE" in the OAuth configuration

After updating the redirect URIs, click the "SAVE" button at the bottom of the OAuth client configuration page.

## 📋 Step 3: Test the Dashboard

After saving the OAuth configuration:

1. **Wait 1-2 minutes** for changes to propagate
2. **Clear your browser cache** (or open an incognito window)
3. **Visit the dashboard URL:**
   ```
   https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard
   ```

## 🔍 Expected Behavior After Fix

1. **Main URL** → Should redirect to login page
2. **Login page** → Should show Google Sign-In button
3. **Click Sign-In** → Should work without "Page not found" error
4. **After authentication** → Should return to dashboard

## ⚠️ If Still Not Working

If you still get errors after updating the OAuth configuration, run this command:

```bash
# Check function logs for detailed errors
gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard' --limit=10
```

## 🎯 Quick Test Commands

```bash
# Test main dashboard (should redirect to login)
curl -I https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard

# Test login page (should return 200 OK)
curl -I https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard/login
```

Both should work without errors after the OAuth fix.
