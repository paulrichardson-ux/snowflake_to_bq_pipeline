# 🔐 Secure Karbon Pipeline Dashboard Setup

This guide walks you through setting up a secure, Google SSO-protected dashboard for monitoring your Karbon pipeline functions.

## 🎯 Security Features

- ✅ **Google SSO Authentication** - Users must sign in with Google
- ✅ **Domain Restriction** - Only @fiskalfinance.com email addresses allowed
- ✅ **Session Management** - Secure, encrypted sessions
- ✅ **API Protection** - All endpoints require authentication
- ✅ **Auto-logout** - Sessions expire for security
- 🆕 **Secret Manager** - Snowflake credentials stored securely in Google Cloud Secret Manager
- 🆕 **Audit Logging** - All credential access is logged and auditable

## 📋 Setup Steps

### Step 1: Configure Google OAuth

1. **Run the OAuth setup script:**
   ```bash
   ./setup_oauth_credentials.sh
   ```

2. **Follow the interactive prompts to:**
   - Create Google OAuth Client ID in Cloud Console
   - Configure OAuth consent screen
   - Set authorized domains and redirect URIs
   - Store credentials securely

### Step 2: Deploy Secured Dashboard with Comparison Features

**🔐 Recommended: Using Secret Manager for Snowflake credentials**
```bash
# First, set up secrets securely
./setup_secrets.sh

# Then deploy with Secret Manager integration
./deploy_secure_dashboard_with_secrets.sh
```

**Alternative: Using environment variables**
```bash
./deploy_secure_dashboard_with_comparison.sh
```

This will:
- Deploy the enhanced dashboard with authentication
- Set up BigQuery and Snowflake connections
- Configure IAM policies and Secret Manager access
- Set up domain restrictions
- Test the deployment including comparison features

For the basic dashboard without comparison:
```bash
./deploy_secure_dashboard.sh
```

### Step 3: Access Your Dashboard

1. Open the dashboard URL (provided after deployment)
2. Click "Sign in with Google"
3. Use your @fiskalfinance.com Google account
4. Start monitoring your pipeline!
5. 🆕 Click "📊 Data Comparison" to validate data accuracy between BigQuery and Snowflake

## 🌐 Dashboard URL

After deployment, your secure dashboard will be available at:
```

https://us-central1-red-octane-444308-f4.cloudfunctions.net/karbon-pipeline-dashboard
```

## 🔒 Access Control

### Who Can Access
- ✅ Any user with an @fiskalfinance.com email address
- ✅ Users must authenticate via Google SSO
- ❌ External users are automatically denied

### Authentication Flow
1. User visits dashboard URL
2. Redirected to Google OAuth login
3. User signs in with Google account
4. System verifies email domain is fiskalfinance.com
5. If verified, user gains access to dashboard
6. If not verified, access is denied with clear message

## 📊 Dashboard Features

### Real-time Monitoring
- **Function Status** - All Cloud Functions with active/error states
- **Scheduler Status** - All scheduled jobs with last/next run times
- **Health Overview** - Summary statistics and alerts
- **Auto-refresh** - Updates every 30 seconds

### 🆕 Data Comparison (New!)
- **BigQuery vs Snowflake** - Compare data accuracy between systems
- **Discrepancy Detection** - Identify data inconsistencies automatically
- **Budget vs Actual Analysis** - Validate time tracking and budget data
- **Interactive Filtering** - Search and filter comparison results
- **Match Rate Analytics** - View overall data quality metrics

### User Experience
- **Welcome Message** - Shows logged-in user's name
- **Logout Option** - Secure session termination
- **Mobile Responsive** - Works on all devices
- **Modern UI** - Clean, professional interface
- **Multi-page Navigation** - Easy switching between dashboard and comparison

## 🔧 Management

### View Logs
```bash
gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard'
```

### Update Dashboard
```bash
# Make changes to dashboard/main.py
./deploy_secure_dashboard.sh
```

### Update OAuth Settings
```bash
# Update OAuth configuration
./setup_oauth_credentials.sh
./deploy_secure_dashboard.sh
```

### Delete Dashboard
```bash
gcloud functions delete karbon-pipeline-dashboard --region=us-central1
```

## 🛡️ Security Best Practices

### OAuth Consent Screen Configuration
- **Application Name**: Karbon Pipeline Dashboard
- **User Support Email**: your-email@fiskalfinance.com
- **Authorized Domains**: fiskalfinance.com
- **Scopes**: email, profile, openid

### Domain Verification
- Ensure fiskalfinance.com is verified in Google Search Console
- Configure OAuth consent screen for internal use
- Test with multiple fiskalfinance.com accounts

### Session Security
- Sessions use cryptographically secure keys
- Automatic session expiration
- Secure cookie settings
- HTTPS-only access

## 🚨 Troubleshooting

### "Access Denied" Error
- Verify user has @fiskalfinance.com email
- Check OAuth consent screen configuration
- Ensure domain is properly verified

### Authentication Loop
- Clear browser cookies/cache
- Verify OAuth Client ID is correct
- Check redirect URIs in OAuth configuration

### Dashboard Not Loading
- Check function deployment status
- Verify IAM permissions
- Review function logs for errors

## 📞 Support

For issues with the secure dashboard:

1. **Check function logs** for detailed error messages
2. **Verify OAuth configuration** in Google Cloud Console
3. **Test with different fiskalfinance.com accounts**
4. **Review IAM policies** for service account permissions

## 🎉 Success Checklist

After setup, verify:

- [ ] Dashboard URL loads and shows login page
- [ ] Google SSO button appears and works
- [ ] fiskalfinance.com users can sign in successfully
- [ ] External users are denied access
- [ ] Dashboard shows pipeline function status
- [ ] Auto-refresh works every 30 seconds
- [ ] Logout functionality works
- [ ] Mobile/tablet access works

## 🔄 Maintenance

### Regular Tasks
- **Monitor function logs** for authentication issues
- **Update OAuth credentials** if expired
- **Review user access** periodically
- **Test authentication flow** monthly

### Updates
- Dashboard code updates: Redeploy with `./deploy_secure_dashboard.sh`
- OAuth changes: Run `./setup_oauth_credentials.sh` then redeploy
- Security updates: Monitor Google OAuth library updates

---

**🎯 Your secure Karbon pipeline dashboard is now ready for the fiskalfinance.com team!**
