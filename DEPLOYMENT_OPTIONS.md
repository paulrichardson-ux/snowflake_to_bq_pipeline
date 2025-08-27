# 🚀 Karbon Dashboard Deployment Options

This document outlines the different deployment options available for the Karbon Pipeline Dashboard with data comparison features.

## 🎯 Overview

The Karbon Dashboard offers multiple deployment approaches depending on your security requirements and infrastructure preferences.

## 🔐 Option 1: Secret Manager (Recommended)

**Best for: Production environments, high-security requirements**

### Features
- ✅ Snowflake credentials stored in Google Cloud Secret Manager
- ✅ Encryption at rest and in transit
- ✅ Audit logging of all credential access
- ✅ IAM-based access control
- ✅ Secret rotation capabilities
- ✅ Compliance-ready

### Quick Start
```bash
# Step 1: Store credentials securely
./setup_secrets.sh

# Step 2: Deploy with Secret Manager integration
./deploy_secure_dashboard_with_secrets.sh
```

### Documentation
- [SECRET_MANAGER_SETUP.md](SECRET_MANAGER_SETUP.md) - Detailed Secret Manager guide
- [DATA_COMPARISON_SETUP.md](DATA_COMPARISON_SETUP.md) - Data comparison features

---

## 🔧 Option 2: Environment Variables

**Best for: Development, testing, quick setup**

### Features
- ✅ Simple setup with environment variables
- ✅ Quick deployment
- ⚠️ Less secure than Secret Manager
- ⚠️ Credentials visible in function configuration

### Quick Start
```bash
# Step 1: Configure environment
cd dashboard
./setup_environment.sh
# Edit .env file with your credentials

# Step 2: Deploy with environment variables
./deploy_secure_dashboard_with_comparison.sh
```

---

## 📊 Feature Comparison

| Feature | Secret Manager | Environment Variables |
|---------|---------------|----------------------|
| **Security** | 🟢 Excellent | 🟡 Good |
| **Setup Complexity** | 🟡 Medium | 🟢 Simple |
| **Audit Logging** | ✅ Yes | ❌ No |
| **Credential Rotation** | ✅ Easy | 🟡 Manual |
| **Compliance Ready** | ✅ Yes | 🟡 Depends |
| **Production Ready** | ✅ Yes | ⚠️ With caution |
| **Cost** | 💰 Minimal | 💰 Free |

## 🛡️ Security Considerations

### Secret Manager Advantages
- **Encryption**: All secrets encrypted with Google-managed keys
- **Access Control**: Fine-grained IAM permissions
- **Audit Trail**: Complete logging of who accessed what and when
- **Rotation**: Built-in support for credential rotation
- **Compliance**: Meets SOC, ISO, and other compliance requirements

### Environment Variable Limitations
- **Visibility**: Credentials visible in Cloud Console
- **Logging**: No detailed access logging
- **Rotation**: Manual process requiring redeployment
- **Audit**: Limited audit capabilities

## 🚀 Deployment Scripts

### Secret Manager Scripts
- `setup_secrets.sh` - Store credentials in Secret Manager
- `deploy_secure_dashboard_with_secrets.sh` - Deploy with Secret Manager integration

### Environment Variable Scripts
- `dashboard/setup_environment.sh` - Configure environment variables
- `deploy_secure_dashboard_with_comparison.sh` - Deploy with environment variables

### Basic Dashboard (No Comparison)
- `deploy_secure_dashboard.sh` - Basic dashboard without data comparison

## 📋 Prerequisites

### Common Requirements
- Google Cloud CLI (`gcloud`) installed and authenticated
- Project: `red-octane-444308-f4`
- Google OAuth Client ID configured
- Snowflake account with read access to KARBON database

### Secret Manager Additional Requirements
- Secret Manager API enabled
- Service account with `secretmanager.secretAccessor` role

## 🔄 Migration Path

### From Environment Variables to Secret Manager
1. **Store secrets**: Run `./setup_secrets.sh`
2. **Redeploy**: Run `./deploy_secure_dashboard_with_secrets.sh`
3. **Verify**: Test the comparison functionality
4. **Cleanup**: Remove old environment variables if desired

The code includes fallback logic, so migration is seamless.

## 🧪 Testing Your Deployment

After deployment, verify:
1. **Dashboard loads**: Access the main dashboard URL
2. **Authentication works**: Sign in with @fiskalfinance.com account
3. **Pipeline monitoring**: View function and scheduler status
4. **Data comparison**: Navigate to comparison tab
5. **Snowflake connection**: Verify data loads from Snowflake
6. **BigQuery connection**: Confirm BigQuery data appears

## 📊 Monitoring and Maintenance

### Secret Manager Monitoring
```bash
# View secret access logs
gcloud logging read 'protoPayload.serviceName="secretmanager.googleapis.com"' --limit=20

# List secrets
gcloud secrets list --filter="labels.component=karbon-dashboard"

# Update a secret
echo "new-value" | gcloud secrets versions add SECRET_NAME --data-file=-
```

### Environment Variable Monitoring
```bash
# View function logs
gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard' --limit=50

# Update environment variables (requires redeployment)
# Edit deployment script and redeploy
```

## 🆘 Troubleshooting

### Common Issues

#### Secret Manager
- **"Secret not found"**: Run `./setup_secrets.sh`
- **"Permission denied"**: Check IAM roles for service account
- **"Connection failed"**: Verify secret values are correct

#### Environment Variables
- **"Variable not set"**: Check deployment script configuration
- **"Connection failed"**: Verify environment variables are correct
- **"Access denied"**: Check Snowflake credentials

### Debug Commands
```bash
# Test secret access
gcloud secrets versions access latest --secret=snowflake-user

# View function logs
gcloud logging read 'resource.type=cloud_function' --limit=10

# Test API endpoints
curl -s "YOUR_FUNCTION_URL/api/status"
curl -s "YOUR_FUNCTION_URL/api/comparison"
```

## 📞 Support

For deployment issues:
1. Check the relevant documentation (SECRET_MANAGER_SETUP.md or DATA_COMPARISON_SETUP.md)
2. Review Cloud Function logs
3. Verify credentials and permissions
4. Test individual components

## 🎯 Recommendations

### For Production
- ✅ Use Secret Manager deployment
- ✅ Enable audit logging
- ✅ Set up monitoring alerts
- ✅ Implement regular credential rotation
- ✅ Review access logs monthly

### For Development
- ✅ Environment variables acceptable for speed
- ✅ Use separate dev/prod credentials
- ✅ Test Secret Manager before production
- ✅ Keep credentials secure even in dev

---

**Choose the deployment option that best fits your security requirements and operational preferences!**
