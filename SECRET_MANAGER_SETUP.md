# 🔐 Secret Manager Setup for Karbon Dashboard

This guide explains how to securely store and manage Snowflake credentials using Google Cloud Secret Manager for the Karbon Pipeline Dashboard.

## 🎯 Why Use Secret Manager?

Using Google Cloud Secret Manager provides several security advantages over environment variables:

- **🔒 Encryption**: Secrets are encrypted at rest and in transit
- **🔍 Audit Logging**: All secret access is logged and auditable
- **⚡ Fine-grained Access**: IAM-based access control
- **🔄 Rotation**: Built-in secret rotation capabilities
- **🛡️ Compliance**: Meets enterprise security requirements
- **📊 Monitoring**: Integration with Cloud Monitoring and Logging

## 🚀 Quick Start

### Step 1: Store Secrets
```bash
./setup_secrets.sh
```

### Step 2: Deploy Dashboard
```bash
./deploy_secure_dashboard_with_secrets.sh
```

### Step 3: Access Dashboard
Navigate to your dashboard URL and enjoy secure, credential-free operation!

## 📋 Detailed Setup

### Prerequisites
- Google Cloud CLI (`gcloud`) installed and authenticated
- Project ID: `red-octane-444308-f4`
- Appropriate IAM permissions for Secret Manager

### Secret Configuration

The following secrets are created in Secret Manager:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `snowflake-user` | Snowflake username | `your_username` |
| `snowflake-password` | Snowflake password | `your_secure_password` |
| `snowflake-account` | Snowflake account identifier | `abc12345.us-east-1` |
| `snowflake-warehouse` | Snowflake warehouse | `COMPUTE_WH` |
| `snowflake-database` | Snowflake database | `KARBON` |
| `snowflake-schema` | Snowflake schema | `PUBLIC` |

### IAM Permissions

The Cloud Function service account requires:
- `roles/secretmanager.secretAccessor` - To read secrets

This is automatically configured by the setup scripts.

## 🔧 Management Commands

### List All Karbon Dashboard Secrets
```bash
gcloud secrets list --filter="labels.component=karbon-dashboard"
```

### View Secret Metadata
```bash
gcloud secrets describe snowflake-user
```

### Access Secret Value (for debugging)
```bash
gcloud secrets versions access latest --secret=snowflake-user
```

### Update a Secret
```bash
echo "new-password" | gcloud secrets versions add snowflake-password --data-file=-
```

### Delete a Secret
```bash
gcloud secrets delete snowflake-user
```

## 🔄 Secret Rotation

### Manual Rotation
1. Update the secret in Secret Manager:
   ```bash
   echo "new-password" | gcloud secrets versions add snowflake-password --data-file=-
   ```

2. The dashboard will automatically use the new secret on the next request (no redeployment needed!)

### Automated Rotation (Future Enhancement)
- Set up Cloud Scheduler to trigger secret rotation
- Integrate with Snowflake's password policy
- Configure alerts for rotation events

## 📊 Monitoring and Logging

### View Secret Access Logs
```bash
gcloud logging read 'protoPayload.serviceName="secretmanager.googleapis.com"' --limit=20
```

### Monitor Dashboard Secret Usage
```bash
gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard AND textPayload:"secret"' --limit=10
```

### Set Up Alerts
Create monitoring alerts for:
- Failed secret access attempts
- Unusual access patterns
- Secret rotation events

## 🔒 Security Best Practices

### Access Control
- Use least-privilege IAM roles
- Regularly review service account permissions
- Monitor secret access logs

### Secret Management
- Rotate passwords regularly (quarterly recommended)
- Use strong, unique passwords
- Never log or expose secret values
- Delete unused secret versions

### Monitoring
- Set up alerts for failed secret access
- Monitor unusual access patterns
- Review access logs regularly

## 🚨 Troubleshooting

### Common Issues

#### "Secret not found" Error
```bash
# Check if secret exists
gcloud secrets list --filter="name:snowflake-user"

# If not found, run setup script
./setup_secrets.sh
```

#### "Permission denied" Error
```bash
# Check service account permissions
gcloud projects get-iam-policy red-octane-444308-f4 --flatten="bindings[].members" --filter="bindings.members:serviceAccount:red-octane-444308-f4@appspot.gserviceaccount.com"

# Grant permissions if needed
gcloud projects add-iam-policy-binding red-octane-444308-f4 \
    --member="serviceAccount:red-octane-444308-f4@appspot.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

#### "Snowflake connection failed" Error
```bash
# Test secret values
gcloud secrets versions access latest --secret=snowflake-user
gcloud secrets versions access latest --secret=snowflake-account

# Update secrets if needed
./setup_secrets.sh
```

### Debug Mode
Enable detailed logging by checking Cloud Function logs:
```bash
gcloud logging read 'resource.type=cloud_function AND resource.labels.function_name=karbon-pipeline-dashboard' --limit=50
```

## 🔄 Migration from Environment Variables

If you're migrating from the environment variable approach:

1. **Store secrets**: Run `./setup_secrets.sh`
2. **Deploy updated code**: Run `./deploy_secure_dashboard_with_secrets.sh`
3. **Verify functionality**: Test the comparison page
4. **Clean up**: Remove old environment variables from previous deployments

The new code includes fallback logic, so it will work during the transition period.

## 📈 Performance Considerations

### Caching
- Secrets are cached in memory during function execution
- New function instances fetch secrets on first use
- Minimal performance impact compared to environment variables

### Cost
- Secret Manager pricing: $0.06 per 10,000 secret access operations
- Typical usage: <100 accesses per day = negligible cost
- Much cheaper than security incident remediation!

## 🔮 Future Enhancements

Planned improvements:
- **Automatic rotation** with Cloud Scheduler
- **Multi-region replication** for high availability
- **Secret versioning** for rollback capability
- **Integration** with external secret stores
- **Compliance reporting** for audit requirements

## 📞 Support

For issues with Secret Manager setup:

1. **Check this documentation** for common solutions
2. **Review Cloud Logging** for detailed error messages
3. **Verify IAM permissions** for the service account
4. **Test secret access** manually using gcloud commands
5. **Check API enablement** for Secret Manager

## ✅ Success Checklist

After setup, verify:
- [ ] Secrets are created in Secret Manager
- [ ] Service account has secretAccessor role
- [ ] Dashboard deploys successfully
- [ ] Comparison page loads data from Snowflake
- [ ] No credentials visible in environment variables
- [ ] Access logging is working
- [ ] Secret values are correct and current

---

**🔐 Your Snowflake credentials are now securely managed with Google Cloud Secret Manager!**
