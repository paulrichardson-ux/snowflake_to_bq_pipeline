#!/bin/bash
set -e

echo "🔧 Gmail App Password Update Script"
echo "===================================="
echo ""

# Check if the secret exists
echo "Checking existing email configuration..."
USERNAME_SECRET=$(gcloud secrets versions access latest --secret="PIPELINE_MONITOR_EMAIL_USERNAME" 2>/dev/null || echo "NOT_FOUND")
if [ "$USERNAME_SECRET" = "NOT_FOUND" ]; then
    echo "❌ Email username secret not found. Please run ./setup_email_notifications.sh first"
    exit 1
fi

echo "✅ Current email username: $USERNAME_SECRET"
echo ""

echo "📋 Gmail App Password Setup Instructions:"
echo "1. Open: https://myaccount.google.com/"
echo "2. Go to Security → 2-Step Verification"
echo "3. Scroll down to 'App passwords'"
echo "4. Select 'Mail' and click 'Generate'"
echo "5. Copy the 16-character password (format: abcd efgh ijkl mnop)"
echo ""

# Interactive password input
echo "Enter your Gmail app password below:"
echo "(Note: Input will be hidden for security)"
read -s -p "Gmail App Password (16 characters, no spaces): " APP_PASSWORD
echo ""

# Validate password format (should be 16 characters)
if [ ${#APP_PASSWORD} -ne 16 ]; then
    echo "⚠️  Warning: App password should be exactly 16 characters"
    echo "Current length: ${#APP_PASSWORD}"
    echo ""
    read -p "Continue anyway? (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Cancelled. Please try again with the correct app password."
        exit 1
    fi
fi

# Update the secret
echo ""
echo "🔄 Updating Gmail app password in Secret Manager..."
echo "$APP_PASSWORD" | gcloud secrets versions add PIPELINE_MONITOR_EMAIL_PASSWORD --data-file=-

if [ $? -eq 0 ]; then
    echo "✅ Gmail app password updated successfully!"
    echo ""
    echo "🧪 Testing email functionality..."
    
    # Test the email functionality
    python3 pipeline_scheduler_monitor.py --daily-report --no-email > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "✅ Monitor script runs successfully"
        echo ""
        echo "🚀 Ready to test email notifications!"
        echo ""
        read -p "Would you like to test sending a daily report email now? (y/n): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo ""
            echo "📧 Sending test email to: $USERNAME_SECRET"
            python3 pipeline_scheduler_monitor.py --daily-report
            echo ""
            echo "Check your email inbox for the test message!"
        fi
    else
        echo "⚠️  Monitor script has issues, but password was updated"
    fi
    
    echo ""
    echo "✅ Setup complete!"
    echo "📧 Daily emails will be sent to: $USERNAME_SECRET"
    echo "⏰ Schedule: 11:00 AM CAT (09:00 UTC)"
    echo "🔍 Health checks: Every 4 hours with auto-fix"
    
else
    echo "❌ Failed to update app password. Please try again."
    exit 1
fi

