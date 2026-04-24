#!/bin/bash
# POST-SHIP HOOK: Triggered automatically after Windsurf 'ship' completes
# This runs LOCALLY and executes the deploy script on the remote server

set -e

# Configuration - update these for your server
SERVER_USER="root"
SERVER_IP="157.180.92.230"
REMOTE_PATH="/root/Nasdaq_SaaS_Project"

echo "🚀 [POST-SHIP] Deployment triggered. Connecting to server..."

# Execute deploy script on remote server
ssh "${SERVER_USER}@${SERVER_IP}" "cd ${REMOTE_PATH} && bash deploy.sh"

if [ $? -eq 0 ]; then
    echo "✅ [POST-SHIP] Auto-deploy completed successfully."
else
    echo "❌ [POST-SHIP] Auto-deploy failed. Check server logs."
    exit 1
fi
