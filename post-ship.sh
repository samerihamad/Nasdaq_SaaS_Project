#!/bin/bash
# POST-SHIP HOOK: Triggered automatically after Windsurf 'ship' completes
# This runs LOCALLY and executes the deploy script on the remote server

set -e

# Configuration - update these for your server
SERVER_USER="root"
SERVER_IP="157.180.92.230"
REMOTE_PATH="/root/Nasdaq_SaaS_Project"

# SSH IDENTITY: If you need a specific key, use one of these methods:
# 1. Add to ~/.ssh/config: Host 157.180.92.230 IdentityFile ~/.ssh/your_key
# 2. Use ssh-agent: eval $(ssh-agent) && ssh-add ~/.ssh/your_key
# 3. Export SSH_KEY env var and use: ssh -i "$SSH_KEY" ...
# (NO HARDCODED KEYS in this script per security best practices)

echo "🚀 [POST-SHIP] Deployment triggered. Connecting to server..."

# Execute deploy script on remote server
ssh "${SERVER_USER}@${SERVER_IP}" "cd ${REMOTE_PATH} && bash deploy.sh"

if [ $? -eq 0 ]; then
    echo "✅ [POST-SHIP] Auto-deploy completed successfully."
else
    echo "❌ [POST-SHIP] Auto-deploy failed. Check server logs."
    exit 1
fi
