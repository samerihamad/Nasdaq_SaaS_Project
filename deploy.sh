#!/bin/bash
# AUTO-DEPLOY SCRIPT: Clean-deploy for NATB Engine
# Triggered automatically after 'ship' operation completes

set -e

# Dynamic path detection (AI_GUIDELINES.md Rule 4)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 [AUTO-DEPLOY] New files detected. Starting cleanup..."

# Cleanup Python cache
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "*.pyo" -delete 2>/dev/null || true

echo "🧹 [AUTO-DEPLOY] Cache cleaned."

# Kill existing main.py processes safely
if pgrep -f "main.py" > /dev/null; then
    echo "🛑 [AUTO-DEPLOY] Stopping existing main.py processes..."
    pkill -9 -f "main.py" 2>/dev/null || true
    sleep 2
fi

# Restart systemd service
echo "🔁 [AUTO-DEPLOY] Restarting natb-engine.service..."
if systemctl is-active --quiet natb-engine.service 2>/dev/null; then
    systemctl restart natb-engine.service
    echo "✅ [AUTO-DEPLOY] Service restarted."
else
    echo "⚠️  [AUTO-DEPLOY] natb-engine.service not active or not found."
    echo "   Manual start may be required: python main.py"
fi

echo "✅ [AUTO-DEPLOY] Fresh code is now live."
echo "📊 [AUTO-DEPLOY] Verify status: systemctl status natb-engine.service"
