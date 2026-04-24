#!/bin/bash
# AUTO-DEPLOY SCRIPT: Clean-deploy for NATB Engine
# Triggered automatically after 'ship' operation completes

set -e

# Dynamic path detection (AI_GUIDELINES.md Rule 4)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 [AUTO-DEPLOY] New files detected. Starting cleanup..."

# Cleanup Python cache (exclude venv to avoid permission issues)
find . -name '__pycache__' -type d -not -path './venv/*' -exec rm -rf {} +
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete

echo '🧹 [CLEANUP] Project cache cleared successfully.'

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
