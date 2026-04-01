#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash tools/sync_server_fast_tier_env.sh [/absolute/path/to/.env]
#
# Default server path (adjust if needed):
#   /root/Nasdaq_SaaS_Project/.env
#
# This script only updates strategy constants (safe whitelist).
# It never touches API keys, encryption keys, backup provider settings, or paths.

ENV_FILE="${1:-/root/Nasdaq_SaaS_Project/.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[INFO] .env not found at $ENV_FILE, creating it."
  touch "$ENV_FILE"
fi

cp "$ENV_FILE" "${ENV_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ)"

KEYS=(
  FAST_MIN_CONFIDENCE
  GOLDEN_MIN_CONFIDENCE
  FAST_MR_RSI_EXTREME_OVERSOLD
  FAST_MR_RSI_EXTREME_OVERBOUGHT
  FAST_MOM_ADX_THRESHOLD
  FAST_MOM_VOL_RATIO
  FAST_MOM_VOL_RATIO_HIGH_RSI
  FAST_MOM_RSI_VOL_TIER_HIGH
  FAST_MOM_RSI_BUY_MAX
  FAST_MOM_LOW_VOL_AI_MIN
  FAST_MOM_MACD_BYPASS_AI_MIN
)

value_for_key() {
  case "$1" in
    FAST_MIN_CONFIDENCE) echo "57" ;;
    GOLDEN_MIN_CONFIDENCE) echo "67" ;;
    FAST_MR_RSI_EXTREME_OVERSOLD) echo "25" ;;
    FAST_MR_RSI_EXTREME_OVERBOUGHT) echo "75" ;;
    FAST_MOM_ADX_THRESHOLD) echo "18" ;;
    FAST_MOM_VOL_RATIO) echo "1.0" ;;
    FAST_MOM_VOL_RATIO_HIGH_RSI) echo "0.8" ;;
    FAST_MOM_RSI_VOL_TIER_HIGH) echo "70" ;;
    FAST_MOM_RSI_BUY_MAX) echo "77" ;;
    FAST_MOM_LOW_VOL_AI_MIN) echo "65" ;;
    FAST_MOM_MACD_BYPASS_AI_MIN) echo "60" ;;
    *) return 1 ;;
  esac
}

for key in "${KEYS[@]}"; do
  val="$(value_for_key "$key")"
  if grep -Eq "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${val}|g" "$ENV_FILE"
    echo "[UPDATE] ${key}=${val}"
  else
    printf "%s=%s\n" "$key" "$val" >> "$ENV_FILE"
    echo "[ADD] ${key}=${val}"
  fi
done

echo
echo "[DONE] Applied strategy sync to: $ENV_FILE"
echo "[CHECK] Current values:"
grep -E "^(FAST_MIN_CONFIDENCE|GOLDEN_MIN_CONFIDENCE|FAST_MR_RSI_EXTREME_OVERSOLD|FAST_MR_RSI_EXTREME_OVERBOUGHT|FAST_MOM_ADX_THRESHOLD|FAST_MOM_VOL_RATIO|FAST_MOM_VOL_RATIO_HIGH_RSI|FAST_MOM_RSI_VOL_TIER_HIGH|FAST_MOM_RSI_BUY_MAX|FAST_MOM_LOW_VOL_AI_MIN|FAST_MOM_MACD_BYPASS_AI_MIN)=" "$ENV_FILE" || true
