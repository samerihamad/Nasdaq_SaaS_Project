"""
CAPITAL.COM API CONSTRAINTS PROBE SCRIPT
========================================

Run this manually to check broker constraints for DEMO account.
This is a diagnostic tool only — not integrated into the trading engine.

Usage:
    python probe_capital_constraints.py

Requires:
    - CAPITAL_API_KEY, CAPITAL_EMAIL, CAPITAL_PASSWORD in .env
    - Valid session with Capital.com
"""

import os
import sys
import json
import requests
from pathlib import Path

# Load .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuration
PROJECT_ROOT = Path(__file__).parent
API_KEY = os.getenv("CAPITAL_API_KEY", "")
EMAIL = os.getenv("CAPITAL_EMAIL", "")
PASSWORD = os.getenv("CAPITAL_PASSWORD", "")
IS_DEMO = os.getenv("CAPITAL_IS_DEMO", "true").lower() == "true"

BASE_URL = "https://demo-api-capital.backend-capital.com" if IS_DEMO else "https://api-capital.backend-capital.com"


def get_session():
    """Authenticate with Capital.com and get session headers."""
    headers = {
        "X-CAP-API-KEY": API_KEY,
        "Content-Type": "application/json",
    }
    
    payload = {
        "identifier": EMAIL,
        "password": PASSWORD,
    }
    
    print(f"🔐 Authenticating with Capital.com ({'DEMO' if IS_DEMO else 'LIVE'})...")
    
    try:
        res = requests.post(f"{BASE_URL}/session", headers=headers, json=payload, timeout=10)
        if res.status_code == 200:
            data = res.json()
            token = data.get("CST")
            security_token = data.get("X-SECURITY-TOKEN")
            
            session_headers = {
                "X-SECURITY-TOKEN": security_token,
                "CST": token,
                "Content-Type": "application/json",
            }
            print("✅ Session established")
            return session_headers
        else:
            print(f"❌ Authentication failed: {res.status_code}")
            print(f"   Response: {res.text[:500]}")
            return None
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return None


def probe_account_info(headers):
    """Fetch account balance and leverage info."""
    print("\n" + "="*60)
    print("💰 ACCOUNT INFORMATION")
    print("="*60)
    
    try:
        res = requests.get(f"{BASE_URL}/accounts", headers=headers, timeout=10)
        if res.status_code == 200:
            data = res.json()
            accounts = data.get("accounts", [])
            
            for acc in accounts:
                balance = acc.get("balance", {})
                print(f"\nAccount ID: {acc.get('accountId', 'N/A')}")
                print(f"  Balance: ${float(balance.get('balance', 0)):.2f} {balance.get('currency', 'USD')}")
                print(f"  Available: ${float(balance.get('available', 0)):.2f}")
                print(f"  Margin Used: ${float(balance.get('deposit', 0)):.2f}")
                print(f"  Leverage: {acc.get('leverage', 'N/A')}x")
                
            return accounts[0] if accounts else None
        else:
            print(f"❌ Failed to fetch accounts: {res.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def probe_symbol_details(headers, epic):
    """Fetch minDistance and other constraints for a symbol."""
    print(f"\n" + "="*60)
    print(f"📊 SYMBOL CONSTRAINTS: {epic}")
    print("="*60)
    
    try:
        res = requests.get(f"{BASE_URL}/markets/{epic}", headers=headers, timeout=10)
        if res.status_code == 200:
            data = res.json()
            market = data.get("market", {})
            details = data.get("dealingRules", {})
            
            print(f"\nMarket Name: {market.get('instrumentName', 'N/A')}")
            print(f"  Type: {market.get('instrumentType', 'N/A')}")
            print(f"  Currency: {market.get('currencies', [{}])[0].get('name', 'N/A')}")
            print(f"  Margin Requirement: {market.get('margin', {}).get('margin', 'N/A')}%")
            
            print(f"\n  📏 DISTANCE RULES (minDistance):")
            min_dist = details.get("minDistance", {})
            print(f"    Stop Loss: {min_dist.get('stop', 'N/A')}")
            print(f"    Take Profit: {min_dist.get('profit', 'N/A')}")
            print(f"    Step: {min_dist.get('step', 'N/A')}")
            
            print(f"\n  📐 POSITION LIMITS:")
            limits = details.get("minDealSize", {})
            print(f"    Min Deal Size: {limits.get('value', 'N/A')} {limits.get('unit', 'N/A')}")
            
            max_size = details.get("maxDealSize", {})
            print(f"    Max Deal Size: {max_size.get('value', 'N/A')} {max_size.get('unit', 'N/A')}")
            
            return {
                "epic": epic,
                "min_stop_distance": min_dist.get("stop"),
                "min_tp_distance": min_dist.get("profit"),
                "step": min_dist.get("step"),
                "margin_pct": market.get("margin", {}).get("margin"),
            }
        else:
            print(f"❌ Failed to fetch {epic}: {res.status_code}")
            try:
                err_data = res.json()
                print(f"   Error: {err_data}")
            except:
                print(f"   Response: {res.text[:500]}")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def calculate_margin_required(balance, leverage, symbol_data):
    """Calculate estimated margin for a position."""
    print(f"\n" + "="*60)
    print("🧮 MARGIN CALCULATION (Estimated)")
    print("="*60)
    
    if not symbol_data or not symbol_data.get("margin_pct"):
        print("❌ Cannot calculate — missing symbol data")
        return None
    
    margin_pct = float(symbol_data.get("margin_pct", 0))
    
    # For a $1000 position at 3x leverage
    position_size = 1000  # Example position size
    
    # Margin required = Position Size / Leverage * (Margin% / 100)
    margin_required = position_size / leverage * (margin_pct / 100)
    
    print(f"\n  Account Balance: ${balance:.2f}")
    print(f"  Leverage: {leverage}x")
    print(f"  Symbol Margin Requirement: {margin_pct}%")
    print(f"\n  📊 Example Position (${position_size}):")
    print(f"    Margin Required: ${margin_required:.2f}")
    print(f"    % of Balance: {(margin_required/balance)*100:.1f}%")
    print(f"    Remaining After: ${balance - margin_required:.2f}")
    
    return margin_required


def main():
    """Run the Capital.com constraints probe."""
    print("="*60)
    print("🔍 CAPITAL.COM API CONSTRAINTS PROBE")
    print("="*60)
    print(f"Environment: {'DEMO' if IS_DEMO else 'LIVE'}")
    print(f"API Key: {API_KEY[:10]}..." if API_KEY else "❌ MISSING")
    print(f"Email: {EMAIL[:5]}..." if EMAIL else "❌ MISSING")
    
    if not all([API_KEY, EMAIL, PASSWORD]):
        print("\n❌ Missing credentials. Set CAPITAL_API_KEY, CAPITAL_EMAIL, CAPITAL_PASSWORD in .env")
        sys.exit(1)
    
    # Get session
    headers = get_session()
    if not headers:
        print("\n❌ Cannot proceed without valid session")
        sys.exit(1)
    
    # Get account info
    account = probe_account_info(headers)
    if not account:
        print("\n❌ Cannot proceed without account info")
        sys.exit(1)
    
    balance_data = account.get("balance", {})
    balance = float(balance_data.get("balance", 0))
    leverage = float(account.get("leverage", 1))
    
    # Probe symbols
    symbols_to_test = ["TTD", "AAPL", "NVDA", "TSLA", "MSFT"]
    results = []
    
    for symbol in symbols_to_test:
        data = probe_symbol_details(headers, symbol)
        if data:
            results.append(data)
    
    # Calculate margin for first symbol
    if results:
        calculate_margin_required(balance, leverage, results[0])
    
    # Summary
    print("\n" + "="*60)
    print("📋 SUMMARY")
    print("="*60)
    
    for r in results:
        epic = r["epic"]
        min_stop = r.get("min_stop_distance", "N/A")
        min_tp = r.get("min_tp_distance", "N/A")
        print(f"\n{epic}:")
        print(f"  minStopDistance: {min_stop}")
        print(f"  minTPDistance: {min_tp}")
        
        # Check if our 0.5% slippage might violate
        if min_stop and isinstance(min_stop, (int, float)):
            print(f"  ⚠️  If SL is within {min_stop} points, order will be REJECTED")
    
    print("\n" + "="*60)
    print("✅ Probe complete")
    print("="*60)


if __name__ == "__main__":
    main()
