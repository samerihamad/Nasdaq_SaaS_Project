"""
Capital.com History Ledger API Sniffer - Reverse Engineering Tool

Purpose: Discover the true relationship between Open Deal IDs and Close Deal IDs
Hypothesis: Capital.com uses a Two-Leg system where:
  - Opening creates Deal A (dealId)
  - Closing creates Deal B (different dealId)
  - P/L is stored under Deal B or parent Position ID

Usage:
  1. Set environment variables:
     export CAPITAL_API_KEY="your_api_key"
     export CAPITAL_PASSWORD="your_password"
     export CAPITAL_EMAIL="your_email"
     export CAPITAL_DEMO="true"  # or "false" for live
  
  2. Run: python api_sniffer.py

Output: Pretty-printed JSON showing exactly how Capital.com structures
the history ledger and where P/L fields are located.
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any

class CapitalAPISniffer:
    """
    Diagnostic tool to reverse-engineer Capital.com history ledger API.
    """
    
    DEMO_URL = "https://demo-api-capital.backend-capital.com/api/v1"
    LIVE_URL = "https://api-capital.backend-capital.com/api/v1"
    
    def __init__(self, api_key: str, password: str, email: str, is_demo: bool = True):
        self.api_key = api_key
        self.password = password
        self.email = email
        self.base_url = self.DEMO_URL if is_demo else self.LIVE_URL
        self.session_headers: Optional[Dict[str, str]] = None
        self.cst_token: Optional[str] = None
        self.xst_token: Optional[str] = None
        
    def authenticate(self) -> bool:
        """Authenticate and obtain session tokens."""
        print("=" * 80)
        print("STEP 1: AUTHENTICATION")
        print("=" * 80)
        
        headers = {
            "X-CAP-API-KEY": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        payload = {
            "identifier": self.email,
            "password": self.password
        }
        
        url = f"{self.base_url}/session"
        print(f"POST {url}")
        print(f"Headers: {json.dumps({k: v for k, v in headers.items() if k != 'X-CAP-API-KEY'}, indent=2)}")
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=20)
            print(f"\nResponse Status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"ERROR: Authentication failed")
                print(f"Response: {response.text[:500]}")
                return False
            
            # Extract tokens from headers (Capital.com puts auth in headers, not body)
            self.cst_token = response.headers.get("CST")
            self.xst_token = response.headers.get("X-SECURITY-TOKEN")
            
            print(f"CST Token: {'Present' if self.cst_token else 'MISSING'}")
            print(f"X-SECURITY-TOKEN: {'Present' if self.xst_token else 'MISSING'}")
            
            if not self.cst_token or not self.xst_token:
                print("ERROR: Missing authentication tokens in response headers")
                return False
            
            self.session_headers = {
                **headers,
                "CST": self.cst_token,
                "X-SECURITY-TOKEN": self.xst_token,
            }
            
            # Print account info from response body
            account_data = response.json()
            print(f"\nAccount Info:")
            print(f"  Account ID: {account_data.get('accountId', 'N/A')}")
            print(f"  Account Type: {account_data.get('accountType', 'N/A')}")
            print(f"  Currency: {account_data.get('currency', 'N/A')}")
            
            return True
            
        except Exception as e:
            print(f"ERROR: Exception during authentication: {e}")
            return False
    
    def fetch_positions(self) -> List[Dict]:
        """Fetch currently open positions."""
        print("\n" + "=" * 80)
        print("STEP 2: FETCH OPEN POSITIONS (for reference)")
        print("=" * 80)
        
        url = f"{self.base_url}/positions"
        print(f"GET {url}")
        
        try:
            response = requests.get(url, headers=self.session_headers, timeout=20)
            print(f"Response Status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"ERROR: {response.text[:300]}")
                return []
            
            data = response.json()
            positions = data.get("positions", [])
            
            print(f"\nFound {len(positions)} open positions:")
            for i, pos in enumerate(positions[:3], 1):  # Show first 3
                deal_id = pos.get("position", {}).get("dealId", "N/A")
                epic = pos.get("market", {}).get("epic", "N/A")
                direction = pos.get("position", {}).get("direction", "N/A")
                size = pos.get("position", {}, {}).get("size", "N/A")
                print(f"  {i}. Deal ID: {deal_id} | {epic} | {direction} | Size: {size}")
            
            return positions
            
        except Exception as e:
            print(f"ERROR: {e}")
            return []
    
    def fetch_history_transactions(self, max_items: int = 50) -> List[Dict]:
        """
        Fetch history transactions - this is where the P/L data lives.
        """
        print("\n" + "=" * 80)
        print("STEP 3: FETCH HISTORY TRANSACTIONS (The Critical Data)")
        print("=" * 80)
        
        # استخدام التوقيت الحديث لتجنب التحذيرات
        from datetime import timezone
        to_date = datetime.now(timezone.utc).replace(microsecond=0)
        from_date = to_date - timedelta(days=1)

        # تحويلها إلى النص الذي يقبله Capital.com
        from_time = from_date.strftime('%Y-%m-%dT%H:%M:%S')
        to_time = to_date.strftime('%Y-%m-%dT%H:%M:%S')
                
        url = f"{self.base_url}/history/transactions"
        
        # الإصلاح هنا: وضعنا from_time النصية بدلاً من from_date
        params = {
            "from": from_time,
            "to": to_time,
            "max": max_items
        }
        
        print(f"GET {url}")
        print(f"Params: {json.dumps(params, indent=2)}")
        
        try:
            response = requests.get(url, headers=self.session_headers, params=params, timeout=20)
            print(f"\nResponse Status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"ERROR: {response.text[:500]}")
                return []
            
            data = response.json()
            transactions = data.get("transactions", [])
            
            print(f"\nRaw JSON Structure Keys: {list(data.keys())}")
            print(f"Total Transactions: {len(transactions)}")
            
            return transactions
            
        except Exception as e:
            print(f"ERROR: {e}")
            return []
            
    def analyze_transaction_structure(self, transactions: List[Dict]) -> None:
        """
        Deep analysis of transaction structure to find P/L fields and Deal ID relationships.
        """
        print("\n" + "=" * 80)
        print("STEP 4: STRUCTURAL ANALYSIS - HUNTING FOR P/L FIELDS")
        print("=" * 80)
        
        if not transactions:
            print("No transactions to analyze")
            return
        
        # Collect all unique keys across all transactions
        all_keys = set()
        for tx in transactions:
            if isinstance(tx, dict):
                all_keys.update(tx.keys())
        
        print(f"\nAll Top-Level Keys Found in Transactions:")
        for key in sorted(all_keys):
            print(f"  - {key}")
        
        # Look for P/L related fields
        pnl_candidates = [k for k in all_keys if any(term in k.lower() for term in 
            ["profit", "loss", "pnl", "rpl", "upl", "realized", "gross", "net"])]
        
        print(f"\n>>> P/L CANDIDATE FIELDS (Profits/Losses):")
        for field in pnl_candidates:
            print(f"  * {field}")
        
        # Look for ID fields
        id_candidates = [k for k in all_keys if any(term in k.lower() for term in
            ["id", "deal", "position", "order", "reference", "transaction"])]
        
        print(f"\n>>> ID CANDIDATE FIELDS (Deal/Position Linking):")
        for field in id_candidates:
            print(f"  * {field}")
        
        # Analyze each transaction type
        print(f"\n>>> TRANSACTION TYPE ANALYSIS:")
        tx_types = {}
        for tx in transactions:
            tx_type = tx.get("transactionType", "UNKNOWN")
            if tx_type not in tx_types:
                tx_types[tx_type] = []
            tx_types[tx_type].append(tx)
        
        for tx_type, txs in tx_types.items():
            print(f"\n  Type: {tx_type} ({len(txs)} occurrences)")
            # Show first transaction of this type
            if txs:
                sample = txs[0]
                print(f"  Sample keys: {list(sample.keys())}")
                
                # Print values for key fields
                for key in ["dealId", "positionId", "orderId", "transactionId", "dealReference"]:
                    if key in sample:
                        print(f"    {key}: {sample[key]}")
    
    def print_last_5_closed_trades(self, transactions: List[Dict]) -> None:
        """
        Pretty-print the last 5 transactions that likely represent closed trades.
        """
        print("\n" + "=" * 80)
        print("STEP 5: RAW JSON FOR LAST 5 CLOSED-LIKE TRANSACTIONS")
        print("=" * 80)
        
        # Filter for transactions that look like closes
        # These typically have transactionType = "TRADE" or "CLOSE" or contain rpl/upl
        close_candidates = []
        
        for tx in transactions:
            if not isinstance(tx, dict):
                continue
            
            # Look for transactions that have P/L data
            has_pnl = any(k in tx for k in ["rpl", "RPL", "upl", "UPL", "profitAndLoss", "realisedPnl"])
            is_trade = tx.get("transactionType", "").upper() in ["TRADE", "CLOSE", "POSITION_CLOSE"]
            
            if has_pnl or is_trade:
                close_candidates.append(tx)
        
        # Show last 5
        print(f"\nFound {len(close_candidates)} potential closed trade transactions")
        print(f"Showing last {min(5, len(close_candidates))}:\n")
        
        for i, tx in enumerate(close_candidates[:5], 1):
            print(f"{'='*80}")
            print(f"TRANSACTION #{i} - COMPLETE JSON DUMP")
            print(f"{'='*80}")
            print(json.dumps(tx, indent=2, default=str))
            print()
    
    def analyze_two_leg_hypothesis(self, transactions: List[Dict]) -> None:
        """
        Test the Two-Leg hypothesis: Does closing create a NEW dealId?
        """
        print("\n" + "=" * 80)
        print("STEP 6: TWO-LEG HYPOTHESIS TEST")
        print("=" * 80)
        print("""
HYPOTHESIS: Capital.com creates TWO deal records for each round-trip trade:
  - Leg 1 (Open): dealId = "A" (stored in our DB when opening)
  - Leg 2 (Close): dealId = "B" (NEW ID created when closing)
  - P/L is only visible on Leg 2 (dealId "B")
  - Our bot searches for dealId "A" in history and finds nothing

TESTING: Looking for pairs of transactions with linking fields...
""")
        
        # Look for transactions that reference other dealIds
        linking_analysis = []
        
        for tx in transactions:
            if not isinstance(tx, dict):
                continue
            
            # Check all possible linking fields
            links = {
                "transactionId": tx.get("transactionId"),
                "dealId": tx.get("dealId"),
                "positionId": tx.get("positionId"),
                "orderId": tx.get("orderId"),
                "relatedDealId": tx.get("relatedDealId"),
                "openingDealId": tx.get("openingDealId"),
                "openDealId": tx.get("openDealId"),
                "closingDealId": tx.get("closingDealId"),
                "dealReference": tx.get("dealReference"),
                "relatedDealReference": tx.get("relatedDealReference"),
            }
            
            has_links = any(v for v in links.values() if v)
            has_pnl = any(k in tx for k in ["rpl", "RPL", "upl", "UPL", "profitAndLoss"])
            
            if has_links:
                linking_analysis.append({
                    "links": {k: v for k, v in links.items() if v},
                    "has_pnl": has_pnl,
                    "transaction_type": tx.get("transactionType", "UNKNOWN"),
                    "full_tx": tx
                })
        
        print(f"Found {len(linking_analysis)} transactions with ID linking data\n")
        
        for i, analysis in enumerate(linking_analysis[:5], 1):
            print(f"Transaction {i}:")
            print(f"  Type: {analysis['transaction_type']}")
            print(f"  Has P/L: {analysis['has_pnl']}")
            print(f"  ID Links: {json.dumps(analysis['links'], indent=4)}")
            
            # Show P/L if present
            if analysis['has_pnl']:
                tx = analysis['full_tx']
                for key in ["rpl", "RPL", "upl", "UPL", "profitAndLoss", "realisedPnl", "realizedPnl"]:
                    if key in tx:
                        print(f"  {key}: {tx[key]}")
            print()
    
    def run_full_diagnostic(self) -> None:
        """Execute complete diagnostic flow."""
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║           CAPITAL.COM HISTORY LEDGER API - REVERSE ENGINEERING               ║
║                         (Two-Leg Trade Investigation)                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
        
        # Step 1: Authenticate
        if not self.authenticate():
            print("\nFATAL: Cannot proceed without authentication")
            sys.exit(1)
        
        # Step 2: Fetch positions (reference)
        positions = self.fetch_positions()
        
        # Step 3: Fetch history
        transactions = self.fetch_history_transactions(max_items=50)
        
        if not transactions:
            print("\nWARNING: No history transactions found")
            print("Possible reasons:")
            print("  - No trades closed in last 24 hours")
            print("  - Demo account has no trade history")
            print("  - API permissions issue")
            return
        
        # Step 4: Structural analysis
        self.analyze_transaction_structure(transactions)
        
        # Step 5: Raw JSON dump
        self.print_last_5_closed_trades(transactions)
        
        # Step 6: Two-leg hypothesis test
        self.analyze_two_leg_hypothesis(transactions)
        
        # Final summary
        print("\n" + "=" * 80)
        print("FINAL SUMMARY - WHAT WE'RE HUNTING FOR")
        print("=" * 80)
        print("""
The bot currently fails to find P/L because it likely searches history using
the OPEN dealId. We need to discover:

1. OPEN DEAL ID vs CLOSE DEAL ID:
   - When we open: Capital returns dealId "A" (we store this)
   - When we close: Does Capital create dealId "B"?
   - Is there a field like "relatedDealId" or "openingDealId" linking A→B?

2. P/L FIELD LOCATION:
   - Is P/L on the CLOSE transaction (dealId B)?
   - Field name: "rpl", "RPL", "profitAndLoss", "realisedPnl", "upl"?
   - Is it on a POSITION-level aggregation, not deal-level?

3. POSITION vs DEAL ARCHITECTURE:
   - Does Capital.com use positionId as parent container?
   - Are deals just legs within a position?
   - Does history return position-level or deal-level data?

EXAMINE THE JSON ABOVE for these specific patterns!
""")


def main():
    """Entry point with credential loading."""
    # Load credentials from environment (NEVER hardcode in production)
    api_key = os.getenv("CAPITAL_API_KEY")
    password = os.getenv("CAPITAL_PASSWORD")
    email = os.getenv("CAPITAL_EMAIL")
    is_demo = os.getenv("CAPITAL_DEMO", "true").lower() == "true"
    
    # Check for missing credentials
    missing = []
    if not api_key:
        missing.append("CAPITAL_API_KEY")
    if not password:
        missing.append("CAPITAL_PASSWORD")
    if not email:
        missing.append("CAPITAL_EMAIL")
    
    if missing:
        print("ERROR: Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nPlease set these variables before running:")
        print("  export CAPITAL_API_KEY=\"your_key_here\"")
        print("  export CAPITAL_PASSWORD=\"your_password\"")
        print("  export CAPITAL_EMAIL=\"your_email\"")
        print("  export CAPITAL_DEMO=\"true\"  # or \"false\" for live")
        sys.exit(1)
    
    print(f"Environment: {'DEMO' if is_demo else 'LIVE'}")
    print(f"API Key: {api_key[:8]}...{api_key[-4:]}")
    print(f"Email: {email}")
    print()
    
    # Create sniffer and run
    sniffer = CapitalAPISniffer(
        api_key=api_key,
        password=password,
        email=email,
        is_demo=is_demo
    )
    
    sniffer.run_full_diagnostic()


if __name__ == "__main__":
    main()
