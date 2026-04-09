"""
Fetch broker PnL for CLOSED + PENDING_FINAL rows and set sync_status=SYNCED.
Usage: python tools/finalize_pending_trade_ids.py 82 83
"""
import os
import sqlite3
import sys
from datetime import datetime as _dt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

DB_PATH = os.path.join(ROOT, "database", "trading_saas.db")


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/finalize_pending_trade_ids.py <trade_id> [...]")
        sys.exit(1)
    tids = [int(x) for x in sys.argv[1:]]
    from core.executor import get_user_credentials, get_session
    from core.sync import fetch_closed_deal_final_data
    from core.trade_session_finalize import after_trade_leg_closed

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for tid in tids:
        row = c.execute(
            "SELECT chat_id, deal_id, COALESCE(deal_reference,''), COALESCE(capital_order_id,''), "
            "symbol, direction, entry_price, size, COALESCE(leg_role,''), COALESCE(parent_session,''), "
            "stop_distance, trailing_stop, closed_at "
            "FROM trades WHERE trade_id=? AND status='CLOSED' "
            "AND COALESCE(sync_status,'')='PENDING_FINAL'",
            (tid,),
        ).fetchone()
        if not row:
            print(f"trade_id={tid}: skip (not CLOSED+PENDING_FINAL)")
            continue
        (
            chat_id,
            deal_id,
            deal_reference,
            capital_order_id,
            symbol,
            direction,
            entry_price,
            size,
            leg_role,
            parent_session,
            stop_distance,
            trailing_stop,
            closed_at,
        ) = row
        creds = get_user_credentials(str(chat_id))
        if not creds:
            print(f"trade_id={tid}: no credentials")
            continue
        base_url, headers = get_session(creds)
        if not headers:
            print(f"trade_id={tid}: session failed")
            continue
        dr = str(deal_reference or "").strip()
        co = str(capital_order_id or "").strip()
        try:
            size_f = float(size) if size is not None else None
        except (TypeError, ValueError):
            size_f = None
        final = fetch_closed_deal_final_data(
            base_url,
            headers,
            str(deal_id),
            symbol=str(symbol or ""),
            direction=str(direction or ""),
            size=size_f,
            closed_at=str(closed_at or "").strip() or None,
            wait_for_realized=True,
            identifiers=[dr] if dr else None,
            capital_order_id=co or None,
        )
        if not final:
            print(f"trade_id={tid}: history still empty")
            continue
        pnl = float(final["actual_pnl"])
        exit_price = final.get("exit_price")
        part_n = int(final.get("part_count") or 1)
        c.execute(
            "UPDATE trades SET pnl=?, actual_pnl=?, exit_price=?, close_sync_last_error=NULL, "
            "sync_status='SYNCED', pnl_trade_parts=? WHERE trade_id=?",
            (
                pnl,
                pnl,
                float(exit_price) if exit_price is not None else None,
                int(part_n) if part_n > 1 else None,
                int(tid),
            ),
        )
        conn.commit()
        after_trade_leg_closed(str(chat_id), (parent_session or "").strip(), pnl)
        print(f"trade_id={tid}: SYNCED pnl={pnl}")
    conn.close()


if __name__ == "__main__":
    main()
