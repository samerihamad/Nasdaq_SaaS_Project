"""
CLI equivalent of /audit_sync (admin).
Usage:
  python tools/run_audit_sync.py              # all chats, no auto-fix
  python tools/run_audit_sync.py 1044635944   # one chat
  python tools/run_audit_sync.py --fix        # enable reconcile/reopen fixes
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)


def main():
    p = argparse.ArgumentParser(description="Run run_zombie_trade_audit (audit_sync).")
    p.add_argument("chat_id", nargs="?", default=None, help="Optional subscriber chat_id filter")
    p.add_argument("--fix", action="store_true", help="Apply reconcile/reopen fixes")
    args = p.parse_args()
    from core.sync import run_zombie_trade_audit

    print(run_zombie_trade_audit(chat_id_filter=args.chat_id, fix=args.fix))


if __name__ == "__main__":
    main()
