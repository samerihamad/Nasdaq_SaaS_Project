"""Run run_zombie_trade_audit for one chat_id with fix=True."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)


def main():
    if len(sys.argv) != 2:
        print("Usage: python tools/run_audit_fix.py <chat_id>")
        sys.exit(1)
    cid = str(sys.argv[1]).strip()
    from core.sync import run_zombie_trade_audit

    print(run_zombie_trade_audit(chat_id_filter=cid, fix=True))


if __name__ == "__main__":
    main()
