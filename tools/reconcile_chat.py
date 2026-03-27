"""Run core.sync.reconcile for one chat_id (requires valid Capital session)."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)


def main():
    if len(sys.argv) != 2:
        print("Usage: python tools/reconcile_chat.py <chat_id>")
        sys.exit(1)
    cid = str(sys.argv[1]).strip()
    from core.executor import get_user_credentials, get_session
    from core.sync import reconcile

    creds = get_user_credentials(cid)
    if not creds:
        print(f"No credentials for {cid}")
        sys.exit(2)
    base_url, headers = get_session(creds)
    if not headers:
        print(f"Session failed for {cid}")
        sys.exit(3)
    reconcile(cid, base_url, headers, notify=True)
    print(f"reconcile done for {cid}")


if __name__ == "__main__":
    main()
