#!/usr/bin/env python3
"""
Print UTC vs US/Eastern (NYSE clock) and compare local system time.
Run from project root: python tools/check_time_sync.py

On Windows, accurate session scheduling depends on OS time sync (w32tm / NTP).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

_REPO_ROOT = __file__.rsplit("tools", 1)[0].rstrip("\\/")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from utils.market_hours import ET, synchronized_utc_now  # noqa: E402


def main() -> None:
    utc = datetime.now(timezone.utc)
    try:
        sync_utc = synchronized_utc_now()
    except Exception as exc:
        sync_utc = None
        sync_note = f"synchronized_utc_now failed: {exc}"
    else:
        sync_note = "synchronized_utc_now OK"

    et_now = utc.astimezone(ET)
    if sync_utc is not None:
        et_sync = sync_utc.astimezone(ET)
    else:
        et_sync = None

    print("=== Time sync check (NYSE / Eastern) ===")
    print(f"datetime.now(UTC):        {utc.isoformat()}")
    if sync_utc is not None:
        print(f"synchronized_utc_now():   {sync_utc.isoformat()}")
    print(f"US/Eastern (from UTC):    {et_now.isoformat()}  ({et_now.tzname()})")
    if et_sync is not None:
        print(f"US/Eastern (sync fn):     {et_sync.isoformat()}  ({et_sync.tzname()})")
    print(sync_note)
    print(
        "If pre-market alerts are ~30m off, verify Windows: Settings > Time & language > "
        "Date & time > Sync now; or run `w32tm /query /status` in an elevated prompt."
    )


if __name__ == "__main__":
    main()
