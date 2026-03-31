"""
One-shot preflight: Helsinki vs Dubai times (via synchronized_utc_now),
Capital.com /prices max=500 smoke test, requirements + market_scanner imports.
Run from project root: python tools/preflight_check.py
"""
from __future__ import annotations

import ast
import os
import re
import sys
from pathlib import Path

# Project root = parent of tools/
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytz
import requests

from utils.market_hours import synchronized_utc_now
from utils.market_scanner import (
    _get_market_data_session,
    _resolve_epic,
    _DATA_TIMEOUT_SEC,
)

HELSINKI = pytz.timezone("Europe/Helsinki")  # typical server TZ (EET/EEST)
DUBAI = pytz.timezone("Asia/Dubai")  # Admin / Telegram display (GST)


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def check_timezones() -> None:
    section("1) synchronized_utc_now() - Helsinki (server) vs Dubai (admin)")
    utc = synchronized_utc_now()
    hel = utc.astimezone(HELSINKI)
    dxb = utc.astimezone(DUBAI)
    # Wall-clock difference (may vary with EU DST; Dubai has no DST)
    delta_h = (dxb.replace(tzinfo=None) - hel.replace(tzinfo=None)).total_seconds() / 3600.0
    print(f"  UTC (synced):     {utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Helsinki:         {hel.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Dubai (GST):      {dxb.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Dubai minus Helsinki: {delta_h:+.1f} hours (expected 1h or 2h depending on EU DST)")


def check_capital_500_bars() -> None:
    section("2) Capital.com - session + /prices max=500")
    symbols_try = ["US100", "NDX", "AAPL", "MSFT", "GOOGL"]
    base_url, headers = _get_market_data_session(session_context=None)
    if not base_url or not headers:
        print("  SKIP: No Capital credentials in config / .env (api_key, email, password).")
        return
    epic_resolved = None
    used_symbol = None
    for sym in symbols_try:
        e = _resolve_epic(base_url, headers, sym)
        if e:
            epic_resolved, used_symbol = e, sym
            break
    if not epic_resolved:
        print("  FAIL: Could not resolve epic for US100/NASDAQ candidates.")
        return
    print(f"  Resolved epic: {used_symbol} -> {epic_resolved}")
    resolutions = ("MINUTE_15", "HOUR", "HOUR_4", "DAY")
    last_err = ""
    for res in resolutions:
        try:
            r = requests.get(
                f"{base_url}/prices/{epic_resolved}",
                params={"resolution": res, "max": 500},
                headers=headers,
                timeout=_DATA_TIMEOUT_SEC,
            )
            if r.status_code != 200:
                last_err = f"{res}: HTTP {r.status_code}"
                continue
            data = r.json() or {}
            prices = data.get("prices") or data.get("candles") or data.get("data") or []
            n = len(prices) if isinstance(prices, list) else 0
            print(f"  OK: resolution={res} bars_returned={n} (requested max=500)")
            if n >= 1:
                return
            last_err = f"{res}: empty prices list"
        except Exception as exc:
            last_err = f"{res}: {exc}"
    print(f"  FAIL: {last_err}")


def _top_level_imports_from_file(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.append(node.module.split(".")[0])
    return names


def _refs_in_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def check_requirements_and_scanner() -> None:
    section("3) requirements.txt + market_scanner.py imports")
    req_path = ROOT / "requirements.txt"
    lines = [ln.strip() for ln in req_path.read_text(encoding="utf-8").splitlines()]
    pkg_lines = [
        ln
        for ln in lines
        if ln
        and not ln.startswith("#")
        and not ln.startswith("-")
    ]
    print(f"  requirements.txt: {req_path}")
    print(f"  Declared dependency lines: {len(pkg_lines)}")
    suspicious = [ln for ln in pkg_lines if re.search(r"yfinance|yahoo|datareader", ln, re.I)]
    if suspicious:
        print("  WARN: suspicious lines:", suspicious)
    else:
        print("  No yfinance / pandas_datareader / yahoo in requirements (good).")

    scanner = ROOT / "utils" / "market_scanner.py"
    imports = _top_level_imports_from_file(scanner)
    body = _refs_in_file(scanner)
    # Map import root -> usage heuristic (substring in file)
    usage_map = {
        "os": "os.",
        "datetime": "datetime",
        "numpy": "np.",
        "pandas": "pd.",
        "requests": "requests.",
        "config": "config",
    }
    print(f"  market_scanner.py top-level roots: {imports}")
    for root in imports:
        hint = usage_map.get(root, root)
        if root == "config":
            ok = "from config import" in body or "config." in body
        else:
            ok = hint in body
        status = "OK" if ok else "CHECK"
        print(f"    {root}: {status}")


def main() -> None:
    os.chdir(ROOT)
    check_timezones()
    check_capital_500_bars()
    check_requirements_and_scanner()
    print()
    print("Done.")
    print()


if __name__ == "__main__":
    main()
