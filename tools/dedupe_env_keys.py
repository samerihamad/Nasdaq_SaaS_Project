"""
Deduplicate .env keys while keeping the LAST occurrence.

Behavior:
  - Preserves comments, blank lines, and non key/value lines.
  - Removes only earlier duplicate KEY=... entries.
  - Keeps final file order intact (last occurrence position wins).

Usage:
  python tools/dedupe_env_keys.py
  python tools/dedupe_env_keys.py --env ".env" --dry-run
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


KEY_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=")


def _extract_key(line: str) -> str | None:
    stripped = line.lstrip()
    if not stripped or stripped.startswith("#"):
        return None
    m = KEY_RE.match(line)
    if not m:
        return None
    return m.group(1)


def dedupe_env_text(text: str) -> tuple[str, int]:
    lines = text.splitlines(keepends=True)

    last_index_for_key: dict[str, int] = {}
    for idx, line in enumerate(lines):
        key = _extract_key(line)
        if key is not None:
            last_index_for_key[key] = idx

    kept_lines: list[str] = []
    removed = 0
    for idx, line in enumerate(lines):
        key = _extract_key(line)
        if key is None:
            kept_lines.append(line)
            continue
        if last_index_for_key.get(key) == idx:
            kept_lines.append(line)
        else:
            removed += 1

    return "".join(kept_lines), removed


def main() -> int:
    parser = argparse.ArgumentParser(description="Remove duplicate .env keys (keep last occurrence).")
    parser.add_argument("--env", default=".env", help="Path to .env file (default: .env)")
    parser.add_argument("--dry-run", action="store_true", help="Show how many duplicates would be removed.")
    args = parser.parse_args()

    env_path = Path(args.env)
    if not env_path.exists():
        print(f"[ERROR] File not found: {env_path}")
        return 1

    original = env_path.read_text(encoding="utf-8")
    deduped, removed = dedupe_env_text(original)

    if args.dry_run:
        print(f"[DRY RUN] {env_path}: duplicate entries to remove = {removed}")
        return 0

    if removed == 0:
        print(f"[OK] {env_path}: no duplicate keys found.")
        return 0

    backup_path = env_path.with_suffix(env_path.suffix + ".bak")
    backup_path.write_text(original, encoding="utf-8")
    env_path.write_text(deduped, encoding="utf-8")
    print(f"[DONE] {env_path}: removed {removed} duplicate entries.")
    print(f"[BACKUP] {backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

