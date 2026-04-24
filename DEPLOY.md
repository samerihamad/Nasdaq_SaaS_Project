# Deployment notes (VPS / Linux)

The NATB stack is **not** a desktop app: `main.py` and `bot/dashboard.py` are long‑running
**server processes** (Telegram polling, trading loop). The `.bat` helpers are **Windows-only
shortcuts** for local development; on a VPS you run the same Python modules from the shell.

## What runs on a server

| Component | Typical VPS setup |
|-----------|-------------------|
| `main.py` | `python main.py` or `python3 main.py` under **systemd** / **supervisor** / **screen** |
| `bot/dashboard.py` | Separate process (second service), same machine |
| Hourly backup | Already inside `main.py` thread; optional **cron** `python -m utils.backup` as backup |
| SQLite DB | Single file; keep `database/` on disk or attach a volume |

## Paths & environment

- **`.env` is not on GitHub** (see `.gitignore`). You must create it **on each server**:
  - Copy `.env.example` → `.env`, fill values, or `scp` your local `.env` over SSH (secure channel).
- **Linux has no `C:\...`**. Use POSIX paths in `.env` on the VPS, e.g. `/home/ubuntu/Nasdaq_SaaS_Project/config/gdrive_token.json`.
- The app **does not** read secrets from GitHub; without a valid `.env` on the VPS, **nothing will authenticate** (Telegram, Capital.com, backups, SMTP).
- Optional: inject vars via **systemd `EnvironmentFile=`** pointing to a root-only file instead of a repo file.
- `GOOGLE_SERVICE_ACCOUNT_JSON`, `GDRIVE_OAUTH_CLIENT_JSON`, `GDRIVE_OAUTH_TOKEN_JSON` must point to **files that exist on the VPS** (upload the JSON + token after first OAuth).

## Google Drive OAuth on a server (headless)

- **First login** usually opens a **local browser** — fine on your PC.
- On the VPS **without a browser**, either:
  1. Generate `config/gdrive_token.json` (or your `GDRIVE_OAUTH_TOKEN_JSON`) **on your PC**, then **scp** it to the server; or  
  2. Use **SSH port forwarding** / run OAuth once from a machine with a browser and copy the token file; or  
  3. Prefer **Dropbox** (`BACKUP_PROVIDER=dropbox`) or **service account + Workspace** if you want fully automated server-only setup.

## Windows-only files (ignore on Linux)

- `run_main.bat`, `run_dashboard.bat`, `run_backup.bat` — optional; replace with shell scripts or systemd units.

# Suggested production habits

- One **Python venv** on the server: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt` (+ any other deps your app imports).
- **Restart** services after `.env` changes.
- Ensure **timezone** / UTC awareness matches your market logic (see `config.py` / `market_hours`).

This project is **not** tied to Apple or Windows; the same code runs on Ubuntu/Debian VPS once Python and dependencies are installed and paths are Linux-style.
