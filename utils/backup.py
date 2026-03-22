"""
Encrypted Cloud Backup — NATB v2.0

Creates an encrypted snapshot of the SQLite database every hour and uploads it
to the configured cloud provider (Dropbox or Google Drive).

Required .env variables:
  BACKUP_PROVIDER            = dropbox | gdrive  (لا تضع رابط Google؛ يمكن لصق رابط المجلد وسيُستخرج المعرف تلقائياً)
  ENCRYPTION_KEY             = <Fernet key>       (reuses Step 6 key)

  For Dropbox:
    DROPBOX_ACCESS_TOKEN     = <long-lived access token from Dropbox App Console>

  For Google Drive (pick one):
    A) Service account (Workspace / advanced):
       GOOGLE_SERVICE_ACCOUNT_JSON, GDRIVE_FOLDER_ID
       (Personal Gmail often fails with "no storage" — use B instead.)
    B) OAuth (recommended for personal @gmail.com):
       GDRIVE_AUTH=oauth
       GDRIVE_OAUTH_CLIENT_JSON = OAuth Desktop client JSON from Google Cloud
       GDRIVE_OAUTH_TOKEN_JSON  = optional path for saved token (default: config/gdrive_token.json)
       First backup must run once in main thread: python -m utils.backup
       VPS/SSH: GDRIVE_OAUTH_CONSOLE=1 python -m utils.backup — open the printed URL, then paste the redirect URL or code.
       If Google says the app is in testing: OAuth consent screen → Test users → add your Gmail.

Run from the project root folder (where the `utils` folder lives). Do not run from
`.venv/Scripts` — you will get ModuleNotFoundError: No module named 'utils'.
On Windows you can double-click `run_backup.bat` in the project root.

Run as a standalone script (cron):
  0 * * * * cd /path/to/project && python -m utils.backup

Or it is called automatically from the hourly backup thread in main.py.
"""

import json
import os
import re
import shutil
import datetime
import threading
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
from cryptography.fernet import Fernet

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH      = PROJECT_ROOT / 'database' / 'trading_saas.db'
BACKUP_DIR   = PROJECT_ROOT / 'backups'
KEEP_LOCAL   = 3   # number of local encrypted backups to retain


def _load_project_env() -> None:
    """Load .env from project root so `python -m utils.backup` works from any cwd."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(PROJECT_ROOT / '.env')


def _gdrive_oauth_use_console() -> bool:
    """True = OAuth via terminal (URL + auth code). Use on headless VPS over SSH."""
    return os.getenv('GDRIVE_OAUTH_CONSOLE', '').lower() in ('1', 'true', 'yes')


def _parse_oauth_redirect_paste(raw: str) -> str:
    """Extract authorization code from full redirect URL or raw code string."""
    s = raw.strip().strip('"').strip("'")
    if not s:
        raise EnvironmentError("Empty input.")
    if s.startswith("python ") or s.startswith("python3 ") or "-m utils.backup" in s:
        raise EnvironmentError(
            "لصق رابط إعادة التوجيه (يبدأ بـ http) أو قيمة code فقط — وليس أمر python. "
            "شغّل من جديد: GDRIVE_OAUTH_CONSOLE=1 python -m utils.backup ثم الصق عند السؤال فقط."
        )
    if s.startswith("http"):
        parsed = urlparse(s)
        code = (parse_qs(parsed.query).get("code") or [None])[0]
        if not code:
            raise EnvironmentError("Could not find a 'code' in that URL.")
        return unquote(code)
    if "code=" in s:
        m = re.search(r"(?:^|[?&])code=([^&]+)", s)
        if m:
            return unquote(m.group(1))
    return unquote(s)


def _gdrive_oauth_console_flow(flow, port: int):
    """
    google-auth-oauthlib 1.x removed InstalledAppFlow.run_console().
    Headless VPS: open URL on any device/browser, then paste the redirect URL or raw code.
    """
    flow.redirect_uri = f"http://localhost:{port}/"
    auth_url, _ = flow.authorization_url()
    print(
        "\n[VPS OAuth] Open this URL in a browser (signed in to the Google account):\n",
        flush=True,
    )
    print(auth_url, flush=True)
    print(
        "\n---\n"
        "AR: بعد الموافقة، Google يوجّهك إلى localhost — غالباً تظهر \"الصفحة لا تعمل\" أو ERR_CONNECTION_REFUSED.\n"
        "    هذا طبيعي. انسخ الرابط الكامل من شريط عنوان المتصفح (يحتوي code=...) ثم الصقه هنا في نفس الطرفية.\n"
        "    ليس في المتصفح: الصق في نافذة SSH/الترمنال التي تشغّل فيها هذا الأمر.\n"
        "    لا تلصق الرابط في سطر أوامر bash بعد انتهاء السكربت — الرمز & يفسّر كأوامر. الصق فقط عند سؤال Python أدناه.\n"
        "EN: After consent, the redirect page may error — that is OK. Copy the FULL URL from the address bar\n"
        "    (contains code=...) and paste it below in THIS terminal (where you ran python -m utils.backup).\n"
        "    Do not paste the URL at a fresh bash prompt — & will break. Paste only when Python asks below.\n"
        "---\n",
        flush=True,
    )
    raw = input("Paste redirect URL or authorization code: ")
    code = _parse_oauth_redirect_paste(raw)
    flow.fetch_token(code=code)
    return flow.credentials


def _gdrive_supports_all_drives() -> bool:
    """Team / shared drives need True. Personal My Drive folders should use False (default)."""
    return os.getenv('GDRIVE_SUPPORTS_ALL_DRIVES', '').lower() in ('1', 'true', 'yes')


def _normalize_gdrive_folder_id(raw: str) -> str:
    """API requires the folder id only. Accept a full Drive URL and extract the id."""
    s = (raw or '').strip()
    if not s:
        return ''
    m = re.search(r'/folders/([a-zA-Z0-9_-]+)', s)
    if m:
        return m.group(1)
    return s


def _resolve_backup_provider() -> str:
    """
    BACKUP_PROVIDER must be `gdrive` or `dropbox`.
    If a Google Drive folder URL was pasted by mistake, normalize to gdrive and set GDRIVE_FOLDER_ID.
    """
    raw = (os.getenv('BACKUP_PROVIDER') or '').strip()
    if not raw:
        return ''
    lower = raw.lower()
    if lower in ('gdrive', 'dropbox'):
        return lower
    if 'drive.google.com' in lower or '/folders/' in lower:
        m = re.search(r'/folders/([a-zA-Z0-9_-]+)', raw)
        if m:
            os.environ['GDRIVE_FOLDER_ID'] = m.group(1)
            print(
                f"[BACKUP] Extracted Google Drive folder ID from URL: {m.group(1)}",
                flush=True,
            )
        return 'gdrive'
    return lower


# ── Encryption ────────────────────────────────────────────────────────────────

def _cipher() -> Fernet:
    key = os.getenv('ENCRYPTION_KEY', '')
    if not key:
        raise EnvironmentError("ENCRYPTION_KEY missing from .env")
    return Fernet(key.encode())


def create_encrypted_snapshot() -> Path:
    """
    Copy the live DB, encrypt it, and return the path of the .enc file.
    Retains only the last KEEP_LOCAL encrypted files locally.
    """
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH} (run the app once from project root or run create_db())"
        )

    timestamp   = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_path    = BACKUP_DIR / f"natb_{timestamp}.db"
    enc_path    = BACKUP_DIR / f"natb_{timestamp}.db.enc"

    # Snapshot: copy DB while it's in a consistent state
    shutil.copy2(DB_PATH, raw_path)

    # Encrypt — always remove raw copy even if encryption fails
    try:
        with open(raw_path, 'rb') as f:
            encrypted = _cipher().encrypt(f.read())
        with open(enc_path, 'wb') as f:
            f.write(encrypted)
    finally:
        raw_path.unlink(missing_ok=True)  # remove unencrypted copy immediately

    # Prune old local backups
    old_backups = sorted(BACKUP_DIR.glob("natb_*.db.enc"))[:-KEEP_LOCAL]
    for old in old_backups:
        old.unlink(missing_ok=True)

    return enc_path


# ── Upload providers ──────────────────────────────────────────────────────────

def _upload_dropbox(file_path: Path) -> str:
    """Upload to Dropbox. Returns the remote path."""
    import dropbox  # pip install dropbox

    token = os.getenv('DROPBOX_ACCESS_TOKEN', '')
    if not token:
        raise EnvironmentError("DROPBOX_ACCESS_TOKEN missing from .env")

    dbx  = dropbox.Dropbox(token)
    dest = f"/NATB_Backups/{file_path.name}"

    with open(file_path, 'rb') as f:
        dbx.files_upload(
            f.read(), dest,
            mode=dropbox.files.WriteMode.overwrite
        )
    return dest


def _gdrive_verify_folder(service, folder_id: str, sa_email: str) -> None:
    """Fail fast with a clear message if the folder is missing or not shared with the service account."""
    from googleapiclient.errors import HttpError

    sad = _gdrive_supports_all_drives()
    try:
        meta = service.files().get(
            fileId=folder_id,
            fields='id,name,mimeType,capabilities',
            supportsAllDrives=sad,
        ).execute()
    except HttpError as e:
        if e.resp.status == 404:
            raise EnvironmentError(
                "Drive folder not found or not shared with the service account (API may return 404). "
                f"Check GDRIVE_FOLDER_ID matches the folder in the browser, then Share -> Editor: {sa_email}"
            ) from e
        if e.resp.status == 403:
            raise EnvironmentError(
                f"Drive denied access to folder {folder_id}. "
                f"Open that folder in Drive -> Share -> add as Editor: {sa_email}"
            ) from e
        raise

    if meta.get('mimeType') != 'application/vnd.google-apps.folder':
        raise EnvironmentError(
            f"GDRIVE_FOLDER_ID is not a folder (got {meta.get('mimeType')}). Use a folder URL/ID."
        )

    caps = meta.get('capabilities') or {}
    if caps.get('canAddChildren') is False:
        raise EnvironmentError(
            f"Folder '{meta.get('name')}' does not allow adding files. "
            f"Grant Editor (or Content manager) to: {sa_email}"
        )


def _gdrive_oauth_credentials():
    """User OAuth (Desktop). Required for reliable uploads to personal Gmail Drive."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow

    client_path = os.getenv('GDRIVE_OAUTH_CLIENT_JSON', '')
    token_path = os.getenv(
        'GDRIVE_OAUTH_TOKEN_JSON',
        str(PROJECT_ROOT / 'config' / 'gdrive_token.json'),
    )

    if not client_path or not os.path.exists(client_path):
        raise EnvironmentError(
            "GDRIVE_OAUTH_CLIENT_JSON must point to an OAuth 2.0 Desktop client JSON "
            "(APIs & Services > Credentials > Create OAuth client ID > Desktop)."
        )

    scopes = ['https://www.googleapis.com/auth/drive']
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, scopes)

    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        Path(token_path).parent.mkdir(parents=True, exist_ok=True)
        with open(token_path, 'w', encoding='utf-8') as f:
            f.write(creds.to_json())
        return creds

    if threading.current_thread() is not threading.main_thread():
        raise EnvironmentError(
            "GDRIVE OAuth: first login required. Stop the bot, cd to project root, then run once:\n"
            "  Windows/local:  python -m utils.backup\n"
            "  VPS/SSH:        GDRIVE_OAUTH_CONSOLE=1 python -m utils.backup\n"
            "Then: sudo systemctl restart nasdaq.service"
        )

    flow = InstalledAppFlow.from_client_secrets_file(client_path, scopes)
    if _gdrive_oauth_use_console():
        port = int(os.getenv("GDRIVE_OAUTH_PORT", "8080"))
        creds = _gdrive_oauth_console_flow(flow, port)
    else:
        creds = flow.run_local_server(port=0)
    Path(token_path).parent.mkdir(parents=True, exist_ok=True)
    with open(token_path, 'w', encoding='utf-8') as f:
        f.write(creds.to_json())
    return creds


def _upload_gdrive_oauth(file_path: Path) -> str:
    """Upload as the signed-in Google user (uses your Drive quota — works with personal Gmail)."""
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError

    raw_folder = os.getenv('GDRIVE_FOLDER_ID') or ''
    folder_id = _normalize_gdrive_folder_id(raw_folder)
    if folder_id and raw_folder.strip() != folder_id:
        print(f"[BACKUP] Normalized GDRIVE_FOLDER_ID to: {folder_id}", flush=True)
    os.environ['GDRIVE_FOLDER_ID'] = folder_id
    if not folder_id:
        raise EnvironmentError(
            "GDRIVE_FOLDER_ID is empty. Set it to your Drive folder ID (from the folder URL)."
        )

    creds = _gdrive_oauth_credentials()
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)

    try:
        meta = service.files().get(
            fileId=folder_id,
            fields='id,name,mimeType',
            supportsAllDrives=_gdrive_supports_all_drives(),
        ).execute()
    except HttpError as e:
        if e.resp.status in (404, 403):
            raise EnvironmentError(
                "Cannot open GDRIVE_FOLDER_ID. Use the folder ID from the address bar "
                "when you are inside that folder in Drive."
            ) from e
        raise
    if meta.get('mimeType') != 'application/vnd.google-apps.folder':
        raise EnvironmentError("GDRIVE_FOLDER_ID must be a folder, not a file.")

    metadata = {'name': file_path.name, 'parents': [folder_id]}
    sad = _gdrive_supports_all_drives()
    size = file_path.stat().st_size
    resumable = size > 5 * 1024 * 1024

    media = MediaFileUpload(
        str(file_path),
        mimetype='application/octet-stream',
        resumable=resumable,
    )
    result = service.files().create(
        body=metadata,
        media_body=media,
        fields='id',
        supportsAllDrives=sad,
    ).execute()
    return result.get('id', '')


def _upload_gdrive_service_account(file_path: Path) -> str:
    """Upload to Google Drive using a service account. Returns the file ID."""
    from googleapiclient.discovery import build          # pip install google-api-python-client
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    from google.oauth2 import service_account            # pip install google-auth

    creds_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON', '')
    raw_folder = os.getenv('GDRIVE_FOLDER_ID') or ''
    folder_id  = _normalize_gdrive_folder_id(raw_folder)
    if folder_id and raw_folder.strip() != folder_id:
        print(f"[BACKUP] Normalized GDRIVE_FOLDER_ID to: {folder_id}", flush=True)
    os.environ['GDRIVE_FOLDER_ID'] = folder_id

    if not creds_path or not os.path.exists(creds_path):
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON path is invalid or not set")

    if not folder_id:
        raise EnvironmentError(
            "GDRIVE_FOLDER_ID is empty. Set it to your Drive folder ID (from the folder URL)."
        )

    with open(creds_path, encoding='utf-8') as f:
        sa_email = json.load(f).get('client_email', 'the service account in your JSON file')

    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=['https://www.googleapis.com/auth/drive'],
    )
    service = build('drive', 'v3', credentials=credentials)

    _gdrive_verify_folder(service, folder_id, sa_email)

    metadata = {'name': file_path.name, 'parents': [folder_id]}
    sad = _gdrive_supports_all_drives()

    size = file_path.stat().st_size
    resumable = size > 5 * 1024 * 1024  # Drive recommends resumable above 5 MiB

    def _is_quota(err: HttpError) -> bool:
        t = str(err)
        return err.resp.status == 403 and (
            'storageQuota' in t or 'storage quota' in t.lower()
        )

    # Resumable uploads split steps; some setups lose parent context → SA "no storage" quota error.
    # Retry: same, then force multipart, then supportsAllDrives (Team/Shared drives).
    plan = [
        (sad, resumable),
        (sad, False),
        (True, resumable),
        (True, False),
    ]
    seen: set = set()
    unique_plan = []
    for p in plan:
        if p in seen:
            continue
        seen.add(p)
        use_sad, use_res = p
        if not use_res and size > 5 * 1024 * 1024:
            continue
        unique_plan.append(p)

    last_err: HttpError | None = None
    for use_sad, use_res in unique_plan:
        media = MediaFileUpload(
            str(file_path),
            mimetype='application/octet-stream',
            resumable=use_res,
        )
        try:
            result = service.files().create(
                body=metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=use_sad,
            ).execute()
            return result.get('id', '')
        except HttpError as e:
            last_err = e
            if not _is_quota(e):
                raise
            continue

    if last_err is not None:
        raise EnvironmentError(
            "Google Drive: upload still hits service-account storage quota. "
            f"Share folder NATB as Editor with {sa_email}; avoid shortcuts—share the real folder. "
            "If the folder is on a Workspace Shared drive, set GDRIVE_SUPPORTS_ALL_DRIVES=1 in .env."
        ) from last_err
    raise RuntimeError('gdrive upload: no upload attempts')


def _upload_gdrive(file_path: Path) -> str:
    mode = os.getenv('GDRIVE_AUTH', 'service_account').lower()
    if mode in ('oauth', 'user', 'oauth2'):
        return _upload_gdrive_oauth(file_path)
    return _upload_gdrive_service_account(file_path)


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_backup() -> bool:
    """
    Full backup cycle:
      1. Create encrypted local snapshot.
      2. Upload to the configured cloud provider.
      3. Remove the local copy after a successful upload.
      4. Send a Telegram status message to the admin.

    Returns True on success, False on failure.
    Skips silently (returns True) if ENCRYPTION_KEY or provider token is not configured.
    """
    _load_project_env()
    from bot.notifier import send_telegram_message

    admin_id = os.getenv('ADMIN_CHAT_ID', '')
    provider = _resolve_backup_provider()
    now_str  = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    # Skip if not configured rather than crash
    if not os.getenv('ENCRYPTION_KEY'):
        print("[BACKUP] Skipped — ENCRYPTION_KEY not set in .env", flush=True)
        return True
    if not provider:
        print(
            "[BACKUP] Skipped — BACKUP_PROVIDER not set in .env (use 'gdrive' or 'dropbox', not a URL)",
            flush=True,
        )
        return True
    if provider not in ('gdrive', 'dropbox'):
        print(
            f"[BACKUP] FAIL: invalid BACKUP_PROVIDER '{provider}'. Use gdrive or dropbox only.",
            flush=True,
        )
        return False

    try:
        enc_path = create_encrypted_snapshot()
        print(f"[BACKUP] Snapshot created: {enc_path}", flush=True)

        if provider == 'dropbox':
            dest = _upload_dropbox(enc_path)
        elif provider == 'gdrive':
            dest = _upload_gdrive(enc_path)
        else:
            raise ValueError(f"Unknown BACKUP_PROVIDER: '{provider}'. Use 'dropbox' or 'gdrive'.")

        enc_path.unlink(missing_ok=True)  # clean up after upload
        print(f"[BACKUP] OK: uploaded to {provider}: {dest}", flush=True)

        if admin_id:
            send_telegram_message(
                admin_id,
                f"✅ *نسخ احتياطي مشفّر — اكتمل*\n"
                f"الموفر: `{provider}`\n"
                f"الملف: `{enc_path.name}`\n"
                f"الوقت: {now_str}"
            )
        return True

    except Exception as e:
        print(f"[BACKUP] FAIL: {e}", flush=True)
        if admin_id:
            send_telegram_message(
                admin_id,
                f"❌ *فشل النسخ الاحتياطي*\n"
                f"الخطأ: `{e}`\n"
                f"الوقت: {now_str}"
            )
        return False


# ── Standalone entry (cron-compatible) ───────────────────────────────────────

if __name__ == "__main__":
    _load_project_env()
    success = run_backup()
    raise SystemExit(0 if success else 1)
