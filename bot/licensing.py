"""
Encrypted Credential Storage + License Management — NATB v2.0

Credentials (email, api_password, api_key) are encrypted at rest using
Fernet symmetric encryption. The encryption key lives in .env only.

License keys are 32-character hex strings. An admin sets the expiry date
in the `subscribers` table via `set_license()`.

Setup:
    1. Run `python -c "from bot.licensing import generate_encryption_key; generate_encryption_key()"
       once to print a key, then add ENCRYPTION_KEY=<key> to .env.
    2. To issue a license: `from bot.licensing import issue_license; issue_license(chat_id, days=30)`
"""

import os
import secrets
import sqlite3
from datetime import date, timedelta
from cryptography.fernet import Fernet, InvalidToken

DB_PATH = 'database/trading_saas.db'


# ── Encryption ────────────────────────────────────────────────────────────────

def generate_encryption_key():
    """Print a new Fernet key. Run once and add to .env as ENCRYPTION_KEY."""
    key = Fernet.generate_key().decode()
    print(f"ENCRYPTION_KEY={key}")
    return key


def _cipher():
    raw = os.getenv('ENCRYPTION_KEY', '')
    if not raw:
        raise EnvironmentError(
            "ENCRYPTION_KEY missing from .env. "
            "Run `python -c \"from bot.licensing import generate_encryption_key; "
            "generate_encryption_key()\"` and add the result to .env."
        )
    return Fernet(raw.encode())


def encrypt(plaintext: str) -> str:
    """Encrypt a string. Returns a URL-safe base64 token."""
    return _cipher().encrypt(plaintext.encode()).decode()


def decrypt(token: str) -> str:
    """Decrypt a Fernet token. Raises InvalidToken if key/data is wrong."""
    return _cipher().decrypt(token.encode()).decode()


def safe_decrypt(token: str) -> str:
    """Decrypt with graceful fallback — returns the raw token if decryption fails."""
    if not token:
        return ''
    try:
        return decrypt(token)
    except (InvalidToken, Exception):
        return token   # may already be plaintext (legacy records)


# ── License key generation ────────────────────────────────────────────────────

def generate_license_key() -> str:
    """Generate a new 32-character hex license key (uppercase)."""
    return secrets.token_hex(16).upper()


def issue_license(chat_id: str, days: int = 30) -> str:
    """
    Create and assign a license key to `chat_id`, valid for `days` days.
    Returns the license key.
    """
    key    = generate_license_key()
    expiry = str(date.today() + timedelta(days=days))

    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET license_key=?, expiry_date=? WHERE chat_id=?",
        (key, expiry, chat_id)
    )
    conn.commit()
    conn.close()
    return key


# ── License validation ────────────────────────────────────────────────────────

def validate_license_key(input_key: str) -> tuple[str | None, str | None]:
    """
    Check if `input_key` matches any subscriber's license key.
    Returns (chat_id, expiry_date) if valid and not expired, else (None, None).
    """
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT chat_id, expiry_date FROM subscribers WHERE license_key=?",
        (input_key.strip().upper(),)
    )
    row = c.fetchone()
    conn.close()

    if not row:
        return None, None

    chat_id, expiry_str = row
    if expiry_str and date.fromisoformat(expiry_str) < date.today():
        return None, None   # expired

    return chat_id, expiry_str


def check_license(chat_id: str) -> tuple[bool, int | None]:
    """
    Returns (is_valid: bool, days_remaining: int | None).
    Automatically called before any trade signal is processed.
    """
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT expiry_date FROM subscribers WHERE chat_id=?",
        (chat_id,)
    )
    row = c.fetchone()
    conn.close()

    if not row or not row[0]:
        return False, None

    expiry = date.fromisoformat(row[0])
    today  = date.today()
    if expiry < today:
        return False, 0

    return True, (expiry - today).days


# ── Encrypted credential helpers ──────────────────────────────────────────────

def store_credentials(chat_id: str, email: str, api_password: str, api_key: str):
    """Encrypt and persist user credentials."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE subscribers SET email=?, api_password=?, api_key=? WHERE chat_id=?",
        (encrypt(email), encrypt(api_password), encrypt(api_key), chat_id)
    )
    conn.commit()
    conn.close()


def load_credentials(chat_id: str) -> tuple[str, str, str] | None:
    """
    Load and decrypt (email, api_password, api_key).
    Returns None if the user isn't registered.
    """
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT email, api_password, api_key FROM subscribers WHERE chat_id=?",
        (chat_id,)
    )
    row = c.fetchone()
    conn.close()

    if not row:
        return None
    return safe_decrypt(row[0]), safe_decrypt(row[1]), safe_decrypt(row[2])
