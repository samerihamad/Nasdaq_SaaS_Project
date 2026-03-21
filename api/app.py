"""
NATB v2.0 — REST API (FastAPI)

Provides a secure HTTP interface for the web dashboard to:
  - View system status and subscriber list
  - Start / stop individual user sessions (kill switch)
  - Update per-user risk settings
  - Activate / deactivate maintenance mode and master kill switch
  - Query real-time performance metrics per user

Authentication:
  All endpoints require the header:
    X-Admin-Key: <ADMIN_API_SECRET from .env>

  User-scoped endpoints (/users/{chat_id}/*) additionally accept:
    X-User-Key: <USER_API_SECRET from .env> if set per user (future extension).
  For now all endpoints are admin-only.

Run:
    uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload

Install:
    pip install fastapi uvicorn
"""

import os
import sqlite3
from datetime import date
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

load_dotenv()

DB_PATH        = "database/trading_saas.db"
ADMIN_API_KEY  = os.getenv("ADMIN_API_SECRET", "")

app = FastAPI(
    title       = "NATB Trading Platform API",
    description = "Multi-tenant SaaS trading bot management API",
    version     = "2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],   # restrict to your dashboard domain in production
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ── Authentication ────────────────────────────────────────────────────────────

def _require_admin(x_admin_key: str = Header(...)):
    if not ADMIN_API_KEY:
        raise HTTPException(503, "ADMIN_API_SECRET not configured on server")
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(401, "Invalid admin key")


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn():
    return sqlite3.connect(DB_PATH)


def _setting(key: str) -> str:
    c = _conn().cursor()
    c.execute("SELECT value FROM system_settings WHERE key=?", (key,))
    row = c.fetchone()
    return row[0] if row else "false"


def _set_setting(key: str, value: str):
    with _conn() as cx:
        cx.execute(
            "INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)",
            (key, value),
        )


# ── Pydantic models ───────────────────────────────────────────────────────────

class RiskSettings(BaseModel):
    min_risk_pct: float = Field(..., ge=0.1, le=5.0, description="Min risk % per trade")
    max_risk_pct: float = Field(..., ge=0.1, le=5.0, description="Max risk % per trade")
    mode:         Optional[str] = Field(None, pattern="^(AUTO|HYBRID)$")

class KillSwitchRequest(BaseModel):
    active: bool

class MaintenanceRequest(BaseModel):
    active: bool


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    """Public health check endpoint."""
    return {"status": "ok", "version": "2.0"}


# ── System status ─────────────────────────────────────────────────────────────

@app.get("/admin/status", tags=["Admin"], dependencies=[Depends(_require_admin)])
def system_status():
    """Full system snapshot — active users, open trades, pending signals."""
    conn = _conn()
    c    = conn.cursor()
    c.execute("SELECT COUNT(*) FROM subscribers WHERE is_active=1 AND email IS NOT NULL")
    total_subs = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'")
    open_trades = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED'")
    closed_trades = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM pending_signals WHERE status='PENDING'")
    pending_sigs = c.fetchone()[0]
    conn.close()

    return {
        "maintenance_mode":   _setting("MAINTENANCE_MODE")   == "true",
        "master_kill_switch": _setting("MASTER_KILL_SWITCH") == "true",
        "active_subscribers": total_subs,
        "open_trades":        open_trades,
        "closed_trades":      closed_trades,
        "pending_signals":    pending_sigs,
    }


# ── Subscriber list ───────────────────────────────────────────────────────────

@app.get("/admin/subscribers", tags=["Admin"], dependencies=[Depends(_require_admin)])
def list_subscribers():
    """List all active subscribers with their status and risk settings."""
    conn = _conn()
    c    = conn.cursor()
    c.execute(
        "SELECT chat_id, is_active, kill_switch, mode, expiry_date, "
        "risk_percent, max_risk_percent, lang "
        "FROM subscribers WHERE email IS NOT NULL"
    )
    rows = c.fetchall()
    conn.close()

    return [
        {
            "chat_id":         r[0],
            "is_active":       bool(r[1]),
            "kill_switch":     bool(r[2]),
            "mode":            r[3],
            "expiry_date":     r[4],
            "min_risk_pct":    r[5],
            "max_risk_pct":    r[6],
            "lang":            r[7],
        }
        for r in rows
    ]


# ── Master kill switch ────────────────────────────────────────────────────────

@app.post("/admin/killswitch", tags=["Admin"], dependencies=[Depends(_require_admin)])
def master_kill_switch(req: KillSwitchRequest):
    """Activate or deactivate the global kill switch for ALL users."""
    _set_setting("MASTER_KILL_SWITCH", "true" if req.active else "false")
    return {
        "master_kill_switch": req.active,
        "message": "All new entries halted" if req.active else "Trading resumed for all users",
    }


# ── Maintenance mode ──────────────────────────────────────────────────────────

@app.post("/admin/maintenance", tags=["Admin"], dependencies=[Depends(_require_admin)])
def maintenance_mode(req: MaintenanceRequest):
    """Toggle global maintenance mode."""
    _set_setting("MAINTENANCE_MODE", "true" if req.active else "false")
    return {
        "maintenance_mode": req.active,
        "message": "Maintenance mode ON — new entries suspended" if req.active else "Maintenance mode OFF",
    }


# ── Per-user status ───────────────────────────────────────────────────────────

@app.get("/users/{chat_id}/status", tags=["Users"], dependencies=[Depends(_require_admin)])
def user_status(chat_id: str):
    """Real-time snapshot for a specific user."""
    conn = _conn()
    c    = conn.cursor()

    c.execute(
        "SELECT is_active, kill_switch, mode, expiry_date, risk_percent, max_risk_percent "
        "FROM subscribers WHERE chat_id=?",
        (chat_id,),
    )
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"User {chat_id} not found")

    c.execute(
        "SELECT date, consecutive_losses, state FROM daily_risk_state WHERE chat_id=?",
        (chat_id,),
    )
    risk_row = c.fetchone()

    c.execute(
        "SELECT COUNT(*), SUM(pnl) FROM trades WHERE chat_id=? AND status='OPEN'",
        (chat_id,),
    )
    open_r = c.fetchone()

    c.execute(
        "SELECT COUNT(*), SUM(pnl) FROM trades "
        "WHERE chat_id=? AND status='CLOSED' AND DATE(closed_at)=?",
        (chat_id, str(date.today())),
    )
    today_r = c.fetchone()

    conn.close()

    return {
        "chat_id":          chat_id,
        "is_active":        bool(row[0]),
        "kill_switch":      bool(row[1]),
        "mode":             row[2],
        "expiry_date":      row[3],
        "min_risk_pct":     row[4],
        "max_risk_pct":     row[5],
        "risk_state":       risk_row[2] if risk_row else "NORMAL",
        "consecutive_losses": risk_row[1] if risk_row else 0,
        "open_positions":   open_r[0] or 0,
        "open_upl":         round(float(open_r[1] or 0), 2),
        "today_trades":     today_r[0] or 0,
        "today_pnl":        round(float(today_r[1] or 0), 2),
    }


# ── Per-user performance ──────────────────────────────────────────────────────

@app.get("/users/{chat_id}/performance", tags=["Users"], dependencies=[Depends(_require_admin)])
def user_performance(chat_id: str, days: int = 30):
    """Closed-trade performance stats for the last N days."""
    conn = _conn()
    c    = conn.cursor()
    c.execute(
        "SELECT pnl FROM trades "
        "WHERE chat_id=? AND status='CLOSED' "
        "AND closed_at >= datetime('now', ? || ' days')",
        (chat_id, f"-{days}"),
    )
    pnls = [float(r[0]) for r in c.fetchall() if r[0] is not None]
    conn.close()

    if not pnls:
        return {"chat_id": chat_id, "period_days": days, "trades": 0}

    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gp     = sum(wins)
    gl     = abs(sum(losses)) or 1e-9

    return {
        "chat_id":       chat_id,
        "period_days":   days,
        "trades":        len(pnls),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate_pct":  round(len(wins) / len(pnls) * 100, 1),
        "net_pnl":       round(sum(pnls), 2),
        "profit_factor": round(gp / gl, 2),
        "avg_win":       round(sum(wins) / len(wins), 2) if wins else 0,
        "avg_loss":      round(sum(losses) / len(losses), 2) if losses else 0,
        "expectancy":    round(sum(pnls) / len(pnls), 2),
    }


# ── Per-user risk settings ────────────────────────────────────────────────────

@app.post("/users/{chat_id}/settings", tags=["Users"], dependencies=[Depends(_require_admin)])
def update_user_settings(chat_id: str, settings: RiskSettings):
    """Update risk parameters and trading mode for a specific user."""
    if settings.min_risk_pct > settings.max_risk_pct:
        raise HTTPException(400, "min_risk_pct cannot exceed max_risk_pct")

    updates = {
        "risk_percent":     settings.min_risk_pct,
        "max_risk_percent": settings.max_risk_pct,
    }
    if settings.mode:
        updates["mode"] = settings.mode

    set_clause = ", ".join(f"{k}=?" for k in updates)
    values     = list(updates.values()) + [chat_id]

    with _conn() as cx:
        cx.execute(f"UPDATE subscribers SET {set_clause} WHERE chat_id=?", values)

    return {"chat_id": chat_id, "updated": updates}


# ── Per-user kill switch ──────────────────────────────────────────────────────

@app.post("/users/{chat_id}/kill", tags=["Users"], dependencies=[Depends(_require_admin)])
def user_kill(chat_id: str):
    """Halt trading for a specific user immediately."""
    with _conn() as cx:
        cx.execute("UPDATE subscribers SET kill_switch=1 WHERE chat_id=?", (chat_id,))
    return {"chat_id": chat_id, "kill_switch": True}


@app.post("/users/{chat_id}/revive", tags=["Users"], dependencies=[Depends(_require_admin)])
def user_revive(chat_id: str):
    """Re-enable trading for a specific user."""
    with _conn() as cx:
        cx.execute("UPDATE subscribers SET kill_switch=0 WHERE chat_id=?", (chat_id,))
    return {"chat_id": chat_id, "kill_switch": False}


# ── Aggregate metrics ─────────────────────────────────────────────────────────

@app.get("/metrics", tags=["Admin"], dependencies=[Depends(_require_admin)])
def aggregate_metrics(days: int = 7):
    """Platform-wide trade metrics for the last N days."""
    conn = _conn()
    c    = conn.cursor()
    c.execute(
        "SELECT pnl FROM trades "
        "WHERE status='CLOSED' AND closed_at >= datetime('now', ? || ' days')",
        (f"-{days}",),
    )
    pnls = [float(r[0]) for r in c.fetchall() if r[0] is not None]

    c.execute("SELECT COUNT(DISTINCT chat_id) FROM trades WHERE status='OPEN'")
    users_with_open = c.fetchone()[0]
    conn.close()

    if not pnls:
        return {"period_days": days, "trades": 0, "users_with_open_positions": users_with_open}

    wins = [p for p in pnls if p > 0]
    gl   = abs(sum(p for p in pnls if p <= 0)) or 1e-9

    return {
        "period_days":          days,
        "total_trades":         len(pnls),
        "total_wins":           len(wins),
        "platform_win_rate":    round(len(wins) / len(pnls) * 100, 1),
        "platform_net_pnl":     round(sum(pnls), 2),
        "platform_profit_factor": round(sum(wins) / gl, 2) if wins else 0,
        "users_with_open_positions": users_with_open,
    }
