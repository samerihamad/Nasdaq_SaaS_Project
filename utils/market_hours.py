"""
Nasdaq Market Hours — NATB v2.0
DST-Aware | UAE/GST Display Layer

Architecture
────────────
• All market-session logic is evaluated in America/New_York (ET).
  pytz resolves EDT (UTC-4) vs EST (UTC-5) on every call — no static
  offsets, no manual cache, no cron job needed.  The OS/Python clock
  always runs in UTC; pytz converts to ET at call-time.

• Dubai (UAE) runs on GST = UTC+4 year-round (no DST).
  The New York ↔ Dubai gap therefore shifts between:
      Summer  (EDT, UTC-4):  NY + 8h  = Dubai  → opens 17:30 GST
      Winter  (EST, UTC-5):  NY + 9h  = Dubai  → opens 18:30 GST
  This is handled automatically — see `uae_offset_hours()`.

• "Gap weeks" (March/November transitions):
  The US and UAE do not change clocks simultaneously.  Because every
  call to _now_et() re-queries pytz for the current ET offset, the
  correct EDT/EST value is always used.  There is no cached offset that
  could drift during transition weeks.

Storage rule
────────────
  DB timestamps  →  UTC  (use utc_now())
  Display times  →  GST  (use now_uae(), market_open_in_uae(), etc.)
  Market logic   →  ET   (use _now_et(), get_market_status(), etc.)
"""

from datetime import datetime, time, timedelta, timezone
import socket
import struct
import pytz

# ── Timezone constants ────────────────────────────────────────────────────────

ET  = pytz.timezone('America/New_York')   # EDT (UTC-4) ↔ EST (UTC-5), auto-DST
UAE = pytz.timezone('Asia/Dubai')         # GST = UTC+4, no DST ever
UTC = pytz.utc

# Runtime UTC correction from NTP (seconds). We never mutate system clock;
# we keep a process-level offset and use it for all broker-critical timestamps.
_NTP_OFFSET_SECONDS = 0.0
_NTP_LAST_SYNC_UTC: datetime | None = None
_NTP_LAST_SERVER = ""

# ── ET session boundary times (always relative to ET clock) ──────────────────

MARKET_OPEN   = time(9,  30)   # 09:30 ET
MARKET_CLOSE  = time(16,  0)   # 16:00 ET
PRE_MKT_OPEN  = time(9,   0)   # 09:00 ET
AH_CLOSE      = time(20,  0)   # 20:00 ET

# ── Status constants ──────────────────────────────────────────────────────────

STATUS_OPEN       = 'OPEN'
STATUS_PRE_MARKET = 'PRE_MARKET'
STATUS_AFTER_HRS  = 'AFTER_HOURS'
STATUS_CLOSED     = 'CLOSED'


# ── Core clock helpers ────────────────────────────────────────────────────────

def _now_et() -> datetime:
    """
    Current moment expressed in America/New_York.
    pytz applies the correct UTC offset (EDT or EST) at call-time.
    No static offset is ever used.
    """
    return synchronized_utc_now().astimezone(ET)


def utc_now() -> datetime:
    """
    Current UTC timestamp — use this for ALL database writes.
    Produces timezone-aware datetimes; call .isoformat() for storage.
    """
    return synchronized_utc_now()


def utc_today() -> datetime.date:
    """Current UTC calendar date."""
    return utc_now().date()


def now_uae() -> datetime:
    """Current wall-clock time in Dubai (GST, UTC+4)."""
    return synchronized_utc_now().astimezone(UAE)


def synchronized_utc_now() -> datetime:
    """
    UTC now corrected by the in-process NTP offset.
    Falls back to system UTC when no sync exists.
    """
    return datetime.now(timezone.utc) + timedelta(seconds=float(_NTP_OFFSET_SECONDS))


def next_utc_occurrence(
    *,
    hour: int,
    minute: int,
    second: int = 0,
    now: datetime | None = None,
) -> datetime:
    """
    Next occurrence of a fixed UTC wall-clock time (HH:MM:SS) as an aware UTC datetime.

    This helper intentionally uses UTC only (no DST), for fixed UTC deadlines like:
    - 13:00 UTC (17:00 Dubai)
    - 12:40 UTC (16:40 Dubai)
    - 13:30:01 UTC (17:30:01 Dubai)
    """
    base = now if now is not None else synchronized_utc_now()
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    base = base.astimezone(timezone.utc)
    target = base.replace(hour=int(hour), minute=int(minute), second=int(second), microsecond=0)
    if target <= base:
        target = target + timedelta(days=1)
    return target


def seconds_until_utc(dt_utc: datetime, *, now: datetime | None = None) -> float:
    """Seconds until an aware UTC datetime (0 when passed)."""
    base = now if now is not None else synchronized_utc_now()
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    base = base.astimezone(timezone.utc)
    t = dt_utc.astimezone(timezone.utc)
    return max(0.0, (t - base).total_seconds())


def sync_utc_with_ntp(
    ntp_servers: list[str] | None = None,
    timeout_sec: float = 2.5,
) -> dict:
    """
    Sync process UTC offset with a public NTP server.
    Returns diagnostics and never raises.
    """
    global _NTP_OFFSET_SECONDS, _NTP_LAST_SYNC_UTC, _NTP_LAST_SERVER
    servers = ntp_servers or ["pool.ntp.org", "time.google.com", "time.windows.com"]
    timeout = max(0.5, float(timeout_sec))
    # RFC 5905: NTP timestamp is seconds since 1900-01-01.
    ntp_epoch_offset = 2208988800
    packet = b"\x1b" + 47 * b"\0"

    last_error = "ntp_sync_failed"
    for host in servers:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.settimeout(timeout)
            t0 = datetime.now(timezone.utc).timestamp()
            sock.sendto(packet, (host, 123))
            data, _ = sock.recvfrom(48)
            t3 = datetime.now(timezone.utc).timestamp()
            if len(data) < 48:
                last_error = f"short_ntp_reply:{host}"
                continue
            # Receive timestamp (T4/server transmit) is bytes 40:48.
            recv_sec, recv_frac = struct.unpack("!II", data[40:48])
            server_time = float(recv_sec - ntp_epoch_offset) + (float(recv_frac) / 2**32)
            local_midpoint = (t0 + t3) / 2.0
            offset = server_time - local_midpoint
            _NTP_OFFSET_SECONDS = float(offset)
            _NTP_LAST_SYNC_UTC = datetime.now(timezone.utc)
            _NTP_LAST_SERVER = str(host)
            return {
                "ok": True,
                "server": str(host),
                "offset_seconds": float(offset),
                "latency_ms": float((t3 - t0) * 1000.0),
                "synced_at_utc": _NTP_LAST_SYNC_UTC.isoformat(),
            }
        except Exception as exc:
            last_error = f"{host}:{exc}"
        finally:
            try:
                sock.close()
            except Exception:
                pass
    return {"ok": False, "error": str(last_error)}


def get_clock_sync_state() -> dict:
    return {
        "offset_seconds": float(_NTP_OFFSET_SECONDS),
        "last_sync_utc": _NTP_LAST_SYNC_UTC.isoformat() if _NTP_LAST_SYNC_UTC else None,
        "ntp_server": _NTP_LAST_SERVER or None,
    }


def _to_aware_utc(dt: datetime | None) -> datetime:
    if dt is None:
        return utc_now()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_current_timezones(now: datetime | None = None) -> dict:
    """
    Return current time snapshot in UTC, Dubai, and New York.
    Includes NY DST status and UTC offsets.
    """
    base_utc = _to_aware_utc(now)
    now_ny = base_utc.astimezone(ET)
    now_dubai = base_utc.astimezone(UAE)
    return {
        "utc": base_utc.isoformat(),
        "dubai": now_dubai.isoformat(),
        "new_york": now_ny.isoformat(),
        "new_york_is_dst": bool(now_ny.dst()),
        "offset_hours": {
            "utc": 0,
            "dubai": int(now_dubai.utcoffset().total_seconds() // 3600),
            "new_york": int(now_ny.utcoffset().total_seconds() // 3600),
        },
    }


# ── DST diagnostics ───────────────────────────────────────────────────────────

def is_dst_active() -> bool:
    """
    True when New York is on EDT (UTC-4, summer).
    False when New York is on EST (UTC-5, winter).
    Re-evaluated on every call.
    """
    return bool(_now_et().dst())


def uae_offset_hours() -> int:
    """
    Dynamic hour gap between Dubai (GST) and New York.
    Returns 8 during EDT (summer) or 9 during EST (winter).

    Derivation:
        GST  = UTC+4  →  utcoffset = +04:00
        EDT  = UTC-4  →  utcoffset = -04:00  →  gap = 8 h
        EST  = UTC-5  →  utcoffset = -05:00  →  gap = 9 h
    """
    uae_offset = now_uae().utcoffset().total_seconds()
    et_offset  = _now_et().utcoffset().total_seconds()
    return int((uae_offset - et_offset) / 3600)


# ── UAE display helpers ───────────────────────────────────────────────────────

def _et_to_uae(et_naive_date, hour: int, minute: int) -> datetime:
    """
    Build a timezone-aware ET datetime for a given date + time,
    then convert it to UAE/GST.  The ET→GST offset is computed
    dynamically so DST transitions are handled without any manual change.
    """
    et_dt  = ET.localize(datetime(et_naive_date.year,
                                   et_naive_date.month,
                                   et_naive_date.day,
                                   hour, minute))
    return et_dt.astimezone(UAE)


def market_open_in_uae() -> str:
    """
    Market open time for TODAY's ET date, expressed in GST.
    Returns e.g. '05:30 PM GST' (summer) or '06:30 PM GST' (winter).
    """
    uae_dt = _et_to_uae(_now_et().date(), 9, 30)
    return uae_dt.strftime('%I:%M %p GST')


def market_close_in_uae() -> str:
    """
    Market close time for TODAY's ET date, expressed in GST.
    Returns e.g. '01:00 AM GST' (summer/next-day) or '02:00 AM GST' (winter).
    """
    uae_dt = _et_to_uae(_now_et().date(), 16, 0)
    return uae_dt.strftime('%I:%M %p GST')


def next_market_open_in_uae() -> str:
    """
    Next regular-session open expressed in GST (date + time).
    Suitable for display in closed/after-hours status messages.
    """
    opens_et = next_market_open()
    opens_uae = opens_et.astimezone(UAE)
    return opens_uae.strftime('%a %d %b  %I:%M %p GST')


# ── Market-session logic ──────────────────────────────────────────────────────

def _is_trading_day(dt: datetime) -> bool:
    """True if dt (ET-aware) falls on a regular Nasdaq trading day."""
    if dt.weekday() >= 5:          # Saturday or Sunday
        return False
    try:
        import pandas_market_calendars as mcal
        cal   = mcal.get_calendar('NASDAQ')
        sched = cal.schedule(start_date=str(dt.date()), end_date=str(dt.date()))
        return not sched.empty     # empty → holiday
    except Exception:
        return True                # library absent → assume trading day


def is_nyse_trading_day(now_et: datetime | None = None) -> bool:
    """
    True if the America/New_York calendar date is a regular Nasdaq cash session day
    (Monday–Friday, excluding exchange holidays). Uses ``_is_trading_day``:
    weekend filter plus ``pandas_market_calendars`` NASDAQ schedule when installed.
    """
    dt = _now_et() if now_et is None else now_et
    if getattr(dt, "tzinfo", None) is None:
        dt = ET.localize(dt)
    else:
        dt = dt.astimezone(ET)
    return _is_trading_day(dt)


def is_trading_required() -> bool:
    """
    False on U.S. weekends and Nasdaq holidays; True on regular session days.
    Use in background threads to skip Capital.com / broker authentication and API calls.
    """
    return is_nyse_trading_day()


def get_market_status() -> str:
    """
    Return the current Nasdaq session status.
    All comparisons are made against ET time; DST is resolved by pytz
    on every call.
    """
    now = _now_et()
    if not _is_trading_day(now):
        return STATUS_CLOSED
    t = now.time()
    if MARKET_OPEN  <= t < MARKET_CLOSE:
        return STATUS_OPEN
    if PRE_MKT_OPEN <= t < MARKET_OPEN:
        return STATUS_PRE_MARKET
    if MARKET_CLOSE <= t < AH_CLOSE:
        return STATUS_AFTER_HRS
    return STATUS_CLOSED


def session_windows_utc(now: datetime | None = None) -> dict:
    """
    Session windows for the ET trading day expressed in UTC (DST-aware).
    """
    now_et = _to_aware_utc(now).astimezone(ET)
    d = now_et.date()
    pre_open_et = ET.localize(datetime(d.year, d.month, d.day, PRE_MKT_OPEN.hour, PRE_MKT_OPEN.minute))
    regular_open_et = ET.localize(datetime(d.year, d.month, d.day, MARKET_OPEN.hour, MARKET_OPEN.minute))
    regular_close_et = ET.localize(datetime(d.year, d.month, d.day, MARKET_CLOSE.hour, MARKET_CLOSE.minute))
    ah_close_et = ET.localize(datetime(d.year, d.month, d.day, AH_CLOSE.hour, AH_CLOSE.minute))
    return {
        "pre_market_open_utc": pre_open_et.astimezone(timezone.utc),
        "regular_open_utc": regular_open_et.astimezone(timezone.utc),
        "regular_close_utc": regular_close_et.astimezone(timezone.utc),
        "after_hours_close_utc": ah_close_et.astimezone(timezone.utc),
    }


def is_market_open() -> bool:
    return get_market_status() == STATUS_OPEN


def next_market_open() -> datetime:
    """
    ET-aware datetime of the next regular-session open (09:30 ET).
    Iterates up to 8 calendar days to skip weekends and holidays.
    """
    now = _now_et()
    for days_ahead in range(8):
        candidate_date = (now + timedelta(days=days_ahead)).date()
        opens_at = ET.localize(
            datetime(candidate_date.year, candidate_date.month,
                     candidate_date.day, 9, 30)
        )
        if opens_at > now and _is_trading_day(opens_at):
            return opens_at
    return now   # fallback (should never happen)


def minutes_to_open() -> int:
    """Minutes until next regular-session open (0 if market is currently open)."""
    if is_market_open():
        return 0
    delta = next_market_open() - _now_et()
    return max(0, int(delta.total_seconds() / 60))


# ── Strict UTC session gatekeeper (dynamic from ET windows) ───────────────────
# This guard is computed from ET windows each day so NY DST is always respected.
US_CASH_ORDER_CUTOFF_MINUTES = 5  # block new orders in final N minutes of RTH


def utc_time_now() -> time:
    """Current UTC wall-clock time (naive time object)."""
    return synchronized_utc_now().time()


def is_within_us_cash_session_utc(now: datetime | None = None) -> tuple[bool, str]:
    """
    Strict guard for regular session only:
      - allow only during 09:30–16:00 ET converted to UTC dynamically (DST-aware)
      - block at/after final cutoff (default: last 5 minutes of regular session)

    Returns (allowed, reason_code).
    """
    n = _to_aware_utc(now)
    windows = session_windows_utc(n)
    open_utc = windows["regular_open_utc"]
    close_utc = windows["regular_close_utc"]
    cutoff_utc = close_utc - timedelta(minutes=int(US_CASH_ORDER_CUTOFF_MINUTES))

    if n >= cutoff_utc:
        return False, "CUTOFF_BEFORE_REGULAR_CLOSE_UTC"
    if open_utc <= n < close_utc:
        return True, "OK"
    return False, "OUTSIDE_REGULAR_SESSION_UTC"
