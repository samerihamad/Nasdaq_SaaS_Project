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
import pytz

# ── Timezone constants ────────────────────────────────────────────────────────

ET  = pytz.timezone('America/New_York')   # EDT (UTC-4) ↔ EST (UTC-5), auto-DST
UAE = pytz.timezone('Asia/Dubai')         # GST = UTC+4, no DST ever

# ── ET session boundary times (always relative to ET clock) ──────────────────

MARKET_OPEN   = time(9,  30)   # 09:30 ET
MARKET_CLOSE  = time(16,  0)   # 16:00 ET
PRE_MKT_OPEN  = time(4,   0)   # 04:00 ET
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
    return datetime.now(ET)


def utc_now() -> datetime:
    """
    Current UTC timestamp — use this for ALL database writes.
    Produces timezone-aware datetimes; call .isoformat() for storage.
    """
    return datetime.now(timezone.utc)


def now_uae() -> datetime:
    """Current wall-clock time in Dubai (GST, UTC+4)."""
    return datetime.now(UAE)


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
