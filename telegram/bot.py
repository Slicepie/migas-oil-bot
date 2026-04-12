"""
Migas Oil Bot — Telegram interface for Migas-1.5 oil price forecasting.

Commands:
    /start          — welcome message
    /forecast       — WTI 16-day forecast (60 days history)
    /brent          — Brent 16-day forecast (60 days history)
    /alert <price>  — notify when WTI crosses a price level
    /alerts         — list active alerts
    /cancelalert    — cancel all alerts

History window: 60 days — post-war regime only, pre-war data excluded.
Brent is more sensitive to Hormuz/OPEC/Iran; WTI to US domestic policy.
Private: only ALLOWED_USER_IDS can interact with the bot.
"""

import asyncio
import json
import logging
import os
import random
import string
from datetime import datetime, timedelta, timezone
from functools import wraps

import requests
import yfinance as yf
from aiohttp import web
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from news import (
    build_live_summary, build_live_summary_override, get_relevant_trump_posts, refresh_cache,
    score_emoji, append_new_posts, _score_raw_posts, get_scored_posts,
    analogue_signal, format_analogue_signal, net_signal_text,
    is_oil_topic,
)
from tracker import log_signal, follow_up, format_accuracy_report, _current_wti
from llm_score import llm_score_post, llm_score_multi_market, format_llm_followup, format_multi_market_alert, MARKET_TICKERS, THEME_EXPECTATIONS

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BOT_TOKEN        = os.environ["TELEGRAM_BOT_TOKEN"]
RUNPOD_API_KEY   = os.environ["RUNPOD_API_KEY"]
RUNPOD_ENDPOINT  = "https://api.runpod.ai/v2/fxkby0bka43s1i/runsync"

# USOIL.AI dashboard — save forecasts for tracking vs reality
USOIL_AI_URL     = os.environ.get("USOIL_AI_URL", "")          # e.g. https://usoil-ai.vercel.app
FORECAST_SECRET  = os.environ.get("FORECAST_SAVE_SECRET", "changeme")

ALLOWED_USER_IDS = {1038492789}   # @slicepie5

AUTO_FORECAST_MIN_SCORE      = 3    # |score| threshold to trigger auto-forecast
AUTO_FORECAST_COOLDOWN_MIN   = 30   # minutes between auto-forecasts (avoid spam)
AUTO_FORECAST_PRED_LEN       = 5    # days — shorter forecasts are more accurate for events
AUTO_FORECAST_THEMES         = {"IRAN_MILITARY", "IRAN_DIPLOMATIC", "HORMUZ", "TARIFF_RELIEF"}

ROLLING_FORECAST_INTERVAL_H  = 12   # hours between rolling forecast updates
ROLLING_FORECAST_PRED_LEN    = 16   # always forecast 16 days ahead

WEBHOOK_SECRET  = os.environ.get("WEBHOOK_SECRET", "changeme")
WEBHOOK_PORT    = int(os.environ.get("WEBHOOK_PORT", "8080"))
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")

# Trader API webhook — fires BEFORE Telegram for lowest latency
SIGNAL_WEBHOOK_URLS: list[str] = [
    u.strip() for u in os.environ.get("SIGNAL_WEBHOOK_URLS", "").split(",") if u.strip()
]
SIGNAL_WEBHOOK_SECRET = os.environ.get("SIGNAL_WEBHOOK_SECRET", "")

# Discord webhook — public channel updates
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# ---------------------------------------------------------------------------
# SSE stream — in-memory push to connected traders (zero polling)
# ---------------------------------------------------------------------------
# Each connected client gets an asyncio.Queue. When a signal fires we push
# to every queue instantly. Client disconnect removes the queue from the set.

_sse_clients: dict[str, set[asyncio.Queue]] = {}  # market -> set of queues
_sse_ip_count: dict[str, int] = {}                # IP -> active connection count
MAX_SSE_CLIENTS_TOTAL = 200       # hard cap across all markets
MAX_SSE_PER_IP        = 3         # max connections per IP address


def sse_push(market: str, event: dict) -> None:
    """Push an event to all SSE clients subscribed to a market. Non-blocking."""
    clients = _sse_clients.get(market, set())
    dead: list[asyncio.Queue] = []
    for q in clients:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        clients.discard(q)
    if clients:
        log.info("SSE pushed to %d %s clients", len(clients), market)


def sse_push_signal(all_markets: dict, meta: dict, prices: dict,
                    signal_id: str, text: str, ts: str) -> None:
    """Push a multi-market signal to all relevant SSE streams. Skips score=0 markets."""
    for market, data in all_markets.items():
        if data.get("score", 0) == 0:
            continue  # traders only get actionable signals
        event = {
            "signal_id":      signal_id,
            "ts":             ts,
            "market":         market,
            "data":           data,
            "price":          prices.get(market),
            "post_theme":     meta.get("post_theme"),
            "ambiguity_flag": meta.get("ambiguity_flag", False),
            "text_preview":   text[:200],
        }
        sse_push(market, event)

# Twitter/X — async posting after Telegram (never blocks signal speed)
TWITTER_API_KEY          = os.environ.get("TWITTER_API_KEY", "")
TWITTER_API_SECRET       = os.environ.get("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN     = os.environ.get("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET", "")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Upstash Redis REST helpers (shared with Next.js webapp)
# ---------------------------------------------------------------------------

UPSTASH_REDIS_REST_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")


def _redis_cmd(*args) -> dict | None:
    """Execute a single Redis command via the Upstash REST API.
    Returns the parsed JSON response or None on failure."""
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        log.warning("Upstash Redis not configured — skipping command %s", args[0] if args else "?")
        return None
    try:
        resp = requests.post(
            UPSTASH_REDIS_REST_URL,
            headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
            json=list(args),
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        log.error("Redis command %s failed: %s", args, exc)
        return None


# ---------------------------------------------------------------------------
# Points / referral constants
# ---------------------------------------------------------------------------

POINTS_FOLLOW_X        = 100
POINTS_JOIN_TELEGRAM   = 100
POINTS_JOIN_DISCORD    = 50
POINTS_REFERRAL_SIGNUP = 250
POINTS_REFERRAL_STANDARD = 2000
POINTS_REFERRAL_PREMIUM  = 5000


def _user_key(username: str) -> str:
    return f"points:user:{username}"


def _generate_referral_code(username: str) -> str:
    """First 4 chars of username uppercased + 2 random digits, e.g. JAKE42."""
    prefix = username[:4].upper().ljust(4, "X")
    suffix = f"{random.randint(0, 99):02d}"
    return prefix + suffix


def _resolve_username(user) -> str:
    """Return telegram username (lowercased) if set, otherwise str(user.id)."""
    return user.username.lower() if user.username else str(user.id)


def _award_points(username: str, points: int, event_type: str, meta: dict | None = None):
    """Award points to a user: increment hash, update leaderboard, log event."""
    _redis_cmd("HINCRBY", _user_key(username), "points", points)
    _redis_cmd("ZINCRBY", "points:leaderboard", points, username)
    event = {
        "type": event_type,
        "points": points,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if meta:
        event.update(meta)
    _redis_cmd("LPUSH", f"points:events:{username}", json.dumps(event))


# ---------------------------------------------------------------------------
# Auth decorator
# ---------------------------------------------------------------------------

def restricted(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if uid not in ALLOWED_USER_IDS:
            log.warning("Unauthorized access attempt by user %s", uid)
            await update.message.reply_text("⛔ Unauthorized.")
            return
        return await func(update, context)
    return wrapper

# ---------------------------------------------------------------------------
# Price + forecast helpers
# ---------------------------------------------------------------------------

HISTORY_DAYS = 60   # post-war regime only — pre-war dynamics are a different market

TICKERS = {
    "wti":   ("CL=F",  "WTI"),
    "brent": ("BZ=F",  "Brent"),
}

# ---------------------------------------------------------------------------
# Hyperliquid 24/7 pricing fallback (WTI perp on XYZ DEX)
# ---------------------------------------------------------------------------
# CME futures (CL=F) only update during market hours.
# Hyperliquid xyz:CL trades 24/7 — use as fallback when CL=F is stale.

HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"

# Map MARKET_TICKERS symbols to Hyperliquid xyz: symbols
_HL_SYMBOLS: dict[str, str] = {
    "CL=F":   "xyz:CL",       # WTI crude
    "BZ=F":   "xyz:BRENTOIL", # Brent crude
    # BTC-USD, ^GSPC, NG=F — not on Hyperliquid XYZ, skip
}

_hl_cache: dict[str, tuple[float, float]] = {}  # symbol -> (price, timestamp)
_HL_CACHE_TTL = 10  # seconds

import time as _time


def _fetch_hyperliquid_price(hl_symbol: str) -> float | None:
    """Fetch a single price from Hyperliquid. Uses 10s cache to avoid spam."""
    import time
    now = time.time()
    cached = _hl_cache.get(hl_symbol)
    if cached and (now - cached[1]) < _HL_CACHE_TTL:
        return cached[0]

    try:
        resp = requests.post(
            HYPERLIQUID_INFO_URL,
            json={"type": "allMids", "dex": "xyz"},
            timeout=3,
        )
        if resp.status_code == 200:
            mids = resp.json()
            # Cache all prices from this response
            for sym, price_str in mids.items():
                try:
                    _hl_cache[sym] = (float(price_str), now)
                except (ValueError, TypeError):
                    pass
            if hl_symbol in _hl_cache:
                return _hl_cache[hl_symbol][0]
    except Exception as exc:
        log.warning("Hyperliquid price fetch failed: %s", exc)
    return None


# Cache for Hyperliquid detailed market data (volume, OI, funding)
_hl_detail_cache: dict[str, tuple[dict, float]] = {}  # symbol -> (data, timestamp)
_HL_DETAIL_CACHE_TTL = 30  # seconds


def _fetch_hyperliquid_details() -> dict[str, dict]:
    """Fetch detailed market data from Hyperliquid (volume, OI, funding).
    Returns {symbol: {dayNtlVlm, dayBaseVlm, oraclePx, markPx, funding, openInterest}} for all xyz markets.
    """
    import time
    now = time.time()

    # Check if any cached entry is fresh
    if _hl_detail_cache and all((now - v[1]) < _HL_DETAIL_CACHE_TTL for v in _hl_detail_cache.values()):
        return {k: v[0] for k, v in _hl_detail_cache.items()}

    try:
        resp = requests.post(
            HYPERLIQUID_INFO_URL,
            json={"type": "metaAndAssetCtxs", "dex": "xyz"},
            timeout=5,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        meta = data[0]  # {universe: [{name, ...}, ...]}
        ctxs = data[1]  # [{dayNtlVlm, openInterest, ...}, ...]

        result = {}
        for asset_info, ctx in zip(meta.get("universe", []), ctxs):
            name = asset_info.get("name", "")
            _hl_detail_cache[name] = (ctx, now)
            result[name] = ctx
        return result
    except Exception as exc:
        log.warning("Hyperliquid detail fetch failed: %s", exc)
        return {}


def _is_cme_market_open() -> bool:
    """Check if CME crude oil futures are likely trading.
    CME WTI: Sun 5pm CT – Fri 4pm CT, with a daily break 4pm-5pm CT.
    Simplified: if Yahoo returns a price < 30 min old, it's 'open'.
    We use a rough heuristic instead to avoid extra API calls.
    """
    from datetime import datetime, timezone, timedelta
    now_utc = datetime.now(timezone.utc)
    # Convert to CT (UTC-5 standard, UTC-6 daylight — approximate with UTC-5)
    ct_hour = (now_utc.hour - 5) % 24
    ct_weekday = now_utc.weekday()  # Mon=0, Sun=6

    # Closed: Saturday all day, Sunday before 5pm CT, Friday after 4pm CT
    if ct_weekday == 5:  # Saturday
        return False
    if ct_weekday == 6 and ct_hour < 17:  # Sunday before 5pm CT
        return False
    if ct_weekday == 4 and ct_hour >= 16:  # Friday after 4pm CT
        return False
    # Daily maintenance break: 4pm-5pm CT
    if ct_hour == 16:
        return False
    return True


def fetch_price_with_fallback(yf_ticker: str) -> float | None:
    """Fetch price from Yahoo Finance, fall back to Hyperliquid if stale/unavailable."""
    # Try Yahoo first
    try:
        hist = yf.Ticker(yf_ticker).history(period="1d", interval="5m")[["Close"]]
        if not hist.empty:
            price = round(float(hist["Close"].iloc[-1]), 4)
            # Check staleness — if last bar is >30 min old and market should be closed
            last_ts = hist.index[-1]
            from datetime import datetime, timezone
            age_min = (datetime.now(timezone.utc) - last_ts.to_pydatetime().astimezone(timezone.utc)).total_seconds() / 60
            if age_min < 30:
                return price  # fresh — use it
            # Stale — try Hyperliquid
            hl_sym = _HL_SYMBOLS.get(yf_ticker)
            if hl_sym:
                hl_price = _fetch_hyperliquid_price(hl_sym)
                if hl_price:
                    log.info("Using Hyperliquid %s price $%.2f (Yahoo %s stale by %.0f min)",
                             hl_sym, hl_price, yf_ticker, age_min)
                    return round(hl_price, 4)
            return price  # stale Yahoo is better than nothing
    except Exception:
        pass

    # Yahoo failed entirely — try Hyperliquid
    hl_sym = _HL_SYMBOLS.get(yf_ticker)
    if hl_sym:
        hl_price = _fetch_hyperliquid_price(hl_sym)
        if hl_price:
            log.info("Yahoo %s failed, using Hyperliquid %s: $%.2f", yf_ticker, hl_sym, hl_price)
            return round(hl_price, 4)
    return None

# ---------------------------------------------------------------------------
# Volume spike detection config
# ---------------------------------------------------------------------------
VOLUME_SPIKE_MULTIPLIER  = 1.5    # alert if current 5-min vol > 1.5x hourly baseline
VOLUME_BASELINE_DAYS     = 14     # days of hourly history to build baseline
VOLUME_COOLDOWN_MIN      = 30     # minimum minutes between volume spike alerts

# Hourly baseline cache — rebuilt every 24h
_volume_baseline: dict[int, float] = {}   # {hour_of_day: avg_5min_volume}
_volume_baseline_built: datetime | None = None


def _build_volume_baseline() -> dict[int, float]:
    """Build average 5-min CL=F volume by hour-of-day from last 14 days.

    Returns {hour: avg_volume_per_5min_bar} so it can be compared directly
    against a single 5-min bar's volume in check_volume_spike().
    """
    import pandas as pd
    ticker = yf.Ticker("CL=F")
    # Use 5m data (yfinance max ~60 days for 5m) to get per-bar baseline
    hist   = ticker.history(period=f"{VOLUME_BASELINE_DAYS}d", interval="5m")[["Volume"]]
    if hist.empty:
        # Fallback to 1h data divided by 12
        hist = ticker.history(period=f"{VOLUME_BASELINE_DAYS}d", interval="1h")[["Volume"]]
        hist.index = pd.to_datetime(hist.index)
        hist["hour"] = hist.index.hour
        baseline = {h: v / 12 for h, v in hist.groupby("hour")["Volume"].mean().to_dict().items()}
        log.info("Volume baseline built (1h fallback /12): %d hours, avg=%.0f per 5m bar", len(baseline), sum(baseline.values()) / max(len(baseline), 1))
        return baseline
    hist.index = pd.to_datetime(hist.index)
    hist["hour"] = hist.index.hour
    baseline = hist.groupby("hour")["Volume"].mean().to_dict()
    log.info("Volume baseline built (5m): %d hours, avg=%.0f per 5m bar", len(baseline), sum(baseline.values()) / max(len(baseline), 1))
    return baseline


def _get_volume_baseline() -> dict[int, float]:
    """Return cached baseline, rebuilding if older than 24h."""
    global _volume_baseline, _volume_baseline_built
    now = datetime.now(timezone.utc)
    if (
        not _volume_baseline
        or _volume_baseline_built is None
        or (now - _volume_baseline_built).total_seconds() > 86400
    ):
        _volume_baseline       = _build_volume_baseline()
        _volume_baseline_built = now
    return _volume_baseline


def _last_trump_post_age_minutes() -> float:
    """Return minutes since the most recent Trump post in cache, or infinity."""
    from news import _read_cache
    posts = _read_cache()
    if not posts:
        return float("inf")
    # Find most recent post with a date+time
    now = datetime.now(timezone.utc)
    most_recent = None
    for p in posts:
        date_str = p.get("date", "")
        time_str = p.get("time_et", "") or "00:00:00"
        if not date_str:
            continue
        try:
            import pytz
            et   = pytz.timezone("America/New_York")
            dt   = datetime.fromisoformat(f"{date_str}T{time_str}")
            dt   = et.localize(dt).astimezone(timezone.utc)
            if most_recent is None or dt > most_recent:
                most_recent = dt
        except Exception:
            continue
    if most_recent is None:
        return float("inf")
    return (now - most_recent).total_seconds() / 60


def fetch_prices(instrument: str = "wti", days: int = HISTORY_DAYS) -> tuple[list[dict], float]:
    """Fetch oil prices via yfinance with Hyperliquid fallback for off-hours.

    Args:
        instrument: "wti" or "brent"
        days: history window in days (default 60 — post-war regime)

    Returns:
        price_data: list of {t, y_t} dicts sorted by date
        current_price: latest price (Hyperliquid when CME closed, else Yahoo)
    """
    ticker_sym, _ = TICKERS[instrument]
    ticker = yf.Ticker(ticker_sym)
    hist = ticker.history(period=f"{days}d")[["Close"]].reset_index()
    hist = hist.dropna(subset=["Close"])

    price_data = [
        {"t": str(row.Date.date()), "y_t": round(float(row.Close), 4)}
        for _, row in hist.iterrows()
    ]
    current_price = float(hist["Close"].iloc[-1])

    # If CME is closed, use Hyperliquid for a live current price
    if instrument == "wti" and not _is_cme_market_open():
        hl_price = _fetch_hyperliquid_price("CL=F")
        if hl_price:
            current_price = hl_price
            log.info("fetch_prices: using Hyperliquid price $%.2f (CME closed)", hl_price)

    return price_data, current_price


def get_forecast(
    price_data:  list[dict],
    summary:     str,
    pred_len:    int  = 16,
    n_summaries: int  = 5,
    counterfactual: bool = False,
    bullish_predictive: str = "",
    bearish_predictive: str = "",
) -> dict:
    """Call the RunPod Migas-1.5 endpoint.

    Returns a dict with keys:
      - forecast          — main ensemble forecast (always present)
      - chronos_baseline  — text-free baseline (always present)
      - forecast_bullish  — bullish counterfactual (if counterfactual=True)
      - forecast_bearish  — bearish counterfactual (if counterfactual=True)
    """
    payload: dict = {
        "price_data":  price_data,
        "summary":     summary,
        "pred_len":    pred_len,
        "n_summaries": n_summaries,
    }
    if counterfactual:
        payload["counterfactual"] = True
        if bullish_predictive:
            payload["bullish_predictive"] = bullish_predictive
        if bearish_predictive:
            payload["bearish_predictive"] = bearish_predictive

    resp = requests.post(
        RUNPOD_ENDPOINT,
        headers={"Authorization": f"Bearer {RUNPOD_API_KEY}"},
        json={"input": payload},
        timeout=180,
    )
    resp.raise_for_status()
    data = resp.json()

    output = data.get("output", {})
    if "error" in output:
        raise RuntimeError(output["error"])

    return output


def save_forecast_to_dashboard(
    instrument:    str,
    current_price: float,
    output:        dict,
    summary:       str,
    score:         int   = 0,
    direction:     str   = "NEUTRAL",
    trigger_post:  str   = "",
    signals:       list  | None = None,
    pred_len:      int   = 16,
) -> None:
    """POST forecast to USOIL.AI dashboard for tracking vs reality.
    Silently ignores errors so it never breaks the bot."""
    if not USOIL_AI_URL:
        return
    try:
        payload = {
            "secret":           FORECAST_SECRET,
            "instrument":       instrument,
            "generated_at":     datetime.now(timezone.utc).isoformat(),
            "current_price":    current_price,
            "pred_len":         pred_len,
            "forecast":         output.get("forecast", []),
            "chronos_baseline": output.get("chronos_baseline", []),
            "forecast_bullish": output.get("forecast_bullish"),
            "forecast_bearish": output.get("forecast_bearish"),
            "summary":          summary,
            "score":            score,
            "direction":        direction,
            "trigger_post":     trigger_post,
            "signals":          signals or [],
        }
        resp = requests.post(
            f"{USOIL_AI_URL}/api/forecast/save",
            json=payload,
            timeout=10,
        )
        if resp.ok:
            log.info("Forecast saved to dashboard: %s", resp.json().get("id"))
        else:
            log.warning("Dashboard save failed: %s", resp.status_code)
    except Exception as exc:
        log.warning("Dashboard save error (non-fatal): %s", exc)


def save_signal_to_dashboard(
    signal_id:      str,
    ts:             str,
    score:          int,
    direction:      str,
    text:           str,
    signals:        list,
    signal_type:    str   = "trump_post",
    url:            str   = "",
    price_at_alert: float | None = None,
    avg15m:         float | None = None,
    avg1h:          float | None = None,
    est24h:         float | None = None,
    hit_rate_1h:    float | None = None,
    # Multi-market fields
    markets:        dict | None = None,
    prices_at_alert: dict | None = None,
    post_theme:     str | None = None,
    primary_market: str | None = None,
    cross_market_consistency: str | None = None,
) -> None:
    """POST a signal event to USOIL.AI dashboard for the live signal feed.
    Silently ignores errors so it never blocks the bot."""
    if not USOIL_AI_URL:
        return
    try:
        payload = {
            "secret":         FORECAST_SECRET,
            "id":             signal_id,
            "ts":             ts,
            "score":          score,
            "direction":      direction,
            "text":           text,
            "signals":        signals,
            "type":           signal_type,
            "url":            url,
            "price_at_alert": price_at_alert,
            "avg15m":         avg15m,
            "avg1h":          avg1h,
            "est24h":         est24h,
            "hit_rate_1h":    hit_rate_1h,
            "markets":        markets,
            "prices_at_alert": prices_at_alert,
            "post_theme":     post_theme,
            "primary_market": primary_market,
            "cross_market_consistency": cross_market_consistency,
        }
        resp = requests.post(
            f"{USOIL_AI_URL}/api/signals/save",
            json=payload,
            timeout=10,
        )
        if resp.ok:
            log.info("Signal saved to dashboard: %s", resp.json().get("id"))
        else:
            log.warning("Dashboard signal save failed: %s %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("Dashboard signal save error (non-fatal): %s", exc)


import hashlib
import hmac
import time as _time

# ---------------------------------------------------------------------------
# Upstash Redis pub/sub — publishes signals to SSE stream subscribers
# ---------------------------------------------------------------------------

UPSTASH_REDIS_REST_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")


def _publish_to_redis(channel: str, payload: dict) -> None:
    """Publish a signal to Upstash Redis for SSE stream subscribers. Non-blocking, best-effort."""
    if not UPSTASH_REDIS_REST_URL:
        return
    try:
        requests.post(
            f"{UPSTASH_REDIS_REST_URL}/publish/{channel}",
            headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
            json=payload,
            timeout=3,
        )
    except Exception as exc:
        log.warning("Redis publish failed on %s: %s", channel, exc)


async def publish_signal_to_stream(
    signal_id: str,
    ts: str,
    market: str,
    direction: str,
    score: int,
    confidence: str,
    theme: str,
    price_at_signal: float | None,
    text_preview: str,
) -> None:
    """Publish signal to Upstash Redis pub/sub for SSE stream consumers."""
    if not UPSTASH_REDIS_REST_URL:
        return

    expectations = THEME_EXPECTATIONS.get(theme, {})
    event = {
        "signal_id":      signal_id,
        "ts":             ts,
        "market":         market,
        "data": {
            "score":       score,
            "direction":   direction,
            "confidence":  confidence,
            "est_move_pct": expectations.get("est_move_pct"),
            "rationale":   "",
        },
        "price":           price_at_signal,
        "post_theme":      theme,
        "ambiguity_flag":  False,
        "text_preview":    text_preview[:200],
        "hold_window":     expectations.get("hold_window"),
        "hit_rate":        expectations.get("hit_rate"),
    }

    channel = f"market-signal:{market}"
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _publish_to_redis, channel, event)


async def dispatch_signal_webhook(
    signal_id: str,
    ts: str,
    market: str,
    direction: str,
    score: int,
    confidence: str,
    theme: str,
    price_at_signal: float | None,
) -> None:
    """Fire trader webhook ASAP — runs before Telegram, never blocks on failure."""
    if not SIGNAL_WEBHOOK_URLS:
        return

    expectations = THEME_EXPECTATIONS.get(theme, {})
    payload = {
        "signal_id":      signal_id,
        "ts":             ts,
        "market":         market,
        "direction":      direction,
        "score":          score,
        "confidence":     confidence,
        "theme":          theme,
        "price_at_signal": price_at_signal,
        "est_move_pct":   expectations.get("est_move_pct"),
        "hold_window":    expectations.get("hold_window"),
        "hit_rate":       expectations.get("hit_rate"),
    }

    # HMAC signature for webhook auth
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    timestamp = str(int(_time.time()))
    sig_input = f"{timestamp}.{body}"
    signature = hmac.new(
        SIGNAL_WEBHOOK_SECRET.encode(), sig_input.encode(), hashlib.sha256
    ).hexdigest() if SIGNAL_WEBHOOK_SECRET else ""

    headers = {
        "Content-Type": "application/json",
        "X-Signal-Timestamp": timestamp,
        "X-Signal-Signature": signature,
    }

    loop = asyncio.get_event_loop()
    for url in SIGNAL_WEBHOOK_URLS:
        try:
            await loop.run_in_executor(
                None,
                lambda u=url: requests.post(u, data=body, headers=headers, timeout=5),
            )
            log.info("Signal webhook dispatched to %s", url)
        except Exception as exc:
            log.warning("Signal webhook failed for %s: %s", url, exc)


# ---------------------------------------------------------------------------
# Twitter/X posting — fire-and-forget, never blocks signal pipeline
# ---------------------------------------------------------------------------

def _get_twitter_client():
    """Return authenticated tweepy Client + API (for media upload), or (None, None)."""
    if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET]):
        return None, None
    try:
        import tweepy
        # v2 client for posting tweets
        client = tweepy.Client(
            consumer_key=TWITTER_API_KEY,
            consumer_secret=TWITTER_API_SECRET,
            access_token=TWITTER_ACCESS_TOKEN,
            access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
        )
        # v1.1 API for media upload
        auth = tweepy.OAuth1UserHandler(
            TWITTER_API_KEY, TWITTER_API_SECRET,
            TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET,
        )
        api = tweepy.API(auth)
        return client, api
    except Exception as exc:
        log.warning("Twitter client init failed: %s", exc)
        return None, None


async def tweet_signal(
    text_body: str,
    kw_score: int,
    score: int,
    direction: str,
    confidence: str,
    rationale: str,
    all_markets: dict,
    meta: dict,
    prices: dict,
) -> None:
    """Tweet a full signal alert matching Telegram format. Never blocks."""
    client, _ = _get_twitter_client()
    if not client:
        return
    try:
        conf_map = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}
        dir_arrow = "▲" if score > 0 else ("▼" if score < 0 else "—")
        conf_flag = conf_map.get(confidence, "")
        price_now = prices.get("OIL")
        price_str = f"  💵 ${price_now:.2f}" if price_now else ""
        kw_line = f"\nKeyword: {kw_score:+d}" if kw_score != score else ""
        theme = meta.get("post_theme", "")

        # Header
        lines = [
            f"⚡️ Trump posted\n",
            text_body[:280] + "\n",
            f"🛢 Oil: {score_emoji(score)} {score:+d} {dir_arrow} {conf_flag}{price_str}"
            + kw_line,
        ]
        if rationale:
            lines.append(rationale[:200])

        # Multi-market block
        if all_markets and theme and theme != "UNRELATED":
            lines.append(f"\n🌐 Multi-Market Signal — {theme}\n")

            market_order = ["OIL", "CRYPTO", "SP500", "NATGAS"]
            market_labels = {
                "OIL": "🛢 Oil", "CRYPTO": "₿ BTC",
                "SP500": "📊 S&P", "NATGAS": "🔥 NatGas",
            }
            primary = meta.get("primary_market", "OIL")

            for m in market_order:
                d = all_markets.get(m)
                if not d:
                    continue
                m_sc = d.get("score", 0)
                m_dir = d.get("direction", "NEUTRAL")
                m_conf = d.get("confidence", "LOW")
                m_move = d.get("est_move_pct")
                if m_sc == 0 and m_dir == "NEUTRAL":
                    continue

                dir_e = "📈" if m_dir == "BULLISH" else ("📉" if m_dir == "BEARISH" else "⚪")
                conf_e = conf_map.get(m_conf, "")
                label = market_labels.get(m, m)
                star = " ⭐️" if m == primary else ""
                m_price = prices.get(m)
                price_s = f"  ${m_price:.2f}" if m_price else ""
                # Ensure move sign matches direction
                if m_move and m_dir == "BEARISH" and m_move > 0:
                    m_move = -m_move
                elif m_move and m_dir == "BULLISH" and m_move < 0:
                    m_move = -m_move
                move_s = f"  ~{m_move:+.1f}%" if m_move else ""

                lines.append(f"{dir_e} {label}{star}  {m_sc:+d} {conf_e}{price_s}{move_s}")

            consist = meta.get("cross_market_consistency", "")
            consist_e = {"CONSISTENT": "🟢", "SPLIT": "🟡", "CONTRADICTORY": "🔴"}.get(consist, "")
            if consist:
                lines.append(f"\n{consist_e} {consist}")

        tweet_text = "\n".join(lines)

        # Twitter Blue/Premium allows up to 25,000 chars
        # Standard accounts limited to 280 — truncate post text if needed
        if len(tweet_text) > 25000:
            tweet_text = tweet_text[:24997] + "..."

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: client.create_tweet(text=tweet_text))
        log.info("Tweet posted: signal %+d %s", score, direction)
    except Exception as exc:
        log.warning("Tweet failed: %s", exc)


async def tweet_forecast(
    forecast: list[float],
    current_price: float,
    pred_len: int,
    chart_path: str | None = None,
) -> None:
    """Tweet a forecast update with optional chart image."""
    client, api = _get_twitter_client()
    if not client:
        return
    try:
        end_price = forecast[-1]
        change = end_price - current_price
        pct = (change / current_price) * 100
        direction = "📈" if change > 0 else "📉"

        text = (
            f"🛢 WTI {pred_len}-Day Forecast {direction}\n\n"
            f"Current: ${current_price:.2f}\n"
            f"Day {pred_len}: ${end_price:.2f} ({pct:+.1f}%)\n\n"
            f"Powered by Migas-1.5\n\n"
            f"#WTI #CrudeOil #OilForecast #AI"
        )

        media_ids = []
        if chart_path and api:
            try:
                media = api.media_upload(filename=chart_path)
                media_ids = [media.media_id]
            except Exception as exc:
                log.warning("Twitter media upload failed: %s", exc)

        loop = asyncio.get_event_loop()
        if media_ids:
            await loop.run_in_executor(
                None, lambda: client.create_tweet(text=text, media_ids=media_ids)
            )
        else:
            await loop.run_in_executor(None, lambda: client.create_tweet(text=text))
        log.info("Tweet posted: forecast %d-day", pred_len)
    except Exception as exc:
        log.warning("Tweet forecast failed: %s", exc)


async def tweet_volume_spike(
    ratio: float,
    volume: float,
    price: float,
    direction: str = "NEUTRAL",
    insider_flag: bool = False,
) -> None:
    """Tweet a volume spike alert."""
    client, _ = _get_twitter_client()
    if not client:
        return
    try:
        insider = "🚨 No recent Trump post — possible insider flow" if insider_flag else ""
        dir_str = f"Direction: {direction}" if direction != "NEUTRAL" else "Direction pending..."

        text = (
            f"⚡ Volume Spike Detected — CL=F\n\n"
            f"📊 {ratio:.1f}x baseline\n"
            f"💰 {volume:,.0f} contracts\n"
            f"🛢 WTI: ${price:.2f}\n"
            f"{dir_str}\n"
            + (f"{insider}\n" if insider else "")
            + f"\n#WTI #CrudeOil #VolumeSpike #OilTrading"
        )

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: client.create_tweet(text=text))
        log.info("Tweet posted: volume spike %.1fx", ratio)
    except Exception as exc:
        log.warning("Tweet volume spike failed: %s", exc)


def build_summary(current_price: float, instrument: str = "wti") -> str:
    """Build a basic summary for Migas. Will be replaced by live news aggregation later."""
    label = "WTI" if instrument == "wti" else "Brent"
    sensitivity = (
        "sensitive to US domestic production and SPR policy"
        if instrument == "wti"
        else "highly sensitive to OPEC+ decisions, Iran supply risk, and Strait of Hormuz disruptions"
    )
    return (
        f"FACTUAL SUMMARY:\n"
        f"{label} crude oil is currently trading at ${current_price:.2f}/barrel. "
        f"This benchmark is {sensitivity}. "
        f"Markets are in a geopolitically-driven regime following the Middle East conflict. "
        f"OPEC+ production policy, Iran sanctions, and Strait of Hormuz risk are primary drivers.\n\n"
        f"PREDICTIVE SIGNALS:\n"
        f"Ongoing conflict in the Middle East maintains a geopolitical risk premium. "
        f"Any Strait of Hormuz disruption would be strongly bullish. "
        f"Ceasefire or de-escalation signals would be bearish. "
        f"Monitor Trump energy policy statements and OPEC+ emergency meetings."
    )


def generate_forecast_chart(
    price_data: list[dict],
    forecast: list[float],
    current_price: float,
    instrument: str = "wti",
) -> str | None:
    """Generate a forecast chart image and return the file path, or None on failure."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime, timedelta
        import tempfile

        label = "WTI" if instrument == "wti" else "Brent"

        # Historical prices (last 30 days for readability)
        hist_dates = [datetime.strptime(d["t"], "%Y-%m-%d") for d in price_data[-30:]]
        hist_prices = [d["y_t"] for d in price_data[-30:]]

        # Forecast dates
        last_date = hist_dates[-1] if hist_dates else datetime.now()
        fc_dates = [last_date + timedelta(days=i) for i in range(1, len(forecast) + 1)]
        fc_prices = list(forecast)

        # Connect history to forecast
        fc_dates_full = [last_date] + fc_dates
        fc_prices_full = [current_price] + fc_prices

        fig, ax = plt.subplots(figsize=(10, 5))
        fig.patch.set_facecolor("#1a1a2e")
        ax.set_facecolor("#1a1a2e")

        # Historical line
        ax.plot(hist_dates, hist_prices, color="#ffffff", linewidth=1.5, label="Historical")

        # Forecast line
        end_price = forecast[-1]
        fc_color = "#00d4aa" if end_price >= current_price else "#ff6b6b"
        ax.plot(fc_dates_full, fc_prices_full, color=fc_color, linewidth=2, linestyle="--", label="Forecast")
        ax.fill_between(fc_dates_full, fc_prices_full, current_price, alpha=0.15, color=fc_color)

        # Current price line
        ax.axhline(y=current_price, color="#555577", linewidth=0.8, linestyle=":")

        # Labels
        change_pct = (end_price - current_price) / current_price * 100
        direction = "▲" if end_price > current_price else "▼"
        ax.set_title(
            f"{label} {len(forecast)}-Day Forecast  |  ${current_price:.2f} → ${end_price:.2f} ({change_pct:+.1f}%) {direction}",
            color="white", fontsize=13, fontweight="bold", pad=15,
        )
        ax.set_ylabel("Price ($)", color="#aaaacc", fontsize=10)
        ax.tick_params(colors="#aaaacc", labelsize=9)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.xticks(rotation=30)

        for spine in ax.spines.values():
            spine.set_color("#333355")

        ax.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", edgecolor="#333355", labelcolor="white")
        ax.grid(True, alpha=0.15, color="#555577")

        # Watermark
        fig.text(0.98, 0.02, "USOIL.AI · Migas-1.5", ha="right", fontsize=8, color="#555577")

        plt.tight_layout()
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False, prefix="forecast_")
        fig.savefig(tmp.name, dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("Forecast chart saved: %s", tmp.name)
        return tmp.name
    except Exception as exc:
        log.warning("Forecast chart generation failed: %s", exc)
        return None


def format_forecast(forecast: list[float], current_price: float, instrument: str = "wti") -> str:
    """Format forecast list into a readable Telegram message."""
    label     = "WTI" if instrument == "wti" else "Brent"
    direction = "📈" if forecast[-1] > current_price else "📉"
    change    = forecast[-1] - current_price
    pct       = (change / current_price) * 100
    pred_len  = len(forecast)

    lines = [
        f"🛢️ *{label} {pred_len}-Day Forecast* {direction}",
        f"",
        f"Current:     `${current_price:.2f}`",
        f"Day {pred_len}:      `${forecast[-1]:.2f}` ({pct:+.1f}%)",
        f"",
        f"{'Day':<5} {'Price':>8}",
        f"{'---':<5} {'-----':>8}",
    ]
    for i, price in enumerate(forecast, 1):
        arrow = "↑" if price > current_price else "↓"
        lines.append(f"`{i:<4}  ${price:>7.2f}  {arrow}`")

    lines += ["", "_Powered by Migas-1.5 · 60-day post-war window_"]
    return "\n".join(lines)


def format_scenarios(output: dict, current_price: float) -> str:
    """Format bull / base / bear / chronos end-of-horizon targets."""
    base    = output.get("forecast", [])
    bull    = output.get("forecast_bullish")
    bear    = output.get("forecast_bearish")
    chronos = output.get("chronos_baseline")

    if not base:
        return ""

    pred_len = len(base)
    lines = ["", f"📊 *Scenarios — Day {pred_len} target*"]

    if bear:
        ep = bear[-1]; pp = (ep - current_price) / current_price * 100
        lines.append(f"🐻 Bear:    `${ep:.2f}` ({pp:+.1f}%)")

    ep = base[-1]; pp = (ep - current_price) / current_price * 100
    lines.append(f"🎯 Base:    `${ep:.2f}` ({pp:+.1f}%)")

    if bull:
        ep = bull[-1]; pp = (ep - current_price) / current_price * 100
        lines.append(f"🐂 Bull:    `${ep:.2f}` ({pp:+.1f}%)")

    if chronos:
        ep = chronos[-1]; pp = (ep - current_price) / current_price * 100
        lines.append(f"⚙️ Chronos: `${ep:.2f}` ({pp:+.1f}%)")

    if bull and bear:
        spread = abs(bull[-1] - bear[-1])
        lines.append(f"_Spread: ${spread:.2f}_")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = _resolve_username(user)

    # --- Points / referral registration (idempotent) -----------------------
    try:
        existing = _redis_cmd("HGET", _user_key(username), "tg_id")
        is_new = existing is None or existing.get("result") is None

        if is_new:
            ref_code = _generate_referral_code(username)
            now_iso = datetime.now(timezone.utc).isoformat()
            _redis_cmd(
                "HSET", _user_key(username),
                "tg_id", str(user.id),
                "display_name", user.first_name or username,
                "referral_code", ref_code,
                "points", "0",
                "created_at", now_iso,
            )
            # Reverse lookup: code -> username
            _redis_cmd("HSET", f"points:ref:{ref_code}", "username", username)

            # Auto-award Telegram join points
            _award_points(username, POINTS_JOIN_TELEGRAM, "join_telegram")

            # Handle referral deep link: /start REF_XXXX
            if context.args and context.args[0].startswith("REF_"):
                ref_payload = context.args[0][4:]  # strip "REF_"
                referrer_lookup = _redis_cmd("HGET", f"points:ref:{ref_payload}", "username")
                if referrer_lookup and referrer_lookup.get("result"):
                    referrer = referrer_lookup["result"]
                    if referrer != username:  # can't refer yourself
                        _award_points(referrer, POINTS_REFERRAL_SIGNUP, "referral_signup", {"referred": username})
                        # Track referral count
                        _redis_cmd("HINCRBY", _user_key(referrer), "referral_count", 1)
                        log.info("Referral: %s referred %s (+%d pts)", referrer, username, POINTS_REFERRAL_SIGNUP)

            # Initialize on leaderboard
            user_data = _redis_cmd("HGET", _user_key(username), "points")
            pts = int(user_data["result"]) if user_data and user_data.get("result") else 0
            _redis_cmd("ZADD", "points:leaderboard", pts, username)

            log.info("New points user registered: %s (code=%s)", username, ref_code)
    except Exception as exc:
        log.error("Points registration error for %s: %s", username, exc)

    # --- Original welcome message ------------------------------------------
    await update.message.reply_text(
        "🛢️ *Migas Oil Bot*\n\n"
        "Commands:\n"
        "/signal — trader signal (15min/1hr/24hr) from post analogues\n"
        "/signals — list last 10 saved signals\n"
        "/stats — signal accuracy report\n"
        "/forecast — 16-day WTI forecast (Migas-1.5)\n"
        "/brent — 16-day Brent forecast (Migas-1.5)\n"
        "/alert 85.00 — alert when WTI hits $85\n"
        "/alerts — list active alerts\n"
        "/cancelalert — cancel all alerts\n"
        "/referral — your referral link & points\n"
        "/claim — claim points for social tasks\n"
        "/leaderboard — top 10 points\n\n"
        "_60-day post-war history window_\n\n"
        "—\n"
        "🌐 API & Subscription: [www.usoil.ai](https://www.usoil.ai)\n"
        "💬 Help / Community: @Aiyieldai\n"
        "🐦 X: [x.com/aiyieldai](https://x.com/aiyieldai)",
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )


# ---------------------------------------------------------------------------
# /referral — show referral info & link
# ---------------------------------------------------------------------------

async def cmd_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = _resolve_username(user)

    try:
        data = _redis_cmd("HGETALL", _user_key(username))
        if not data or not data.get("result"):
            await update.message.reply_text(
                "You don't have a points account yet. Send /start first!"
            )
            return

        # HGETALL returns flat list: [key, val, key, val, ...]
        raw = data["result"]
        fields = dict(zip(raw[0::2], raw[1::2])) if isinstance(raw, list) else raw

        ref_code = fields.get("referral_code", "???")
        points = fields.get("points", "0")
        referral_count = fields.get("referral_count", "0")

        # Leaderboard rank
        rank_data = _redis_cmd("ZREVRANK", "points:leaderboard", username)
        rank = (int(rank_data["result"]) + 1) if rank_data and rank_data.get("result") is not None else "?"

        ref_link = f"https://t.me/oilapibot?start=REF_{ref_code}"

        await update.message.reply_text(
            f"🏆 *Your Referral Dashboard*\n\n"
            f"📊 Points: *{points}*\n"
            f"🏅 Rank: *#{rank}*\n"
            f"👥 Referrals: *{referral_count}*\n\n"
            f"🔗 Your referral code: `{ref_code}`\n"
            f"🔗 Share link:\n`{ref_link}`\n\n"
            f"_Earn {POINTS_REFERRAL_SIGNUP} pts per referral!_",
            parse_mode="Markdown",
        )
    except Exception as exc:
        log.error("cmd_referral error for %s: %s", username, exc)
        await update.message.reply_text("Something went wrong. Try again later.")


# ---------------------------------------------------------------------------
# /claim — claim points for social tasks (x, discord)
# ---------------------------------------------------------------------------

CLAIM_TASKS = {
    "x": {
        "field": "claimed_x",
        "points": POINTS_FOLLOW_X,
        "event": "follow_x",
        "label": "Following on X",
    },
    "discord": {
        "field": "claimed_discord",
        "points": POINTS_JOIN_DISCORD,
        "event": "join_discord",
        "label": "Joining Discord",
    },
}


async def cmd_claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = _resolve_username(user)

    if not context.args:
        tasks_list = "\n".join(
            f"  `/claim {k}` — {v['label']} (+{v['points']} pts)"
            for k, v in CLAIM_TASKS.items()
        )
        await update.message.reply_text(
            f"🎯 *Claim Points*\n\n{tasks_list}",
            parse_mode="Markdown",
        )
        return

    task_key = context.args[0].lower()
    task = CLAIM_TASKS.get(task_key)
    if not task:
        await update.message.reply_text(
            f"Unknown task `{task_key}`. Options: {', '.join(CLAIM_TASKS.keys())}",
            parse_mode="Markdown",
        )
        return

    try:
        # Check if user exists
        exists = _redis_cmd("HGET", _user_key(username), "tg_id")
        if not exists or not exists.get("result"):
            await update.message.reply_text("Send /start first to create your account!")
            return

        # Check if already claimed
        already = _redis_cmd("HGET", _user_key(username), task["field"])
        if already and already.get("result"):
            await update.message.reply_text(
                f"✅ You already claimed points for *{task['label']}*.",
                parse_mode="Markdown",
            )
            return

        # Award and mark claimed
        _award_points(username, task["points"], task["event"])
        _redis_cmd("HSET", _user_key(username), task["field"], "1")

        # Get new total
        total = _redis_cmd("HGET", _user_key(username), "points")
        total_pts = total["result"] if total and total.get("result") else "?"

        await update.message.reply_text(
            f"🎉 +{task['points']} points for *{task['label']}*!\n\n"
            f"Your total: *{total_pts}* points",
            parse_mode="Markdown",
        )
    except Exception as exc:
        log.error("cmd_claim error for %s: %s", username, exc)
        await update.message.reply_text("Something went wrong. Try again later.")


# ---------------------------------------------------------------------------
# /leaderboard — top 10
# ---------------------------------------------------------------------------

async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = _redis_cmd("ZREVRANGE", "points:leaderboard", "0", "9", "WITHSCORES")
        if not data or not data.get("result"):
            await update.message.reply_text("No leaderboard data yet. Be the first to /start!")
            return

        raw = data["result"]
        # Upstash returns: [member, score, member, score, ...]
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i in range(0, len(raw), 2):
            name = raw[i]
            score = raw[i + 1]
            rank_num = i // 2
            medal = medals[rank_num] if rank_num < len(medals) else f"#{rank_num + 1}"
            # Try to get display name
            display = name
            user_data = _redis_cmd("HGET", _user_key(name), "display_name")
            if user_data and user_data.get("result"):
                display = user_data["result"]
            lines.append(f"{medal} {display} — *{int(float(score))}* pts")

        board = "\n".join(lines)
        await update.message.reply_text(
            f"🏆 *Top 10 Leaderboard*\n\n{board}",
            parse_mode="Markdown",
        )
    except Exception as exc:
        log.error("cmd_leaderboard error: %s", exc)
        await update.message.reply_text("Something went wrong. Try again later.")


async def run_forecast(instrument: str, update: Update):
    """Shared forecast logic for WTI and Brent."""
    label = "WTI" if instrument == "wti" else "Brent"
    msg = await update.message.reply_text(f"⏳ Fetching {label} prices and live news…")
    try:
        price_data, current_price = fetch_prices(instrument)
        summary, sources = build_live_summary(current_price, instrument)

        confirmed   = sources.get("confirmed", 0)
        unconfirmed = sources.get("unconfirmed", 0)
        avg_move    = sources.get("avg_move", 0.0)
        net_label   = sources.get("net_label", "")
        news_line   = (
            f"📰 {confirmed} confirmed moves (avg {avg_move:+.1f}% USO), "
            f"{unconfirmed} unconfirmed signals\n"
            f"Signal: {net_label}"
        )
        # Derive score + direction from net_label for dashboard
        net_score = sources.get("net_score", 0)
        direction = "BULLISH" if net_score > 0 else "BEARISH" if net_score < 0 else "NEUTRAL"

        await msg.edit_text(f"⏳ Running Migas-1.5 forecast + scenarios…\n{news_line}")

        output   = get_forecast(
            price_data, summary,
            n_summaries    = 5,
            counterfactual = True,   # always get bull/bear scenarios
        )
        forecast = output["forecast"]
        text     = format_forecast(forecast, current_price, instrument)
        text    += format_scenarios(output, current_price)
        text    += f"\n\n📰 {net_label}"
        await msg.edit_text(text, parse_mode="Markdown")

        # Save to USOIL.AI dashboard for vs-reality tracking
        save_forecast_to_dashboard(
            instrument    = instrument,
            current_price = current_price,
            output        = output,
            summary       = summary,
            score         = net_score,
            direction     = direction,
            pred_len      = 16,
        )
    except Exception as exc:
        log.exception("Forecast error")
        await msg.edit_text(f"❌ Error: {exc}")


@restricted
async def cmd_forecast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_forecast("wti", update)


@restricted
async def cmd_brent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_forecast("brent", update)


@restricted
async def cmd_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /alert <price>\nExample: /alert 85.00")
        return

    try:
        target = float(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid price. Example: /alert 85.00")
        return

    alerts = context.bot_data.setdefault("alerts", [])
    alerts.append({
        "user_id": update.effective_user.id,
        "chat_id": update.effective_chat.id,
        "target":  target,
    })
    await update.message.reply_text(f"✅ Alert set for *${target:.2f}*", parse_mode="Markdown")


@restricted
async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    alerts = [
        a for a in context.bot_data.get("alerts", [])
        if a["user_id"] == update.effective_user.id
    ]
    if not alerts:
        await update.message.reply_text("No active alerts.")
        return

    lines = ["📋 *Active Alerts*", ""]
    for i, a in enumerate(alerts, 1):
        lines.append(f"{i}. `${a['target']:.2f}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


@restricted
async def cmd_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show trader signal for the strongest Trump post in the last 48h.
    Shows a staleness warning if the post is older than 4 hours.
    Returns 'no recent signal' if nothing in last 48h.
    """
    msg = await update.message.reply_text("⏳ Calculating signal…")
    try:
        import pytz
        _, current_price = fetch_prices("wti")
        posts    = get_scored_posts()
        et_tz    = pytz.timezone("America/New_York")
        now_utc  = datetime.now(timezone.utc)
        cutoff   = now_utc - timedelta(hours=24)

        def post_utc(p: dict):
            try:
                d = datetime.fromisoformat(f"{p['date']}T{p.get('time_et', '00:00:00')}")
                return et_tz.localize(d).astimezone(timezone.utc)
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        recent = [p for p in posts if post_utc(p) >= cutoff]

        if not recent:
            await msg.edit_text(
                "⚪ *No recent signal*\n\n"
                "No oil-relevant Trump posts in the last 24 hours.\n"
                "_Monitoring continues — you'll be alerted when a new post is detected._",
                parse_mode="Markdown",
            )
            return

        top      = max(recent, key=lambda p: abs(p.get("score", 0)))
        text     = format_analogue_signal(top, current_price)

        # Staleness warning if post is older than 4 hours
        age_h = (now_utc - post_utc(top)).total_seconds() / 3600
        if age_h > 4:
            text = f"⚠️ _Signal is {age_h:.0f}h old — market may have already reacted_\n\n" + text

        await msg.edit_text(text, parse_mode="Markdown")
    except Exception as exc:
        log.exception("Signal command error")
        await msg.edit_text(f"❌ Error: {exc}")


@restricted
async def cmd_prices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current prices for all tracked markets with source info."""
    msg = await update.message.reply_text("⏳ Fetching prices…")
    try:
        from llm_score import MARKET_TICKERS

        lines = ["📊 *Current Prices*\n"]
        market_labels = {
            "OIL": "🛢 WTI", "CRYPTO": "₿ BTC",
            "SP500": "📊 S&P 500", "NATGAS": "🔥 NatGas",
        }
        cme_open = _is_cme_market_open()
        lines.append(f"_CME: {'🟢 Open' if cme_open else '🔴 Closed'}_\n")

        for market, ticker in MARKET_TICKERS.items():
            label = market_labels.get(market, market)

            # Try Yahoo first
            yf_price = None
            yf_stale = False
            try:
                hist = yf.Ticker(ticker).history(period="1d", interval="5m")[["Close"]]
                if not hist.empty:
                    yf_price = round(float(hist["Close"].iloc[-1]), 2)
                    age_min = (datetime.now(timezone.utc) - hist.index[-1].to_pydatetime().astimezone(timezone.utc)).total_seconds() / 60
                    yf_stale = age_min > 30
            except Exception:
                pass

            # Try Hyperliquid for oil markets
            hl_price = None
            hl_sym = _HL_SYMBOLS.get(ticker)
            if hl_sym:
                hl_price = _fetch_hyperliquid_price(hl_sym)

            # Pick the best price
            final_price = fetch_price_with_fallback(ticker)
            source = "Yahoo"
            if yf_price and not yf_stale:
                source = "Yahoo ✅"
            elif hl_price and (yf_stale or not yf_price):
                source = "Hyperliquid 🔄"
            elif yf_price and yf_stale:
                source = f"Yahoo ⚠️ stale"

            price_str = f"`${final_price:,.2f}`" if final_price else "`—`"
            hl_str = f"  HL: `${hl_price:,.2f}`" if hl_price else ""

            lines.append(f"{label}: {price_str}  _({source})_{hl_str}")

        # Hyperliquid WTI volume & OI
        hl_details = _fetch_hyperliquid_details()
        cl_detail = hl_details.get("xyz:CL") or hl_details.get("CL")
        if cl_detail:
            ntl_vlm = float(cl_detail.get("dayNtlVlm", 0))
            oi = float(cl_detail.get("openInterest", 0))
            funding = float(cl_detail.get("funding", 0))
            lines.append("")
            lines.append("📊 *Hyperliquid WTI 24h*")
            lines.append(f"Volume: `${ntl_vlm / 1e6:,.1f}M`")
            lines.append(f"Open Interest: `${oi / 1e6:,.1f}M`")
            lines.append(f"Funding: `{funding * 100:.4f}%`")

        await msg.edit_text("\n".join(lines), parse_mode="Markdown")
    except Exception as exc:
        log.exception("Prices command error")
        await msg.edit_text(f"❌ Error: {exc}")


@restricted
async def cmd_signals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the last 10 saved signals from the dashboard."""
    if not USOIL_AI_URL:
        await update.message.reply_text("❌ USOIL_AI_URL not configured.")
        return

    try:
        resp = requests.get(f"{USOIL_AI_URL.rstrip('/')}/api/signals", timeout=10)
        if not resp.ok:
            await update.message.reply_text(f"❌ API error: {resp.status_code}")
            return

        data    = resp.json()
        signals = data.get("signals", [])
        source  = data.get("source", "?")

        if not signals:
            await update.message.reply_text("⚪ No signals saved yet.")
            return

        lines = [f"📡 *Recent Signals* ({'live' if source == 'kv' else 'mock'})\n"]

        for s in signals[:10]:
            ts       = s.get("ts", "")
            score    = s.get("score", 0)
            dirn     = s.get("direction", "NEUTRAL")
            text     = s.get("text", "")[:120]
            stype    = s.get("type", "trump_post")
            avg15m   = s.get("avg15m")
            avg1h    = s.get("avg1h")
            price    = s.get("price_at_alert")

            # Age
            try:
                dt  = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - dt).total_seconds()
                if age < 3600:
                    age_str = f"{int(age/60)}m ago"
                elif age < 86400:
                    age_str = f"{int(age/3600)}h ago"
                else:
                    age_str = f"{int(age/86400)}d ago"
            except Exception:
                age_str = "?"

            # Direction arrow
            arrow = "📈" if dirn == "BULLISH" else ("📉" if dirn == "BEARISH" else "➡️")
            score_str = f"{score:+d}" if isinstance(score, int) else str(score)
            type_tag  = "⚡" if stype == "volume_spike" else "🐦"

            # Moves line
            moves = ""
            if avg15m is not None and avg1h is not None:
                moves = f"  `15m {avg15m:+.1f}% · 1h {avg1h:+.1f}%`\n"

            price_str = f" · `${price:.2f}`" if price else ""
            lines.append(
                f"{arrow} `{score_str}` {type_tag} _{age_str}{price_str}_\n"
                f"_{text}_\n"
                f"{moves}"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as exc:
        log.exception("cmd_signals error")
        await update.message.reply_text(f"❌ Error: {exc}")


@restricted
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show signal accuracy report."""
    text = format_accuracy_report()
    await update.message.reply_text(text, parse_mode="Markdown")


@restricted
async def cmd_cancelalert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    before = len(context.bot_data.get("alerts", []))
    context.bot_data["alerts"] = [
        a for a in context.bot_data.get("alerts", [])
        if a["user_id"] != uid
    ]
    removed = before - len(context.bot_data["alerts"])
    await update.message.reply_text(f"🗑️ Cancelled {removed} alert(s).")

# ---------------------------------------------------------------------------
# Alert job (runs every 5 minutes)
# ---------------------------------------------------------------------------

async def refresh_post_cache(context: ContextTypes.DEFAULT_TYPE):
    """Refresh the full 2-month Trump post cache every 6 hours."""
    log.info("Running scheduled cache refresh…")
    try:
        posts = refresh_cache()
        log.info("Cache refresh complete: %d scored posts", len(posts))
    except Exception:
        log.exception("Cache refresh failed")


def _schedule_follow_ups(job_queue, signal_id: str, price_at_alert: float) -> None:
    """Schedule 15min, 1hr, 24hr follow-up price checks for a signal."""
    async def _check(ctx, window: str):
        price = _current_wti()
        if price:
            follow_up(signal_id, window, price)

    job_queue.run_once(lambda ctx: _check(ctx, "15m"), when=900)
    job_queue.run_once(lambda ctx: _check(ctx, "1h"),  when=3600)
    job_queue.run_once(lambda ctx: _check(ctx, "24h"), when=86400)


async def trigger_auto_forecast(context: ContextTypes.DEFAULT_TYPE, post: dict):
    """Run WTI forecast(s) triggered by a high-scoring Trump post.

    For |score| >= 4: runs A/B test —
      Forecast A: standard 60-day average net label
      Forecast B: current post overrides net label (regime break)
    For |score| < 4: runs Forecast A only.
    """
    sc       = post["score"]
    signals  = ", ".join(post["signals"])
    text     = post["text"]
    emoji    = score_emoji(sc)
    strength = "strongly" if abs(sc) >= 4 else "moderately"
    dirn     = "bullish 📈" if sc > 0 else "bearish 📉"
    ab_test  = abs(sc) >= 4

    # Log signal for accuracy tracking
    price_now = _current_wti()
    if price_now:
        signal_id_a = log_signal(
            signal_type    = "auto_forecast_a",
            direction      = "LONG" if sc > 0 else "SHORT",
            score          = sc,
            price_at_alert = price_now,
            post_text      = text,
            extra          = {"signals": signals, "variant": "A_standard"},
        )
        _schedule_follow_ups(context.job_queue, signal_id_a, price_now)
        if ab_test:
            signal_id_b = log_signal(
                signal_type    = "auto_forecast_b",
                direction      = "LONG" if sc > 0 else "SHORT",
                score          = sc,
                price_at_alert = price_now,
                post_text      = text,
                extra          = {"signals": signals, "variant": "B_override"},
            )
            _schedule_follow_ups(context.job_queue, signal_id_b, price_now)

    for uid in ALLOWED_USER_IDS:
        try:
            ab_note = " *(A/B test — 2 forecasts)*" if ab_test else ""
            msg = await context.bot.send_message(
                chat_id=uid,
                text=(
                    f"⚡ *Auto-forecast triggered* {emoji} `{sc:+d}`{ab_note}\n\n"
                    f"_{text[:300]}_\n\n"
                    f"*{strength} {dirn}* — {signals}\n\n"
                    f"⏳ Running {AUTO_FORECAST_PRED_LEN}-day WTI forecast…"
                ),
                parse_mode="Markdown",
            )

            try:
                price_data, current_price = fetch_prices("wti")
                summary, sources          = build_live_summary(current_price, "wti")
                net_label                 = sources.get("net_label", "")

                # Inject LLM signal into PREDICTIVE SIGNALS section so the
                # text encoder picks it up at maximum weight — not appended
                # after the split point used by splice_summary/counterfactuals.
                llm_reason     = post.get("llm_reason", "")
                llm_direction  = post.get("direction", "NEUTRAL")
                llm_confidence = post.get("llm_confidence", "")
                llm_sigs       = ", ".join(post.get("llm_signals", []))
                llm_strength   = net_signal_text(sc)

                if llm_reason:
                    llm_block = (
                        f"CURRENT POST SIGNAL ({llm_strength}, score {sc:+d}, "
                        f"{llm_confidence} confidence): {llm_reason}"
                        + (f" Key signals: {llm_sigs}." if llm_sigs else "")
                        + "\n"
                    )
                    # Insert at start of PREDICTIVE SIGNALS section
                    summary = summary.replace(
                        "PREDICTIVE SIGNALS:\n",
                        f"PREDICTIVE SIGNALS:\n{llm_block}",
                    )
                    # Also update the NET ASSESSMENT line to match LLM direction
                    summary = summary.replace(
                        f"NET ASSESSMENT: {net_label}.",
                        f"NET ASSESSMENT: {llm_strength} (LLM-scored, overrides keyword).",
                    )

                # For extreme posts run counterfactual (bullish + bearish scenarios)
                output = get_forecast(
                    price_data, summary,
                    pred_len       = AUTO_FORECAST_PRED_LEN,
                    n_summaries    = 5,
                    counterfactual = ab_test,
                )

                forecast = output["forecast"]
                result   = (
                    f"⚡ *Auto-forecast* {emoji} `{sc:+d}`\n\n"
                    f"_{text[:200]}_\n\n"
                    f"*{strength} {dirn}* — {signals}\n\n"
                ) + format_forecast(forecast, current_price, "wti") + f"\n\n📰 {net_label}"
                await msg.edit_text(result, parse_mode="Markdown")

                # Save to USOIL.AI dashboard for vs-reality tracking
                save_forecast_to_dashboard(
                    instrument    = "wti",
                    current_price = current_price,
                    output        = output,
                    summary       = summary,
                    score         = sc,
                    direction     = "BULLISH" if sc > 0 else "BEARISH",
                    trigger_post  = text[:500],
                    signals       = list(post.get("signals", {}).keys()),
                    pred_len      = AUTO_FORECAST_PRED_LEN,
                )

                # Send counterfactual scenarios as a second message
                if ab_test and "forecast_bullish" in output:
                    fc_bull = output["forecast_bullish"]
                    fc_bear = output["forecast_bearish"]
                    end_bull = fc_bull[-1]
                    end_bear = fc_bear[-1]
                    pct_bull = (end_bull - current_price) / current_price * 100
                    pct_bear = (end_bear - current_price) / current_price * 100
                    cf_text  = (
                        f"🔀 *Counterfactual Scenarios* (same price data, different narrative)\n\n"
                        f"🟢 *Escalation scenario:* Day {AUTO_FORECAST_PRED_LEN} → "
                        f"`${end_bull:.2f}` ({pct_bull:+.1f}%)\n"
                        f"🔴 *De-escalation scenario:* Day {AUTO_FORECAST_PRED_LEN} → "
                        f"`${end_bear:.2f}` ({pct_bear:+.1f}%)\n\n"
                        f"_Range: ${end_bear:.2f} – ${end_bull:.2f} · "
                        f"spread {abs(pct_bull - pct_bear):.1f}%_"
                    )
                    await context.bot.send_message(
                        chat_id=uid, text=cf_text, parse_mode="Markdown"
                    )

            except Exception as exc:
                log.exception("Auto-forecast RunPod call failed")
                await msg.edit_text(
                    f"⚡ *{strength} {dirn} signal* {emoji} `{sc:+d}`\n\n"
                    f"_{text[:300]}_\n\n_{signals}_\n\n❌ Forecast failed: {exc}",
                    parse_mode="Markdown",
                )
        except Exception:
            log.exception("Failed to send auto-forecast to %s", uid)


async def check_trump_posts(context: ContextTypes.DEFAULT_TYPE):
    """Check cache for any unalerted oil-relevant posts (no Apify call — webhook handles ingestion)."""
    # We no longer call Apify here — that caused 60 runs/hour and rate-limiting.
    # New posts arrive via the Apify webhook → process_webhook_posts.
    # This job only exists now to catch anything that slipped through the cache.
    posts = []   # no-op until we implement cache-diff alerting
    if not posts:
        return

    # Alert about all new oil-relevant posts
    for uid in ALLOWED_USER_IDS:
        try:
            lines = ["🚨 *Trump posted about oil/energy*\n"]
            for p in posts[:3]:
                lines.append(f"{score_emoji(p['score'])} `{p['score']:+d}` _{p['text'][:300]}_")
            await context.bot.send_message(
                chat_id=uid, text="\n\n".join(lines), parse_mode="Markdown"
            )
        except Exception:
            log.exception("Failed to send Trump alert to %s", uid)

    # Save all new signals to USOIL.AI dashboard
    price_now = _current_wti()
    for p in posts:
        sc  = p.get("score", 0)
        sig_list = list(p.get("signals", {}).keys()) if isinstance(p.get("signals"), dict) else list(p.get("signals", []))
        analogue = analogue_signal(p) if abs(sc) >= 2 else {}
        save_signal_to_dashboard(
            signal_id      = f"sig_{p.get('date', '')}_{abs(hash(p.get('text','')))}",
            ts             = datetime.now(timezone.utc).isoformat(),
            score          = sc,
            direction      = "BULLISH" if sc > 0 else ("BEARISH" if sc < 0 else "NEUTRAL"),
            text           = p.get("text", ""),
            signals        = sig_list,
            signal_type    = "trump_post",
            url            = p.get("url", ""),
            price_at_alert = price_now,
            avg15m         = analogue.get("avg_15m"),
            avg1h          = analogue.get("avg_1h"),
            est24h         = analogue.get("est_24h"),
            hit_rate_1h    = analogue.get("hit_rate_1h"),
        )

    # Auto-forecast if the strongest new post clears the threshold, theme filter, and cooldown
    forecast_candidates = [p for p in posts if p.get("theme", "UNRELATED") in AUTO_FORECAST_THEMES]
    if forecast_candidates:
        top = max(forecast_candidates, key=lambda p: abs(p["score"]))
        if abs(top["score"]) >= AUTO_FORECAST_MIN_SCORE:
            last = context.bot_data.get("last_auto_forecast")
            now  = datetime.now(timezone.utc)
            if last is None or (now - last).total_seconds() > AUTO_FORECAST_COOLDOWN_MIN * 60:
                context.bot_data["last_auto_forecast"] = now
                log.info("Auto-forecast triggered by post (score %+d, theme %s): %s", top["score"], top.get("theme"), top["text"][:80])
                await trigger_auto_forecast(context, top)


async def daily_morning_briefing(context: ContextTypes.DEFAULT_TYPE):
    """Send daily 8:30am ET briefing: WTI forecast + strongest recent signal."""
    log.info("Running daily morning briefing…")
    for uid in ALLOWED_USER_IDS:
        try:
            msg = await context.bot.send_message(
                chat_id=uid,
                text="🌅 *Daily Oil Briefing — 8:30am ET*\n\n⏳ Fetching forecast and signals…",
                parse_mode="Markdown",
            )
            try:
                # --- Signal ---
                posts  = get_scored_posts()
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).date().isoformat()
                recent = [p for p in posts if p.get("date", "") >= cutoff]

                # --- Forecast ---
                price_data, current_price = fetch_prices("wti")
                summary, sources          = build_live_summary(current_price, "wti")
                output                    = get_forecast(price_data, summary, pred_len=AUTO_FORECAST_PRED_LEN, n_summaries=5)
                forecast                  = output["forecast"]
                net_label                 = sources.get("net_label", "")
                forecast_text             = format_forecast(forecast, current_price, "wti")

                # --- Combine ---
                if recent:
                    top          = max(recent, key=lambda p: abs(p.get("score", 0)))
                    signal_text  = format_analogue_signal(top, current_price)
                    full_text    = (
                        f"🌅 *Daily Oil Briefing — 8:30am ET*\n\n"
                        f"*WTI Forecast:*\n{forecast_text}\n\n"
                        f"📰 {net_label}\n\n"
                        f"─────────────────\n\n"
                        f"*Latest Signal:*\n{signal_text}"
                    )
                else:
                    full_text = (
                        f"🌅 *Daily Oil Briefing — 8:30am ET*\n\n"
                        f"*WTI Forecast:*\n{forecast_text}\n\n"
                        f"📰 {net_label}\n\n"
                        f"⚪ No oil-relevant Trump posts in last 48h."
                    )

                await msg.edit_text(full_text, parse_mode="Markdown")

            except Exception as exc:
                log.exception("Daily briefing forecast failed")
                await msg.edit_text(f"🌅 *Daily Briefing*\n\n❌ Forecast failed: {exc}", parse_mode="Markdown")

        except Exception:
            log.exception("Daily briefing failed for %s", uid)


async def rolling_forecast(context: ContextTypes.DEFAULT_TYPE):
    """Run a rolling 16-day WTI forecast every 12 hours.

    - Fetches fresh price data (actuals anchor the model)
    - Runs Migas-1.5 with bull/bear scenarios
    - Saves to dashboard as type="rolling" (originals preserved for tracking)
    - Sends update to Telegram
    """
    log.info("Rolling forecast: starting 12h update…")
    try:
        price_data, current_price = fetch_prices("wti")
        summary, sources          = build_live_summary(current_price, "wti")
        net_label                 = sources.get("net_label", "")
        net_score                 = sources.get("net_score", 0)
        direction                 = "BULLISH" if net_score > 0 else "BEARISH" if net_score < 0 else "NEUTRAL"

        output = get_forecast(
            price_data, summary,
            pred_len       = ROLLING_FORECAST_PRED_LEN,
            n_summaries    = 5,
            counterfactual = True,
        )
        forecast = output["forecast"]

        save_forecast_to_dashboard(
            instrument    = "wti",
            current_price = current_price,
            output        = output,
            summary       = summary,
            score         = net_score,
            direction     = direction,
            trigger_post  = "rolling_12h",
            signals       = ["rolling_forecast"],
            pred_len      = ROLLING_FORECAST_PRED_LEN,
        )

        forecast_text = format_forecast(forecast, current_price, "wti")
        scenarios     = format_scenarios(output, current_price)

        # Generate forecast chart
        chart_path = generate_forecast_chart(price_data, forecast, current_price, "wti")

        msg = (
            f"🔄 *Rolling Forecast Update*\n\n"
            f"{forecast_text}"
            f"{scenarios}\n\n"
            f"📰 {net_label}\n"
            f"_Next update in 12h_\n\n"
            f"—\n"
            f"🌐 API & Subscription: [www.usoil.ai](https://www.usoil.ai)\n"
            f"💬 Help / Community: @Aiyieldai\n"
            f"🐦 X: [x.com/aiyieldai](https://x.com/aiyieldai)"
        )
        for uid in ALLOWED_USER_IDS:
            try:
                if chart_path:
                    await context.bot.send_photo(
                        chat_id=uid, photo=open(chart_path, "rb"),
                        caption=msg, parse_mode="Markdown",
                    )
                else:
                    await context.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown", disable_web_page_preview=True,)
            except Exception:
                log.exception("Failed to send rolling forecast to %s", uid)

        # Tweet forecast with chart (awaited so chart file stays alive)
        await tweet_forecast(
            forecast=forecast, current_price=current_price,
            pred_len=ROLLING_FORECAST_PRED_LEN,
            chart_path=chart_path,
        )

        # Clean up chart file after tweet is done
        if chart_path:
            try:
                import os
                os.unlink(chart_path)
            except Exception:
                pass

        log.info("Rolling forecast: done — %d-day forecast from $%.2f", ROLLING_FORECAST_PRED_LEN, current_price)

    except Exception:
        log.exception("Rolling forecast failed")
        for uid in ALLOWED_USER_IDS:
            try:
                await context.bot.send_message(chat_id=uid, text="🔄 Rolling forecast failed — will retry in 12h", parse_mode="Markdown")
            except Exception:
                pass


async def _volume_direction_check(context: ContextTypes.DEFAULT_TYPE):
    """Called 5 min after a volume spike to resolve direction from price move."""
    try:
        data        = context.job.data
        entry_price = data["entry_price"]
        ratio       = data["ratio"]
        insider     = data["insider_flag"]
        signal_id   = data["signal_id"]

        now_price = _current_wti()
        if not now_price:
            log.warning("Volume direction check: could not fetch price")
            return

        move_pct  = ((now_price - entry_price) / entry_price) * 100
        direction = "BULLISH" if move_pct > 0 else "BEARISH" if move_pct < 0 else "FLAT"
        arrow     = "📈" if move_pct > 0 else "📉" if move_pct < 0 else "➡️"

        insider_label = "\n🚨 *INSIDER FLAG* — volume moved before any Trump post" if insider else ""
        msg = (
            f"⚡ *Volume Spike — Direction Resolved* {arrow}\n\n"
            f"Spike: `{ratio}x` baseline\n"
            f"Entry: `${entry_price:.2f}`\n"
            f"Now:   `${now_price:.2f}`\n"
            f"Move:  `{move_pct:+.2f}%` → *{direction}*"
            f"{insider_label}"
        )
        for uid in ALLOWED_USER_IDS:
            try:
                await context.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
            except Exception:
                log.exception("Failed to send volume direction to %s", uid)

        # Update signal log with resolved direction
        if signal_id:
            follow_up(signal_id, "5m_direction", now_price)

        # Trigger auto-forecast if direction resolved (not FLAT)
        if direction != "FLAT" and abs(move_pct) >= 0.1:
            insider_ctx = " (insider flag — no Trump post)" if insider else ""
            fake_post = {
                "score": 4 if move_pct > 0 else -4,
                "signals": [f"volume_spike_{ratio}x{insider_ctx}"],
                "text": f"Volume spike {ratio}x baseline, price moved {move_pct:+.2f}% in 5 min.{insider_ctx}",
                "direction": direction,
            }
            last = context.bot_data.get("last_auto_forecast")
            now = datetime.now(timezone.utc)
            if last is None or (now - last).total_seconds() > AUTO_FORECAST_COOLDOWN_MIN * 60:
                context.bot_data["last_auto_forecast"] = now
                log.info("Auto-forecast triggered by volume spike (%sx, %+.2f%%)", ratio, move_pct)
                await trigger_auto_forecast(context, fake_post)

    except Exception:
        log.exception("Volume direction check failed")


# ---------------------------------------------------------------------------
# Hyperliquid volume tracking — 1-min polling for off-hours volume spikes
# ---------------------------------------------------------------------------
_hl_last_ntl_vlm: float | None = None          # last dayNtlVlm reading
_hl_last_vlm_ts:  datetime | None = None        # timestamp of last reading
HL_VOLUME_SPIKE_THRESHOLD_M = 5.0               # alert if 1-min notional delta > $5M


async def check_volume_hyperliquid(context: ContextTypes.DEFAULT_TYPE):
    """Poll Hyperliquid WTI volume every 60s. Alert on spikes when CME is closed."""
    global _hl_last_ntl_vlm, _hl_last_vlm_ts

    # Only run when CME is closed — during CME hours, check_volume_spike handles it
    if _is_cme_market_open():
        return

    try:
        details = _fetch_hyperliquid_details()
        cl = details.get("xyz:CL") or details.get("CL")
        if not cl:
            return

        current_ntl = float(cl.get("dayNtlVlm", 0))
        current_px  = float(cl.get("oraclePx", 0) or cl.get("markPx", 0))
        now = datetime.now(timezone.utc)

        if _hl_last_ntl_vlm is None or _hl_last_vlm_ts is None:
            # First reading — just store baseline
            _hl_last_ntl_vlm = current_ntl
            _hl_last_vlm_ts  = now
            return

        # Calculate delta since last reading
        elapsed_sec = (now - _hl_last_vlm_ts).total_seconds()
        if elapsed_sec < 30:
            return  # too soon

        delta_ntl = current_ntl - _hl_last_ntl_vlm
        delta_m   = delta_ntl / 1e6

        # Update baseline
        _hl_last_ntl_vlm = current_ntl
        _hl_last_vlm_ts  = now

        # Day reset — volume went down, means new day
        if delta_ntl < 0:
            return

        if delta_m < HL_VOLUME_SPIKE_THRESHOLD_M:
            return

        # Check cooldown (shared with CME volume spike)
        last_alert = context.bot_data.get("last_volume_alert")
        if last_alert and (now - last_alert).total_seconds() < VOLUME_COOLDOWN_MIN * 60:
            return

        context.bot_data["last_volume_alert"] = now
        post_age_min = _last_trump_post_age_minutes()

        insider_flag = post_age_min > 15
        if post_age_min <= 15:
            trump_ctx = f"⚠️ Trump posted *{post_age_min:.0f} min ago* — spike likely post-driven"
        elif post_age_min <= 60:
            trump_ctx = f"⏱ Last Trump post: {post_age_min:.0f} min ago — no recent post"
        else:
            trump_ctx = f"🔴 No Trump post in {post_age_min:.0f} min — *possible insider/institutional flow*"

        interval_min = elapsed_sec / 60
        signal_text = (
            f"Hyperliquid WTI volume spike: ${delta_m:.1f}M in {interval_min:.0f} min. "
            f"Price: ${current_px:.2f}. {trump_ctx.replace('*','').replace('_','')}"
        )

        signal_id = log_signal(
            signal_type    = "volume_spike",
            direction      = "NEUTRAL",
            score          = 0,
            price_at_alert = current_px,
            extra          = {"source": "hyperliquid", "delta_ntl_m": round(delta_m, 1), "post_age_min": round(post_age_min), "insider_flag": insider_flag},
        )
        _schedule_follow_ups(context.job_queue, signal_id, current_px)

        save_signal_to_dashboard(
            signal_id      = signal_id or f"vol_hl_{int(now.timestamp())}",
            ts             = now.isoformat(),
            score          = 0,
            direction      = "NEUTRAL",
            text           = signal_text,
            signals        = [],
            signal_type    = "volume_spike",
            price_at_alert = current_px,
        )

        insider_label = "\n🚨 *INSIDER FLAG* — no Trump post before spike" if insider_flag else ""
        msg = (
            f"⚡ *Volume Spike — Hyperliquid WTI* (off-hours)\n\n"
            f"Notional: `${delta_m:.1f}M` in {interval_min:.0f} min\n"
            f"Price: `${current_px:.2f}`\n\n"
            f"{trump_ctx}"
            f"{insider_label}\n\n"
            f"⏳ _Direction resolves in 5 min…_"
        )
        for uid in ALLOWED_USER_IDS:
            try:
                await context.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
            except Exception:
                log.exception("Failed to send HL volume alert to %s", uid)

        asyncio.ensure_future(tweet_volume_spike(
            ratio=delta_m, volume=delta_ntl, price=current_px,
            insider_flag=insider_flag,
        ))

        context.job_queue.run_once(
            _volume_direction_check,
            when=300,
            data={
                "entry_price": current_px,
                "ratio": round(delta_m, 1),
                "insider_flag": insider_flag,
                "signal_id": signal_id,
            },
        )

        log.info("HL volume spike: $%.1fM in %.0f min, price $%.2f, Trump post %.0f min ago",
                 delta_m, interval_min, current_px, post_age_min)

    except Exception:
        log.exception("Hyperliquid volume check failed")


async def check_volume_spike(context: ContextTypes.DEFAULT_TYPE):
    """Check CL=F 5-min volume for unusual spikes (CME hours only).

    Fires an alert if:
    1. Current 5-min volume > VOLUME_SPIKE_MULTIPLIER (1.5x) × hourly baseline
    2. Cooldown since last volume alert has passed

    Always fires regardless of Trump post — instead reports post context in message.
    Off-hours volume is monitored by check_volume_hyperliquid instead.
    """
    import pandas as pd

    # Skip when CME is closed — Hyperliquid monitor handles off-hours
    if not _is_cme_market_open():
        return

    try:
        baseline = _get_volume_baseline()
        ticker   = yf.Ticker("CL=F")
        hist5m   = ticker.history(period="1d", interval="5m")[["Close", "Volume"]]
        if hist5m.empty:
            return

        hist5m.index = pd.to_datetime(hist5m.index)
        latest       = hist5m.iloc[-1]
        current_vol  = float(latest["Volume"])
        current_px   = float(latest["Close"])
        hour         = latest.name.hour
        avg_vol      = baseline.get(hour, baseline.get(hour - 1) or 500)

        if avg_vol <= 0:
            return

        ratio = current_vol / avg_vol
        if ratio < VOLUME_SPIKE_MULTIPLIER:
            return

        # Check cooldown
        last_alert = context.bot_data.get("last_volume_alert")
        now        = datetime.now(timezone.utc)
        if last_alert and (now - last_alert).total_seconds() < VOLUME_COOLDOWN_MIN * 60:
            return

        context.bot_data["last_volume_alert"] = now
        notional_m   = (current_vol * current_px * 1000) / 1_000_000
        post_age_min = _last_trump_post_age_minutes()

        # Trump post context — 15 min = insider trading threshold
        insider_flag = post_age_min > 15
        if post_age_min <= 15:
            trump_ctx = f"⚠️ Trump posted *{post_age_min:.0f} min ago* — spike likely post-driven"
        elif post_age_min <= 60:
            trump_ctx = f"⏱ Last Trump post: {post_age_min:.0f} min ago — no recent post"
        else:
            trump_ctx = f"🔴 No Trump post in {post_age_min:.0f} min — *possible insider/institutional flow*"

        signal_text = (
            f"Volume spike {ratio:.1f}x baseline. {current_vol:,.0f} contracts "
            f"(~${notional_m:.0f}M notional). {trump_ctx.replace('*','').replace('_','')}"
        )

        signal_id = log_signal(
            signal_type    = "volume_spike",
            direction      = "NEUTRAL",
            score          = 0,
            price_at_alert = current_px,
            extra          = {"ratio": round(ratio, 2), "volume": current_vol, "notional_m": round(notional_m, 0), "post_age_min": round(post_age_min), "insider_flag": insider_flag},
        )
        _schedule_follow_ups(context.job_queue, signal_id, current_px)

        save_signal_to_dashboard(
            signal_id      = signal_id or f"vol_{int(now.timestamp())}",
            ts             = now.isoformat(),
            score          = 0,
            direction      = "NEUTRAL",
            text           = signal_text,
            signals        = [],
            signal_type    = "volume_spike",
            price_at_alert = current_px,
        )

        log.info("Volume spike detected: %.1fx baseline (%.0f vs %.0f avg), $%.0fM notional, Trump post %.0f min ago",
                 ratio, current_vol, avg_vol, notional_m, post_age_min)

        insider_label = "\n🚨 *INSIDER FLAG* — no Trump post before spike" if insider_flag else ""
        msg = (
            f"⚡ *Volume Spike — CL=F*\n\n"
            f"Volume: `{current_vol:,.0f}` contracts\n"
            f"Baseline: `{avg_vol:,.0f}` contracts\n"
            f"Spike: `{ratio:.1f}x`\n"
            f"Notional: `~${notional_m:.0f}M`\n"
            f"Price: `${current_px:.2f}`\n\n"
            f"{trump_ctx}"
            f"{insider_label}\n\n"
            f"⏳ _Direction resolves in 5 min…_"
        )
        for uid in ALLOWED_USER_IDS:
            try:
                await context.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
            except Exception:
                log.exception("Failed to send volume alert to %s", uid)

        # Tweet volume spike (async, after Telegram)
        asyncio.ensure_future(tweet_volume_spike(
            ratio=ratio, volume=current_vol, price=current_px,
            insider_flag=insider_flag,
        ))

        # Schedule 5-min direction check
        context.job_queue.run_once(
            _volume_direction_check,
            when=300,
            data={
                "entry_price": current_px,
                "ratio": round(ratio, 2),
                "insider_flag": insider_flag,
                "signal_id": signal_id,
            },
        )

    except Exception:
        log.exception("Volume spike check failed")


async def check_price_alerts(context: ContextTypes.DEFAULT_TYPE):
    alerts = context.bot_data.get("alerts", [])
    if not alerts:
        return

    try:
        ticker  = yf.Ticker("CL=F")
        current = float(ticker.history(period="1d")["Close"].iloc[-1])
    except Exception:
        log.exception("Failed to fetch price for alert check")
        return

    remaining = []
    for alert in alerts:
        diff_pct = abs(current - alert["target"]) / alert["target"]
        if diff_pct <= 0.005:   # within 0.5% of target
            try:
                await context.bot.send_message(
                    chat_id=alert["chat_id"],
                    text=(
                        f"🚨 *WTI Price Alert*\n\n"
                        f"Price is `${current:.2f}` — within 0.5% of your target `${alert['target']:.2f}`"
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                log.exception("Failed to send alert to %s", alert["chat_id"])
                remaining.append(alert)   # keep if send failed
        else:
            remaining.append(alert)

    context.bot_data["alerts"] = remaining

# ---------------------------------------------------------------------------
# SSE stream endpoint — GET /stream/{market}
# ---------------------------------------------------------------------------

VALID_STREAM_MARKETS = {"OIL", "CRYPTO", "SP500", "NATGAS"}

async def handle_sse_stream(request: web.Request) -> web.StreamResponse:
    """
    SSE endpoint for traders. Pushes signals instantly via in-memory queue.
    No auth required (public beta). CORS open.

    GET /stream/OIL?min_score=0&direction=&confidence=&post_theme=
    """
    market = request.match_info["market"].upper()
    if market not in VALID_STREAM_MARKETS:
        return web.json_response(
            {"error": f"Unknown market. Valid: {', '.join(sorted(VALID_STREAM_MARKETS))}"},
            status=400,
        )

    # Parse optional filters from query string
    min_score    = int(request.query.get("min_score", "0") or "0")
    filter_dir   = request.query.get("direction", "")
    filter_conf  = request.query.get("confidence", "")
    filter_theme = request.query.get("post_theme", "")

    # ── Connection limits ──────────────────────────────────────────────
    client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote or "unknown"
    total_clients = sum(len(s) for s in _sse_clients.values())

    if total_clients >= MAX_SSE_CLIENTS_TOTAL:
        return web.json_response({"error": "Server at capacity"}, status=503)
    if _sse_ip_count.get(client_ip, 0) >= MAX_SSE_PER_IP:
        return web.json_response({"error": "Too many connections from this IP"}, status=429)

    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type":                "text/event-stream",
            "Cache-Control":               "no-cache, no-transform",
            "Connection":                  "keep-alive",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Headers": "Cache-Control",
        },
    )
    await resp.prepare(request)

    # Send connected event
    connected_data = json.dumps({
        "market": market,
        "filters": {
            "min_score": min_score,
            "direction": filter_dir or None,
            "confidence": filter_conf or None,
            "post_theme": filter_theme or None,
        },
    })
    await resp.write(f"event: connected\ndata: {connected_data}\n\n".encode())

    # Register this client + track IP
    q: asyncio.Queue = asyncio.Queue(maxsize=50)
    if market not in _sse_clients:
        _sse_clients[market] = set()
    _sse_clients[market].add(q)
    _sse_ip_count[client_ip] = _sse_ip_count.get(client_ip, 0) + 1
    log.info("SSE client connected for %s from %s (total: %d)", market, client_ip, total_clients + 1)

    # Connection tracked in logs + /stream/stats — no Telegram notification

    try:
        while True:
            # Wait for signal or send heartbeat every 25s
            try:
                event = await asyncio.wait_for(q.get(), timeout=25.0)
            except asyncio.TimeoutError:
                await resp.write(b": heartbeat\n\n")
                continue

            # Apply filters
            score = event.get("data", {}).get("score", 0)
            if abs(score) < min_score:
                continue
            if filter_dir and event.get("data", {}).get("direction") != filter_dir:
                continue
            if filter_conf and event.get("data", {}).get("confidence") != filter_conf:
                continue
            if filter_theme and event.get("post_theme") != filter_theme:
                continue

            await resp.write(f"event: signal\ndata: {json.dumps(event)}\n\n".encode())

    except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
        pass
    finally:
        _sse_clients.get(market, set()).discard(q)
        _sse_ip_count[client_ip] = max(0, _sse_ip_count.get(client_ip, 1) - 1)
        if _sse_ip_count.get(client_ip) == 0:
            _sse_ip_count.pop(client_ip, None)
        remaining = sum(len(s) for s in _sse_clients.values())
        log.info("SSE client disconnected from %s [%s] (remaining: %d)", market, client_ip, remaining)

    return resp


# ---------------------------------------------------------------------------
# SSE stats endpoint — GET /stream/stats
# ---------------------------------------------------------------------------

async def handle_stream_stats(request: web.Request) -> web.Response:
    """GET /stream/stats — live trader connection stats (public)."""
    per_market = {m: len(clients) for m, clients in _sse_clients.items() if clients}
    total = sum(per_market.values())
    unique_ips = len(_sse_ip_count)
    return web.json_response({
        "total_connections": total,
        "unique_ips":        unique_ips,
        "per_market":        per_market,
        "limits": {
            "max_total": MAX_SSE_CLIENTS_TOTAL,
            "max_per_ip": MAX_SSE_PER_IP,
        },
    })


# ---------------------------------------------------------------------------
# Apify webhook server (aiohttp)
# ---------------------------------------------------------------------------

async def handle_signal_api(request: web.Request) -> web.Response:
    """GET /signal?secret=<WEBHOOK_SECRET>
    Returns trader signal JSON for the strongest recent Trump post.
    Designed for programmatic access — pipe into your own system.
    """
    secret = request.query.get("secret", "")
    if secret != WEBHOOK_SECRET:
        return web.Response(status=401, text="Unauthorized")

    try:
        _, current_price = fetch_prices("wti")
        posts  = get_scored_posts()
        from datetime import date as _date
        today  = _date.today().isoformat()
        recent = [p for p in posts if p.get("date", "") >= today]
        top    = max(recent or posts, key=lambda p: abs(p.get("score", 0)))
        result = analogue_signal(top)
        result["current_wti"]  = round(current_price, 2)
        result["post_text"]    = top.get("text", "")
        result["post_date"]    = top.get("date", "")
        # Remove non-serialisable keys
        result.pop("top_analogues", None)
        return web.json_response(result)
    except Exception as exc:
        log.exception("Signal API error")
        return web.Response(status=500, text=str(exc))


async def handle_apify_webhook(request: web.Request) -> web.Response:
    """Receive Apify webhook when a scheduled Truth Social scrape completes.

    Apify POSTs to /webhook/{WEBHOOK_SECRET} when the actor run succeeds.
    We fetch the run's dataset async (non-blocking), then fire a background task
    to score + alert — returns 200 immediately so Apify doesn't retry.
    """
    secret = request.match_info.get("secret", "")
    if secret != WEBHOOK_SECRET:
        log.warning("Webhook received with wrong secret")
        return web.Response(status=401)

    try:
        body     = await request.json()
        event    = body.get("eventType", "")
        run_id   = body.get("eventData", {}).get("actorRunId", "")

        if event != "ACTOR.RUN.SUCCEEDED" or not run_id:
            return web.Response(status=200)  # ignore non-success events

        log.info("Apify webhook received for run %s", run_id)

        # Fetch dataset async — do NOT use requests.get (blocks event loop)
        import aiohttp as _aiohttp
        url = f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items"
        async with _aiohttp.ClientSession() as session:
            async with session.get(url, params={"token": APIFY_API_TOKEN}, timeout=_aiohttp.ClientTimeout(total=30)) as resp:
                log.info("Dataset fetch: status=%s url=%s", resp.status, url)
                resp.raise_for_status()
                raw_posts = await resp.json()

        log.info("Dataset fetch returned %d items (first 200 chars): %s",
                 len(raw_posts), str(raw_posts)[:200])

        if not raw_posts:
            log.warning("Webhook run %s dataset is empty — nothing to process", run_id)
            return web.Response(status=200)

        log.info("Scheduling processing of %d raw posts from webhook", len(raw_posts))
        ptb_app = request.app["ptb_app"]
        asyncio.create_task(process_webhook_posts(ptb_app, raw_posts))

    except Exception:
        log.exception("Webhook handler error")

    return web.Response(status=200)


async def process_webhook_posts(ptb_app: Application, raw_posts: list) -> None:
    """Score and process posts received via webhook, fire alerts if relevant."""
    try:
        await _process_webhook_posts_inner(ptb_app, raw_posts)
    except Exception:
        log.exception("process_webhook_posts crashed — raw_posts count=%d", len(raw_posts))


async def _process_webhook_posts_inner(ptb_app: Application, raw_posts: list) -> None:
    existing_texts = {p["text"] for p in __import__('news')._read_cache()}
    append_new_posts(raw_posts)

    # Use topic gate (not keyword score) so the LLM can score posts
    # that mention Iran/oil topics but use language keywords don't cover
    new_scored = _score_raw_posts(raw_posts)
    topic_relevant = [
        p for p in raw_posts
        if p.get("text", p.get("content", ""))
        and is_oil_topic(p.get("text", p.get("content", "")))
        and p.get("text", p.get("content", "")) not in existing_texts
    ]
    # Merge: keyword-scored posts + topic-only posts (deduplicated)
    scored_texts = {p["text"] for p in new_scored}
    new_posts = [p for p in new_scored if p["text"] not in existing_texts]
    for p in topic_relevant:
        text = p.get("text", p.get("content", ""))
        if text not in scored_texts:
            # Topic-relevant but keyword score=0 — pass to LLM with score=0
            new_posts.append({
                "text": text,
                "score": 0,
                "signals": [],
                "confirmed": False,
                "uso_pct_5m": None,
                "uso_pct_1h": None,
                "date": p.get("date", ""),
                "time_eastern": p.get("time_eastern", ""),
                "url": p.get("url", ""),
            })

    log.info("Webhook: %d raw → %d keyword-scored → %d topic-relevant → %d new to process",
             len(raw_posts), len(new_scored), len(topic_relevant), len(new_posts))

    if not new_posts:
        return

    log.info("Webhook: firing alerts for %d new posts", len(new_posts))

    class _FakeContext:
        def __init__(self):
            self.bot       = ptb_app.bot
            self.bot_data  = ptb_app.bot_data
            self.job_queue = ptb_app.job_queue

    ctx = _FakeContext()

    # ── Score all posts: multi-market streaming LLM + price fetch in parallel ──
    import asyncio as _asyncio

    async def _fetch_all_prices() -> dict[str, float | None]:
        """Fetch all 8 market prices in parallel (best-effort, never raises)."""
        import yfinance as _yf

        def _fetch_one(ticker: str) -> float | None:
            return fetch_price_with_fallback(ticker)

        loop = _asyncio.get_event_loop()
        tasks = {
            market: loop.run_in_executor(None, _fetch_one, ticker)
            for market, ticker in MARKET_TICKERS.items()
        }
        results: dict[str, float | None] = {}
        for market, coro in tasks.items():
            try:
                results[market] = await coro
            except Exception:
                results[market] = None
        return results

    for p in new_posts[:3]:
        text_body = p["text"]
        kw_score  = p["score"]

        # Collect all market signals and prices in parallel
        all_markets: dict[str, dict] = {}
        meta_block: dict              = {}

        prices_task = _asyncio.ensure_future(_fetch_all_prices())

        async for market, data in llm_score_multi_market(text_body):
            if market == "_meta":
                meta_block = data
            else:
                all_markets[market] = data

        # Await prices (already running while LLM was streaming)
        try:
            prices_at_alert = await prices_task
        except Exception:
            prices_at_alert = {m: None for m in MARKET_TICKERS}

        # OIL is the primary signal for backward compat (Migas context / tracker)
        oil   = all_markets.get("OIL", {})
        sc    = oil.get("score", kw_score)
        dirn  = oil.get("direction", "NEUTRAL")
        conf  = oil.get("confidence", "LOW")
        reason = oil.get("rationale", "")
        price_now = prices_at_alert.get("OIL")

        # Fallback WTI price
        if price_now is None:
            price_now = _current_wti()
        if price_now is None:
            try:
                _, price_now = fetch_prices("wti")
            except Exception:
                pass

        # Override p["score"] so auto-forecast uses LLM score
        if oil:
            p["score"]     = sc
            p["direction"] = dirn

        # ── Trader webhook — fires FIRST for lowest latency ───────────────────
        sig_id = f"sig_{p.get('date', '')}_{abs(hash(text_body))}"
        theme  = meta_block.get("post_theme", "UNRELATED")

        sig_ts = datetime.now(timezone.utc).isoformat()

        if dirn != "NEUTRAL" and sc != 0:
            # Fire webhook + SSE pub/sub in parallel — both before Telegram
            await asyncio.gather(
                dispatch_signal_webhook(
                    signal_id      = sig_id,
                    ts             = sig_ts,
                    market         = "OIL",
                    direction      = dirn,
                    score          = sc,
                    confidence     = conf,
                    theme          = theme,
                    price_at_signal = price_now,
                ),
                publish_signal_to_stream(
                    signal_id      = sig_id,
                    ts             = sig_ts,
                    market         = "OIL",
                    direction      = dirn,
                    score          = sc,
                    confidence     = conf,
                    theme          = theme,
                    price_at_signal = price_now,
                    text_preview   = text_body,
                ),
            )

        # ── Skip unrelated posts — don't spam Telegram ──────────────────────
        llm_theme = meta_block.get("post_theme", "UNRELATED")
        if sc == 0 and llm_theme == "UNRELATED":
            log.info("Skipping UNRELATED post (score=0, theme=UNRELATED): %.80s", text_body)
            continue

        # ── Send alert ────────────────────────────────────────────────────────
        if not all_markets and kw_score != 0:
            # LLM returned nothing — fallback to keyword-only alert
            log.warning("LLM returned empty for post (kw_score=%d), using keyword fallback", kw_score)
            all_markets = {"OIL": {"score": kw_score, "direction": "BULLISH" if kw_score > 0 else "BEARISH", "confidence": "MEDIUM", "rationale": ""}}
            sc   = kw_score
            dirn = "BULLISH" if kw_score > 0 else "BEARISH"
            conf = "MEDIUM"

        if all_markets:
            # Multi-market alert
            primary = meta_block.get("primary_market", "OIL")
            consist = meta_block.get("cross_market_consistency", "")
            ambig   = meta_block.get("ambiguity_flag", False)
            theme   = meta_block.get("post_theme", "")

            # Header: post + OIL signal (fastest to read)
            dir_arrow = "▲" if sc > 0 else ("▼" if sc < 0 else "—")
            conf_flag = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(conf, "")
            kw_line   = f"\n_Keyword: `{kw_score:+d}`_" if kw_score != sc else ""
            price_line = f"  💵 `${price_now:.2f}`" if price_now else ""

            header = (
                f"⚡ *Trump posted*\n\n"
                f"_{text_body[:300]}_\n\n"
                f"🛢 Oil: {score_emoji(sc)} `{sc:+d} {dir_arrow}` {conf_flag}{price_line}"
                f"{kw_line}"
                + (f"\n_{reason}_" if reason else "")
            )

            multi_body = format_multi_market_alert(all_markets, meta_block, prices_at_alert)
            alert_text = header + "\n\n" + multi_body
        else:
            # Fallback: oil-only (same as before)
            dir_arrow = "▲" if sc > 0 else ("▼" if sc < 0 else "—")
            dir_label = "BULL" if sc > 0 else ("BEAR" if sc < 0 else "FLAT")
            conf_flag = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(conf, "")
            kw_line   = f"\n_Keyword score: `{kw_score:+d}`_" if kw_score != sc else ""
            price_line = f"\n💵 WTI: `${price_now:.2f}`" if price_now else ""
            alert_text = (
                f"⚡ *Trump posted*\n\n"
                f"_{text_body[:400]}_\n\n"
                f"{score_emoji(sc)} *{sc:+d} {dir_arrow} {dir_label}* {conf_flag}"
                f"{kw_line}"
                f"{price_line}"
                + (f"\n\n_{reason}_" if reason else "")
            )

        # ── Push to SSE stream FIRST — traders before Telegram ────────────
        sig_ts = datetime.now(timezone.utc).isoformat()
        if all_markets:
            sse_push_signal(
                all_markets     = all_markets,
                meta            = meta_block,
                prices          = prices_at_alert,
                signal_id       = sig_id,
                text            = text_body,
                ts              = sig_ts,
            )

        alert_text += (
            "\n\n—\n"
            "🛢 OIL trader API access: [www.usoil.ai](https://www.usoil.ai)"
        )

        for uid in ALLOWED_USER_IDS:
            try:
                await ptb_app.bot.send_message(
                    chat_id=uid, text=alert_text, parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
            except Exception:
                log.exception("Webhook alert send failed for uid %s", uid)

        # ── Tweet signal (async, after Telegram — never blocks) ──────────────
        if sc != 0:
            asyncio.ensure_future(tweet_signal(
                text_body=text_body,
                kw_score=kw_score,
                score=sc, direction=dirn, confidence=conf,
                rationale=reason,
                all_markets=all_markets,
                meta=meta_block,
                prices=prices_at_alert,
            ))

        # ── Save to dashboard (oil signal + multi-market markets block) ───────
        sig_list = (list(p.get("signals", {}).keys()) if isinstance(p.get("signals"), dict) else list(p.get("signals", [])))
        analogue = analogue_signal(p) if abs(sc) >= 2 else {}
        save_signal_to_dashboard(
            signal_id      = sig_id,
            ts             = datetime.now(timezone.utc).isoformat(),
            score          = sc,
            direction      = dirn,
            text           = text_body,
            signals        = sig_list,
            signal_type    = "trump_post",
            url            = p.get("url", ""),
            price_at_alert = price_now,
            avg15m         = analogue.get("avg_15m"),
            avg1h          = analogue.get("avg_1h"),
            est24h         = analogue.get("est_24h"),
            hit_rate_1h    = analogue.get("hit_rate_1h"),
            markets        = all_markets or None,
            prices_at_alert= {k: v for k, v in prices_at_alert.items() if v is not None} or None,
            post_theme     = meta_block.get("post_theme"),
            primary_market = meta_block.get("primary_market"),
            cross_market_consistency = meta_block.get("cross_market_consistency"),
        )

        if price_now and sc != 0:
            local_sig_id = log_signal(
                signal_type    = "trump_post",
                direction      = "LONG" if sc > 0 else "SHORT",
                score          = sc,
                price_at_alert = price_now,
                post_text      = text_body,
                extra          = {"signals": sig_list, "dashboard_id": sig_id, "markets": list(all_markets.keys())},
            )
            _schedule_follow_ups(ctx.job_queue, local_sig_id, price_now)

    # Auto-forecast — use highest-scoring post from approved themes only
    forecast_candidates = [p for p in new_posts if p.get("theme", "UNRELATED") in AUTO_FORECAST_THEMES]
    if forecast_candidates:
        top = max(forecast_candidates, key=lambda p: abs(p["score"]))
        if abs(top["score"]) >= AUTO_FORECAST_MIN_SCORE:
            last = ptb_app.bot_data.get("last_auto_forecast")
            now  = datetime.now(timezone.utc)
            if last is None or (now - last).total_seconds() > AUTO_FORECAST_COOLDOWN_MIN * 60:
                ptb_app.bot_data["last_auto_forecast"] = now
                await trigger_auto_forecast(ctx, top)


# ---------------------------------------------------------------------------
# Main — runs PTB + aiohttp webhook server in the same event loop
# ---------------------------------------------------------------------------

async def main_async():
    # Build PTB app
    ptb_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    ptb_app.add_handler(CommandHandler("start",        cmd_start))
    ptb_app.add_handler(CommandHandler("forecast",     cmd_forecast))
    ptb_app.add_handler(CommandHandler("brent",        cmd_brent))
    ptb_app.add_handler(CommandHandler("prices",       cmd_prices))
    ptb_app.add_handler(CommandHandler("signals",      cmd_signals))
    ptb_app.add_handler(CommandHandler("stats",        cmd_stats))
    ptb_app.add_handler(CommandHandler("alert",        cmd_alert))
    ptb_app.add_handler(CommandHandler("alerts",       cmd_alerts))
    ptb_app.add_handler(CommandHandler("cancelalert",  cmd_cancelalert))
    ptb_app.add_handler(CommandHandler("referral",     cmd_referral))
    ptb_app.add_handler(CommandHandler("claim",        cmd_claim))
    ptb_app.add_handler(CommandHandler("leaderboard",  cmd_leaderboard))

    ptb_app.job_queue.run_repeating(check_price_alerts,  interval=300,    first=15)
    ptb_app.job_queue.run_repeating(check_volume_spike,        interval=300,  first=30)
    ptb_app.job_queue.run_repeating(check_volume_hyperliquid, interval=60,   first=15)
    ptb_app.job_queue.run_repeating(refresh_post_cache,  interval=6*3600, first=10)
    ptb_app.job_queue.run_repeating(check_trump_posts,   interval=600,    first=60)
    ptb_app.job_queue.run_repeating(rolling_forecast,    interval=ROLLING_FORECAST_INTERVAL_H * 3600, first=120)

    # Daily morning briefing — 8:30am ET = 12:30 UTC
    import pytz
    et = pytz.timezone("America/New_York")
    ptb_app.job_queue.run_daily(daily_morning_briefing, time=datetime.now(et).replace(
        hour=8, minute=30, second=0, microsecond=0
    ).timetz())

    # Build aiohttp webhook server
    async def handle_test_post(request: web.Request) -> web.Response:
        """POST /test_post/{secret} — inject a fake Trump post for testing."""
        secret = request.match_info.get("secret", "")
        if secret != WEBHOOK_SECRET:
            return web.Response(status=401)
        try:
            body = await request.json()
            posts = body.get("posts", [body] if "text" in body else [])
            if not posts:
                return web.Response(text="No posts", status=400)
            ptb_app = request.app["ptb_app"]
            asyncio.create_task(process_webhook_posts(ptb_app, posts))
            return web.json_response({"ok": True, "count": len(posts)})
        except Exception as exc:
            log.exception("test_post handler error")
            return web.Response(text=str(exc), status=500)

    aio_app = web.Application()
    aio_app["ptb_app"] = ptb_app   # webhook handler needs this to fire tasks
    aio_app.router.add_post("/webhook/{secret}", handle_apify_webhook)
    aio_app.router.add_post("/test_post/{secret}", handle_test_post)
    aio_app.router.add_get("/signal",            handle_signal_api)
    aio_app.router.add_get("/stream/stats",       handle_stream_stats)
    aio_app.router.add_get("/stream/{market}",   handle_sse_stream)

    runner = web.AppRunner(aio_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", WEBHOOK_PORT).start()
    log.info("Webhook server listening on port %d", WEBHOOK_PORT)

    # Start PTB
    await ptb_app.initialize()
    await ptb_app.start()
    await ptb_app.updater.start_polling(drop_pending_updates=True)
    log.info("Migas Oil Bot started")

    # Start Truth Social Playwright poller (primary ingest — 3-5s latency, $0 cost)
    # Replaces Apify ($900/mo) and broken WebSocket stream
    from truthsocial_poller import run_truthsocial_poller
    ts_poller_task = asyncio.create_task(
        run_truthsocial_poller(ptb_app),
        name="truthsocial_poller",
    )
    log.info("Truth Social poller started")

    # Start Hormuz vessel congestion monitor (aisstream.io WebSocket)
    hormuz_task = None
    try:
        from hormuz_monitor import run_hormuz_monitor
        hormuz_task = asyncio.create_task(
            run_hormuz_monitor(ptb_app),
            name="hormuz_monitor",
        )
        log.info("Hormuz vessel monitor started")
    except ImportError:
        log.warning("hormuz_monitor not available — skipping")

    # Keep running until interrupted — webhook tasks fire via asyncio.create_task()
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("Shutting down…")
        ts_poller_task.cancel()
        if hormuz_task:
            hormuz_task.cancel()
        try:
            await ts_poller_task
        except asyncio.CancelledError:
            pass
        if hormuz_task:
            try:
                await hormuz_task
            except asyncio.CancelledError:
                pass
        await ptb_app.updater.stop()
        await ptb_app.stop()
        await ptb_app.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main_async())
