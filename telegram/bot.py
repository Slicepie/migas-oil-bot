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
import logging
import os
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
    analogue_signal, format_analogue_signal,
)
from tracker import log_signal, follow_up, format_accuracy_report, _current_wti

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

WEBHOOK_SECRET  = os.environ.get("WEBHOOK_SECRET", "changeme")
WEBHOOK_PORT    = int(os.environ.get("WEBHOOK_PORT", "8080"))
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

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
# Volume spike detection config
# ---------------------------------------------------------------------------
VOLUME_SPIKE_MULTIPLIER  = 3.0    # alert if current 5-min vol > 3x hourly baseline
VOLUME_BASELINE_DAYS     = 14     # days of hourly history to build baseline
VOLUME_POST_LOCKOUT_MIN  = 60     # ignore spikes if Trump posted within this many minutes
VOLUME_COOLDOWN_MIN      = 30     # minimum minutes between volume spike alerts

# Hourly baseline cache — rebuilt every 24h
_volume_baseline: dict[int, float] = {}   # {hour_of_day: avg_5min_volume}
_volume_baseline_built: datetime | None = None


def _build_volume_baseline() -> dict[int, float]:
    """Build average 5-min CL=F volume by hour-of-day from last 14 days."""
    import pandas as pd
    ticker = yf.Ticker("CL=F")
    hist   = ticker.history(period=f"{VOLUME_BASELINE_DAYS}d", interval="1h")[["Volume"]]
    hist.index = pd.to_datetime(hist.index)
    hist["hour"] = hist.index.hour
    baseline = hist.groupby("hour")["Volume"].mean().to_dict()
    log.info("Volume baseline built: %d hours, avg=%.0f", len(baseline), sum(baseline.values()) / max(len(baseline), 1))
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
    """Fetch oil prices via yfinance.

    Args:
        instrument: "wti" or "brent"
        days: history window in days (default 60 — post-war regime)

    Returns:
        price_data: list of {t, y_t} dicts sorted by date
        current_price: latest closing price
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

@restricted
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        "/cancelalert — cancel all alerts\n\n"
        "_60-day post-war history window_",
        parse_mode="Markdown",
    )


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
    """Poll Truth Social for new posts (incremental) and alert if oil-relevant."""
    posts = get_relevant_trump_posts()   # list[dict] sorted by |score|
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
            avg15m         = analogue.get("avg_15m_move"),
            avg1h          = analogue.get("avg_1h_move"),
            est24h         = analogue.get("est_24h_move"),
            hit_rate_1h    = analogue.get("hit_rate_1h"),
        )

    # Auto-forecast if the strongest new post clears the threshold and cooldown has passed
    top = max(posts, key=lambda p: abs(p["score"]))
    if abs(top["score"]) >= AUTO_FORECAST_MIN_SCORE:
        last = context.bot_data.get("last_auto_forecast")
        now  = datetime.now(timezone.utc)
        if last is None or (now - last).total_seconds() > AUTO_FORECAST_COOLDOWN_MIN * 60:
            context.bot_data["last_auto_forecast"] = now
            log.info("Auto-forecast triggered by post (score %+d): %s", top["score"], top["text"][:80])
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


async def check_volume_spike(context: ContextTypes.DEFAULT_TYPE):
    """Check CL=F 5-min volume for unusual spikes with no recent Trump post.

    Fires an alert if:
    1. Current 5-min volume > VOLUME_SPIKE_MULTIPLIER × hourly baseline
    2. No Trump post in the last VOLUME_POST_LOCKOUT_MIN minutes
    3. Cooldown since last volume alert has passed
    """
    import pandas as pd

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

        # Check Trump post lockout — ignore spike if post came recently
        post_age_min = _last_trump_post_age_minutes()
        if post_age_min < VOLUME_POST_LOCKOUT_MIN:
            log.info("Volume spike %.1fx but Trump posted %.0f min ago — suppressed", ratio, post_age_min)
            return

        context.bot_data["last_volume_alert"] = now
        notional_m = (current_vol * current_px * 1000) / 1_000_000

        # Log for accuracy tracking — direction unknown, mark LONG as default
        # (volume spikes before Iran posts have been bullish historically)
        signal_id = log_signal(
            signal_type    = "volume_spike",
            direction      = "LONG",
            score          = 0,
            price_at_alert = current_px,
            extra          = {"ratio": round(ratio, 2), "volume": current_vol, "notional_m": round(notional_m, 0)},
        )
        _schedule_follow_ups(context.job_queue, signal_id, current_px)

        save_signal_to_dashboard(
            signal_id      = signal_id or f"vol_{int(now.timestamp())}",
            ts             = now.isoformat(),
            score          = 0,
            direction      = "NEUTRAL",
            text           = f"Volume spike detected — {ratio:.1f}x baseline. {current_vol:,.0f} contracts (~${notional_m:.0f}M notional). No Trump post in last {VOLUME_POST_LOCKOUT_MIN} min.",
            signals        = [],
            signal_type    = "volume_spike",
            price_at_alert = current_px,
        )

        log.info("Volume spike detected: %.1fx baseline (%.0f vs %.0f avg), $%.0fM notional",
                 ratio, current_vol, avg_vol, notional_m)

        msg = (
            f"⚠️ *Unusual Oil Volume Spike*\n\n"
            f"CL=F 5-min volume: `{current_vol:,.0f}` contracts\n"
            f"Baseline (hour avg): `{avg_vol:,.0f}` contracts\n"
            f"Spike: `{ratio:.1f}x` baseline\n"
            f"Notional: `~${notional_m:.0f}M`\n"
            f"Price: `${current_px:.2f}`\n\n"
            f"⏱ No Trump post in last {VOLUME_POST_LOCKOUT_MIN} min\n"
            f"_Watch for a post — Mar 23 pattern: spike → post → -7% in 15 min_"
        )
        for uid in ALLOWED_USER_IDS:
            try:
                await context.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
            except Exception:
                log.exception("Failed to send volume alert to %s", uid)

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
    We fetch the run's dataset, score the posts, and fire alerts immediately —
    no polling lag.
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

        # Fetch posts from this specific run's dataset
        resp = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
            params={"token": APIFY_API_TOKEN},
            timeout=30,
        )
        resp.raise_for_status()
        raw_posts = resp.json()

        if not raw_posts:
            return web.Response(status=200)

        # Put in queue for the main loop to process
        request.app["post_queue"].put_nowait(raw_posts)
        log.info("Queued %d raw posts from webhook", len(raw_posts))

    except Exception:
        log.exception("Webhook handler error")

    return web.Response(status=200)


async def process_webhook_posts(ptb_app: Application, raw_posts: list) -> None:
    """Score and process posts received via webhook, fire alerts if relevant."""
    existing_texts = {p["text"] for p in __import__('news')._read_cache()}
    append_new_posts(raw_posts)
    new_scored = _score_raw_posts(raw_posts)
    new_posts  = [p for p in new_scored if p["text"] not in existing_texts]

    if not new_posts:
        return

    log.info("Webhook: %d new oil-relevant posts", len(new_posts))

    # Reuse the same alert + auto-forecast logic as the polling job
    # Inject into the PTB context via a fake job context
    class _FakeContext:
        def __init__(self):
            self.bot      = ptb_app.bot
            self.bot_data = ptb_app.bot_data

    ctx = _FakeContext()

    # Alert
    for uid in ALLOWED_USER_IDS:
        try:
            lines = ["⚡ *Trump posted* (via webhook)\n"]
            for p in new_posts[:3]:
                lines.append(f"{score_emoji(p['score'])} `{p['score']:+d}` _{p['text'][:300]}_")
            await ptb_app.bot.send_message(
                chat_id=uid, text="\n\n".join(lines), parse_mode="Markdown"
            )
        except Exception:
            log.exception("Webhook alert send failed")

    # Save all new signals to USOIL.AI dashboard
    price_now = _current_wti()
    for p in new_posts:
        sc       = p.get("score", 0)
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
            avg15m         = analogue.get("avg_15m_move"),
            avg1h          = analogue.get("avg_1h_move"),
            est24h         = analogue.get("est_24h_move"),
            hit_rate_1h    = analogue.get("hit_rate_1h"),
        )

    # Auto-forecast
    top = max(new_posts, key=lambda p: abs(p["score"]))
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
    ptb_app.add_handler(CommandHandler("signal",       cmd_signal))
    ptb_app.add_handler(CommandHandler("signals",      cmd_signals))
    ptb_app.add_handler(CommandHandler("stats",        cmd_stats))
    ptb_app.add_handler(CommandHandler("alert",        cmd_alert))
    ptb_app.add_handler(CommandHandler("alerts",       cmd_alerts))
    ptb_app.add_handler(CommandHandler("cancelalert",  cmd_cancelalert))

    ptb_app.job_queue.run_repeating(check_price_alerts, interval=300,    first=15)
    ptb_app.job_queue.run_repeating(check_volume_spike, interval=300,    first=30)
    ptb_app.job_queue.run_repeating(refresh_post_cache, interval=6*3600, first=10)
    ptb_app.job_queue.run_repeating(check_trump_posts,  interval=60,     first=60)

    # Daily morning briefing — 8:30am ET = 12:30 UTC
    import pytz
    et = pytz.timezone("America/New_York")
    ptb_app.job_queue.run_daily(daily_morning_briefing, time=datetime.now(et).replace(
        hour=8, minute=30, second=0, microsecond=0
    ).timetz())

    # Build aiohttp webhook server
    post_queue = asyncio.Queue()
    aio_app    = web.Application()
    aio_app["post_queue"] = post_queue
    aio_app.router.add_post("/webhook/{secret}", handle_apify_webhook)
    aio_app.router.add_get("/signal",            handle_signal_api)

    runner = web.AppRunner(aio_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", WEBHOOK_PORT).start()
    log.info("Webhook server listening on port %d", WEBHOOK_PORT)

    # Start PTB
    await ptb_app.initialize()
    await ptb_app.start()
    await ptb_app.updater.start_polling(drop_pending_updates=True)
    log.info("Migas Oil Bot started")

    # Main loop — drain webhook queue
    try:
        while True:
            try:
                raw_posts = await asyncio.wait_for(post_queue.get(), timeout=5.0)
                await process_webhook_posts(ptb_app, raw_posts)
            except asyncio.TimeoutError:
                pass   # nothing in queue, keep looping
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("Shutting down…")
        await ptb_app.updater.stop()
        await ptb_app.stop()
        await ptb_app.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main_async())
