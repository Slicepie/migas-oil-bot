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


def get_forecast(price_data: list[dict], summary: str, pred_len: int = 16) -> list[float]:
    """Call the RunPod Migas-1.5 endpoint and return the forecast array."""
    resp = requests.post(
        RUNPOD_ENDPOINT,
        headers={"Authorization": f"Bearer {RUNPOD_API_KEY}"},
        json={"input": {"price_data": price_data, "summary": summary, "pred_len": pred_len}},
        timeout=180,
    )
    resp.raise_for_status()
    data = resp.json()

    if "error" in data.get("output", {}):
        raise RuntimeError(data["output"]["error"])

    return data["output"]["forecast"]


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

# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

@restricted
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛢️ *Migas Oil Bot*\n\n"
        "Commands:\n"
        "/signal — trader signal (15min/1hr/24hr) from post analogues\n"
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

        # Show what news was found
        confirmed   = sources.get("confirmed", 0)
        unconfirmed = sources.get("unconfirmed", 0)
        avg_move    = sources.get("avg_move", 0.0)
        net_label   = sources.get("net_label", "")
        news_line   = (
            f"📰 {confirmed} confirmed moves (avg {avg_move:+.1f}% USO), "
            f"{unconfirmed} unconfirmed signals\n"
            f"Signal: {net_label}"
        )
        await msg.edit_text(f"⏳ Running Migas-1.5 forecast…\n{news_line}")

        forecast = get_forecast(price_data, summary)
        text     = format_forecast(forecast, current_price, instrument)
        text    += f"\n\n📰 {net_label}"
        await msg.edit_text(text, parse_mode="Markdown")
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
        _, current_price = fetch_prices("wti")
        posts    = get_scored_posts()

        # Only look at posts from the last 48 hours
        cutoff   = (datetime.now(timezone.utc) - timedelta(hours=48)).date().isoformat()
        recent   = [p for p in posts if p.get("date", "") >= cutoff]

        if not recent:
            await msg.edit_text(
                "⚪ *No recent signal*\n\n"
                "No oil-relevant Trump posts in the last 48 hours.\n"
                "_Monitoring continues — you'll be alerted when a new post is detected._",
                parse_mode="Markdown",
            )
            return

        top      = max(recent, key=lambda p: abs(p.get("score", 0)))
        text     = format_analogue_signal(top, current_price)

        # Add staleness warning if post is older than 4 hours
        post_date = top.get("date", "")
        post_time = top.get("time_et", "") or "00:00:00"
        try:
            import pytz
            et  = pytz.timezone("America/New_York")
            dt  = datetime.fromisoformat(f"{post_date}T{post_time}")
            dt  = et.localize(dt).astimezone(timezone.utc)
            age = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            if age > 4:
                text = f"⚠️ _Signal is {age:.0f}h old — market may have already reacted_\n\n" + text
        except Exception:
            pass

        await msg.edit_text(text, parse_mode="Markdown")
    except Exception as exc:
        log.exception("Signal command error")
        await msg.edit_text(f"❌ Error: {exc}")


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

                # --- Forecast A: standard summary ---
                summary_a, sources_a = build_live_summary(current_price, "wti")
                forecast_a           = get_forecast(price_data, summary_a, pred_len=AUTO_FORECAST_PRED_LEN)
                net_label_a          = sources_a.get("net_label", "")
                result_a             = (
                    f"⚡ *Forecast A — Standard* {emoji} `{sc:+d}`\n"
                    f"_60-day average net label_\n\n"
                    f"_{text[:150]}_\n\n"
                ) + format_forecast(forecast_a, current_price, "wti") + f"\n\n📰 {net_label_a}"

                if ab_test:
                    # --- Forecast B: current post overrides net label ---
                    summary_b, sources_b = build_live_summary_override(current_price, post, "wti")
                    forecast_b           = get_forecast(price_data, summary_b, pred_len=AUTO_FORECAST_PRED_LEN)
                    net_label_b          = sources_b.get("net_label", "")
                    result_b             = (
                        f"⚡ *Forecast B — Regime Override* {emoji} `{sc:+d}`\n"
                        f"_Current post overrides 60-day trend_\n\n"
                        f"_{text[:150]}_\n\n"
                    ) + format_forecast(forecast_b, current_price, "wti") + f"\n\n📰 {net_label_b}"

                    await msg.edit_text(result_a, parse_mode="Markdown")
                    await context.bot.send_message(
                        chat_id=uid, text=result_b, parse_mode="Markdown"
                    )
                else:
                    result_a = result_a.replace("*Forecast A — Standard*", "*Auto-forecast*")
                    await msg.edit_text(result_a, parse_mode="Markdown")

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

    # Auto-forecast if the strongest new post clears the threshold and cooldown has passed
    top = max(posts, key=lambda p: abs(p["score"]))
    if abs(top["score"]) >= AUTO_FORECAST_MIN_SCORE:
        last = context.bot_data.get("last_auto_forecast")
        now  = datetime.now(timezone.utc)
        if last is None or (now - last).total_seconds() > AUTO_FORECAST_COOLDOWN_MIN * 60:
            context.bot_data["last_auto_forecast"] = now
            log.info("Auto-forecast triggered by post (score %+d): %s", top["score"], top["text"][:80])
            await trigger_auto_forecast(context, top)


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
    ptb_app.add_handler(CommandHandler("stats",        cmd_stats))
    ptb_app.add_handler(CommandHandler("alert",        cmd_alert))
    ptb_app.add_handler(CommandHandler("alerts",       cmd_alerts))
    ptb_app.add_handler(CommandHandler("cancelalert",  cmd_cancelalert))

    ptb_app.job_queue.run_repeating(check_price_alerts, interval=300,    first=15)
    ptb_app.job_queue.run_repeating(check_volume_spike, interval=300,    first=30)
    ptb_app.job_queue.run_repeating(refresh_post_cache, interval=6*3600, first=10)
    ptb_app.job_queue.run_repeating(check_trump_posts,  interval=60,     first=60)

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
