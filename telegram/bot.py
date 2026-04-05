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

import logging
import os
from datetime import datetime, timezone
from functools import wraps

import requests
import yfinance as yf
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from news import build_live_summary, get_relevant_trump_posts, refresh_cache, score_emoji

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
        "/forecast — 16-day WTI forecast\n"
        "/brent — 16-day Brent forecast\n"
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


async def trigger_auto_forecast(context: ContextTypes.DEFAULT_TYPE, post: dict):
    """Run a short WTI forecast triggered by a high-scoring Trump post."""
    sc       = post["score"]
    signals  = ", ".join(post["signals"])
    text     = post["text"]
    emoji    = score_emoji(sc)
    strength = "strongly" if abs(sc) >= 4 else "moderately"
    dirn     = "bullish 📈" if sc > 0 else "bearish 📉"

    for uid in ALLOWED_USER_IDS:
        try:
            msg = await context.bot.send_message(
                chat_id=uid,
                text=(
                    f"⚡ *Auto-forecast triggered* {emoji} `{sc:+d}`\n\n"
                    f"_{text[:300]}_\n\n"
                    f"*{strength} {dirn}* — {signals}\n\n"
                    f"⏳ Running {AUTO_FORECAST_PRED_LEN}-day WTI forecast…"
                ),
                parse_mode="Markdown",
            )

            try:
                price_data, current_price = fetch_prices("wti")
                summary, sources          = build_live_summary(current_price, "wti")
                forecast                  = get_forecast(price_data, summary, pred_len=AUTO_FORECAST_PRED_LEN)
                net_label                 = sources.get("net_label", "")
                result                    = (
                    f"⚡ *Auto-forecast* {emoji} `{sc:+d}`\n\n"
                    f"_{text[:200]}_\n\n"
                    f"*{strength} {dirn}* — {signals}\n\n"
                ) + format_forecast(forecast, current_price, "wti") + f"\n\n📰 {net_label}"
                await msg.edit_text(result, parse_mode="Markdown")

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
# Main
# ---------------------------------------------------------------------------

def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("forecast",     cmd_forecast))
    app.add_handler(CommandHandler("brent",        cmd_brent))
    app.add_handler(CommandHandler("alert",        cmd_alert))
    app.add_handler(CommandHandler("alerts",       cmd_alerts))
    app.add_handler(CommandHandler("cancelalert",  cmd_cancelalert))

    # Check price alerts every 5 minutes
    app.job_queue.run_repeating(check_price_alerts, interval=300, first=15)

    # Full cache refresh every 6 hours — runs at startup (first=10s) then every 6h
    app.job_queue.run_repeating(refresh_post_cache, interval=6*3600, first=10)

    # Incremental poll for new Trump posts every 60 seconds
    app.job_queue.run_repeating(check_trump_posts, interval=60, first=60)

    log.info("Migas Oil Bot starting…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
