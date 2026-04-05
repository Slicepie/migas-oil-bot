"""
Migas Oil Bot — Telegram interface for Migas-1.5 WTI forecasting.

Commands:
    /start          — welcome message
    /forecast       — fetch real WTI prices, run Migas-1.5, return 16-day forecast
    /alert <price>  — notify when WTI crosses a price level
    /alerts         — list active alerts
    /cancelalert    — cancel all alerts

Private: only ALLOWED_USER_IDS can interact with the bot.
"""

import logging
import os
from functools import wraps

import requests
import yfinance as yf
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BOT_TOKEN        = os.environ["TELEGRAM_BOT_TOKEN"]
RUNPOD_API_KEY   = os.environ["RUNPOD_API_KEY"]
RUNPOD_ENDPOINT  = "https://api.runpod.ai/v2/fxkby0bka43s1i/runsync"

ALLOWED_USER_IDS = {1038492789}   # @slicepie5

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

def fetch_wtf_prices(days: int = 90) -> tuple[list[dict], float]:
    """Fetch WTI crude oil prices via yfinance.

    Returns:
        price_data: list of {t, y_t} dicts sorted by date
        current_price: latest closing price
    """
    ticker = yf.Ticker("CL=F")
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


def build_summary(current_price: float) -> str:
    """Build a basic summary for Migas. Will be replaced by live news later."""
    return (
        f"FACTUAL SUMMARY:\n"
        f"WTI crude oil is currently trading at ${current_price:.2f}/barrel. "
        f"Markets are monitoring OPEC+ production decisions, US inventory data, "
        f"and geopolitical developments in the Middle East including Iran and "
        f"the Strait of Hormuz.\n\n"
        f"PREDICTIVE SIGNALS:\n"
        f"No major supply disruptions confirmed. Demand outlook remains cautious "
        f"amid global macro uncertainty."
    )


def format_forecast(forecast: list[float], current_price: float) -> str:
    """Format forecast list into a readable Telegram message."""
    direction = "📈" if forecast[-1] > current_price else "📉"
    change    = forecast[-1] - current_price
    pct       = (change / current_price) * 100

    lines = [
        f"🛢️ *WTI 16-Day Forecast* {direction}",
        f"",
        f"Current:  `${current_price:.2f}`",
        f"Day 16:   `${forecast[-1]:.2f}` ({pct:+.1f}%)",
        f"",
        f"{'Day':<5} {'Price':>8}",
        f"{'---':<5} {'-----':>8}",
    ]
    for i, price in enumerate(forecast, 1):
        arrow = "↑" if price > current_price else "↓"
        lines.append(f"`{i:<4}  ${price:>7.2f}  {arrow}`")

    lines += ["", "_Powered by Migas-1.5_"]
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
        "/alert 85.00 — alert when WTI hits $85\n"
        "/alerts — list active alerts\n"
        "/cancelalert — cancel all alerts",
        parse_mode="Markdown",
    )


@restricted
async def cmd_forecast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Fetching WTI prices and running forecast…")

    try:
        price_data, current_price = fetch_wtf_prices()
        summary  = build_summary(current_price)
        forecast = get_forecast(price_data, summary)
        text     = format_forecast(forecast, current_price)
        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as exc:
        log.exception("Forecast error")
        await msg.edit_text(f"❌ Error: {exc}")


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
    app.add_handler(CommandHandler("alert",        cmd_alert))
    app.add_handler(CommandHandler("alerts",       cmd_alerts))
    app.add_handler(CommandHandler("cancelalert",  cmd_cancelalert))

    # Check price alerts every 5 minutes
    app.job_queue.run_repeating(check_price_alerts, interval=300, first=15)

    log.info("Migas Oil Bot starting…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
