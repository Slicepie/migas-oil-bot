"""
USOIL.AI — Hyperliquid WTI Trader
Listens to the SSE signal stream and trades CL perps on Hyperliquid xyz dex.

Environment variables:
  HL_SECRET_KEY     — Hyperliquid API wallet private key
  HL_WALLET         — Hyperliquid wallet address (public)
  HL_ACCOUNT        — Account address (if using API wallet, else same as HL_WALLET)
  STREAM_URL        — SSE stream URL (default: http://localhost:8080/stream/OIL)
  TRADE_SIZE_USD    — Base trade size in USD (default: 50)
  MAX_POSITION_USD  — Max total position size in USD (default: 500)
  LEVERAGE          — Leverage multiplier (default: 3)
  MIN_SCORE         — Minimum |score| to trade on (default: 3)
  STOP_LOSS_PCT     — Stop loss percentage (default: 2.0)
  TAKE_PROFIT_PCT   — Take profit percentage (default: 3.0)
  DRY_RUN           — Set to "false" to enable live trading (default: true)
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime, timezone

import requests
import sseclient  # pip install sseclient-py
import eth_account
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

# ─── Config ──────────────────────────────────────────────────────────────────

SECRET_KEY      = os.environ.get("HL_SECRET_KEY", "")
WALLET          = os.environ.get("HL_WALLET", "").lower()
ACCOUNT         = (os.environ.get("HL_ACCOUNT", "") or WALLET).lower()
STREAM_URL      = os.environ.get("STREAM_URL", "http://localhost:8080/stream/OIL")
TRADE_SIZE_USD  = float(os.environ.get("TRADE_SIZE_USD", "20"))
MAX_POSITION_USD = float(os.environ.get("MAX_POSITION_USD", "100"))
LEVERAGE        = int(os.environ.get("LEVERAGE", "3"))
MIN_SCORE       = int(os.environ.get("MIN_SCORE", "3"))
STOP_LOSS_PCT   = float(os.environ.get("STOP_LOSS_PCT", "2.0"))
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "3.0"))
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() != "false"

COIN = "xyz:CL"  # WTI crude on Hyperliquid xyz dex

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("trader")

# ─── Hyperliquid Client ─────────────────────────────────────────────────────

exchange: Exchange | None = None
info: Info | None = None


def init_hyperliquid():
    global exchange, info
    if not SECRET_KEY or not WALLET:
        log.warning("HL_SECRET_KEY / HL_WALLET not set — dry run only")
        return

    account: LocalAccount = eth_account.Account.from_key(SECRET_KEY)
    exchange = Exchange(
        wallet=account,
        base_url=constants.MAINNET_API_URL,
        account_address=ACCOUNT,
        perp_dexs=["xyz"],
    )
    info = Info(
        base_url=constants.MAINNET_API_URL,
        skip_ws=True,
    )

    # Set leverage
    try:
        exchange.update_leverage(LEVERAGE, COIN, is_cross=True)
        log.info("Leverage set to %dx cross for %s", LEVERAGE, COIN)
    except Exception as e:
        log.warning("Failed to set leverage: %s", e)

    # Print account state
    show_account()


def show_account():
    if not info:
        return
    try:
        state = info.user_state(ACCOUNT, dex="xyz")
        margin = state.get("marginSummary", {})
        equity = float(margin.get("accountValue", 0))
        used = float(margin.get("totalMarginUsed", 0))
        log.info("Account: equity=$%.2f, margin_used=$%.2f", equity, used)

        positions = state.get("assetPositions", [])
        for p in positions:
            pos = p["position"]
            if float(pos.get("szi", 0)) != 0:
                log.info(
                    "  Position: %s size=%s entry=$%s pnl=$%s",
                    pos["coin"], pos["szi"], pos.get("entryPx", "?"),
                    pos.get("unrealizedPnl", "?"),
                )
    except Exception as e:
        log.error("Failed to fetch account state: %s", e)


# ─── Position tracking ──────────────────────────────────────────────────────

def get_position() -> dict | None:
    """Get current CL position, if any."""
    if not info:
        return None
    try:
        state = info.user_state(ACCOUNT, dex="xyz")
        for p in state.get("assetPositions", []):
            pos = p["position"]
            if pos["coin"] == COIN and float(pos.get("szi", 0)) != 0:
                return pos
    except Exception as e:
        log.error("Failed to get position: %s", e)
    return None


def get_mid_price() -> float | None:
    """Get current CL mid price from Hyperliquid."""
    try:
        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "allMids", "dex": "xyz"},
            timeout=5,
        )
        if resp.status_code == 200:
            mids = resp.json()
            price_str = mids.get("xyz:CL") or mids.get("CL")
            if price_str:
                return float(price_str)
    except Exception as e:
        log.error("Failed to get mid price: %s", e)
    return None


# ─── Trading logic ───────────────────────────────────────────────────────────

def calc_size(score: int, price: float) -> float:
    """Calculate position size in contracts. Fixed $20 per trade."""
    usd_size = TRADE_SIZE_USD

    # Check max position
    pos = get_position()
    if pos:
        current_value = abs(float(pos.get("positionValue", 0)))
        if current_value + usd_size > MAX_POSITION_USD:
            usd_size = max(0, MAX_POSITION_USD - current_value)
            if usd_size <= 0:
                log.warning("Max position reached ($%.0f), skipping", MAX_POSITION_USD)
                return 0

    # Convert USD to contracts (CL = 1 barrel, price is per barrel)
    size = round(usd_size / price, 2)
    return max(size, 0.01)  # min size


def should_flip(direction: str, pos: dict) -> bool:
    """Check if signal is opposite to current position."""
    current_size = float(pos.get("szi", 0))
    is_long = current_size > 0
    signal_is_buy = direction == "BULLISH"
    return is_long != signal_is_buy


def execute_trade(score: int, direction: str, theme: str, price_at_alert: float | None):
    """Execute a trade based on signal."""
    if abs(score) < MIN_SCORE:
        log.info("Score %+d below threshold %d, skipping", score, MIN_SCORE)
        return

    is_buy = direction == "BULLISH"
    mid = get_mid_price()
    if not mid:
        log.error("Cannot get mid price, skipping trade")
        return

    # Check if we need to close/flip existing position
    pos = get_position()
    if pos and should_flip(direction, pos):
        log.info("Flipping position: closing %s before opening %s",
                 "LONG" if float(pos["szi"]) > 0 else "SHORT", direction)
        close_position("flip")

    size = calc_size(score, mid)
    if size <= 0:
        return

    log.info(
        "TRADE: %s %s %.2f contracts @ ~$%.2f (score=%+d, theme=%s)",
        "BUY" if is_buy else "SELL", COIN, size, mid, score, theme,
    )

    if DRY_RUN:
        log.info("DRY RUN — trade not executed")
        return

    try:
        result = exchange.market_open(
            COIN,
            is_buy=is_buy,
            sz=size,
            slippage=0.01,  # 1% max slippage
        )

        if result.get("status") == "ok":
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            for s in statuses:
                if "filled" in s:
                    filled = s["filled"]
                    log.info(
                        "FILLED: %s %.4f @ $%s (oid=%s)",
                        "LONG" if is_buy else "SHORT",
                        float(filled["totalSz"]),
                        filled["avgPx"],
                        filled["oid"],
                    )
                elif "error" in s:
                    log.error("Order error: %s", s["error"])
        else:
            log.error("Order failed: %s", result)

    except Exception as e:
        log.error("Trade execution failed: %s", e)


def close_position(reason: str = "manual"):
    """Close entire CL position."""
    pos = get_position()
    if not pos:
        log.info("No position to close")
        return

    size = abs(float(pos.get("szi", 0)))
    entry = float(pos.get("entryPx", 0))
    pnl = float(pos.get("unrealizedPnl", 0))

    log.info("CLOSING: %s %.4f @ entry $%.2f, pnl=$%.2f, reason=%s",
             COIN, size, entry, pnl, reason)

    if DRY_RUN:
        log.info("DRY RUN — close not executed")
        return

    try:
        result = exchange.market_close(COIN, slippage=0.01)
        if result.get("status") == "ok":
            log.info("Position closed successfully")
        else:
            log.error("Close failed: %s", result)
    except Exception as e:
        log.error("Close execution failed: %s", e)


# ─── Stop loss / Take profit monitor ────────────────────────────────────────

def monitor_position():
    """Background thread: check position every 30s for SL/TP."""
    while True:
        try:
            pos = get_position()
            if pos:
                entry = float(pos.get("entryPx", 0))
                mid = get_mid_price()
                if entry and mid:
                    size = float(pos.get("szi", 0))
                    is_long = size > 0
                    pct = ((mid - entry) / entry) * 100
                    if not is_long:
                        pct = -pct  # flip for shorts

                    if pct <= -STOP_LOSS_PCT:
                        log.warning("STOP LOSS triggered: %.2f%% (limit: -%.1f%%)", pct, STOP_LOSS_PCT)
                        close_position("stop_loss")
                    elif pct >= TAKE_PROFIT_PCT:
                        log.info("TAKE PROFIT triggered: +%.2f%% (limit: +%.1f%%)", pct, TAKE_PROFIT_PCT)
                        close_position("take_profit")
        except Exception as e:
            log.error("Position monitor error: %s", e)

        time.sleep(30)


# ─── SSE Stream listener ────────────────────────────────────────────────────

def listen_stream():
    """Connect to SSE stream and process signals."""
    while True:
        try:
            log.info("Connecting to stream: %s", STREAM_URL)
            resp = requests.get(STREAM_URL, stream=True, timeout=300)
            client = sseclient.SSEClient(resp)

            for event in client.events():
                if event.event == "connected":
                    log.info("Stream connected: %s", event.data)
                    continue

                if event.event == "signal":
                    # Log raw event arrival BEFORE parsing so we have evidence the signal reached us
                    log.info("SSE event received (%d bytes): %s", len(event.data or ""), (event.data or "")[:300])
                    try:
                        sig = json.loads(event.data)
                        data = sig.get("data", {})
                        score = data.get("score", 0)
                        direction = data.get("direction", "NEUTRAL")
                        confidence = data.get("confidence", "")
                        theme = sig.get("post_theme", "unknown")
                        price = sig.get("price")
                        rationale = data.get("rationale", "")

                        log.info(
                            "Signal: %s score=%+d conf=%s theme=%s price=$%s — %s",
                            direction, score, confidence, theme, price,
                            rationale[:100],
                        )

                        if direction != "NEUTRAL" and abs(score) >= MIN_SCORE:
                            execute_trade(score, direction, theme, price)
                        else:
                            log.info(
                                "SKIP trade: direction=%s score=%+d (threshold=±%d, must be non-NEUTRAL)",
                                direction, score, MIN_SCORE,
                            )

                    except json.JSONDecodeError:
                        log.warning("Bad signal JSON: %s", event.data[:200])

                elif event.event == "heartbeat":
                    pass  # keep-alive

        except requests.exceptions.ConnectionError:
            log.warning("Stream disconnected, reconnecting in 10s...")
        except requests.exceptions.Timeout:
            log.warning("Stream timeout, reconnecting in 10s...")
        except Exception as e:
            log.error("Stream error: %s, reconnecting in 10s...", e)

        time.sleep(10)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    log.info("=" * 60)
    log.info("USOIL.AI Trader — %s", mode)
    log.info("  Coin:       %s", COIN)
    log.info("  Size:       $%.0f per trade", TRADE_SIZE_USD)
    log.info("  Max pos:    $%.0f", MAX_POSITION_USD)
    log.info("  Leverage:   %dx", LEVERAGE)
    log.info("  Min score:  %d", MIN_SCORE)
    log.info("  Stop loss:  %.1f%%", STOP_LOSS_PCT)
    log.info("  Take profit:%.1f%%", TAKE_PROFIT_PCT)
    log.info("  Stream:     %s", STREAM_URL)
    log.info("=" * 60)

    if not SECRET_KEY:
        log.error("HL_SECRET_KEY not set — exiting")
        sys.exit(1)

    init_hyperliquid()

    # Start position monitor in background
    monitor_thread = threading.Thread(target=monitor_position, daemon=True)
    monitor_thread.start()
    log.info("Position monitor started (SL=%.1f%%, TP=%.1f%%)", STOP_LOSS_PCT, TAKE_PROFIT_PCT)

    # Listen to signal stream (blocks)
    listen_stream()


if __name__ == "__main__":
    main()
