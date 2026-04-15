#!/bin/bash
set -e

REPO="https://github.com/Slicepie/migas-oil-bot.git"
BRANCH="${GIT_BRANCH:-main}"

echo "[startup] Pulling latest code from $REPO ($BRANCH)..."
rm -rf /tmp/repo
git clone --depth=1 --branch "$BRANCH" "$REPO" /tmp/repo

# Show commit hash so we can verify we got the latest
echo "[startup] Latest commit: $(cd /tmp/repo && git log --oneline -1)"

cp /tmp/repo/telegram/bot.py                /app/bot.py
cp /tmp/repo/telegram/news.py               /app/news.py
cp /tmp/repo/telegram/tracker.py            /app/tracker.py
cp /tmp/repo/telegram/llm_score.py           /app/llm_score.py
cp /tmp/repo/telegram/truthsocial_poller.py  /app/truthsocial_poller.py
cp /tmp/repo/telegram/hormuz_monitor.py      /app/hormuz_monitor.py
cp /tmp/repo/telegram/trader.py              /app/trader.py 2>/dev/null || true
rm -rf /tmp/repo

echo "[startup] Code updated."

# ─── Auto-start Hyperliquid trader (if HL_SECRET_KEY is set) ────────────────
if [ -n "$HL_SECRET_KEY" ]; then
    echo "[startup] HL_SECRET_KEY detected — starting trader in background..."

    # Defaults — override via RunPod pod env vars if needed
    export HL_WALLET="${HL_WALLET:-0x8AB3183037382f22ed608A8bb29eD034213978E6}"
    export HL_ACCOUNT="${HL_ACCOUNT:-$HL_WALLET}"
    export STREAM_URL="${STREAM_URL:-http://localhost:8080/stream/OIL}"
    export TRADE_SIZE_USD="${TRADE_SIZE_USD:-20}"
    export MAX_POSITION_USD="${MAX_POSITION_USD:-100}"
    export LEVERAGE="${LEVERAGE:-3}"
    export MIN_SCORE="${MIN_SCORE:-3}"
    export STOP_LOSS_PCT="${STOP_LOSS_PCT:-2.0}"
    export TAKE_PROFIT_PCT="${TAKE_PROFIT_PCT:-3.0}"
    export DRY_RUN="${DRY_RUN:-false}"

    # Wait 10s for bot's SSE server to come up, then launch trader
    (sleep 10 && python -u /app/trader.py > /app/trader.log 2>&1) &
    echo "[startup] Trader will start in 10s (log: /app/trader.log)"
else
    echo "[startup] HL_SECRET_KEY not set — skipping trader"
fi

echo "[startup] Starting bot..."
exec python -u bot.py
