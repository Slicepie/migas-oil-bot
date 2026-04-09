#!/bin/bash
set -e

REPO="https://github.com/Slicepie/migas-oil-bot.git"
BRANCH="${GIT_BRANCH:-main}"

echo "[startup] Pulling latest code from $REPO ($BRANCH)..."
rm -rf /tmp/repo
git clone --depth=1 --branch "$BRANCH" "$REPO" /tmp/repo
cp /tmp/repo/telegram/bot.py                /app/bot.py
cp /tmp/repo/telegram/news.py               /app/news.py
cp /tmp/repo/telegram/tracker.py            /app/tracker.py
cp /tmp/repo/telegram/llm_score.py           /app/llm_score.py
cp /tmp/repo/telegram/truthsocial_poller.py  /app/truthsocial_poller.py
rm -rf /tmp/repo

echo "[startup] Code updated. Starting bot..."
exec python -u bot.py
