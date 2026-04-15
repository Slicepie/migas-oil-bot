#!/bin/bash
# Get a trade idea based on the most recent OIL signal.
# Does NOT execute — user runs the trade themselves on Hyperliquid.
# Usage: trade_idea.sh [size_usd=50] [market=OIL]
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

size="${1:-50}"
market="${2:-OIL}"

uapi POST "/api/v1/trade/idea" "{\"market\":\"${market}\",\"size_usd\":${size}}"
