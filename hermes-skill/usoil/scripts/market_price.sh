#!/bin/bash
# Live Hyperliquid perp mid price.
# Usage: market_price.sh [symbol=CL]     (CL | WTI | BRENT | BZ)
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

symbol="${1:-CL}"

uapi GET "/api/v1/market/price?symbol=${symbol}"
