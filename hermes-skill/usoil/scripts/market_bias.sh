#!/bin/bash
# Current OIL market bias aggregated from recent signals.
# Usage: market_bias.sh [hours=6] [market=OIL]
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

hours="${1:-6}"
market="${2:-OIL}"

uapi GET "/api/v1/market/bias?market=${market}&hours=${hours}"
