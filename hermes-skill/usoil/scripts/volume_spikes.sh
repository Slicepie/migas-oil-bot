#!/bin/bash
# Recent volume-spike events (≥4x baseline CME, ≥$5M delta HL).
# Usage: volume_spikes.sh [hours=24]
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

hours="${1:-24}"

uapi GET "/api/v1/volume/spikes?hours=${hours}"
