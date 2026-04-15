#!/bin/bash
# Fetch recent scored Trump posts from usoil.ai.
# Usage: recent_posts.sh [hours=24] [min_score=0]
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

hours="${1:-24}"
min_score="${2:-0}"

uapi GET "/api/v1/posts/recent?hours=${hours}&min_score=${min_score}"
