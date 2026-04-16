#!/bin/bash
# Shared lib for usoil skill scripts. Sourced by each helper.
# Exports USOIL_API and a `uapi` helper for GET/POST.

USOIL_API="${USOIL_API_BASE:-http://api.usoil.ai:34412}"

# Usage: uapi GET /api/v1/posts/recent?hours=24
#        uapi POST /api/v1/trade/idea '{"market":"OIL"}'
uapi() {
  local method="$1"
  local path="$2"
  local body="$3"

  if [ "$method" = "GET" ]; then
    curl -s --max-time 10 "${USOIL_API}${path}"
  else
    curl -s --max-time 10 -X "$method" \
      -H "Content-Type: application/json" \
      -d "${body:-{}}" \
      "${USOIL_API}${path}"
  fi
}
