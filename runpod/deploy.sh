#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy.sh — Register (or update) the RunPod serverless endpoint.
#
# The Docker image is built and pushed automatically by GitHub Actions
# (.github/workflows/build-push.yml) on every push to main.
#
# Run this script once to create the endpoint, or re-run to update it.
#
# Usage:
#   ./deploy.sh
#
# Required env vars (set in your shell — never commit them):
#   GITHUB_USER       your GitHub username  (image owner on ghcr.io)
#   RUNPOD_API_KEY    RunPod API key  (RunPod console → Settings → API Keys)
#
# Optional:
#   IMAGE_TAG         defaults to "latest"
#   ENDPOINT_NAME     defaults to "migas-oil-bot"
#   GPU_TYPE          defaults to "NVIDIA GeForce RTX 3090"
# ---------------------------------------------------------------------------
set -euo pipefail

# --- config ------------------------------------------------------------------
GITHUB_USER="${GITHUB_USER:?Set GITHUB_USER to your GitHub username}"
RUNPOD_API_KEY="${RUNPOD_API_KEY:?Set RUNPOD_API_KEY}"

IMAGE_TAG="${IMAGE_TAG:-latest}"
FULL_IMAGE="ghcr.io/${GITHUB_USER}/migas-oil-bot-worker:${IMAGE_TAG}"

ENDPOINT_NAME="${ENDPOINT_NAME:-migas-oil-bot}"
GPU_TYPE="${GPU_TYPE:-NVIDIA GeForce RTX 3090}"

# ---------------------------------------------------------------------------
# NOTE: ghcr.io packages are private by default.
# RunPod needs to pull the image, so either:
#   (a) Make the package public:
#       GitHub → your package → Package settings → "Change visibility" → Public
#   (b) Or pass a registry credential when creating the endpoint (see RunPod docs).
#
# Option (a) is simplest for a serverless worker with no sensitive code.
# ---------------------------------------------------------------------------

echo "==> Image  : ${FULL_IMAGE}"
echo "==> Endpoint: ${ENDPOINT_NAME}"
echo ""

# --- Create / update RunPod serverless endpoint ------------------------------
echo "==> Registering RunPod serverless endpoint..."

RUNPOD_GQL="https://api.runpod.io/graphql?api_key=${RUNPOD_API_KEY}"

ESCAPED_IMAGE=$(printf '%s' "${FULL_IMAGE}" | sed 's/"/\\"/g')
ESCAPED_NAME=$(printf '%s' "${ENDPOINT_NAME}" | sed 's/"/\\"/g')
ESCAPED_GPU=$(printf '%s' "${GPU_TYPE}" | sed 's/"/\\"/g')

MUTATION=$(cat <<GRAPHQL
mutation {
  saveEndpoint(input: {
    name:              "${ESCAPED_NAME}"
    templateId:        null
    dockerArgs:        ""
    containerDiskInGb: 20
    volumeInGb:        0
    minWorkers:        0
    maxWorkers:        3
    idleTimeout:       60
    locations:         null
    networkVolumeId:   null
    gpuIds:            "${ESCAPED_GPU}"
    imageName:         "${ESCAPED_IMAGE}"
    scalerType:        "QUEUE_DELAY"
    scalerValue:       4
  }) {
    id
    name
  }
}
GRAPHQL
)

RESPONSE=$(curl -s -X POST "${RUNPOD_GQL}" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg q "${MUTATION}" '{"query": $q}')")

echo "${RESPONSE}" | jq .

ENDPOINT_ID=$(echo "${RESPONSE}" | jq -r '.data.saveEndpoint.id // empty')

if [[ -z "${ENDPOINT_ID}" ]]; then
  echo "ERROR: Failed to create endpoint. See response above." >&2
  exit 1
fi

echo ""
echo "==> Done!"
echo "    Endpoint ID  : ${ENDPOINT_ID}"
echo "    Endpoint URL : https://api.runpod.ai/v2/${ENDPOINT_ID}/run"
echo ""
echo "    Test with:"
echo "    curl -X POST https://api.runpod.ai/v2/${ENDPOINT_ID}/runsync \\"
echo "      -H 'Authorization: Bearer \${RUNPOD_API_KEY}' \\"
echo "      -H 'Content-Type: application/json' \\"
echo "      -d '{\"input\":{\"price_data\":[{\"t\":\"2024-01-01\",\"y_t\":82.5}],\"summary\":\"OPEC holds output\",\"pred_len\":8}}'"
