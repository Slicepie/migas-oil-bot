# ---------------------------------------------------------------------------
# migas-oil-bot · RunPod serverless worker
# ---------------------------------------------------------------------------
# Base image ships CUDA 12.1, cuDNN 8, Python 3.11, and a working pip.
# See https://hub.docker.com/r/runpod/pytorch for available tags.
# ---------------------------------------------------------------------------
FROM runpod/pytorch:2.2.0-py3.11-cuda12.1.1-devel-ubuntu22.04

WORKDIR /app

# System deps (git needed for any VCS pip installs; curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Python deps — install before copying source so layer is cached on re-builds
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download model weights at build time so they're baked into the image.
# This avoids cold-start latency on the first request.
# If the models are large, consider mounting a network volume instead and
# removing these RUN steps — set MODEL_CACHE_DIR env var accordingly.
RUN python - <<'EOF'
from sentence_transformers import SentenceTransformer
SentenceTransformer("ProsusAI/finbert")
print("FinBERT downloaded.")
EOF

RUN python - <<'EOF'
from migaseval import Migas
Migas.from_pretrained("migas-1.5")
print("Migas-1.5 downloaded.")
EOF

# Copy worker source
COPY handler.py .

# RunPod serverless requires this exact entrypoint
CMD ["python", "-u", "handler.py"]
