# ---------------------------------------------------------------------------
# migas-oil-bot · RunPod serverless worker
# ---------------------------------------------------------------------------
# Base image ships CUDA 12.1, cuDNN 8, Python 3.11, and a working pip.
# See https://hub.docker.com/r/runpod/pytorch for available tags.
# ---------------------------------------------------------------------------
FROM runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04

WORKDIR /app

# System deps (git needed for any VCS pip installs; curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Python deps — install before copying source so layer is cached on re-builds
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Model weights are NOT baked into the image — they are downloaded on first
# cold-start and cached to /root/.cache (HuggingFace default).
# To avoid re-downloading on every new worker, mount a RunPod network volume
# at /root/.cache and set TRANSFORMERS_CACHE=/root/.cache/huggingface.

# Copy worker source
COPY handler.py .

# RunPod serverless requires this exact entrypoint
CMD ["python", "-u", "handler.py"]
