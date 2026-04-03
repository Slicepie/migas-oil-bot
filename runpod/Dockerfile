# ---------------------------------------------------------------------------
# migas-oil-bot · RunPod serverless worker
# ---------------------------------------------------------------------------
FROM runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel-ubuntu22.04

WORKDIR /app

# Consistent HuggingFace cache directory — mount a RunPod network volume
# here to persist weights across worker restarts and avoid re-downloads.
ENV HF_HOME=/workspace/hf_cache

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    pkg-config \
    libfreetype6-dev \
    libpng-dev \
 && rm -rf /var/lib/apt/lists/*

# Python deps — layer cached unless requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Bake model weights into the image at build time.
# Cold starts go from ~minutes (downloading 2-3 GB) to ~seconds.
#
# Revisions are pinned to specific commits for reproducible builds.
# Re-pin by checking the latest commit hash on each model's HF page.
# ---------------------------------------------------------------------------
RUN python -c "\
from huggingface_hub import snapshot_download; \
snapshot_download('Synthefy/migas-1.5', revision='d99b2f8'); \
print('Migas-1.5 weights downloaded.')"

# Copy worker source
COPY handler.py .

# RunPod serverless requires this exact entrypoint
CMD ["python", "-u", "handler.py"]
