"""
RunPod serverless worker for Migas-1.5 oil price forecasting.

Expected input payload:
{
    "price_data": [{"t": "2024-01-01", "y_t": 82.5}, ...],
    "summary":    "OPEC cut production by 500k bpd...",
    "pred_len":   16          # optional, default 16
}

Returns:
{
    "forecast": [83.1, 84.2, ...]   # pred_len values
}
"""

import os
import runpod
import torch
import numpy as np
from sentence_transformers import SentenceTransformer
from migaseval import Migas

# ---------------------------------------------------------------------------
# Model initialisation (runs once at container start, not per request)
# ---------------------------------------------------------------------------

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

print(f"[init] Loading FinBERT embedder on {DEVICE}...")
_embedder = SentenceTransformer("ProsusAI/finbert", device=DEVICE)

print("[init] Loading Migas-1.5...")
_model = Migas.from_pretrained("migas-1.5", device=DEVICE)
_model.eval()

print("[init] Ready.")

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _encode_text(summary: str) -> np.ndarray:
    """Encode a free-text summary with FinBERT → (1, hidden_dim) float32."""
    with torch.no_grad():
        embedding = _embedder.encode(
            [summary],
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
    return embedding  # shape (1, hidden_dim)


def _parse_price_series(price_data: list[dict]) -> np.ndarray:
    """
    Convert [{"t": ..., "y_t": float}, ...] to a (T,) float32 array.
    Sorted by timestamp so callers don't have to guarantee order.
    """
    sorted_data = sorted(price_data, key=lambda x: x["t"])
    return np.array([row["y_t"] for row in sorted_data], dtype=np.float32)


# ---------------------------------------------------------------------------
# RunPod handler
# ---------------------------------------------------------------------------

def handler(job: dict) -> dict:
    job_input = job.get("input", {})

    # --- validate required fields ---
    if "price_data" not in job_input:
        return {"error": "Missing required field: price_data"}
    if "summary" not in job_input:
        return {"error": "Missing required field: summary"}

    price_data: list[dict] = job_input["price_data"]
    summary: str = job_input["summary"]
    pred_len: int = int(job_input.get("pred_len", 16))

    if len(price_data) < 1:
        return {"error": "price_data must contain at least one data point"}

    try:
        # 1. Encode text context
        text_embedding = _encode_text(summary)  # (1, hidden_dim)

        # 2. Build price series tensor
        price_series = _parse_price_series(price_data)  # (T,)

        # 3. Run Migas-1.5 inference
        with torch.no_grad():
            forecast = _model.predict(
                price_series=price_series,
                text_embedding=text_embedding,
                pred_len=pred_len,
                device=DEVICE,
            )  # expected: np.ndarray or list of length pred_len

        # 4. Normalise output to a plain Python list of floats
        if isinstance(forecast, (np.ndarray, torch.Tensor)):
            forecast = forecast.flatten().tolist()
        else:
            forecast = [float(v) for v in forecast]

        return {"forecast": forecast}

    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})
