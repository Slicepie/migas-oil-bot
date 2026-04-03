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
os.environ.setdefault("HF_HOME", "/workspace/hf_cache")  # must be set before HF imports

import runpod
import torch
import numpy as np
import pandas as pd
from migaseval import MigasPipeline

# ---------------------------------------------------------------------------
# Model initialisation (runs once at container start, not per request)
# ---------------------------------------------------------------------------

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

print(f"[init] Loading Migas-1.5 on {DEVICE}...")
_pipeline = MigasPipeline.from_pretrained(
    "Synthefy/migas-1.5",
    device=DEVICE,
)

print("[init] Ready.")

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _build_dataframe(price_data: list[dict]) -> pd.DataFrame:
    """
    Convert [{"t": "YYYY-MM-DD", "y_t": float}, ...] to a DataFrame
    sorted by date, as expected by MigasPipeline.predict_from_dataframe().
    """
    df = pd.DataFrame(price_data)
    df = df.rename(columns={"t": "t", "y_t": "y_t"})
    df = df.sort_values("t").reset_index(drop=True)
    return df


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
        # 1. Build DataFrame from price_data
        df = _build_dataframe(price_data)

        # 2. Run Migas-1.5 inference — pipeline handles FinBERT encoding internally
        forecast = _pipeline.predict_from_dataframe(
            df,
            pred_len=pred_len,
            summaries=summary,
        )

        # 3. Normalise output to a plain Python list of floats
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
