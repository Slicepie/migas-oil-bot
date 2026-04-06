"""
RunPod serverless worker for Migas-1.5 oil price forecasting.

Expected input payload:
{
    "price_data":          [{"t": "2024-01-01", "y_t": 82.5}, ...],
    "summary":             "FACTUAL SUMMARY:\n...\n\nPREDICTIVE SIGNALS:\n...",
    "pred_len":            16,          # optional, default 16
    "n_summaries":         5,           # optional ensemble size, default 5
    "counterfactual":      true,        # optional — also run bullish/bearish scenarios
    "bullish_predictive":  "...",       # optional — custom bullish PREDICTIVE SIGNALS
    "bearish_predictive":  "...",       # optional — custom bearish PREDICTIVE SIGNALS
}

Returns:
{
    "forecast":         [83.1, 84.2, ...],   # main ensemble forecast (pred_len values)
    "forecast_bullish": [85.1, 86.2, ...],   # only if counterfactual=true
    "forecast_bearish": [81.1, 80.2, ...],   # only if counterfactual=true
    "chronos_baseline": [82.5, 82.8, ...],   # text-free Chronos-2 baseline
}
"""

import logging
import os

os.environ.setdefault("HF_HOME", "/workspace/hf_cache")  # must be set before HF imports

# Suppress noisy library logs
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
logging.getLogger("chronos").setLevel(logging.ERROR)
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)

import runpod
import numpy as np
import pandas as pd
import torch
from migaseval import MigasPipeline
from migaseval.counterfactual_utils import extract_factual, splice_summary

# ---------------------------------------------------------------------------
# Model initialisation — runs once at container start, not per request
# ---------------------------------------------------------------------------

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

print(f"[init] Loading Migas-1.5 on {DEVICE}…")
_pipeline = MigasPipeline.from_pretrained(
    "Synthefy/migas-1.5",
    device=DEVICE,
    text_embedder_device=DEVICE,
)
print("[init] Ready.")

# Default counterfactual narratives used when caller doesn't supply custom ones
_DEFAULT_BULLISH = (
    "PREDICTIVE SIGNALS:\n"
    "Geopolitical escalation in the Middle East intensifies. "
    "Strait of Hormuz closure risk rises sharply. "
    "Iranian oil infrastructure under sustained attack. "
    "OPEC+ emergency production cut likely. "
    "Supply disruption premium expected to widen significantly. "
    "Oil prices likely to spike in the near term."
)

_DEFAULT_BEARISH = (
    "PREDICTIVE SIGNALS:\n"
    "Iran ceasefire or peace deal signals emerging. "
    "Strait of Hormuz remains open, tanker traffic normalising. "
    "De-escalation reduces geopolitical risk premium. "
    "OPEC+ considering output increase to stabilise markets. "
    "Demand concerns weigh on prices. "
    "Oil prices likely to decline in the near term."
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_list(x) -> list[float]:
    """Normalise numpy / torch / list output to plain Python list of floats."""
    if isinstance(x, (np.ndarray, torch.Tensor)):
        return x.flatten().tolist()
    return [float(v) for v in x]


def _build_dataframe(price_data: list[dict]) -> pd.DataFrame:
    """Convert price_data list to a sorted DataFrame for MigasPipeline."""
    df = pd.DataFrame(price_data)
    df = df.sort_values("t").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# RunPod handler
# ---------------------------------------------------------------------------

def handler(job: dict) -> dict:
    job_input = job.get("input", {})

    # --- validate ---
    if "price_data" not in job_input:
        return {"error": "Missing required field: price_data"}
    if "summary" not in job_input:
        return {"error": "Missing required field: summary"}

    price_data:   list[dict] = job_input["price_data"]
    summary:      str        = job_input["summary"]
    pred_len:     int        = int(job_input.get("pred_len", 16))
    n_summaries:  int        = int(job_input.get("n_summaries", 5))
    counterfact:  bool       = bool(job_input.get("counterfactual", False))

    if len(price_data) < 1:
        return {"error": "price_data must contain at least one data point"}

    try:
        df = _build_dataframe(price_data)

        # --- build ensemble summaries ---
        # Pass the same summary n_summaries times — MigasPipeline median-aggregates
        # them for a more stable forecast (reduces variance from text encoding)
        summaries = [summary] * n_summaries

        # --- main ensemble forecast + Chronos-2 baseline ---
        migas_fc, chronos_fc = _pipeline.predict_from_dataframe(
            df,
            pred_len=pred_len,
            summaries=summaries,
            return_univariate=True,
        )

        result = {
            "forecast":         _to_list(migas_fc),
            "chronos_baseline": _to_list(chronos_fc),
        }

        # --- optional counterfactual ---
        if counterfact:
            bullish_predictive = job_input.get("bullish_predictive", _DEFAULT_BULLISH)
            bearish_predictive = job_input.get("bearish_predictive", _DEFAULT_BEARISH)

            # Keep factual section, swap only the predictive signals
            bullish_summaries = splice_summary(summaries, bullish_predictive)
            bearish_summaries = splice_summary(summaries, bearish_predictive)

            bullish_fc = _pipeline.predict_from_dataframe(
                df, pred_len=pred_len, summaries=bullish_summaries
            )
            bearish_fc = _pipeline.predict_from_dataframe(
                df, pred_len=pred_len, summaries=bearish_summaries
            )

            result["forecast_bullish"] = _to_list(bullish_fc)
            result["forecast_bearish"] = _to_list(bearish_fc)

        return result

    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})
