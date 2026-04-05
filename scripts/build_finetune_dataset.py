"""
Build fine-tuning dataset from HuggingFace trump-truth-social dataset.

Filters all 32k+ posts for those with a significant USO (oil ETF) price
reaction, then pairs each post with the historical USO price series via
yfinance to create (price_history, post_text) → (actual_future_prices) pairs.

Output: data/finetune_dataset.jsonl
Each line:
{
    "post_id":    "...",
    "date":       "YYYY-MM-DD",
    "time_et":    "HH:MM:SS",
    "text":       "...",
    "url":        "...",
    "uso_before": 121.44,
    "uso_after":  112.97,
    "uso_pct_5m": -6.97,
    "uso_pct_1h": -6.97,
    "price_data": [{"t": "YYYY-MM-DD", "y_t": 121.44}, ...],   # 60 days before post
    "future_prices": [{"t": "YYYY-MM-DD", "y_t": 112.97}, ...] # 16 days after post
}

Usage:
    python3 scripts/build_finetune_dataset.py
    python3 scripts/build_finetune_dataset.py --min-move 3.0 --out data/big_moves.jsonl
"""

import argparse
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HF_ENDPOINT   = "https://datasets-server.huggingface.co/rows"
HF_DATASET    = "chrissoria/trump-truth-social"
TICKER        = "USO"        # US Oil Fund ETF — proxy for WTI
HISTORY_DAYS  = 60           # price history window fed to Migas
FUTURE_DAYS   = 16           # prediction target window
OUTPUT_DIR    = Path(__file__).parent.parent / "data"


# ---------------------------------------------------------------------------
# HuggingFace pagination
# ---------------------------------------------------------------------------

def fetch_all_hf_posts(min_uso_move_pct: float = 2.0) -> list[dict]:
    """Fetch all posts from HF dataset, filter for significant USO moves.

    Args:
        min_uso_move_pct: minimum absolute % USO move to include (default 2%)

    Returns:
        List of filtered HF row dicts, newest first.
    """
    total_fetched  = 0
    total_filtered = 0
    offset         = 0
    filtered       = []

    print(f"Fetching all posts from HuggingFace (filter: |USO 5m move| ≥ {min_uso_move_pct}%)…")

    while True:
        try:
            resp = requests.get(
                HF_ENDPOINT,
                params={
                    "dataset": HF_DATASET,
                    "config":  "default",
                    "split":   "train",
                    "offset":  offset,
                    "length":  100,
                },
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json().get("rows", [])
        except Exception as exc:
            print(f"  Error at offset {offset}: {exc} — retrying in 5s")
            time.sleep(5)
            continue

        if not rows:
            break

        total_fetched += len(rows)

        for row in rows:
            p          = row["row"]
            uso_before = p.get("uso_5min_before")
            uso_after  = p.get("uso_5min_after")
            text       = p.get("text", "")

            if not text or not uso_before or not uso_after or uso_before <= 0:
                continue

            pct_5m = ((uso_after - uso_before) / uso_before) * 100
            if abs(pct_5m) >= min_uso_move_pct:
                total_filtered += 1
                filtered.append(p)

        offset += 100
        if offset % 1000 == 0:
            print(f"  Scanned {total_fetched:,} posts, kept {total_filtered:,}…")

        # Polite rate limit
        time.sleep(0.1)

    print(f"Done. Scanned {total_fetched:,} total posts, kept {total_filtered:,} with |move| ≥ {min_uso_move_pct}%")
    return filtered


# ---------------------------------------------------------------------------
# yfinance price series
# ---------------------------------------------------------------------------

def fetch_uso_series(around_date: date, history_days: int, future_days: int) -> tuple[list[dict], list[dict]]:
    """Fetch USO price series before and after a given date.

    Args:
        around_date:  the date of the Trump post
        history_days: days of history before the post (for Migas input)
        future_days:  days of future prices after the post (for Migas target)

    Returns:
        (price_data, future_prices) — lists of {t, y_t} dicts
    """
    start = around_date - timedelta(days=history_days + 10)  # +10 buffer for weekends
    end   = around_date + timedelta(days=future_days  + 10)

    ticker = yf.Ticker(TICKER)
    hist   = ticker.history(start=start.isoformat(), end=end.isoformat())[["Close"]].reset_index()
    hist   = hist.dropna(subset=["Close"])

    rows = [
        {"t": str(row.Date.date()), "y_t": round(float(row.Close), 4)}
        for _, row in hist.iterrows()
    ]

    # Split on post date
    price_data    = [r for r in rows if date.fromisoformat(r["t"]) <= around_date][-history_days:]
    future_prices = [r for r in rows if date.fromisoformat(r["t"]) >  around_date][:future_days]

    return price_data, future_prices


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_dataset(min_move: float, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    posts = fetch_all_hf_posts(min_uso_move_pct=min_move)
    print(f"\nFetching USO price series for {len(posts)} posts via yfinance…")

    written  = 0
    skipped  = 0

    with out_path.open("w") as f:
        for i, p in enumerate(posts):
            date_str = p.get("date", "")
            if not date_str:
                skipped += 1
                continue

            try:
                post_date    = date.fromisoformat(date_str)
                uso_before   = p.get("uso_5min_before")
                uso_after    = p.get("uso_5min_after")
                uso_1hr      = p.get("uso_1hr_after")
                uso_pct_5m   = round(((uso_after - uso_before) / uso_before) * 100, 2) if uso_before and uso_after and uso_before > 0 else None
                uso_pct_1h   = round(((uso_1hr   - uso_before) / uso_before) * 100, 2) if uso_before and uso_1hr   and uso_before > 0 else None

                price_data, future_prices = fetch_uso_series(post_date, HISTORY_DAYS, FUTURE_DAYS)

                if len(price_data) < 20 or len(future_prices) < 5:
                    skipped += 1
                    continue

                record = {
                    "post_id":       str(p.get("post_id", "")),
                    "date":          date_str,
                    "time_et":       p.get("time_eastern", ""),
                    "text":          p.get("text", ""),
                    "url":           p.get("url", ""),
                    "uso_before":    uso_before,
                    "uso_after":     uso_after,
                    "uso_pct_5m":    uso_pct_5m,
                    "uso_pct_1h":    uso_pct_1h,
                    "during_market": p.get("during_market_hours", None),
                    "price_data":    price_data,
                    "future_prices": future_prices,
                }
                f.write(json.dumps(record) + "\n")
                written += 1

                if (i + 1) % 50 == 0:
                    print(f"  [{i+1}/{len(posts)}] Written {written}, skipped {skipped}")

                # Avoid hammering yfinance
                time.sleep(0.3)

            except Exception as exc:
                print(f"  Error on post {date_str}: {exc}")
                skipped += 1

    print(f"\nDataset saved → {out_path}")
    print(f"  Records written: {written}")
    print(f"  Records skipped: {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Migas fine-tuning dataset from Trump Truth Social posts")
    parser.add_argument("--min-move", type=float, default=2.0,
                        help="Minimum absolute USO 5-min move %% to include (default: 2.0)")
    parser.add_argument("--out", type=str, default=str(OUTPUT_DIR / "finetune_dataset.jsonl"),
                        help="Output JSONL path")
    args = parser.parse_args()

    build_dataset(min_move=args.min_move, out_path=Path(args.out))
