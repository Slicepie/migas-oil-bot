"""
Signal accuracy tracker for Migas Oil Bot.

Every time an alert fires (volume spike, Trump post, auto-forecast),
we log it with the current price and schedule follow-up checks at
15min, 1hr, and 24hr to record whether the signal was correct.

Stored in signal_log.jsonl — one JSON record per line, never deleted.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

log = logging.getLogger(__name__)

LOG_FILE = Path(__file__).parent / "signal_log.jsonl"


def _current_wti() -> float | None:
    """Fetch current WTI price from Hyperliquid xyz:CL (24/7).
    Falls back to Yahoo CL=F if Hyperliquid unavailable.
    """
    # Primary: Hyperliquid
    try:
        import requests
        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "allMids", "dex": "xyz"},
            timeout=3,
        )
        if resp.status_code == 200:
            mids = resp.json()
            price_str = mids.get("xyz:CL")
            if price_str:
                return round(float(price_str), 2)
    except Exception:
        pass
    # Fallback: bot module
    try:
        from bot import fetch_price_with_fallback
        price = fetch_price_with_fallback("CL=F")
        if price:
            return round(price, 2)
    except (ImportError, Exception):
        pass
    # Last resort: plain Yahoo
    try:
        ticker = yf.Ticker("CL=F")
        hist   = ticker.history(period="1d", interval="5m")[["Close"]]
        if not hist.empty:
            return round(float(hist["Close"].iloc[-1]), 2)
    except Exception as exc:
        log.warning("tracker: failed to fetch CL=F price: %s", exc)
    return None


def log_signal(
    signal_type: str,       # "trump_post" | "volume_spike" | "auto_forecast"
    direction: str,         # "LONG" | "SHORT" | "NEUTRAL"
    score: int,             # signal score at time of alert
    price_at_alert: float,  # WTI price when alert fired
    post_text: str = "",    # Trump post text (if applicable)
    extra: dict | None = None,
) -> str:
    """Log a new signal. Returns the signal_id for follow-up checks."""
    now       = datetime.now(timezone.utc)
    signal_id = f"{signal_type}_{now.strftime('%Y%m%d_%H%M%S')}"

    record = {
        "signal_id":       signal_id,
        "signal_type":     signal_type,
        "direction":       direction,
        "score":           score,
        "price_at_alert":  price_at_alert,
        "post_text":       post_text[:300],
        "fired_at":        now.isoformat(),
        "checks":          {},   # filled in by follow_up()
        "extra":           extra or {},
    }

    with LOG_FILE.open("a") as f:
        f.write(json.dumps(record) + "\n")

    log.info("Signal logged: %s %s score=%+d price=%.2f", signal_id, direction, score, price_at_alert)
    return signal_id


def follow_up(signal_id: str, window: str, price_now: float) -> None:
    """Record the actual price at a follow-up window (15m, 1h, 24h).

    Reads the log, finds the signal by ID, adds the check result, rewrites.
    """
    if not LOG_FILE.exists():
        return

    lines   = LOG_FILE.read_text().splitlines()
    updated = []
    for line in lines:
        try:
            record = json.loads(line)
        except Exception:
            updated.append(line)
            continue

        if record.get("signal_id") == signal_id:
            price_at   = record["price_at_alert"]
            direction  = record["direction"]
            pct_change = round((price_now - price_at) / price_at * 100, 2)
            correct    = (
                (direction == "LONG"  and pct_change > 0) or
                (direction == "SHORT" and pct_change < 0)
            )
            record["checks"][window] = {
                "price":      price_now,
                "pct_change": pct_change,
                "correct":    correct,
            }
            updated.append(json.dumps(record))
        else:
            updated.append(line)

    LOG_FILE.write_text("\n".join(updated) + "\n")
    log.info("Follow-up %s for %s: price=%.2f", window, signal_id, price_now)


def get_accuracy_report() -> dict:
    """Compute hit rates across all logged signals.

    Returns:
    {
        "total": int,
        "by_type": {"trump_post": {...}, "volume_spike": {...}, ...},
        "by_window": {"15m": {"total": n, "correct": n, "hit_rate": 0.xx}, ...},
        "recent": [last 5 signals with outcomes]
    }
    """
    if not LOG_FILE.exists():
        return {"total": 0, "by_type": {}, "by_window": {}, "recent": []}

    records = []
    for line in LOG_FILE.read_text().splitlines():
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except Exception:
            continue

    windows   = ["15m", "1h", "24h"]
    by_window = {w: {"total": 0, "correct": 0} for w in windows}
    by_type   = {}

    for r in records:
        stype = r.get("signal_type", "unknown")
        if stype not in by_type:
            by_type[stype] = {w: {"total": 0, "correct": 0} for w in windows}

        for w in windows:
            check = r.get("checks", {}).get(w)
            if check:
                by_window[w]["total"]   += 1
                by_type[stype][w]["total"] += 1
                if check.get("correct"):
                    by_window[w]["correct"]    += 1
                    by_type[stype][w]["correct"] += 1

    # Compute hit rates
    for w in windows:
        t = by_window[w]["total"]
        by_window[w]["hit_rate"] = round(by_window[w]["correct"] / t, 2) if t else None
    for stype in by_type:
        for w in windows:
            t = by_type[stype][w]["total"]
            by_type[stype][w]["hit_rate"] = round(by_type[stype][w]["correct"] / t, 2) if t else None

    recent = sorted(records, key=lambda x: x.get("fired_at", ""), reverse=True)[:5]

    return {
        "total":     len(records),
        "by_window": by_window,
        "by_type":   by_type,
        "recent":    recent,
    }


def format_accuracy_report() -> str:
    """Format accuracy report as a Telegram message."""
    r = get_accuracy_report()
    if r["total"] == 0:
        return "📊 *Signal Tracker*\n\nNo signals logged yet — waiting for first alert."

    lines = [f"📊 *Signal Accuracy Report* ({r['total']} signals)\n"]

    # Overall by window
    lines.append("*Overall hit rates:*")
    for w in ["15m", "1h", "24h"]:
        d = r["by_window"][w]
        if d["total"] == 0:
            lines.append(f"  {w}: no data yet")
        else:
            hr  = d["hit_rate"]
            bar = "🟢" if hr >= 0.65 else ("🟡" if hr >= 0.50 else "🔴")
            lines.append(f"  {w}: {bar} `{hr:.0%}` ({d['correct']}/{d['total']})")

    # By signal type
    lines.append("")
    lines.append("*By signal type:*")
    type_labels = {"trump_post": "Trump post", "volume_spike": "Volume spike", "auto_forecast": "Auto-forecast"}
    for stype, data in r["by_type"].items():
        label = type_labels.get(stype, stype)
        parts = []
        for w in ["15m", "1h", "24h"]:
            t = data[w]["total"]
            if t:
                parts.append(f"{w}: {data[w]['hit_rate']:.0%} ({t})")
        if parts:
            lines.append(f"  {label}: {' | '.join(parts)}")

    # Recent signals
    lines.append("")
    lines.append("*Recent signals:*")
    for sig in r["recent"]:
        fired  = sig.get("fired_at", "")[:16].replace("T", " ")
        stype  = type_labels.get(sig.get("signal_type"), sig.get("signal_type"))
        dirn   = sig.get("direction", "")
        score  = sig.get("score", 0)
        checks = sig.get("checks", {})
        outcomes = []
        for w in ["15m", "1h", "24h"]:
            c = checks.get(w)
            if c:
                mark = "✅" if c["correct"] else "❌"
                outcomes.append(f"{w}: {mark} {c['pct_change']:+.1f}%")
        outcome_str = "  ".join(outcomes) if outcomes else "pending"
        lines.append(f"  [{fired}] {stype} {dirn} `{score:+d}` — {outcome_str}")

    return "\n".join(lines)
