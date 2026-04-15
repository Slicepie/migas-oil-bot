---
name: usoil
description: Query live oil-market intelligence from usoil.ai — Trump Truth Social posts scored for oil impact, real-time WTI signals, Hyperliquid crude prices, volume spikes, and trade ideas. Use this skill whenever the user asks about oil, WTI, crude, OPEC, Iran/Hormuz, Trump's latest posts on energy, or wants a trading view on oil markets.
---

# usoil — Oil Market Intelligence

A free skill connecting to the **usoil.ai** public API. Every Trump Truth Social post is scored by an LLM for oil-market impact, combined with real-time Hyperliquid WTI price and volume data, and served as structured JSON.

## When to use this skill

Invoke whenever the user's question involves:

- Trump posts about oil, Iran, OPEC, Hormuz, Venezuela, sanctions, energy
- "What's moving WTI / crude / oil today?"
- "Should I long / short oil right now?"
- Unusual volume on CL (crude futures)
- Recent Hyperliquid WTI price
- Market bias or directional read on oil

Do **not** invoke for equity, crypto, nat-gas, or non-oil questions.

## Configuration

The skill calls a REST API. Base URL default:

```
http://103.196.86.91:34412
```

Users can override with `USOIL_API_BASE` env var. All endpoints are free, no auth. Rate-limited at 30 requests/minute per IP.

## Endpoints

All return JSON. CORS open.

### GET /api/v1/posts/recent

Scored Trump Truth Social posts in the last N hours.

**Query params:**
- `hours` (default 24, max 720)
- `min_score` (default 0) — filter by `|score| >= min_score`

**Response:**
```json
{
  "window_hours": 24,
  "count": 3,
  "posts": [
    {
      "ts": "2026-04-15T17:42:11+00:00",
      "date": "2026-04-15",
      "time_et": "13:42:11",
      "text": "Iran will pay a heavy price...",
      "score": -4,
      "signals": ["iran", "threat"],
      "url": "https://truthsocial.com/@realDonaldTrump/...",
      "confirmed": true,
      "uso_pct_5m": 1.8,
      "uso_pct_1h": 2.3
    }
  ]
}
```

Score range: `-5` (max bearish oil) to `+5` (max bullish oil). `confirmed: true` means the historical USO ETF moved ≥ 0.5% within 5 min of the post.

### GET /api/v1/market/bias

Aggregate directional bias from recent live signals.

**Query params:**
- `market` (default `OIL`, also supports `CRYPTO`, `SP500`, `NATGAS` if scored)
- `hours` (default 6, max 168)

**Response:**
```json
{
  "market": "OIL",
  "window_hours": 6,
  "signal_count": 2,
  "bullish": 0,
  "bearish": 2,
  "avg_score": -3.5,
  "direction": "BEARISH",
  "latest_ts": "2026-04-15T17:42:11+00:00"
}
```

`direction` is one of `BULLISH` / `BEARISH` / `NEUTRAL` / `NO_DATA`.

### GET /api/v1/market/price

Live Hyperliquid perp mid price (10s cached).

**Query params:**
- `symbol` (default `CL`, also `WTI`, `BRENT`, `BZ`)

**Response:**
```json
{
  "symbol": "CL",
  "hl_symbol": "xyz:CL",
  "price": 72.34,
  "source": "hyperliquid",
  "ts": "2026-04-15T17:42:11+00:00"
}
```

### GET /api/v1/volume/spikes

Unusual volume events detected on CME CL futures (≥ 4× hourly baseline) or Hyperliquid WTI perp (≥ $5M delta in 1 min).

**Query params:**
- `hours` (default 24, max 720)

**Response:**
```json
{
  "window_hours": 24,
  "threshold": "4.0x baseline (CME) / $5M delta (HL)",
  "count": 1,
  "spikes": [
    {
      "signal_id": "vol_1728392814",
      "ts": "2026-04-15T14:22:09+00:00",
      "source": "cme",
      "ratio": 5.2,
      "volume": 8420,
      "baseline": 1620,
      "notional_m": 609,
      "price": 71.88,
      "post_age_min": 138.0,
      "insider_flag": true
    }
  ]
}
```

`insider_flag: true` means no Trump post in the 15 min before the spike — possibly institutional / informed flow.

### POST /api/v1/trade/idea

Returns a trade recommendation based on the most recent live signal for a market. Returns `status: "no_signal"` or `status: "neutral"` when there's nothing actionable.

**Body:**
```json
{ "market": "OIL", "size_usd": 50 }
```

Both fields optional. `size_usd` clamped to [10, 10000].

**Response (actionable):**
```json
{
  "market": "OIL",
  "status": "idea",
  "direction": "BEARISH",
  "score": -4,
  "confidence": "HIGH",
  "size_usd": 50,
  "entry_hint": 72.34,
  "stop_loss": 73.79,
  "take_profit": 70.18,
  "hold_window": "2–6h",
  "hit_rate": 0.63,
  "post_theme": "IRAN_MILITARY",
  "rationale": "Trump threatens strike on Iran; escalation unlikely short-term...",
  "signal_ts": "2026-04-15T17:42:11+00:00",
  "execute_link": "https://app.hyperliquid.xyz/trade/CL",
  "disclaimer": "Informational only. Not investment advice. Execute at your own risk."
}
```

**This skill never executes trades.** It returns an idea; the user decides and executes manually on Hyperliquid (or any broker).

## Helper scripts

Shell helpers in `scripts/` wrap the endpoints with sensible defaults:

- `scripts/recent_posts.sh [hours]` — prints today's scored Trump posts
- `scripts/market_bias.sh [hours]` — current OIL bias
- `scripts/market_price.sh [symbol]` — live HL price
- `scripts/volume_spikes.sh [hours]` — recent volume events
- `scripts/trade_idea.sh [size_usd]` — latest trade recommendation

## Example interactions

**"What has Trump posted about oil today?"**
→ Call `/api/v1/posts/recent?hours=24&min_score=2`, summarize top 3 posts with their score and theme. Cite the URL of the most impactful one.

**"What's WTI doing right now?"**
→ Call `/api/v1/market/price` for the live mid, then `/api/v1/market/bias?hours=6` for directional read. One-line synthesis: "WTI trading $72.34 on Hyperliquid; last 6h signal bias is mildly bearish (-1.5 avg across 2 signals)."

**"Should I long oil?"**
→ Call `/api/v1/trade/idea`. If `status: idea`, surface direction + SL/TP. Always repeat the disclaimer that the user executes themselves. If `status: neutral` or `no_signal`, say so — don't invent a direction.

**"Anything weird on crude today?"**
→ Call `/api/v1/volume/spikes?hours=24`. Highlight any `insider_flag: true` entries — those are the interesting ones.

## Scoring model notes

- **Score** (`-5` to `+5`): LLM (Claude Haiku 4.5) judges each Trump post's likely impact on oil prices. Positive = bullish (supply disruption, sanctions tightening). Negative = bearish (diplomacy, supply release).
- **Direction**: `BULLISH` / `BEARISH` / `NEUTRAL` — maps from score sign.
- **Confidence**: `HIGH` / `MEDIUM` / `LOW` — LLM's self-rated certainty.
- **Theme**: `IRAN_MILITARY`, `HORMUZ_THREAT`, `OPEC_COORDINATION`, `VENEZUELA_SANCTIONS`, etc. — used for historical hit-rate lookup.

## Not included in free tier

- Real-time signal streaming (SSE) — available on Pro plan at [usoil.ai/pricing](https://usoil.ai/pricing)
- Trade execution on Hyperliquid — always user-operated; this skill returns ideas only
- Backtesting / historical signal export — Pro plan

## Limits

- 30 requests/minute per IP (shared across all endpoints)
- No authentication required
- Endpoint may return 503 if upstream (Hyperliquid) is temporarily unavailable — retry after 10s
