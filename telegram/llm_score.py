"""
LLM-based oil sentiment scoring using Claude.

Used as a second-pass after keyword detection — understands context that
keyword matching misses (e.g. "suspend bombing" is bearish, not bullish).
"""

import json
import logging
import os

import anthropic

log = logging.getLogger(__name__)

_client: anthropic.AsyncAnthropic | None = None

def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        _client = anthropic.AsyncAnthropic(api_key=api_key)
    return _client


SYSTEM_PROMPT = """\
You are an oil market analyst. Your job is to read a Trump Truth Social post and determine its impact on WTI crude oil prices.

Ground truth anchors you must internalize:
- "Open the Strait of Hormuz" (military threat) → strongly BULLISH (+4 to +5)
- "Ceasefire with Iran / peace deal / suspend bombing" → strongly BEARISH (-4 to -5)
- "Iran agreed to talks / deal close" → BEARISH (-2 to -4)
- "Bombing Iran, destroying power plants" → BULLISH (+3 to +5)
- "Drill baby drill / open up drilling / energy dominance" → mildly BEARISH (-1 to -2, more supply)
- "Sanctions on Iran/Russia" → BULLISH (+2 to +3)
- Pure political endorsements, domestic issues, tariffs (non-oil) → NEUTRAL (0)

Respond ONLY with a JSON object, no other text:
{
  "direction": "BULLISH" | "BEARISH" | "NEUTRAL",
  "score": <integer -5 to +5>,
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "reason": "<1-2 sentences explaining the oil price impact>",
  "key_signals": ["<phrase1>", "<phrase2>"]
}
"""


async def llm_score_post(text: str) -> dict | None:
    """
    Score a Trump post using Claude Haiku for speed.

    Returns dict with keys: direction, score, confidence, reason, key_signals
    Returns None on failure (never throws — caller decides fallback behavior).
    """
    try:
        client = _get_client()
        msg = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": f"Post:\n\n{text}"}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.warning("llm_score_post failed: %s", exc)
        return None


def format_llm_followup(post_text: str, kw_score: int, llm: dict, price: float | None) -> str:
    """Format the Telegram follow-up message with LLM analysis."""
    direction = llm.get("direction", "NEUTRAL")
    score     = llm.get("score", 0)
    reason    = llm.get("reason", "")
    confidence = llm.get("confidence", "")
    signals   = llm.get("key_signals", [])

    dir_emoji = "📈" if direction == "BULLISH" else ("📉" if direction == "BEARISH" else "⚪")
    conf_emoji = "🟢" if confidence == "HIGH" else ("🟡" if confidence == "MEDIUM" else "🔴")

    kw_agrees = (kw_score > 0 and score > 0) or (kw_score < 0 and score < 0) or (kw_score == 0 and score == 0)
    divergence = "" if kw_agrees else f"\n⚠️ *Diverges from keyword score* (`{kw_score:+d}`)"

    price_line = f"\n💵 WTI at alert: `${price:.2f}`" if price else ""

    signals_line = ""
    if signals:
        signals_line = f"\n🏷 {' · '.join(f'`{s}`' for s in signals[:4])}"

    return (
        f"🤖 *LLM Analysis*\n"
        f"\n"
        f"{dir_emoji} *{direction}* `{score:+d}` {conf_emoji} {confidence}"
        f"{divergence}"
        f"{price_line}"
        f"\n\n_{reason}_"
        f"{signals_line}"
    )
