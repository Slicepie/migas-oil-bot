"""
Multi-market LLM scoring using Claude Haiku streaming.

Scores Trump / political posts across 4 core markets simultaneously.
Uses streaming so each market's signal is yielded as its JSON line
completes — no waiting for all 4 markets before dispatching.

Markets: OIL, CRYPTO, SP500, NATGAS

Public API:
  llm_score_multi_market(text)  → AsyncGenerator[(market, data)]
  llm_score_post(text)          → dict | None  (oil-only, backward compat)
  format_llm_followup(...)      → str          (Telegram message)
"""

import json
import logging
import os
from collections.abc import AsyncGenerator

import anthropic

log = logging.getLogger(__name__)

_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        _client = anthropic.AsyncAnthropic(api_key=api_key)
    return _client


# ---------------------------------------------------------------------------
# Markets config — used by caller to build prices_at_alert dict
# ---------------------------------------------------------------------------

MARKET_TICKERS: dict[str, str] = {
    "OIL":    "CL=F",
    "CRYPTO": "BTC-USD",
    "SP500":  "^GSPC",
    "NATGAS": "NG=F",
}

# Hardcoded per-theme expectations from v7 calibration — no runtime calculation needed
THEME_EXPECTATIONS: dict[str, dict] = {
    "IRAN_MILITARY":    {"est_move_pct": 1.0, "hold_window": "5m",  "hit_rate": 0.60},
    "IRAN_DIPLOMATIC":  {"est_move_pct": 1.0, "hold_window": "1h",  "hit_rate": 0.80},
    "HORMUZ":           {"est_move_pct": 1.0, "hold_window": "5m",  "hit_rate": 0.67},
    "OPEC":             {"est_move_pct": 1.5, "hold_window": "1h",  "hit_rate": None},
    "TARIFF_ESCALATION": {"est_move_pct": 0.5, "hold_window": "5m", "hit_rate": None},
    "TARIFF_RELIEF":    {"est_move_pct": 0.5, "hold_window": "5m",  "hit_rate": 0.67},
}

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

MULTI_MARKET_SYSTEM_PROMPT = """\
You are a quant analyst scoring Trump / political posts for their directional impact on 3 markets.

For each market output exactly one line in this format (no extra text):
MARKET:<NAME> {"score": <-5..+5>, "direction": "BULLISH"|"BEARISH"|"NEUTRAL", "confidence": "HIGH"|"MEDIUM"|"LOW", "est_move_pct": <float, 0 if NEUTRAL>, "rationale": "<1 sentence>"}

Markets to score in this exact order:
  OIL    (WTI crude, CL=F)
  CRYPTO (BTC-USD)
  SP500  (^GSPC)
  NATGAS (NG=F)

After all 4 market lines, output one META line:
META {"post_theme": "<THEME>", "primary_market": "<NAME>", "cross_market_consistency": "CONSISTENT"|"SPLIT"|"CONTRADICTORY", "ambiguity_flag": <bool>}

post_theme must be one of:
  IRAN_MILITARY | IRAN_DIPLOMATIC | FED_ATTACK | TARIFF_ESCALATION | TARIFF_RELIEF
  CHINA_TRADE | CRYPTO_POLICY | ENERGY_DOMESTIC | HORMUZ | OPEC | RUSSIA_UKRAINE | UNRELATED

primary_market = market with highest abs(score). ambiguity_flag = true when signals are CONTRADICTORY or OIL/SP500 score ≥ 3 in opposite directions.

━━━ CONFIDENCE CRITERIA — direction is the only signal that matters ━━━
HIGH:   Post explicitly names a specific action with a clear, direct market mechanism.
        Examples: "ceasefire signed with Iran", "bombing Iran tonight", "strategic bitcoin
        reserve confirmed by executive order", "25% tariff on all Chinese goods Monday",
        "Powell fired". A trader could act on direction alone with full conviction.

MEDIUM: Post implies likely action but lacks official confirmation or specific details.
        Examples: "talks going well with Iran", "considering tariffs on China",
        "thinking about replacing Powell". Direction probable but not locked in.

LOW:    Tangentially related, ambiguous, or indirect market impact.
        When in doubt between LOW and NEUTRAL → choose NEUTRAL.

Rule: fewer HIGH signals, better accuracy. Do not inflate confidence.

━━━ NEUTRAL DISCIPLINE — these MUST be NEUTRAL 0 on all 4 markets ━━━
  • Endorsements of any candidate or official
  • Personal attacks on opponents, media, judges
  • Rally speeches, patriotism posts, "America is winning" boasts
  • Vague forward-looking language with no named action ("very soon", "big announcement coming")
  • Domestic policy without named international trade/military component
  • Social commentary, religion, sports, entertainment
  • Polls, fundraising, event announcements

Only score when the post names a SPECIFIC action, rate, country, or instrument.
If no specific action is identifiable → ALL markets NEUTRAL 0.

━━━ DIRECTIONAL ANCHORS ━━━
OIL (CL=F — WTI crude):

  ★ HARD RULE — Iran posts are binary, no gradient ★
  ANY post about Iran escalation, aggression, military action, threats, strikes,
  Hormuz blockade, "maximum pressure", new sanctions on Iran, regime change rhetoric,
  or war framing → BULLISH +5, HIGH confidence, est ~3.0%

  ANY post about Iran peace, ceasefire, de-escalation, "talks going well",
  nuclear deal progress, suspending strikes, Iran cooperation, or diplomatic resolution
  → BEARISH -5, HIGH confidence, est ~3.0%

  Do not split hairs on Iran. There is no "mild" Iran post — every Iran-related post
  is either escalation (+5) or de-escalation (-5). This is the strongest signal we have.

  ★ HARD RULE — HORMUZ / Middle East shipping is binary ★
  Post about Strait of Hormuz, Houthis, Red Sea, Gulf of Aden, Bab el-Mandeb,
  Persian Gulf, "the Gulf" (assume Persian Gulf), Gulf of Oman, Suez Canal,
  tanker attacks, tanker seizure, naval blockade, mining shipping lanes,
  maritime threats, shipping disruption in Middle East, Yemen (Houthi context),
  freedom of navigation, oil transit routes, or any threat to energy shipping:

  Strait CLOSED / blockade / attack / threat / disruption / Houthis attacking ships
  → BULLISH +5, HIGH confidence, est ~3.0%  (supply chokepoint at risk)

  Strait OPEN / safe passage / shipping secured / Houthis defeated / maritime insurance /
  naval escort / trade route reopened / shipping protection guaranteed
  → BEARISH -5, HIGH confidence, est ~3.0%  (supply flows freely, risk premium drops)

  Theme = HORMUZ (not IRAN_MILITARY) when the post is primarily about shipping/strait/Houthis.
  Only use IRAN_MILITARY when Iran itself (government, military, nuclear) is the subject.

  Other OIL triggers (only when no Iran/Hormuz content present):
  - OPEC production cut announced → BULLISH +3, est ~1.5%
  - OPEC production increase → BEARISH -3, est ~1.5%
  - New US sanctions on Iran (if not already covered above) → BULLISH +3, est ~1.5%

  OIL must be NEUTRAL on:
  - Tariffs, China trade (no 1h impact on WTI)
  - Russia/Ukraine peace or war (Russian crude is already sanctioned, EU not US driver)
  - Fed policy / rate cuts (indirect, no 1h impact)
  - Domestic drilling / "drill baby drill" / energy dominance (production takes months)
  - Domestic politics, endorsements, social commentary

CRYPTO (BTC-USD):

  ★ HARD RULE — CRYPTO is directional by default ★
  BTC reacts to nearly every macro, geopolitical, fiscal, and monetary post.
  If ANY of the 3 other markets (OIL, SP500, NATGAS) are non-NEUTRAL, CRYPTO MUST
  also emit a direction — never NEUTRAL alongside another directional market.

  CRYPTO is NEUTRAL ONLY for these post types:
  - Pure domestic politics / endorsements / rally speeches
  - Personal attacks on opponents / media / judges
  - Social commentary, sports, religion, entertainment
  - Announcements with zero macro or geopolitical content
  Everything else → emit a direction.

  Direct crypto triggers (HIGH confidence, score ±4 to ±5):
  - Strategic Bitcoin reserve / executive order on crypto → BULLISH +5, est ~4.0%
  - New pro-crypto legislation (FIT21, market structure, SAB 121 repeal) → BULLISH +4, est ~3.0%
  - Stablecoin legislation / GENIUS Act / pro-stablecoin policy → BULLISH +4, est ~2.5%
  - Anti-CBDC stance / "no central bank digital dollar" → BULLISH +3, est ~2.0%
  - Anti-crypto regulatory action / SEC enforcement / restrictive law → BEARISH -4, est ~3.0%
  - Specific token mention (BTC, ETH, crypto company endorsement) → BULLISH +3, est ~2.0%

  Macro debasement triggers (HIGH confidence, score ±3 to ±4):
  - Fed money printing / QE / "print money" / balance sheet expansion → BULLISH +4, est ~2.5%
  - Rate cut pressure / "Powell must cut" / Fed attack / fire Powell → BULLISH +3, est ~2.0%
  - Rising US debt / debt ceiling raise / massive spending bill → BULLISH +3, est ~2.0%
  - Dollar weakness / "weak dollar good for exports" → BULLISH +3, est ~1.5%
  - Inflation acceptance / dismissing inflation concerns → BULLISH +2, est ~1.5%

  Geopolitical risk-off triggers (MEDIUM-HIGH confidence, score ±2 to ±4):
  - Iran/Hormuz military strike / bombing / attack → BEARISH -3, est ~2.0%
  - Major war escalation (any region) → BEARISH -3, est ~2.5%
  - Tariff escalation on China / major trade partner → BEARISH -3, est ~2.0%
  - Broad risk-off shock / flight to safety → BEARISH -3, est ~2.0%

  Geopolitical risk-on triggers (MEDIUM-HIGH confidence, score ±2 to ±4):
  - Iran ceasefire / peace deal / suspend strikes → BULLISH +3, est ~2.0%
  - Russia-Ukraine ceasefire / peace talks → BULLISH +3, est ~2.0%
  - Trade deal signed / tariff rollback → BULLISH +3, est ~2.0%
  - De-escalation of any major conflict → BULLISH +2, est ~1.5%

  When uncertain between directions on a complex macro post, default to
  the same direction as OIL or SP500's call on the same post. Never leave
  CRYPTO NEUTRAL while other markets are directional.

SP500 (^GSPC):
- Major tariff escalation (new rates named) → BEARISH -2 to -4, est ~0.4–1.0%
- Tariff relief / trade deal signed → BULLISH +2 to +4, est ~0.4–1.0%
- Iran/Hormuz military escalation → BEARISH -2 to -3, est ~0.4–0.8%
- Iran ceasefire / peace deal → BULLISH +1 to +3, est ~0.3–0.7%
- Fed attack / rate cut pressure → BULLISH +1 to +2, est ~0.2–0.5%
- Tax cuts / deregulation announcement → BULLISH +1 to +3, est ~0.3–0.7%

NATGAS (NG=F — highly sensitive to supply disruption and LNG policy):
- Sanctions on Russia / Nord Stream disruption → BULLISH +3 to +4, est ~2.0–3.5%
- Iran/Hormuz military tension (LNG route risk) → BULLISH +2 to +3, est ~1.5–2.5%
- LNG export terminal approval / energy dominance → BULLISH +2 to +3, est ~1.0–2.0%
- Iran ceasefire / Russia peace deal → BEARISH -2 to -3, est ~1.5–2.5%
- Domestic drilling expansion / permit approvals → BEARISH -1 to -2, est ~0.5–1.5%
- Unrelated posts → NEUTRAL 0  (NATGAS ignores tariffs and crypto policy)

Pure domestic politics, endorsements, social issues → NEUTRAL 0 on all 4 markets.
Output nothing except the 4 MARKET lines and 1 META line. No preamble, no explanation.
"""

# Oil-only prompt kept for backward compat
_OIL_SYSTEM_PROMPT = """\
You are an oil market analyst. Read a Trump Truth Social post and determine its impact on WTI crude oil prices.

Ground truth anchors:
- "Open the Strait of Hormuz" (military threat) → BULLISH (+4 to +5)
- "Ceasefire with Iran / peace deal / suspend bombing" → BEARISH (-4 to -5)
- "Iran agreed to talks / deal close" → BEARISH (-2 to -4)
- "Bombing Iran, destroying power plants" → BULLISH (+3 to +5)
- "Drill baby drill / open up drilling / energy dominance" → mildly BEARISH (-1 to -2)
- "Sanctions on Iran/Russia" → BULLISH (+2 to +3)
- Pure political endorsements, domestic issues, tariffs (non-oil) → NEUTRAL (0)

Respond ONLY with a JSON object:
{
  "direction": "BULLISH" | "BEARISH" | "NEUTRAL",
  "score": <integer -5 to +5>,
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "reason": "<1-2 sentences>",
  "key_signals": ["<phrase1>", "<phrase2>"]
}
"""


# ---------------------------------------------------------------------------
# Multi-market streaming scorer
# ---------------------------------------------------------------------------

async def llm_score_multi_market(text: str) -> AsyncGenerator[tuple[str, dict], None]:
    """
    Stream multi-market LLM scoring. Yields (market_name, data) tuples as each
    market line completes (no waiting for all 8). Final yield is ("_meta", {...}).

    market_name values: "OIL" | "GOLD" | "CRYPTO" | "SP500" | "NASDAQ" |
                        "BONDS" | "USD" | "NATGAS" | "_meta"

    Yields nothing on parse failure — caller should handle missing markets.
    Never raises — logs warnings and returns cleanly on any error.
    """
    try:
        client = _get_client()
        buffer = ""

        async with client.messages.stream(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=MULTI_MARKET_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": f"Post:\n\n{text}"}],
        ) as stream:
            async for chunk in stream.text_stream:
                buffer += chunk
                # Dispatch completed lines immediately
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue

                    if line.startswith("MARKET:"):
                        # Format: MARKET:OIL {"score": ...}
                        space = line.find(" ")
                        if space == -1:
                            continue
                        market = line[7:space]  # strip "MARKET:"
                        json_part = line[space + 1:]
                        try:
                            # Strip invalid JSON `+` prefix on positive numbers: +4 → 4
                            import re as _re
                            clean = _re.sub(r':\s*\+(\d)', r': \1', json_part)
                            data = json.loads(clean)
                            yield (market, data)
                        except json.JSONDecodeError:
                            log.warning("llm_score: bad market JSON for %s: %s", market, json_part[:120])

                    elif line.startswith("META "):
                        try:
                            meta = json.loads(line[5:])
                            yield ("_meta", meta)
                        except json.JSONDecodeError:
                            log.warning("llm_score: bad META JSON: %s", line[5:120])

            # Flush any remaining buffer after stream ends
            for line in buffer.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith("MARKET:"):
                    space = line.find(" ")
                    if space == -1:
                        continue
                    market = line[7:space]
                    json_part = line[space + 1:]
                    try:
                        import re as _re
                        yield (market, json.loads(_re.sub(r':\s*\+(\d)', r': \1', json_part)))
                    except json.JSONDecodeError:
                        pass
                elif line.startswith("META "):
                    try:
                        yield ("_meta", json.loads(line[5:]))
                    except json.JSONDecodeError:
                        pass

    except Exception as exc:
        log.warning("llm_score_multi_market failed: %s", exc)


# ---------------------------------------------------------------------------
# Oil-only backward-compat wrapper
# ---------------------------------------------------------------------------

async def llm_score_post(text: str) -> dict | None:
    """
    Oil-only score for backward compatibility with existing bot.py calls.
    Calls the multi-market scorer and returns the OIL market block, reformatted
    to match the original {direction, score, confidence, reason, key_signals} shape.
    Falls back to single-market call if multi-market scorer fails.
    """
    oil_data = None
    try:
        async for market, data in llm_score_multi_market(text):
            if market == "OIL":
                oil_data = data
                break  # got what we need, caller can collect rest separately
    except Exception:
        pass

    if oil_data:
        return {
            "direction":   oil_data.get("direction", "NEUTRAL"),
            "score":       oil_data.get("score", 0),
            "confidence":  oil_data.get("confidence", "LOW"),
            "reason":      oil_data.get("rationale", ""),
            "key_signals": [],          # multi-market scorer doesn't emit per-market key_signals
        }

    # Fallback: direct oil-only call
    try:
        client = _get_client()
        msg = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=_OIL_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": f"Post:\n\n{text}"}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.warning("llm_score_post fallback failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Telegram formatting
# ---------------------------------------------------------------------------

def format_llm_followup(post_text: str, kw_score: int, llm: dict, price: float | None) -> str:
    """Format the Telegram follow-up message with LLM analysis (oil signal)."""
    direction  = llm.get("direction", "NEUTRAL")
    score      = llm.get("score", 0)
    reason     = llm.get("reason", "")
    confidence = llm.get("confidence", "")
    signals    = llm.get("key_signals", [])

    dir_emoji  = "📈" if direction == "BULLISH" else ("📉" if direction == "BEARISH" else "⚪")
    conf_emoji = "🟢" if confidence == "HIGH" else ("🟡" if confidence == "MEDIUM" else "🔴")

    kw_agrees  = (kw_score > 0 and score > 0) or (kw_score < 0 and score < 0) or (kw_score == 0 and score == 0)
    divergence = "" if kw_agrees else f"\n⚠️ *Diverges from keyword score* (`{kw_score:+d}`)"

    price_line   = f"\n💵 WTI at alert: `${price:.2f}`" if price else ""
    signals_line = f"\n🏷 {' · '.join(f'`{s}`' for s in signals[:4])}" if signals else ""

    return (
        f"🤖 *LLM Analysis*\n"
        f"\n"
        f"{dir_emoji} *{direction}* `{score:+d}` {conf_emoji} {confidence}"
        f"{divergence}"
        f"{price_line}"
        f"\n\n_{reason}_"
        f"{signals_line}"
    )


def format_multi_market_alert(markets: dict[str, dict], meta: dict, prices: dict[str, float | None]) -> str:
    """
    Format a multi-market signal as a Telegram message.

    markets: {market_name: {score, direction, confidence, est_move_pct, rationale}}
    meta:    {post_theme, primary_market, cross_market_consistency, ambiguity_flag}
    prices:  {market_name: float | None}
    """
    theme    = meta.get("post_theme", "UNKNOWN")
    primary  = meta.get("primary_market", "")
    consist  = meta.get("cross_market_consistency", "")
    ambig    = meta.get("ambiguity_flag", False)

    consist_emoji = {"CONSISTENT": "🟢", "SPLIT": "🟡", "CONTRADICTORY": "🔴"}.get(consist, "⚪")
    ambig_line    = "\n⚠️ *Ambiguous signal — verify before trading*" if ambig else ""

    lines = [f"🌐 *Multi-Market Signal* — `{theme}`{ambig_line}\n"]

    market_order = ["OIL", "CRYPTO", "SP500", "NATGAS"]
    market_labels = {
        "OIL":    "🛢 Oil",
        "CRYPTO": "₿ BTC",
        "SP500":  "📊 S&P",
        "NATGAS": "🔥 NatGas",
    }

    for m in market_order:
        d = markets.get(m)
        if not d:
            continue
        sc   = d.get("score", 0)
        dirn = d.get("direction", "NEUTRAL")
        conf = d.get("confidence", "LOW")
        move = d.get("est_move_pct")

        if sc == 0 and dirn == "NEUTRAL":
            continue  # skip neutral markets to keep message short

        dir_emoji  = "📈" if dirn == "BULLISH" else ("📉" if dirn == "BEARISH" else "⚪")
        conf_emoji = "🟢" if conf == "HIGH" else ("🟡" if conf == "MEDIUM" else "🔴")
        label      = market_labels.get(m, m)
        star       = " ⭐" if m == primary else ""
        price_str  = f"  `${prices[m]:.2f}`" if prices.get(m) else ""
        move_str   = f"  ~{move:+.1f}%" if move else ""

        lines.append(f"{dir_emoji} *{label}*{star}  `{sc:+d}` {conf_emoji}{price_str}{move_str}")

    lines.append(f"\n{consist_emoji} {consist}")
    return "\n".join(lines)
