"""
News aggregator for Migas Oil Bot.

Pulls Trump Truth Social posts via Apify, scores them for oil price impact,
and builds a structured summary for Migas-1.5.

Signal scoring: -5 (strongly bearish for oil) to +5 (strongly bullish for oil)
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
APIFY_ACTOR_ID  = "muhammetakkurtt~truth-social-scraper"
APIFY_ENDPOINT  = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"

CACHE_FILE      = Path(__file__).parent / "posts_cache.json"
CACHE_TTL_HOURS = 6   # refresh full history every 6 hours

# ---------------------------------------------------------------------------
# Signal definitions — Trump's actual language mapped to oil price impact
# Score: +5 = strongly bullish (price up), -5 = strongly bearish (price down)
# ---------------------------------------------------------------------------

SIGNALS = [

    # -------------------------------------------------------------------------
    # IRAN — MILITARY / BOMBING (strongly bullish)
    # -------------------------------------------------------------------------
    (["bomb iran", "bombing iran", "bombed iran",
      "strike iran", "striking iran", "struck iran",
      "attack iran", "attacking iran", "attacked iran",
      "hit iran", "hitting iran",
      "military action against iran",
      "take out iran", "wipe out iran",
      "destroy iran", "obliterate iran",
      "iran will be hit", "iran is next",
      "iranian nuclear site", "nuclear facility",
      "take out their nuclear"], +5, "🔴 Iran military strike"),

    # -------------------------------------------------------------------------
    # HORMUZ — CLOSED / BLOCKED (strongly bullish)
    # -------------------------------------------------------------------------
    (["strait of hormuz", "hormuz closed", "hormuz blocked",
      "hormuz threatened", "close hormuz", "block hormuz",
      "ships attacked", "tankers attacked", "oil tankers seized",
      "tanker seized", "vessels attacked", "ships seized",
      "red sea attack", "houthi attack", "houthis attacked",
      "houthis struck", "shipping lane", "blockade",
      "persian gulf attack", "gulf attack",
      "oil flow stopped", "oil supply cut"], +5, "🔴 Hormuz/shipping threat"),

    # -------------------------------------------------------------------------
    # IRAN — ESCALATION / SANCTIONS TIGHTENING (bullish)
    # -------------------------------------------------------------------------
    (["maximum pressure", "sanctions on iran", "sanction iran",
      "iranian sanctions", "tighten sanctions", "new sanctions",
      "iran sanctions", "cut off iran", "isolate iran",
      "iran proxies", "proxy war", "irgc", "revolutionary guard",
      "ayatollah", "khamenei", "tehran must",
      "iran responsible", "iran behind", "iran funded",
      "hezbollah", "hamas funded by iran",
      "iran enriching", "iran nuclear program",
      "iran nukes", "iran weapons",
      "iran 60 days", "iran deadline",
      "iran deal is dead", "no deal with iran"], +3, "🟠 Iran escalation"),

    # -------------------------------------------------------------------------
    # WAR / CONFLICT ESCALATION (bullish)
    # -------------------------------------------------------------------------
    (["world war", "major war", "war is coming",
      "troops deployed", "sending troops",
      "military force", "use of force",
      "nuclear threat", "nuclear war",
      "escalate", "escalation",
      "conflict expanding", "war expanding",
      "attack on israel", "israel attacked",
      "middle east war", "regional war",
      "drone attack", "missile attack",
      "explosion", "explosions"], +2, "🟠 War escalation"),

    # -------------------------------------------------------------------------
    # SAUDI / OPEC — PRODUCTION CUT (bullish)
    # -------------------------------------------------------------------------
    (["opec cut", "production cut", "cutting production",
      "reduce output", "opec reduce",
      "saudi cut", "saudis cutting",
      "less oil", "lower production",
      "restrict supply", "supply cut"], +2, "🟠 OPEC/Saudi cut"),

    # -------------------------------------------------------------------------
    # VENEZUELA / OTHER SANCTIONS (mildly bullish)
    # -------------------------------------------------------------------------
    (["sanction venezuela", "venezuela sanctions",
      "maduro sanctions", "cut off venezuela",
      "sanction russia oil", "russian oil ban",
      "ban russian oil", "russian energy sanctions"], +2, "🟡 Supply sanctions"),

    # -------------------------------------------------------------------------
    # GENERAL ENERGY PRICES UP / BULLISH HINTS
    # -------------------------------------------------------------------------
    (["energy prices", "gas prices going up",
      "oil prices rising", "higher energy",
      "drill restrictions", "stop drilling",
      "cancel permits", "block pipeline",
      "keystone blocked"], +1, "🟡 Energy price pressure"),

    # -------------------------------------------------------------------------
    # IRAN — DEAL / SANCTIONS LIFTED (strongly bearish)
    # -------------------------------------------------------------------------
    (["deal with iran", "iran deal", "nuclear deal",
      "iranian deal", "iran agreement", "iran accord",
      "lift sanctions on iran", "lifting iran sanctions",
      "remove iran sanctions", "end iran sanctions",
      "iran sanctions relief", "iran sanctions waiver",
      "iran back to deal", "new iran deal",
      "negotiate with iran", "iran negotiations",
      "iran talks", "talks with iran",
      "meeting with iran", "iran meeting",
      "iran open to deal", "iran willing",
      "iran compromise", "iran concession",
      "we made a deal with iran",
      "great deal with iran", "beautiful deal with iran",
      "iran signed", "iran agreed"], -5, "🟢 Iran deal/sanctions lift"),

    # -------------------------------------------------------------------------
    # RUSSIA / UKRAINE — PEACE / CEASEFIRE (bearish)
    # -------------------------------------------------------------------------
    (["ceasefire", "cease fire", "peace deal",
      "peace agreement", "peace talks",
      "end the war", "stop the war", "war is over",
      "war ending", "war ended",
      "ukraine deal", "deal with ukraine",
      "deal with russia", "russia deal",
      "russia agreement", "met with putin",
      "meeting with putin", "putin agreed",
      "zelensky agreed", "ukraine agreed",
      "truce", "armistice",
      "we made peace", "peace in ukraine",
      "great peace deal", "beautiful peace",
      "negotiations going well", "talks going well",
      "close to a deal", "almost a deal",
      "russia ukraine solved"], -4, "🟢 Russia/Ukraine peace"),

    # -------------------------------------------------------------------------
    # HORMUZ — OPEN / SAFE (bearish)
    # -------------------------------------------------------------------------
    (["hormuz open", "hormuz safe", "hormuz secured",
      "shipping lanes open", "ships moving freely",
      "houthis stopped", "houthis defeated",
      "houthis surrendered", "red sea safe",
      "tankers safe", "oil flowing",
      "supply restored", "supply secured"], -3, "🟢 Hormuz/shipping safe"),

    # -------------------------------------------------------------------------
    # IRAN — DE-ESCALATION (bearish)
    # -------------------------------------------------------------------------
    (["iran backing down", "iran retreating",
      "iran standing down", "iran agreed",
      "iran complying", "iran cooperating",
      "iran stopped enriching", "iran paused",
      "iran freeze", "iran halt",
      "good news from iran", "iran progress"], -3, "🟢 Iran de-escalation"),

    # -------------------------------------------------------------------------
    # DRILL / SUPPLY EXPANSION (bearish)
    # -------------------------------------------------------------------------
    (["drill baby drill", "drill, baby, drill",
      "liquid gold", "unleash energy",
      "energy dominance", "energy independence",
      "drill everywhere", "open up drilling",
      "approve pipeline", "keystone approved",
      "lng exports", "lng terminals",
      "fracking approved", "fracking open",
      "more oil", "increase production",
      "flood the market", "pump more oil",
      "opec increase", "saudi increase output",
      "more supply", "energy abundance",
      "lower gas prices", "bring prices down",
      "lower oil prices", "cheap energy"], -2, "🟢 Supply expansion"),

    # -------------------------------------------------------------------------
    # SPR RELEASE (bearish)
    # -------------------------------------------------------------------------
    (["strategic reserve", "spr release", "release spr",
      "release from reserves", "tap reserves",
      "strategic petroleum"], -2, "🟢 SPR release"),

]

# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

def score_post(text: str) -> tuple[int, list[str]]:
    """Score a post for oil price impact.

    Returns:
        score: integer from -5 to +5
        matched_signals: list of signal labels that matched
    """
    text_lower = text.lower()
    total_score = 0
    matched     = []

    for phrases, score, label in SIGNALS:
        for phrase in phrases:
            if phrase in text_lower:
                total_score += score
                matched.append(label)
                break   # only count each signal once per post

    # Clamp to [-5, +5]
    total_score = max(-5, min(5, total_score))
    return total_score, list(set(matched))


def score_emoji(score: int) -> str:
    if score >= 4:  return "🔴🔴"
    if score >= 2:  return "🔴"
    if score >= 1:  return "🟠"
    if score == 0:  return "⚪"
    if score >= -1: return "🟡"
    if score >= -3: return "🟢"
    return "🟢🟢"


# ---------------------------------------------------------------------------
# Apify scraper
# ---------------------------------------------------------------------------

def fetch_trump_posts(
    max_posts: int = 500,
    use_last_post_id: bool = True,
) -> list[dict]:
    """Fetch Trump Truth Social posts via Apify.

    Args:
        max_posts: 500 covers ~2 months of posting history
        use_last_post_id: True for incremental polling, False for full history pull
    """
    if not APIFY_API_TOKEN:
        log.error("APIFY_API_TOKEN not set")
        return []

    try:
        resp = requests.post(
            APIFY_ENDPOINT,
            params={"token": APIFY_API_TOKEN},
            json={
                "username":      "realDonaldTrump",
                "maxPosts":      max_posts,
                "useLastPostId": use_last_post_id,
                "cleanContent":  True,
                "onlyReplies":   False,
                "onlyMedia":     False,
            },
            timeout=120,
        )
        resp.raise_for_status()
        posts = resp.json()
        log.info("Apify returned %d Trump posts", len(posts))
        return posts

    except Exception as exc:
        log.error("Apify fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Post cache — full history pulled once, refreshed every CACHE_TTL_HOURS
# ---------------------------------------------------------------------------

def _cache_age_hours() -> float:
    """Return age of cache in hours, or infinity if cache doesn't exist."""
    if not CACHE_FILE.exists():
        return float("inf")
    try:
        data = json.loads(CACHE_FILE.read_text())
        fetched_at = datetime.fromisoformat(data["fetched_at"])
        age = datetime.now(timezone.utc) - fetched_at
        return age.total_seconds() / 3600
    except Exception:
        return float("inf")


def _read_cache() -> list[dict]:
    """Read scored posts from cache file. Returns [] if missing or corrupt."""
    try:
        data = json.loads(CACHE_FILE.read_text())
        return data.get("scored_posts", [])
    except Exception:
        return []


def _write_cache(scored_posts: list[dict]) -> None:
    """Write scored posts to cache with current timestamp."""
    data = {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "scored_posts": scored_posts,
    }
    CACHE_FILE.write_text(json.dumps(data, indent=2))
    log.info("Cache written: %d scored posts → %s", len(scored_posts), CACHE_FILE)


def refresh_cache() -> list[dict]:
    """Pull full 2-month history from Apify, score all posts, write cache.

    Called once on bot startup and then every CACHE_TTL_HOURS by a background job.
    Returns the scored posts list.
    """
    log.info("Refreshing Trump post cache (full 2-month pull)…")
    posts = fetch_trump_posts(max_posts=500, use_last_post_id=False)
    scored = _score_raw_posts(posts)
    _write_cache(scored)
    return scored


def append_new_posts(new_raw_posts: list[dict]) -> list[dict]:
    """Score new incremental posts and merge into cache (no duplicates).

    Called by the 5-min polling job with useLastPostId=True results.
    Returns the updated full scored list.
    """
    if not new_raw_posts:
        return _read_cache()

    existing = _read_cache()
    existing_texts = {p["text"] for p in existing}

    new_scored = _score_raw_posts(new_raw_posts)
    added = [p for p in new_scored if p["text"] not in existing_texts]

    if added:
        merged = added + existing  # newest first
        merged.sort(key=lambda x: abs(x["score"]), reverse=True)
        _write_cache(merged)
        log.info("Appended %d new scored posts to cache", len(added))
        return merged

    return existing


def _score_raw_posts(raw_posts: list[dict]) -> list[dict]:
    """Score a list of raw Apify post dicts. Returns only non-zero scored posts."""
    scored = []
    for p in raw_posts:
        text = p.get("content", p.get("text", ""))
        if not text:
            continue
        score, signals = score_post(text)
        if score != 0:
            scored.append({
                "text":    text,
                "score":   score,
                "signals": signals,
                "url":     p.get("url", ""),
            })
    scored.sort(key=lambda x: abs(x["score"]), reverse=True)
    return scored


def get_scored_posts() -> list[dict]:
    """Return scored posts from cache, refreshing if stale or missing."""
    if _cache_age_hours() >= CACHE_TTL_HOURS:
        return refresh_cache()
    cached = _read_cache()
    if not cached:
        return refresh_cache()
    log.info("Using cached posts (%d scored, %.1fh old)", len(cached), _cache_age_hours())
    return cached


def get_relevant_trump_posts() -> list[str]:
    """Return list of oil-relevant Trump post texts (for backward compat)."""
    # Incremental pull — only new posts since last check
    new_raw = fetch_trump_posts(max_posts=50, use_last_post_id=True)
    if not new_raw:
        return []
    updated = append_new_posts(new_raw)
    # Return only the newly added texts (for alerting)
    existing_before = {p["text"] for p in _read_cache()} - {p["text"] for p in _score_raw_posts(new_raw)}
    return [p["text"] for p in _score_raw_posts(new_raw) if p["text"] not in existing_before]


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def build_live_summary(current_price: float, instrument: str = "wti") -> tuple[str, dict]:
    """Build a Migas-ready summary from live Trump signals."""
    label = "WTI" if instrument == "wti" else "Brent"
    sensitivity = (
        "sensitive to US domestic production and SPR policy"
        if instrument == "wti"
        else "highly sensitive to OPEC+ decisions, Iran supply risk, and Strait of Hormuz disruptions"
    )

    scored_posts = get_scored_posts()  # reads from cache — no Apify call
    net_score    = sum(p["score"] for p in scored_posts)
    top_posts    = scored_posts[:5]

    # Net signal interpretation
    if net_score >= 5:
        net_label = "STRONGLY BULLISH — major supply risk signals dominate"
    elif net_score >= 2:
        net_label = "BULLISH — geopolitical risk premium supported"
    elif net_score >= 1:
        net_label = "MILDLY BULLISH — slight upward pressure"
    elif net_score == 0:
        net_label = "NEUTRAL — mixed signals"
    elif net_score >= -2:
        net_label = "MILDLY BEARISH — slight downward pressure"
    elif net_score >= -4:
        net_label = "BEARISH — de-escalation signals dominate"
    else:
        net_label = "STRONGLY BEARISH — major supply increase or deal signals"

    factual_lines = [
        f"{label} crude oil is currently trading at ${current_price:.2f}/barrel. "
        f"This benchmark is {sensitivity}.",
        f"",
        f"Trump Truth Social signal analysis (last 2 months, {len(scored_posts)} relevant posts):",
        f"Net oil signal: {score_emoji(net_score)} {net_signal_text(net_score)} (score: {net_score:+d})",
    ]

    if top_posts:
        factual_lines.append("")
        factual_lines.append("Top signals:")
        for p in top_posts:
            emoji  = score_emoji(p["score"])
            labels = ", ".join(p["signals"])
            factual_lines.append(
                f"{emoji} [{p['score']:+d}] {labels}: \"{p['text'][:200]}\""
            )

    predictive_lines = [
        f"Based on Trump's recent posts, the political signal is {net_label}. "
        f"Ongoing Middle East conflict maintains a geopolitical risk premium. "
        f"Any Strait of Hormuz disruption would be strongly bullish. "
        f"A confirmed Iran deal or Russia/Ukraine ceasefire would be sharply bearish."
    ]

    summary = (
        "FACTUAL SUMMARY:\n" + "\n".join(factual_lines) +
        "\n\nPREDICTIVE SIGNALS:\n" + " ".join(predictive_lines)
    )

    sources = {
        "trump_posts":    len(scored_posts),
        "net_score":      net_score,
        "net_label":      net_label,
    }

    return summary, sources


def net_signal_text(score: int) -> str:
    if score >= 4:  return "STRONGLY BULLISH"
    if score >= 2:  return "BULLISH"
    if score >= 1:  return "MILDLY BULLISH"
    if score == 0:  return "NEUTRAL"
    if score >= -1: return "MILDLY BEARISH"
    if score >= -3: return "BEARISH"
    return "STRONGLY BEARISH"
