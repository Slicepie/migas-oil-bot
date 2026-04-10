"""
News aggregator for Migas Oil Bot.

Historical data: HuggingFace dataset chrissoria/trump-truth-social
  — free, 32k+ posts, includes pre-computed USO oil price reactions
  — updated daily, covers all of Trump's second term

Real-time alerts: Apify Truth Social scraper (incremental, 5-min polling)
  — only used for new posts not yet in the HF dataset

Signal scoring: -5 (strongly bearish for oil) to +5 (strongly bullish for oil)
Ground truth anchor: Mar 23 2026 Iran peace post → -6.97% USO in 5 minutes = score -5
"""

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HuggingFace dataset — primary historical source
# ---------------------------------------------------------------------------
HF_ENDPOINT  = "https://datasets-server.huggingface.co/rows"
HF_DATASET   = "chrissoria/trump-truth-social"
HF_DAYS      = 60   # how far back to pull for the Migas summary window

# ---------------------------------------------------------------------------
# Apify — real-time incremental polling only
# ---------------------------------------------------------------------------
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
    # Ground truth patches from Apr 3-4 2026 posts:
    #   "Our Military hasn't even started destroying what's left in Iran. Bridges next, then Electric Power Plants."
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
      "take out their nuclear",
      # Apr 3-4 2026 patches
      "destroying what's left in iran",
      "destroy what's left in iran",
      "hasn't even started",           # "hasn't even started destroying" context
      "bridges next",                  # "Bridges next, then Electric Power Plants" — escalation
      "electric power plants",         # bombing Iranian infrastructure
      # Apr 5 2026 patch — "Tuesday will be Power Plant Day, and Bridge Day, all wrapped up in one, in Iran"
      "power plant day",               # Trump's new naming convention for bombing campaigns
      "bridge day",                    # "Bridge Day, all wrapped up in one, in Iran"
      "bomb tehran", "bombing tehran", "strike tehran", "hit tehran",
      "destroy tehran", "obliterate tehran",
      "military action against tehran",
      "tehran will be hit", "tehran is next"], +5, "🔴 Iran military strike"),

    # -------------------------------------------------------------------------
    # HORMUZ — CLOSED / BLOCKED / SEIZED (strongly bullish)
    # Ground truth patches from Apr 3-4 2026 posts:
    #   "OPEN THE HORMUZ STRAIT, TAKE THE OIL, & MAKE A FORTUNE"
    #   "Remember when I gave Iran ten days to MAKE A DEAL or OPEN UP THE HORMUZ STRAIT"
    # Note: "open the hormuz strait" here = military seizure, NOT diplomatic (bullish)
    # -------------------------------------------------------------------------
    (["strait of hormuz", "hormuz closed", "hormuz blocked",
      "hormuz threatened", "close hormuz", "block hormuz",
      "ships attacked", "tankers attacked", "oil tankers seized",
      "tanker seized", "vessels attacked", "ships seized",
      "red sea attack", "houthi attack", "houthis attacked",
      "houthis struck", "shipping lane", "blockade",
      "persian gulf attack", "gulf attack",
      "oil flow stopped", "oil supply cut",
      # Apr 3-4 2026 patches — aggressive seizure framing
      "hormuz strait",                 # Trump reverses word order vs "strait of hormuz"
      "open the hormuz strait",        # military coercion context (paired with "take the oil")
      "open up the hormuz strait",
      "take the oil",                  # seizing Iranian oil
      "keep the oil",                  # occupying Iranian oil fields
      "seize the oil",
      "take their oil",
      "we take the oil",
      "10 days to make a deal",
      "ten days to make a deal",
      "48 hours",                      # "48 hours before all Hell will reign down on them"
      "all hell will reign",
      "hell will reign",
      "make a deal or",                # ultimatum framing — bullish threat
      "days or else",
      "iran deadline",
      "gusher",
      # Apr 5 2026 patch — "Open the Fuckin' Strait, you crazy bastards, or you'll be living in Hell"
      "open the strait",               # Trump drops "Hormuz" in casual threats — still means Hormuz
      "open the fuckin",               # "Open the Fuckin' Strait" — unmistakable Hormuz coercion
      "open the f*ckin",               # censored variant
      "open that strait",
      "living in hell",                # ultimatum threat
      "crazy bastards",
      # Apr 10 2026 patch — indirect Hormuz reference
      "using international waterway",  # "extortion of the World by using International Waterways"
      "using international waterways"], +5, "🔴 Hormuz/shipping threat"),

    # -------------------------------------------------------------------------
    # OIL / GAS INFRASTRUCTURE ATTACKS (strongly bullish)
    # Ground truth: Mar 19 2026 — Israel hits South Pars Gas Field in Iran
    # Any attack on oil/gas production = immediate supply disruption signal
    # -------------------------------------------------------------------------
    (["south pars",                          # Iran's largest gas field
      "south pars gas field",
      "oil field in iran", "gas field in iran",
      "oil field attack", "gas field attack",
      "oil infrastructure", "gas infrastructure",
      "refinery attack", "refinery hit", "refinery struck",
      "oil facility", "gas facility",
      "oil platform attack", "rig attack",
      "aramco attack", "aramco hit", "aramco struck",
      "sabotage oil", "sabotage pipeline",
      "pipeline exploded", "pipeline attack",
      "oil terminal attack"], +5, "🔴 Oil/gas infrastructure attack"),

    # -------------------------------------------------------------------------
    # IRAN — ESCALATION / SANCTIONS TIGHTENING (bullish)
    # Ground truth patches Mar 2026:
    #   "Iranian Terror State" / "terrorist regime of Iran"  → +3
    #   "finished off what's left of the Iranian Terror State" → +3
    #   "state sponsor of terror / putting them out of business" → +3
    #   "evil Empire, Iran, from having Nuclear Weapons" → +3
    #   "so called 'Strait?'" (Trump's oblique Hormuz reference) → +3
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
      "iran deal is dead", "no deal with iran",
      # Deadline/ultimatum language
      "days to make a deal",
      "hours to comply",
      "last chance for iran",
      "last warning to iran",
      # Mar 2026 patches — Trump's actual language for Iran escalation
      "terrorist regime of iran", "terrorist regime",
      "iranian terror state", "terror state",
      "state sponsor of terror",
      "finish off iran", "finishing off iran", "finished off",
      "putting iran out of business", "putting them out of business",
      "evil empire",                         # "stopping an evil Empire, Iran"
      "from having nuclear weapons",         # "stopping Iran from having Nuclear Weapons"
      "iran nuclear weapons", "nuclear weapons program",
      "so called strait", "so called straight",  # Trump's oblique Hormuz reference
      "be responsible for the strait",
      "be responsible for the straight",
      # War-continues / no-deal language (fixes "no deal with iran" false match)
      "no deal with iran",                   # explicit rejection of deal = war continues
      "no deal with tehran",
      "unconditional surrender",             # "no deal except unconditional surrender"
      "will not make a deal",
      "not making a deal with iran",
      "beat to hell",                        # "Iran is being beat to HELL"
      "being destroyed",                     # "Iran is being destroyed"
      "being decimated",
      # Apr 7 2026 patches — regime-change / civilisation-ending language
      "regime change",                       # "Complete and Total Regime Change"
      "total regime change",
      "complete and total regime",
      "47 years of",                         # "47 years of extortion, corruption"
      "will die tonight",                    # "a whole civilization will die tonight"
      "civilization will die",
      "never to be brought back",
      "god bless the great people of iran",
      "great people of iran",
      "people of iran",                      # sympathetic framing = regime falling
      "iran will be free",
      "free iran",
      "liberate iran",
      # Apr 10 2026 patches — coercion / intimidation language
      "only reason they are alive",          # "only reason they are alive today is to negotiate"
      "have no cards",                       # "Iranians don't seem to realize they have no cards"
      "no cards",
      "extortion of the world",              # "extortion of the World by using International Waterways"
      "international waterway",
      "international waterways"], +3, "🟠 Iran escalation"),

    # -------------------------------------------------------------------------
    # WAR / CONFLICT ESCALATION (bullish)
    # Ground truth: Mar 7 2026 — UK aircraft carriers Middle East (+10.97% USO day)
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
      "explosion", "explosions",
      # Mar 2026 patches
      "aircraft carriers to the middle east",
      "carriers to the middle east",
      "aircraft carrier to the middle east",
      "sending carriers",
      "naval forces to the",
      "warships to the middle east"], +2, "🟠 War escalation"),

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
    # Ground truth: Mar 23 2026 — "VERY GOOD AND PRODUCTIVE CONVERSATIONS WITH
    # TEHRAN REGARDING A TOTAL RESOLUTION OF HOSTILITIES" → oil -10–15%
    # Key lesson: Trump says "Tehran" not "Iran", uses "conversations" not "talks"
    # -------------------------------------------------------------------------
    (["iran deal", "nuclear deal",
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
      "iran signed", "iran agreed",
      # Tehran-specific (Trump uses "Tehran" not "Iran" in diplomatic posts)
      "conversations with tehran", "talks with tehran",
      "meeting with tehran", "tehran agreed",
      "tehran responded", "tehran reached out",
      "tehran wants", "tehran willing",
      "message from tehran", "letter from tehran",
      "tehran has agreed", "tehran signed",
      # Diplomatic language patterns (Mar 23 2026 event)
      "resolution of hostilities",
      "total resolution",
      "end of hostilities",
      "end hostilities",
      "pause strikes",
      "pausing strikes",
      "halt strikes",
      "halting strikes",
      "ceasefire with iran",
      "ceasefire with tehran",
      "productive conversations",   # Trump's signature phrase for positive outcomes
      "very good and productive",
      "great and productive",
      "iran wants peace",
      "tehran wants peace",
      "iran reached out",
      "tehran reached out",
      "iran is ready",
      "tehran is ready",
      "iran is open",
      "progress with iran",
      "progress with tehran",
      "great progress on iran",
      "breakthrough with iran",
      "breakthrough with tehran"], -5, "🟢 Iran deal/sanctions lift"),

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
    # IRAN — DEFEATED / WAR ENDING (bearish)
    # Ground truth patches Mar 2026 — as Iran gets destroyed, supply risk resolves
    #   "blown Iran off the map...their navy and air force are dead...want to make a deal" → -6.97% USO
    #   "death of Iran" → -6.97% USO
    #   "Iran totally defeated and wants a deal" → -3.14% USO
    #   "Iran's Navy is gone, Air Force is no longer" → -3.05% USO
    #   "JUST LIKE IRAN ITSELF, THOSE PLANS ARE NOW DEAD" → -3.14% USO
    #   "Militarily ineffective and weak" → -3.14% USO
    # Key insight: Iran being destroyed = war is ENDING = oil supply risk resolves = bearish
    # -------------------------------------------------------------------------
    (["death of iran",
      "iran is dead", "iran itself is dead",
      "iran is defeated", "iran is totally defeated",
      "totally defeated iran", "totally defeated and wants",
      "iran is gone", "iran is finished", "iran is no more",
      "iran is done",
      "iran's navy is gone", "their navy is gone",
      "navy and air force are dead", "navy is dead",
      "iran's air force is gone", "air force is no longer",
      "their air force is gone", "air force are dead",
      "iran has no defense", "they have no defense", "absolutely no defense",
      "iran's military is gone", "iran has no military",
      "iran is militarily dead", "militarily ineffective",
      "iran is weak", "iran is helpless",
      "blown iran off the map",            # "we've blown Iran off the map"
      "blown off the map",
      "iran's plans are dead", "plans are now dead",
      "just like iran itself",             # "JUST LIKE IRAN ITSELF, THOSE PLANS ARE NOW DEAD"
      "iran wants to make a deal",         # Iran seeking exit = pre-cursor to deal
      "iran wants a deal",
      "they want to make a deal",          # Trump referring to Iran wanting out
      "wants to make a deal",
      "iran is seeking a deal",
      "iran is begging",
      "iran is on its knees",
      "iran's leadership is gone",
      "leadership is gone",
      "iran's missiles are gone", "their missiles are gone",
      "iran's drones are gone",
      "we are winning against iran",       # war progressing toward end
      "iran is losing",
      "iran has lost",
      # Winding-down language (Mar 20 2026: "winding down our great Military efforts in Iran")
      "winding down our military",
      "winding down military",
      "wind down our military",
      "winding down in iran",
      "winding down the war",
      "meeting our objectives",            # "close to meeting objectives" = war ending
      "mission accomplished",
      "mission is accomplished",
      # Iran surrender language
      "iran has surrendered", "iran surrendered",
      "iran apologized and surrendered",
      "apologized and surrendered",
      "iran is surrendering",
      "iran capitulated", "iran has capitulated",
      "iran waved the white flag",
      "iran gave up"], -3, "🟢 Iran defeated/war ending"),

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
      "liquid gold", "unleash american energy",
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

OIL_TOPICS = [
        "iran", "iranian", "tehran", "hormuz", "strait of hormuz",
        "russia", "russian", "ukraine", "ukrainian", "moscow",
        "opec", "saudi", "aramco", "riyadh",
        "oil", "crude", "petroleum", "barrel", "brent", "wti",
        "gas prices", "gasoline", "fuel",
        "war", "warfare", "military strike", "airstrike", "air strike",
        "bomb", "bombing", "missile", "missiles", "rocket",
        "attack", "attacked", "invade", "invasion",
        "sanction", "sanctions", "embargo",
        "pipeline", "refinery", "tanker",
        "ceasefire", "cease fire", "peace deal", "peace talks",
        "nuclear", "enrichment",
        "drill baby drill", "drill everywhere", "open up drilling",
        "lng", "liquefied natural gas",
        "venezuela", "caracas",
        "israel", "hamas", "hezbollah", "gaza",
        "strait", "blockade", "waterway", "waterways",
        "negotiate", "negotiation", "negotiations",
]


def is_oil_topic(text: str) -> bool:
    """Return True if the post text matches any oil-relevant topic keyword."""
    text_lower = text.lower()
    return any(topic in text_lower for topic in OIL_TOPICS)


def score_post(text: str) -> tuple[int, list[str]]:
    """Score a post for oil price impact.

    Returns:
        score: integer from -5 to +5
        matched_signals: list of signal labels that matched

    Returns (0, []) immediately if the post is not oil-market relevant.
    """
    if not is_oil_topic(text):
        return 0, []

    text_lower = text.lower()

    # ── Direction scoring ─────────────────────────────────────────────────────
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
# HuggingFace dataset fetch — free historical source with USO price reactions
# ---------------------------------------------------------------------------

def fetch_posts_hf(days: int = HF_DAYS) -> list[dict]:
    """Fetch Trump posts from HuggingFace dataset for the last N days.

    Free — no API key. Includes pre-computed USO oil ETF price reactions
    (5min before/after, 1hr after) for every post. Updated daily.

    Returns raw HF row dicts (newest first).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date()
    posts  = []
    offset = 0

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
            if not rows:
                break

            done = False
            for row in rows:
                p = row["row"]
                date_str = p.get("date", "")
                if not date_str:
                    continue
                try:
                    post_date = date.fromisoformat(date_str)
                except ValueError:
                    continue
                if post_date < cutoff:
                    done = True
                    break
                posts.append(p)

            if done:
                break
            offset += 100

        except Exception as exc:
            log.error("HuggingFace fetch failed at offset %d: %s", offset, exc)
            break

    log.info("HuggingFace returned %d posts (last %d days)", len(posts), days)
    return posts


# ---------------------------------------------------------------------------
# Apify — real-time incremental polling only (new posts not yet in HF)
# ---------------------------------------------------------------------------

def fetch_posts_apify_incremental(max_posts: int = 50) -> list[dict]:
    """Fetch only new Trump posts via Apify (useLastPostId=True).

    Used for the 5-minute real-time alert polling.
    Returns raw Apify post dicts.
    """
    if not APIFY_API_TOKEN:
        log.warning("APIFY_API_TOKEN not set — real-time polling disabled")
        return []

    try:
        resp = requests.post(
            APIFY_ENDPOINT,
            params={"token": APIFY_API_TOKEN},
            json={
                "username":     "realDonaldTrump",
                "maxPosts":     max_posts,
                "cleanContent": True,
                "onlyReplies":  False,
                "onlyMedia":    False,
            },
            timeout=60,
        )
        resp.raise_for_status()
        posts = resp.json()
        log.info("Apify incremental poll returned %d new posts", len(posts))
        return posts

    except Exception as exc:
        log.error("Apify incremental poll failed: %s", exc)
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
    """Pull 60-day history from HuggingFace, score all posts, write cache.

    Called once on bot startup and then every CACHE_TTL_HOURS by a background job.
    HuggingFace is free and includes pre-computed USO price reactions.
    Returns the scored posts list.
    """
    log.info("Refreshing Trump post cache from HuggingFace…")
    posts  = fetch_posts_hf(days=HF_DAYS)
    scored = _score_raw_posts(posts)
    _write_cache(scored)
    return scored


def append_new_posts(new_raw_posts: list[dict]) -> list[dict]:
    """Score new incremental posts and merge into cache (no duplicates).

    Called by the 5-min polling job with Apify real-time results.
    Returns the updated full scored list.
    """
    if not new_raw_posts:
        return _read_cache()

    existing       = _read_cache()
    existing_texts = {p["text"] for p in existing}

    new_scored = _score_raw_posts(new_raw_posts)
    added      = [p for p in new_scored if p["text"] not in existing_texts]

    if added:
        merged = added + existing
        # Confirmed posts (real USO data) rank above unconfirmed (keyword-only)
        merged.sort(key=lambda x: abs(x.get("uso_pct_5m") or 0), reverse=True)
        _write_cache(merged)
        log.info("Appended %d new scored posts to cache", len(added))
        return merged

    return existing


def _score_raw_posts(raw_posts: list[dict]) -> list[dict]:
    """Score a list of raw posts (HF or Apify format). Returns only non-zero scored posts.

    HF rows include USO price reaction data; Apify rows do not.
    Output schema is normalised across both sources.

    Ranking priority:
      1. Confirmed posts (have USO data, |move| >= 0.5%) — ranked by |uso_pct_5m|
      2. Unconfirmed posts (keyword-only, e.g. new Apify posts) — ranked by |score|
    """
    scored = []
    for p in raw_posts:
        text = p.get("text", p.get("content", ""))
        if not text:
            continue
        score, signals = score_post(text)
        if score == 0:
            continue

        uso_before = p.get("uso_5min_before")
        uso_after  = p.get("uso_5min_after")
        uso_1hr    = p.get("uso_1hr_after")
        uso_pct_5m = None
        uso_pct_1h = None
        if uso_before and uso_after and uso_before > 0:
            uso_pct_5m = round(((uso_after - uso_before) / uso_before) * 100, 2)
        if uso_before and uso_1hr and uso_before > 0:
            uso_pct_1h = round(((uso_1hr   - uso_before) / uso_before) * 100, 2)

        # A post is "confirmed" if it has an actual USO market reaction >= 0.5%
        confirmed = uso_pct_5m is not None and abs(uso_pct_5m) >= 0.5

        # Normalise date/time — HF posts have "date"/"time_eastern",
        # Apify posts have "created_at" (UTC ISO string)
        raw_date    = p.get("date", "")
        raw_time_et = p.get("time_eastern", "")
        if not raw_date and p.get("created_at"):
            try:
                import pytz as _pytz
                _utc_dt = datetime.fromisoformat(p["created_at"].replace("Z", "+00:00"))
                _et_dt  = _utc_dt.astimezone(_pytz.timezone("America/New_York"))
                raw_date    = _et_dt.strftime("%Y-%m-%d")
                raw_time_et = _et_dt.strftime("%H:%M:%S")
            except Exception:
                pass

        scored.append({
            "text":       text,
            "score":      score,
            "signals":    signals,
            "url":        p.get("url", ""),
            "date":       raw_date,
            "time_et":    raw_time_et,
            "uso_pct_5m": uso_pct_5m,
            "uso_pct_1h": uso_pct_1h,
            "confirmed":  confirmed,
        })

    # Confirmed first (by |uso_pct_5m|), then unconfirmed (by |score|)
    confirmed_posts   = sorted([p for p in scored if     p["confirmed"]], key=lambda x: abs(x["uso_pct_5m"]), reverse=True)
    unconfirmed_posts = sorted([p for p in scored if not p["confirmed"]], key=lambda x: abs(x["score"]),      reverse=True)
    return confirmed_posts + unconfirmed_posts


def get_scored_posts() -> list[dict]:
    """Return scored posts from cache, refreshing if stale or missing."""
    if _cache_age_hours() >= CACHE_TTL_HOURS:
        return refresh_cache()
    cached = _read_cache()
    if not cached:
        return refresh_cache()
    log.info("Using cached posts (%d scored, %.1fh old)", len(cached), _cache_age_hours())
    return cached


def get_relevant_trump_posts() -> list[dict]:
    """Poll for new posts via Apify (real-time), append to cache, return new oil-relevant posts.

    Returns list of scored post dicts: {text, score, signals, uso_pct_5m, confirmed, ...}
    Sorted by |score| descending so the caller can act on the strongest signal first.
    """
    existing_texts = {p["text"] for p in _read_cache()}
    new_raw        = fetch_posts_apify_incremental()
    log.info("Apify returned %d raw posts; %d already in cache",
             len(new_raw), sum(1 for p in new_raw if p.get("text", p.get("content","")) in existing_texts))
    if not new_raw:
        return []
    append_new_posts(new_raw)
    new_scored = _score_raw_posts(new_raw)
    relevant   = [p for p in new_scored if p["text"] not in existing_texts]
    log.info("New oil-relevant posts after scoring: %d", len(relevant))
    return relevant


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def build_live_summary(current_price: float, instrument: str = "wti") -> tuple[str, dict]:
    """Build a Migas-ready summary from live Trump signals.

    Uses actual USO % price moves (from HuggingFace) as the primary signal for
    historical posts. Falls back to keyword scoring for new posts without price data.
    FinBERT receives concrete financial language ('USO fell 6.97%') rather than
    abstract invented scores.
    """
    label = "WTI" if instrument == "wti" else "Brent"
    sensitivity = (
        "sensitive to US domestic production and SPR policy"
        if instrument == "wti"
        else "highly sensitive to OPEC+ decisions, Iran supply risk, and Strait of Hormuz disruptions"
    )

    all_posts   = get_scored_posts()
    confirmed   = [p for p in all_posts if p.get("confirmed")]
    unconfirmed = [p for p in all_posts if not p.get("confirmed")]

    # --- Net signal: primary from confirmed USO moves, secondary from keyword scores ---
    avg_move = (sum(p["uso_pct_5m"] for p in confirmed) / len(confirmed)) if confirmed else 0.0
    max_post = max(confirmed, key=lambda x: abs(x["uso_pct_5m"])) if confirmed else None
    max_move = max_post["uso_pct_5m"] if max_post else 0.0
    kw_score = sum(p["score"] for p in unconfirmed)   # keyword signal for unconfirmed posts

    # Net label: confirmed moves take precedence; keyword signal breaks ties
    if avg_move <= -3 or (avg_move <= -1 and kw_score <= -3):
        net_label = "STRONGLY BEARISH — confirmed oil sell-offs dominate"
    elif avg_move <= -1 or kw_score <= -3:
        net_label = "BEARISH — de-escalation or deal signals dominate"
    elif avg_move >= 3 or (avg_move >= 1 and kw_score >= 3):
        net_label = "STRONGLY BULLISH — confirmed oil spikes dominate"
    elif avg_move >= 1 or kw_score >= 3:
        net_label = "BULLISH — geopolitical risk premium signals"
    elif kw_score >= 1:
        net_label = "MILDLY BULLISH — unconfirmed escalation signals"
    elif kw_score <= -1:
        net_label = "MILDLY BEARISH — unconfirmed de-escalation signals"
    else:
        net_label = "NEUTRAL — no significant oil signals"

    lines = [
        f"{label} crude oil is currently trading at ${current_price:.2f}/barrel. "
        f"This benchmark is {sensitivity}.",
        "",
        "TRUMP TRUTH SOCIAL — OIL PRICE SIGNALS (last 60 days):",
    ]

    # --- Confirmed section: actual USO % moves ---
    top_confirmed = confirmed[:5]
    if top_confirmed:
        lines.append("")
        lines.append(f"CONFIRMED MARKET REACTIONS ({len(confirmed)} posts with observed USO price data):")
        for p in top_confirmed:
            uso_5m = p["uso_pct_5m"]
            uso_1h = p.get("uso_pct_1h")
            dt     = p.get("date", "")
            te     = (p.get("time_et") or "").strip()
            timing = f"{dt} {te}".strip()
            move   = f"USO {uso_5m:+.2f}% in 5 min"
            if uso_1h is not None:
                move += f", {uso_1h:+.2f}% in 1 hr"
            lines.append(f'  [{timing}] {move}: "{p["text"][:220]}"')

        direction = "bearish" if avg_move < 0 else "bullish"
        lines.append(f"  Average confirmed USO move: {avg_move:+.2f}% ({direction} across {len(confirmed)} posts)")
        lines.append(f"  Largest single move: {max_move:+.2f}%")
    else:
        lines.append("")
        lines.append("CONFIRMED MARKET REACTIONS: none available in current window.")

    # --- Unconfirmed section: new posts without price data yet ---
    top_unconfirmed = unconfirmed[:4]
    if top_unconfirmed:
        lines.append("")
        lines.append("UNCONFIRMED SIGNALS (recent posts, price reaction not yet observed):")
        for p in top_unconfirmed:
            sc       = p["score"]
            strength = "strongly" if abs(sc) >= 4 else ("moderately" if abs(sc) >= 2 else "mildly")
            dirn     = "bullish" if sc > 0 else "bearish"
            labels   = ", ".join(p["signals"])
            lines.append(f'  [{strength} {dirn} — {labels}]: "{p["text"][:200]}"')

    lines += [
        "",
        f"NET ASSESSMENT: {net_label}.",
    ]

    predictive = (
        f"Based on Trump's recent Truth Social posts, the net oil price signal is {net_label}. "
        f"The largest confirmed price reaction was {max_move:+.2f}% in USO (oil ETF). "
        f"Ongoing Middle East tensions maintain a geopolitical risk premium. "
        f"A confirmed Iran deal or Strait of Hormuz resolution would be strongly bearish for oil. "
        f"Any military escalation, oil infrastructure attack, or Hormuz blockade threat would be strongly bullish."
    )

    summary = "FACTUAL SUMMARY:\n" + "\n".join(lines) + "\n\nPREDICTIVE SIGNALS:\n" + predictive

    sources = {
        "trump_posts":  len(all_posts),
        "confirmed":    len(confirmed),
        "unconfirmed":  len(unconfirmed),
        "avg_move":     round(avg_move, 2),
        "max_move":     round(max_move, 2),
        "net_label":    net_label,
    }

    return summary, sources


def build_live_summary_override(current_price: float, trigger_post: dict, instrument: str = "wti") -> tuple[str, dict]:
    """Build a Migas summary where the triggering post overrides the 60-day net label.

    Used for Forecast B (A/B test) when |score| >= 4.
    The current post IS the regime — historical average is just context.
    """
    # Get the standard summary first
    summary, sources = build_live_summary(current_price, instrument)

    score    = trigger_post.get("score", 0)
    text     = trigger_post.get("text", "")
    signals  = ", ".join(trigger_post.get("signals", []))
    strength = "STRONGLY BULLISH" if score >= 4 else ("STRONGLY BEARISH" if score <= -4 else net_signal_text(score))
    dirn     = "bullish — expect oil price spike" if score > 0 else "bearish — expect oil price drop"

    override_block = (
        f"\n\nREGIME OVERRIDE — EXTREME POST DETECTED (score {score:+d}):\n"
        f'Trump just posted: "{text[:300]}"\n'
        f"Signals: {signals}\n"
        f"This post scores {score:+d} ({strength}). "
        f"Treat this as a REGIME BREAK, not a trend continuation. "
        f"The 60-day historical average is context only. "
        f"The current post is {dirn}. "
        f"Forecast should weight this post heavily over the historical trend."
    )

    # Replace the NET ASSESSMENT line with the override
    summary_b = summary.replace(
        f"NET ASSESSMENT: {sources['net_label']}.",
        f"NET ASSESSMENT: {strength} — REGIME BREAK (current post overrides 60-day trend)."
    ) + override_block

    sources_b = {**sources, "net_label": strength, "override": True}
    return summary_b, sources_b


def net_signal_text(score: int) -> str:
    if score >= 4:  return "STRONGLY BULLISH"
    if score >= 2:  return "BULLISH"
    if score >= 1:  return "MILDLY BULLISH"
    if score == 0:  return "NEUTRAL"
    if score >= -1: return "MILDLY BEARISH"
    if score >= -3: return "BEARISH"
    return "STRONGLY BEARISH"


# ---------------------------------------------------------------------------
# Post-analogue signal estimate
# ---------------------------------------------------------------------------

def analogue_signal(post: dict) -> dict:
    """Given a scored post, find historical confirmed analogues and estimate
    the expected USO move at 15min, 1hr, and 24hr timeframes.

    Uses confirmed posts from cache with matching score bracket.
    USO 5min = 15min proxy (closest we have), 1hr = 1hr, 24hr extrapolated.

    Returns a dict:
    {
        "score":          int,
        "signals":        [str],
        "direction":      "LONG" | "SHORT" | "NEUTRAL",
        "n_analogues":    int,
        "avg_15m":        float,   # avg USO % move (5min data as proxy)
        "avg_1h":         float,
        "est_24h":        float,   # extrapolated (1h * decay factor)
        "hit_rate_15m":   float,   # % of analogues that moved in correct direction
        "hit_rate_1h":    float,
        "max_move_15m":   float,
        "max_move_1h":    float,
        "action":         str,     # plain-english trader instruction
        "top_analogues":  [dict],  # up to 3 most similar confirmed posts
    }
    """
    score   = post.get("score", 0)
    signals = post.get("signals", [])

    all_posts  = get_scored_posts()
    confirmed  = [p for p in all_posts if p.get("confirmed")]

    # Match posts in the same score bracket (±1 from post score, same direction)
    if score > 0:
        analogues = [p for p in confirmed if p.get("uso_pct_5m") is not None and p["uso_pct_5m"] > 0]
        # Weight towards higher-score matches
        analogues = sorted(analogues, key=lambda x: abs(x.get("score", 0) - score))
    elif score < 0:
        analogues = [p for p in confirmed if p.get("uso_pct_5m") is not None and p["uso_pct_5m"] < 0]
        analogues = sorted(analogues, key=lambda x: abs(x.get("score", 0) - score))
    else:
        return {
            "score": 0, "signals": signals, "direction": "NEUTRAL",
            "n_analogues": 0, "avg_15m": 0.0, "avg_1h": 0.0, "est_24h": 0.0,
            "hit_rate_15m": 0.0, "hit_rate_1h": 0.0,
            "max_move_15m": 0.0, "max_move_1h": 0.0,
            "action": "No signal — stay flat.",
            "top_analogues": [],
        }

    # Closest score bracket first, take up to 20
    close_analogues = [p for p in analogues if abs(p.get("score", 0) - score) <= 1][:20]
    if not close_analogues:
        close_analogues = analogues[:10]   # fall back to same-direction posts

    n = len(close_analogues)
    if n == 0:
        return {
            "score": score, "signals": signals, "direction": "NEUTRAL",
            "n_analogues": 0, "avg_15m": 0.0, "avg_1h": 0.0, "est_24h": 0.0,
            "hit_rate_15m": 0.0, "hit_rate_1h": 0.0,
            "max_move_15m": 0.0, "max_move_1h": 0.0,
            "action": "No confirmed analogues yet — signal unconfirmed.",
            "top_analogues": [],
        }

    moves_15m = [p["uso_pct_5m"] for p in close_analogues]
    moves_1h  = [p["uso_pct_1h"] for p in close_analogues if p.get("uso_pct_1h") is not None]

    avg_15m     = sum(moves_15m) / len(moves_15m)
    avg_1h      = sum(moves_1h)  / len(moves_1h) if moves_1h else avg_15m * 1.3
    est_24h     = avg_1h * 1.5   # 24h decay: moves partially revert, partial persistence

    direction   = "LONG" if score > 0 else "SHORT"
    correct_dir = (lambda x: x > 0) if score > 0 else (lambda x: x < 0)

    hit_rate_15m = sum(1 for m in moves_15m if correct_dir(m)) / len(moves_15m)
    hit_rate_1h  = sum(1 for m in moves_1h  if correct_dir(m)) / len(moves_1h) if moves_1h else hit_rate_15m

    max_move_15m = max(moves_15m, key=abs)
    max_move_1h  = max(moves_1h,  key=abs) if moves_1h else avg_1h

    # Trader action — scale with conviction
    conviction = hit_rate_15m
    size = "FULL SIZE" if conviction >= 0.75 else ("HALF SIZE" if conviction >= 0.60 else "SMALL SIZE")
    side = "BUY" if direction == "LONG" else "SELL"

    action = (
        f"{side} WTI/USO — {size}\n"
        f"Expected: {avg_15m:+.1f}% in 15min, {avg_1h:+.1f}% in 1hr, {est_24h:+.1f}% in 24hr\n"
        f"Hit rate: {hit_rate_15m:.0%} directional accuracy ({n} analogues)\n"
        f"Stop: reverse if {'-' if direction == 'LONG' else '+'}{abs(avg_15m) * 0.5:.1f}% within 15min"
    )

    top_analogues = sorted(close_analogues, key=lambda x: abs(x["uso_pct_5m"]), reverse=True)[:3]

    return {
        "score":         score,
        "signals":       signals,
        "direction":     direction,
        "n_analogues":   n,
        "avg_15m":       round(avg_15m, 2),
        "avg_1h":        round(avg_1h, 2),
        "est_24h":       round(est_24h, 2),
        "hit_rate_15m":  round(hit_rate_15m, 2),
        "hit_rate_1h":   round(hit_rate_1h, 2),
        "max_move_15m":  round(max_move_15m, 2),
        "max_move_1h":   round(max_move_1h, 2),
        "action":        action,
        "top_analogues": top_analogues,
    }


def format_analogue_signal(post: dict, current_price: float) -> str:
    """Format analogue signal as a trader-ready Telegram message."""
    r       = analogue_signal(post)
    score   = r["score"]
    emoji   = score_emoji(score)
    side    = "📈 LONG" if r["direction"] == "LONG" else "📉 SHORT"
    signals = ", ".join(r["signals"])

    if r["n_analogues"] == 0:
        return (
            f"{emoji} *Signal: {net_signal_text(score)}* `{score:+d}`\n\n"
            f"_{post.get('text', '')[:300]}_\n\n"
            f"⚠️ No confirmed analogues yet — post is unconfirmed.\n"
            f"Signals: {signals}"
        )

    # Dollar-amount equivalents at current price
    d15m = current_price * r["avg_15m"] / 100
    d1h  = current_price * r["avg_1h"]  / 100
    d24h = current_price * r["est_24h"] / 100

    lines = [
        f"{emoji} *Trader Signal* — {side} `{score:+d}`",
        f"",
        f"_{post.get('text', '')[:250]}_",
        f"",
        f"*Signals:* {signals}",
        f"",
        f"*⏱ Expected moves* ({r['n_analogues']} analogues)",
        f"┌─────────────────────────────────────",
        f"│ *15 min*  `{r['avg_15m']:+.2f}%`  `${d15m:+.2f}`   hit {r['hit_rate_15m']:.0%}",
        f"│  *1 hr*   `{r['avg_1h']:+.2f}%`  `${d1h:+.2f}`   hit {r['hit_rate_1h']:.0%}",
        f"│ *24 hr*   `{r['est_24h']:+.2f}%`  `${d24h:+.2f}`   extrapolated",
        f"└─────────────────────────────────────",
        f"",
        f"*Action:*",
    ]
    for line in r["action"].split("\n"):
        lines.append(f"  {line}")

    if r["top_analogues"]:
        lines.append("")
        lines.append("*Best analogues:*")
        for p in r["top_analogues"]:
            dt  = p.get("date", "")
            m5  = p["uso_pct_5m"]
            m1h = p.get("uso_pct_1h")
            m1h_str = f", {m1h:+.1f}% 1hr" if m1h else ""
            lines.append(f"  [{dt}] `{m5:+.1f}% 15min{m1h_str}` — _{p['text'][:120]}_")

    lines += ["", f"_Current WTI: ${current_price:.2f} · {r['n_analogues']} confirmed analogues_"]
    return "\n".join(lines)
