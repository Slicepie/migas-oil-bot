"""
News aggregator for Migas Oil Bot.

Currently pulls:
- Trump posts from Truth Social via Apify scraper

Planned:
- OPEC press releases
- Iran/Hormuz news via NewsAPI
- EIA inventory reports
"""

import logging
import os
from datetime import datetime, timezone

import requests

log = logging.getLogger(__name__)

APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
APIFY_ACTOR_ID  = "muhammetakkurtt~truth-social-scraper"
APIFY_ENDPOINT  = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"

# ---------------------------------------------------------------------------
# Oil/energy relevance keywords
# ---------------------------------------------------------------------------

OIL_KEYWORDS = [
    "oil", "gas", "energy", "crude", "petroleum", "fuel", "gasoline",
    "opec", "barrel", "brent", "wti", "lng", "pipeline",
    "iran", "hormuz", "saudi", "russia", "ukraine", "middle east",
    "sanctions", "embargo", "supply", "drill", "refinery", "spr",
    "strategic reserve", "tariff", "production", "prices",
]

# ---------------------------------------------------------------------------
# Apify Trump scraper
# ---------------------------------------------------------------------------

def fetch_trump_posts(max_posts: int = 20, use_last_post_id: bool = True) -> list[dict]:
    """Fetch Trump's latest Truth Social posts via Apify.

    Args:
        max_posts: Maximum number of posts to fetch.
        use_last_post_id: If True, only fetch new posts since last run.

    Returns:
        List of post dicts with keys: id, content, created_at, url
    """
    if not APIFY_API_TOKEN:
        log.error("APIFY_API_TOKEN not set")
        return []

    try:
        resp = requests.post(
            APIFY_ENDPOINT,
            params={"token": APIFY_API_TOKEN},
            json={
                "username":       "realDonaldTrump",
                "maxPosts":       max_posts,
                "useLastPostId":  use_last_post_id,
                "cleanContent":   True,
                "onlyReplies":    False,
                "onlyMedia":      False,
            },
            timeout=60,
        )
        resp.raise_for_status()
        posts = resp.json()

        log.info("Apify returned %d Trump posts", len(posts))
        return posts

    except Exception as exc:
        log.error("Apify fetch failed: %s", exc)
        return []


def is_oil_relevant(text: str) -> bool:
    """Return True if a post is oil/energy relevant."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in OIL_KEYWORDS)


def get_relevant_trump_posts(use_last_post_id: bool = True) -> list[str]:
    """Return list of oil-relevant Trump post texts."""
    posts = fetch_trump_posts(use_last_post_id=use_last_post_id)
    relevant = [
        p.get("content", p.get("text", ""))
        for p in posts
        if is_oil_relevant(p.get("content", p.get("text", "")))
    ]
    log.info("%d/%d Trump posts are oil-relevant", len(relevant), len(posts))
    return relevant


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def build_live_summary(current_price: float, instrument: str = "wti") -> tuple[str, dict]:
    """Build a Migas-ready summary from live news sources.

    Returns:
        summary: formatted string for Migas
        sources: dict of what was found (for transparency)
    """
    label = "WTI" if instrument == "wti" else "Brent"
    sensitivity = (
        "sensitive to US domestic production and SPR policy"
        if instrument == "wti"
        else "highly sensitive to OPEC+ decisions, Iran supply risk, and Strait of Hormuz disruptions"
    )

    trump_posts = get_relevant_trump_posts()

    factual_parts = [
        f"{label} crude oil is currently trading at ${current_price:.2f}/barrel. "
        f"This benchmark is {sensitivity}. "
        f"Markets are in a geopolitically-driven regime."
    ]

    if trump_posts:
        factual_parts.append(
            f"\nRecent Trump statements on energy/oil ({len(trump_posts)} post(s)):\n"
            + "\n".join(f"- {p[:300]}" for p in trump_posts[:5])
        )

    predictive_parts = [
        "Ongoing Middle East conflict maintains a geopolitical risk premium. "
        "Any Strait of Hormuz disruption would be strongly bullish. "
        "Ceasefire or de-escalation signals would be bearish. "
        "Monitor OPEC+ emergency meetings and Iran sanctions developments."
    ]

    if trump_posts:
        predictive_parts.append(
            "Trump energy statements may signal SPR releases, drill policy changes, "
            "or sanctions escalation — each carrying directional price implications."
        )

    summary = (
        "FACTUAL SUMMARY:\n"
        + " ".join(factual_parts)
        + "\n\nPREDICTIVE SIGNALS:\n"
        + " ".join(predictive_parts)
    )

    sources = {"trump_posts": len(trump_posts)}
    return summary, sources
