"""
News aggregator for Migas Oil Bot.

Currently pulls:
- Trump posts from Truth Social RSS feed (filtered for oil/energy relevance)

Planned:
- OPEC press releases
- Iran/Hormuz news via NewsAPI
- EIA inventory reports
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import requests
import feedparser

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Oil/energy relevance keywords
# ---------------------------------------------------------------------------

OIL_KEYWORDS = [
    # commodities
    "oil", "gas", "energy", "crude", "petroleum", "fuel", "gasoline",
    "opec", "barrel", "brent", "wti", "lng", "pipeline",
    # geopolitical
    "iran", "hormuz", "saudi", "russia", "ukraine", "middle east",
    "sanctions", "embargo", "supply",
    # policy
    "drill", "refinery", "spr", "strategic reserve",
    "tariff", "import", "export", "production",
]

# ---------------------------------------------------------------------------
# Truth Social — Trump RSS
# ---------------------------------------------------------------------------

TRUMP_RSS_URL = "https://truthsocial.com/@realDonaldTrump.rss"


def fetch_trump_posts(max_posts: int = 20, days_back: int = 7) -> list[dict]:
    """Fetch recent Trump posts from Truth Social RSS.

    Args:
        max_posts: Maximum number of posts to fetch.
        days_back: Only return posts from the last N days.

    Returns:
        List of dicts with keys: title, text, published, url, relevant
    """
    try:
        feed = feedparser.parse(TRUMP_RSS_URL)
    except Exception as exc:
        log.error("Failed to fetch Truth Social RSS: %s", exc)
        return []

    cutoff = datetime.now(timezone.utc).timestamp() - (days_back * 86400)
    posts  = []

    for entry in feed.entries[:max_posts]:
        # Parse timestamp
        published_ts = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            published_ts = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).timestamp()

        if published_ts and published_ts < cutoff:
            continue

        # Strip HTML tags from content
        raw_text = entry.get("summary", entry.get("title", ""))
        text     = re.sub(r"<[^>]+>", " ", raw_text).strip()
        text     = re.sub(r"\s+", " ", text)

        posts.append({
            "text":      text,
            "published": entry.get("published", ""),
            "url":       entry.get("link", ""),
            "relevant":  is_oil_relevant(text),
        })

    return posts


def is_oil_relevant(text: str) -> bool:
    """Return True if a post is oil/energy relevant."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in OIL_KEYWORDS)


def get_relevant_trump_posts(days_back: int = 7) -> list[str]:
    """Return list of oil-relevant Trump post texts from the last N days."""
    posts = fetch_trump_posts(days_back=days_back)
    relevant = [p["text"] for p in posts if p["relevant"]]
    log.info(
        "Trump posts: %d fetched, %d oil-relevant (last %d days)",
        len(posts), len(relevant), days_back,
    )
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

    trump_posts = get_relevant_trump_posts(days_back=7)

    # Build factual section
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

    # Build predictive section
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

    sources = {
        "trump_posts": len(trump_posts),
    }

    return summary, sources
