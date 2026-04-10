"""
Truth Social poller using ScrapeCreators API.

Polls Trump's posts via ScrapeCreators (handles Cloudflare bypass).
Fires process_webhook_posts() when a new post is detected.

Usage:
    asyncio.create_task(run_truthsocial_poller(ptb_app))
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timezone

import requests

log = logging.getLogger(__name__)

TRUMP_USER_ID = "107780257626128497"
SCRAPECREATORS_URL = "https://api.scrapecreators.com/v1/truthsocial/user/posts"
SCRAPECREATORS_KEY = os.environ.get("SCRAPECREATORS_API_KEY", "")

POLL_INTERVAL_S = int(os.environ.get("TS_POLL_INTERVAL", "30"))
RECONNECT_BASE_S = 5
RECONNECT_MAX_S = 120


def _normalize_post(post: dict) -> dict | None:
    """Convert a ScrapeCreators post to the shape process_webhook_posts expects."""
    # Use text field directly if available, otherwise strip HTML from content
    text = post.get("text", "")
    if not text:
        content = post.get("content", "")
        text = re.sub(r"<[^>]+>", "", content).strip()
        text = (text.replace("&amp;", "&")
                    .replace("&lt;", "<")
                    .replace("&gt;", ">")
                    .replace("&quot;", '"')
                    .replace("&#39;", "'"))

    if not text or len(text) < 10:
        return None

    return {
        "text":   text,
        "date":   post.get("created_at", datetime.now(timezone.utc).isoformat()),
        "url":    post.get("url", ""),
        "id":     post.get("id", ""),
        "source": "scrapecreators",
    }


def _fetch_statuses() -> list[dict]:
    """Fetch Trump's latest posts via ScrapeCreators."""
    if not SCRAPECREATORS_KEY:
        log.warning("TS poller: SCRAPECREATORS_API_KEY not set")
        return []
    try:
        resp = requests.get(
            SCRAPECREATORS_URL,
            params={"user_id": TRUMP_USER_ID, "trim": "true"},
            headers={"x-api-key": SCRAPECREATORS_KEY},
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning("TS poller: HTTP %d — %s", resp.status_code, resp.text[:200])
            return []
        data = resp.json()
        if not data.get("success"):
            log.warning("TS poller: API returned success=false")
            return []
        return data.get("posts", [])
    except Exception as exc:
        log.warning("TS poller: fetch failed: %s", exc)
        return []


async def _poll_loop(ptb_app) -> None:
    """Core polling loop."""
    last_seen_id: str | None = None
    consecutive_errors = 0
    loop = asyncio.get_event_loop()

    while True:
        try:
            statuses = await loop.run_in_executor(None, _fetch_statuses)

            if not statuses:
                consecutive_errors += 1
                if consecutive_errors >= 20:
                    log.error("TS poller: %d consecutive errors", consecutive_errors)
                    consecutive_errors = 0
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            consecutive_errors = 0
            latest_id = statuses[0].get("id", "")

            # First poll — just record the latest ID
            if last_seen_id is None:
                last_seen_id = latest_id
                log.info("TS poller: initialized — latest post ID: %s", latest_id)
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # Check for new posts
            if latest_id != last_seen_id:
                new_posts = []
                for status in statuses:
                    if status.get("id") == last_seen_id:
                        break
                    post = _normalize_post(status)
                    if post:
                        new_posts.append(post)

                if new_posts:
                    log.info("TS poller: %d new post(s) detected!", len(new_posts))
                    from bot import process_webhook_posts
                    asyncio.create_task(process_webhook_posts(ptb_app, new_posts))

                last_seen_id = latest_id

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("TS poller: error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL_S)


async def run_truthsocial_poller(ptb_app) -> None:
    """Main entry point. Runs the poll loop with reconnection."""
    delay = RECONNECT_BASE_S

    while True:
        try:
            log.info("TS poller: starting — polling every %ds (ScrapeCreators)", POLL_INTERVAL_S)
            statuses = _fetch_statuses()
            if statuses:
                log.info("TS poller: connection OK — got %d posts", len(statuses))
                delay = RECONNECT_BASE_S
            else:
                log.warning("TS poller: initial fetch failed — retrying in %ds", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_S)
                continue

            await _poll_loop(ptb_app)

        except asyncio.CancelledError:
            log.info("TS poller: cancelled — shutting down")
            return
        except Exception as exc:
            log.warning("TS poller: crashed — %s. Restarting in %ds", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_S)
