"""
Truth Social poller using curl_cffi.

Bypasses Cloudflare by impersonating browser TLS fingerprints.
No headless browser needed — just a pip package.

Polls Trump's statuses endpoint every few seconds and fires
process_webhook_posts() when a new post is detected.

Usage:
    asyncio.create_task(run_truthsocial_poller(ptb_app))
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timezone

from curl_cffi import requests as cffi_requests

log = logging.getLogger(__name__)

TRUMP_ACCOUNT_ID = "107780257626128497"
TRUMP_STATUSES_URL = f"https://truthsocial.com/api/v1/accounts/{TRUMP_ACCOUNT_ID}/statuses?limit=5&exclude_replies=true"

POLL_INTERVAL_S = int(os.environ.get("TS_POLL_INTERVAL", "3"))
RECONNECT_BASE_S = 5
RECONNECT_MAX_S = 120


def _normalize_post(post: dict) -> dict | None:
    """Convert a Mastodon API status to the shape process_webhook_posts expects."""
    account = post.get("account", {})
    if account.get("id") != TRUMP_ACCOUNT_ID:
        return None
    if post.get("reblog"):
        return None

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
        "source": "truthsocial_poller",
    }


def _fetch_statuses() -> list[dict]:
    """Fetch Trump's latest statuses using curl_cffi to bypass Cloudflare."""
    try:
        resp = cffi_requests.get(
            TRUMP_STATUSES_URL,
            impersonate="chrome120",
            timeout=10,
        )
        if resp.status_code != 200:
            log.warning("TS poller: HTTP %d — %s", resp.status_code, resp.text[:200])
            return []
        data = resp.json()
        if isinstance(data, list):
            return data
        log.warning("TS poller: unexpected response: %s", type(data))
        return []
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
            log.info("TS poller: starting — polling every %ds", POLL_INTERVAL_S)
            # Test connection
            statuses = _fetch_statuses()
            if statuses:
                log.info("TS poller: connection OK — got %d statuses", len(statuses))
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
