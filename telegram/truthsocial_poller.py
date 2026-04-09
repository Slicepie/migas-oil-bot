"""
Truth Social Playwright poller.

Replaces both Apify (60s, $900/mo) and the broken WebSocket stream.
Uses a persistent headless Chromium browser to bypass Cloudflare and
polls Trump's profile page every few seconds.

How it works:
  1. Playwright launches headless Chromium (once, on startup)
  2. Navigates to Trump's Truth Social profile → Cloudflare solves automatically
  3. Polls the Mastodon-compatible API using the browser's valid session cookies
  4. Compares latest post ID with last seen → fires process_webhook_posts() on new post
  5. If cookies expire / Cloudflare re-challenges → re-navigates the profile page

Cost: $0 (runs on existing RunPod pod, ~200MB extra RAM)
Latency: 3-5s post-to-detection

Usage:
    asyncio.create_task(run_truthsocial_poller(ptb_app))
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# Trump's Truth Social account
TRUMP_PROFILE_URL = "https://truthsocial.com/@realDonaldTrump"
TRUMP_ACCOUNT_ID  = "107780257626128497"
TRUMP_STATUSES_API = f"https://truthsocial.com/api/v1/accounts/{TRUMP_ACCOUNT_ID}/statuses?limit=5&exclude_replies=true"

# Polling config
POLL_INTERVAL_S       = int(os.environ.get("TS_POLL_INTERVAL", "3"))
COOKIE_REFRESH_EVERY  = 300   # re-navigate profile every 5 min to keep cookies fresh
MAX_CONSECUTIVE_ERRS  = 10    # after this many errors, do a full browser restart

# Reconnection
RECONNECT_BASE_S  = 5
RECONNECT_MAX_S   = 120


def _normalize_post(post: dict) -> dict | None:
    """Convert a Mastodon API status object to the shape process_webhook_posts expects."""
    account = post.get("account", {})

    # Must be Trump's post
    if account.get("id") != TRUMP_ACCOUNT_ID:
        return None

    # Skip reblogs
    if post.get("reblog"):
        return None

    # Extract text — strip HTML tags
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
        "source": "truthsocial_playwright",
    }


async def _solve_cloudflare(page) -> bool:
    """
    Navigate to Trump's profile to get valid Cloudflare cookies.
    Returns True if successful, False if blocked.
    """
    try:
        log.info("TS poller: solving Cloudflare — loading %s", TRUMP_PROFILE_URL)
        response = await page.goto(TRUMP_PROFILE_URL, wait_until="domcontentloaded", timeout=30000)

        # Wait for Cloudflare challenge to resolve (up to 15s)
        for _ in range(30):
            title = await page.title()
            content = await page.content()

            # Cloudflare challenge pages have these markers
            if "Just a moment" in title or "Checking your browser" in content:
                await asyncio.sleep(0.5)
                continue

            # Blocked
            if "you have been blocked" in content.lower():
                log.error("TS poller: Cloudflare blocked us")
                return False

            # Success — we see the profile page or API response
            log.info("TS poller: Cloudflare solved — page title: %s", title[:60])
            return True

        log.warning("TS poller: Cloudflare challenge timed out")
        return False

    except Exception as exc:
        log.warning("TS poller: Cloudflare solve failed: %s", exc)
        return False


async def _fetch_statuses(page) -> list[dict]:
    """
    Fetch Trump's latest statuses using the browser's session cookies.
    Uses page.evaluate() to make the request inside the browser context
    so all cookies/headers are included automatically.
    """
    try:
        result = await page.evaluate("""
            async (url) => {
                try {
                    const resp = await fetch(url, {
                        credentials: 'include',
                        headers: { 'Accept': 'application/json' }
                    });
                    if (!resp.ok) return { error: resp.status, text: await resp.text() };
                    return { data: await resp.json() };
                } catch (e) {
                    return { error: e.message };
                }
            }
        """, TRUMP_STATUSES_API)

        if "error" in result:
            error = result["error"]
            # 403 / Cloudflare re-challenge → need cookie refresh
            if error in (403, 401) or (isinstance(error, str) and "blocked" in str(error).lower()):
                log.warning("TS poller: API returned %s — cookies expired, refreshing", error)
                return []
            log.warning("TS poller: API error: %s", error)
            return []

        data = result.get("data", [])
        if isinstance(data, list):
            return data
        log.warning("TS poller: unexpected API response shape: %s", type(data))
        return []

    except Exception as exc:
        log.warning("TS poller: fetch_statuses failed: %s", exc)
        return []


async def _poll_loop(ptb_app, page) -> None:
    """
    Core polling loop. Fetches statuses every POLL_INTERVAL_S seconds.
    Calls process_webhook_posts() when a new post is detected.
    """
    last_seen_id: str | None = None
    consecutive_errors = 0
    polls_since_refresh = 0

    while True:
        try:
            # Periodic cookie refresh
            if polls_since_refresh * POLL_INTERVAL_S >= COOKIE_REFRESH_EVERY:
                log.info("TS poller: periodic cookie refresh")
                if not await _solve_cloudflare(page):
                    raise RuntimeError("Cookie refresh failed")
                polls_since_refresh = 0

            statuses = await _fetch_statuses(page)

            if not statuses:
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRS:
                    log.warning("TS poller: %d consecutive errors — refreshing cookies", consecutive_errors)
                    if not await _solve_cloudflare(page):
                        raise RuntimeError("Cookie refresh after errors failed")
                    consecutive_errors = 0
                await asyncio.sleep(POLL_INTERVAL_S)
                polls_since_refresh += 1
                continue

            consecutive_errors = 0
            latest = statuses[0]
            latest_id = latest.get("id", "")

            # First poll — just record the latest ID, don't fire
            if last_seen_id is None:
                last_seen_id = latest_id
                log.info("TS poller: initialized — latest post ID: %s", latest_id)
                await asyncio.sleep(POLL_INTERVAL_S)
                polls_since_refresh += 1
                continue

            # Check for new posts (there could be multiple)
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
                    # Import here to avoid circular import
                    from bot import process_webhook_posts
                    asyncio.create_task(process_webhook_posts(ptb_app, new_posts))

                last_seen_id = latest_id

        except asyncio.CancelledError:
            raise
        except RuntimeError:
            # Cookie refresh failed — bubble up for full restart
            raise
        except Exception as exc:
            log.warning("TS poller: poll error: %s", exc)
            consecutive_errors += 1

        await asyncio.sleep(POLL_INTERVAL_S)
        polls_since_refresh += 1


async def run_truthsocial_poller(ptb_app) -> None:
    """
    Main entry point. Launches Playwright browser and runs the poll loop.
    Reconnects with exponential backoff on failure.
    """
    delay = RECONNECT_BASE_S

    while True:
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as pw:
                log.info("TS poller: launching Chromium...")
                browser = await pw.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-blink-features=AutomationControlled",
                    ],
                )

                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 720},
                    java_script_enabled=True,
                )

                page = await context.new_page()

                # Initial Cloudflare solve
                if not await _solve_cloudflare(page):
                    log.error("TS poller: initial Cloudflare solve failed — retrying in %ds", delay)
                    await browser.close()
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, RECONNECT_MAX_S)
                    continue

                log.info("TS poller: running — polling every %ds", POLL_INTERVAL_S)
                delay = RECONNECT_BASE_S  # reset backoff on success

                try:
                    await _poll_loop(ptb_app, page)
                finally:
                    await browser.close()

        except asyncio.CancelledError:
            log.info("TS poller: cancelled — shutting down")
            return
        except Exception as exc:
            log.warning("TS poller: crashed — %s. Restarting in %ds", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_S)
