"""
Strait of Hormuz vessel congestion monitor.

Connects to aisstream.io WebSocket, tracks unique vessels in key zones
over rolling 6-hour windows.  Fires a Telegram alert when vessel density
in any zone exceeds the rolling baseline by a configurable multiplier.

Usage:
    asyncio.create_task(run_hormuz_monitor(ptb_app))

Requires env var:  AISSTREAM_API_KEY  (free key from aisstream.io)
"""

import asyncio
import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import websockets

log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY", "")
AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# Persian Gulf + Gulf of Oman bounding box
BBOX = [[22.0, 48.0], [30.5, 60.0]]

# Rolling window size and snapshot interval
WINDOW_HOURS = 6
SNAPSHOT_INTERVAL_S = int(os.environ.get("HORMUZ_SNAPSHOT_INTERVAL", "900"))  # 15 min
MIN_BASELINE_SNAPSHOTS = 4  # need ≥4 snapshots (~1h) before alerting

# Alert: current unique vessel count > baseline_avg * multiplier
CONGESTION_MULTIPLIER = float(os.environ.get("HORMUZ_CONGESTION_MULT", "1.5"))
# Cooldown between alerts per zone (seconds)
ALERT_COOLDOWN_S = int(os.environ.get("HORMUZ_ALERT_COOLDOWN", "3600"))  # 1h

# Per-vessel throttle: only process one position per MMSI per N seconds
POSITION_THROTTLE_S = 120

RECONNECT_BASE_S = 10
RECONNECT_MAX_S = 120

# ── Monitoring zones ────────────────────────────────────────────────────────
# Zones near the strait chokepoint — congestion here = potential disruption
ZONES = {
    "Strait Waiting Area": {"lat": 26.30, "lon": 56.80, "radius_nm": 10},
    "Fujairah Anchorage":  {"lat": 25.15, "lon": 56.40, "radius_nm": 10},
    "Bandar Abbas":        {"lat": 27.15, "lon": 56.30, "radius_nm": 8},
    "Khor Fakkan":         {"lat": 25.35, "lon": 56.40, "radius_nm": 5},
    "Dubai / Jebel Ali":   {"lat": 25.05, "lon": 55.05, "radius_nm": 12},
}


# ── Helpers ─────────────────────────────────────────────────────────────────
def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R_NM = 3440.065
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R_NM * 2 * math.asin(math.sqrt(a))


def _vessel_in_zone(lat: float, lon: float, zone: dict) -> bool:
    return _haversine_nm(lat, lon, zone["lat"], zone["lon"]) <= zone["radius_nm"]


def _normalize_timestamp(raw: str) -> str:
    """Convert aisstream timestamp to ISO 8601."""
    if not raw:
        return ""
    try:
        parts = raw.split(" +")
        base = parts[0] if len(parts) >= 2 else raw.rstrip(" UTC")
        base = base.replace(" ", "T", 1)
        if "." in base:
            main, frac = base.split(".", 1)
            base = main + "." + frac[:6]
        return base
    except Exception:
        return ""


# ── In-memory state ─────────────────────────────────────────────────────────
class ZoneTracker:
    """Tracks unique vessel counts per zone over rolling windows."""

    def __init__(self):
        # Current window: zone_name → set of MMSIs seen
        self.current_window: dict[str, set[int]] = defaultdict(set)
        # Rolling baseline: zone_name → list of (timestamp, vessel_count) snapshots
        self.baseline: dict[str, list[tuple[float, int]]] = defaultdict(list)
        # Per-vessel throttle
        self.last_seen: dict[int, float] = {}
        # Alert cooldown: zone_name → last alert timestamp
        self.last_alert: dict[str, float] = {}
        # Total positions received (for logging)
        self.total_positions = 0

    def record_position(self, mmsi: int, lat: float, lon: float) -> None:
        """Record a vessel position — assign to zones if within radius."""
        now = time.monotonic()
        prev = self.last_seen.get(mmsi, 0)
        if now - prev < POSITION_THROTTLE_S:
            return
        self.last_seen[mmsi] = now
        self.total_positions += 1

        for zone_name, zone in ZONES.items():
            if _vessel_in_zone(lat, lon, zone):
                self.current_window[zone_name].add(mmsi)

    def take_snapshot(self) -> dict[str, int]:
        """Snapshot current window counts, rotate into baseline, reset window.
        Returns {zone_name: current_count}."""
        now = time.monotonic()
        cutoff = now - (WINDOW_HOURS * 3600)
        counts = {}

        for zone_name in ZONES:
            count = len(self.current_window.get(zone_name, set()))
            counts[zone_name] = count

            # Append to baseline
            self.baseline[zone_name].append((now, count))
            # Prune old entries outside the rolling window
            self.baseline[zone_name] = [
                (ts, c) for ts, c in self.baseline[zone_name] if ts > cutoff
            ]

        # Reset current window
        self.current_window = defaultdict(set)
        return counts

    def get_baseline_avg(self, zone_name: str) -> float | None:
        """Average vessel count across baseline snapshots for a zone."""
        entries = self.baseline.get(zone_name, [])
        if len(entries) < MIN_BASELINE_SNAPSHOTS:
            return None  # not enough data yet
        return sum(c for _, c in entries) / len(entries)

    def check_alerts(self) -> list[dict]:
        """Check each zone for congestion above baseline. Returns alert dicts."""
        alerts = []
        now = time.monotonic()

        for zone_name in ZONES:
            avg = self.get_baseline_avg(zone_name)
            if avg is None or avg < 3:  # skip if baseline too low / not enough data
                continue

            current = len(self.current_window.get(zone_name, set()))
            if current <= 0:
                continue

            ratio = current / avg
            if ratio < CONGESTION_MULTIPLIER:
                continue

            # Cooldown check
            last = self.last_alert.get(zone_name, 0)
            if now - last < ALERT_COOLDOWN_S:
                continue

            self.last_alert[zone_name] = now
            alerts.append({
                "zone": zone_name,
                "current": current,
                "baseline_avg": round(avg, 1),
                "ratio": round(ratio, 2),
            })

        return alerts

    def cleanup_throttle_cache(self) -> None:
        """Prune stale entries from last_seen to prevent memory creep."""
        now = time.monotonic()
        cutoff = now - (POSITION_THROTTLE_S * 3)
        self.last_seen = {
            mmsi: ts for mmsi, ts in self.last_seen.items() if ts > cutoff
        }


# ── WebSocket collector ─────────────────────────────────────────────────────
async def _ws_collector(tracker: ZoneTracker, stop_event: asyncio.Event) -> None:
    """Connect to aisstream.io and feed positions to the zone tracker."""
    subscribe_msg = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": [BBOX],
        "FilterMessageTypes": ["PositionReport"],
    }

    delay = RECONNECT_BASE_S
    while not stop_event.is_set():
        try:
            async with websockets.connect(AISSTREAM_URL) as ws:
                await ws.send(json.dumps(subscribe_msg))
                log.info("Hormuz monitor: connected to aisstream.io")
                delay = RECONNECT_BASE_S

                async for raw in ws:
                    if stop_event.is_set():
                        return
                    try:
                        msg = json.loads(raw)
                        if msg.get("MessageType") != "PositionReport":
                            continue
                        pos = msg.get("Message", {}).get("PositionReport", {})
                        mmsi = msg.get("MetaData", {}).get("MMSI")
                        if not mmsi:
                            continue
                        lat = pos.get("Latitude")
                        lon = pos.get("Longitude")
                        if lat is None or lon is None:
                            continue
                        tracker.record_position(mmsi, lat, lon)
                    except (json.JSONDecodeError, KeyError):
                        pass

        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.warning("Hormuz monitor: WS error — %s. Reconnecting in %ds", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_S)


# ── Snapshot + alert loop ───────────────────────────────────────────────────
async def _snapshot_loop(tracker: ZoneTracker, ptb_app, stop_event: asyncio.Event) -> None:
    """Periodically snapshot zone counts, check for congestion, alert on Telegram."""
    from bot import ALLOWED_USER_IDS

    while not stop_event.is_set():
        await asyncio.sleep(SNAPSHOT_INTERVAL_S)
        if stop_event.is_set():
            return

        counts = tracker.take_snapshot()
        tracker.cleanup_throttle_cache()
        active_zones = {z: c for z, c in counts.items() if c > 0}

        log.info(
            "Hormuz snapshot: %d positions total | zones: %s",
            tracker.total_positions,
            {z: c for z, c in counts.items() if c > 0} or "none active",
        )

        # Check for congestion alerts
        alerts = tracker.check_alerts()
        if not alerts:
            continue

        for alert in alerts:
            zone = alert["zone"]
            msg = (
                f"🚢 *Hormuz Congestion Alert — {zone}*\n\n"
                f"Vessels now: `{alert['current']}`\n"
                f"6h baseline: `{alert['baseline_avg']}`\n"
                f"Ratio: `{alert['ratio']}x`\n\n"
                f"⚠️ *{alert['ratio']}x normal density* — potential strait disruption"
            )
            log.warning("Hormuz congestion: %s — %dx baseline", zone, alert["ratio"])

            for uid in ALLOWED_USER_IDS:
                try:
                    await ptb_app.bot.send_message(
                        chat_id=uid, text=msg, parse_mode="Markdown"
                    )
                except Exception:
                    log.exception("Failed to send Hormuz alert to %s", uid)


# ── Entry point ─────────────────────────────────────────────────────────────
async def run_hormuz_monitor(ptb_app) -> None:
    """Main entry point — runs WebSocket collector + snapshot loop concurrently."""
    if not AISSTREAM_API_KEY:
        log.warning("Hormuz monitor: AISSTREAM_API_KEY not set — skipping")
        return

    log.info(
        "Hormuz monitor: starting — %d zones, snapshot every %ds, alert at %.1fx baseline",
        len(ZONES), SNAPSHOT_INTERVAL_S, CONGESTION_MULTIPLIER,
    )

    tracker = ZoneTracker()
    stop_event = asyncio.Event()

    try:
        await asyncio.gather(
            _ws_collector(tracker, stop_event),
            _snapshot_loop(tracker, ptb_app, stop_event),
        )
    except asyncio.CancelledError:
        stop_event.set()
        log.info("Hormuz monitor: shutting down")
