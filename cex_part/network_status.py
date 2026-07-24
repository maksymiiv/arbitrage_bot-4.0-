"""
Per-CEX deposit / withdraw status refresher.

Two-tier cadence:

  PASSIVE  — every ~5 min refresh ALL currencies on each CEX (one bulk
             API call per CEX per cycle).
  ACTIVE   — when a token currently has a logged spread, the same loop
             switches to a much faster cadence (~8 s) so we notice if
             the exchange suddenly closes deposits / withdrawals (scams)
             or opens them (good arbitrage window). The spread runner
             registers (cex, key) pairs into the hot-watch set with a
             timestamp; entries idle for more than HOT_TTL_SEC are
             dropped automatically.

Kraken is intentionally excluded — its public Assets endpoint doesn't
expose per-chain deposit / withdraw status, and per-coin private calls
would burn quota for little real benefit. Per project decision: assume
Kraken D/W is always open and rely on `is_route_open` returning None
for kraken (which the caller treats as "don't filter").

This module owns no persistence — `cex_tokens.json` is the durable
home for `networks_cex` flags; `_HOT_WATCH` is in-memory only and
naturally rebuilt as new spreads appear.
"""

from __future__ import annotations

import asyncio
import time

from engine.logger import get_logger

from . import cache_manager


log = get_logger(__name__)


# In-memory hot-watch set: (cex_lower, cache_key) -> last-seen-spread ts.
_HOT_WATCH: dict[tuple[str, str], float] = {}

PASSIVE_INTERVAL_SEC = 300.0     # 5 min — slow path
HOT_INTERVAL_SEC = 8.0           # 8 s — fast path while spreads are live
HOT_TTL_SEC = 2 * 3600.0         # entry stops being "hot" after 2 h idle


# --------------------------------------------------------------------------
# hot-watch state
# --------------------------------------------------------------------------

def register_hot_watch(cex: str, key: str) -> None:
    """Mark (cex, key) as having an active spread right now."""
    if not cex or not key:
        return
    _HOT_WATCH[(cex.lower(), key)] = time.time()


def evict_stale() -> None:
    now = time.time()
    stale = [k for k, ts in _HOT_WATCH.items() if (now - ts) >= HOT_TTL_SEC]
    for k in stale:
        del _HOT_WATCH[k]


def has_hot_for_cex(cex: str) -> bool:
    cex_l = cex.lower()
    now = time.time()
    return any(
        c == cex_l and (now - ts) < HOT_TTL_SEC
        for (c, _), ts in _HOT_WATCH.items()
    )


def hot_count() -> int:
    return len(_HOT_WATCH)


# --------------------------------------------------------------------------
# refresh implementations (one bulk call per CEX)
# --------------------------------------------------------------------------

async def _refresh_bybit() -> int:
    from .cex_metadata.bybit_metadata import fetch_bybit_all_currencies

    rows = await fetch_bybit_all_currencies()
    if not rows:
        return 0
    networks = {sym: data["networks"] for sym, data in rows.items() if data.get("networks")}
    return await cache_manager.update_networks_for("bybit", networks)


async def _refresh_gate() -> int:
    from .cex_metadata.gate_metadata import GateMetadataCache

    try:
        cache = await GateMetadataCache.fetch()
    except Exception as e:
        log.warning("gate bulk fetch failed: %s", e)
        return 0

    networks = cache.all_networks()
    if not networks:
        return 0
    return await cache_manager.update_networks_for("gate", networks)


_REFRESHERS = {"bybit": _refresh_bybit, "gate": _refresh_gate}


# --------------------------------------------------------------------------
# adaptive loop
# --------------------------------------------------------------------------

async def cex_network_loop(cex: str) -> None:
    """
    One adaptive loop per CEX. Sleep duration switches between
    HOT_INTERVAL_SEC and PASSIVE_INTERVAL_SEC depending on whether
    `_HOT_WATCH` has any entry for this CEX.
    """
    refresh = _REFRESHERS.get(cex.lower())
    if not refresh:
        log.warning("network_status: no refresher registered for cex=%s", cex)
        return

    log.info(
        "network_status loop started for %s (passive=%.0fs hot=%.0fs ttl=%.0fs)",
        cex, PASSIVE_INTERVAL_SEC, HOT_INTERVAL_SEC, HOT_TTL_SEC,
    )

    while True:
        try:
            evict_stale()
            changed = await refresh()
            if changed:
                log.info(
                    "[%s] network status refreshed: %d entries updated (hot=%d)",
                    cex.upper(), changed, hot_count(),
                )
        except Exception as e:
            log.error("[%s] network refresh error: %s", cex.upper(), e)

        sleep = HOT_INTERVAL_SEC if has_hot_for_cex(cex) else PASSIVE_INTERVAL_SEC
        await asyncio.sleep(sleep)
