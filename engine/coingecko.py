"""
CoinGecko bridge — gives us two pieces of data the CEX/DEX side cannot:

1. `(chain, contract) -> coingecko_id`         (resolve any token we know
   the on-chain address of into a stable, exchange-agnostic identifier)
2. `set of coingecko_ids actually traded on Kraken`
   (verify whether a Kraken-listed ticker really corresponds to *this*
   token or to a different token sharing the same ticker)

Two cache files in `cex_part/cex_cache/`:

  coingecko_platforms.json   — bulk dump of /coins/list?include_platform=true
                               + a `_meta` block with the fetch timestamp.
                               ~3 MB, ~17k coins, refreshed every 24h.

  coingecko_kraken_pairs.json — paginated dump of /exchanges/kraken/tickers
                               + a `_meta` block. Tens of KB, ~10 paged
                               requests, refreshed every 24h.

Public API (everything safe to call from anywhere — empty cache = empty
results, never raises):

  init_cache()                          load both files, build indices
  get_id_by_contract(chain, addr)       (chain, addr) -> coingecko_id
  get_info_by_id(cg_id)                 cg_id -> {symbol, name, platforms}
  find_ids_by_symbol(symbol)            "LIT" -> [list of cg_ids]
  is_traded_on_kraken(cg_id)            bool
  kraken_verified_ids()                 frozenset[str]

  refresh_platforms()                   async, fetches /coins/list bulk
  refresh_kraken_pairs()                async, fetches Kraken tickers
  refresh_loop(interval)                async background task — calls both
                                        on a 24h schedule with persistent
                                        last_refresh timestamp.

The HTTP path uses the shared rate_limiter ("coingecko" bucket) and a
short retry budget. On any failure (network, 429, malformed JSON) we
log and keep the existing cache — the bot never breaks because of CG.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Iterable, Optional

import aiohttp

from engine.atomic_io import atomic_write_json
from engine.config import COINGECKO_API_KEY, COINGECKO_REFRESH_INTERVAL
from engine.logger import get_logger
from engine.rate_limiter import acquire as rl_acquire


log = get_logger(__name__)

# --------------------------------------------------------------------------
# paths / endpoints / chain mapping
# --------------------------------------------------------------------------

CACHE_DIR = Path(__file__).resolve().parents[1] / "cex_part" / "cex_cache"
PLATFORMS_FILE = CACHE_DIR / "coingecko_platforms.json"
KRAKEN_PAIRS_FILE = CACHE_DIR / "coingecko_kraken_pairs.json"

# Active retry queue for fresh Kraken listings that CG hadn't indexed
# yet at the moment they appeared. The retry loop polls this every
# ~15 min and refreshes both CG endpoints when entries are due.
PENDING_FILE = CACHE_DIR / "kraken_pending.json"

# A pending entry is retried no more often than this. CG normally
# indexes a fresh listing within hours, sometimes a day, so 14 min
# gives a snappy pickup without burning quota.
PENDING_RETRY_INTERVAL_SEC = 14 * 60

# After this much time without a successful resolve, give up: CG
# probably won't ever index this token (or it's on a chain we don't
# scan). The cleanup pass will then drop the entry from cex_tokens
# on the next bot startup.
PENDING_MAX_AGE_SEC = 7 * 24 * 3600

CG_BASE = "https://api.coingecko.com/api/v3"
CG_PRO_BASE = "https://pro-api.coingecko.com/api/v3"

# CoinGecko platform name -> our internal chain key.
# Anything not in this map is ignored (other chains we don't track).
PLATFORM_TO_CHAIN = {
    "ethereum": "eth",
    "binance-smart-chain": "bsc",
    "base": "base",
    "solana": "sol",
}

# How many tickers per page Kraken returns. 100 is the CG default/max.
KRAKEN_PAGE_SIZE = 100
# Hard cap on pages — if Kraken ever has more than 5000 pairs something
# else is wrong; better to truncate than loop forever.
KRAKEN_MAX_PAGES = 50


# --------------------------------------------------------------------------
# in-memory state
# --------------------------------------------------------------------------

# (chain_lower, addr_lower) -> coingecko_id
_BY_CONTRACT: dict[tuple[str, str], str] = {}

# coingecko_id -> {"symbol": str, "name": str, "platforms": dict}
_BY_ID: dict[str, dict] = {}

# SYMBOL_UPPER -> [coingecko_id, ...]   (multi-valued; same ticker often
# appears under several different CG ids)
_BY_SYMBOL: dict[str, list[str]] = {}

# coingecko_ids that are actually traded on Kraken right now
_KRAKEN_VERIFIED_IDS: frozenset[str] = frozenset()


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _now() -> int:
    return int(time.time())


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("CG cache %s unreadable, ignoring: %s", path.name, e)
        return None


def _http_headers() -> dict[str, str]:
    headers = {"accept": "application/json", "user-agent": "arbitrage-bot/4.0"}
    if COINGECKO_API_KEY:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return headers


def _http_base() -> str:
    return CG_PRO_BASE if COINGECKO_API_KEY else CG_BASE


# --------------------------------------------------------------------------
# index building (called from init_cache and after each refresh)
# --------------------------------------------------------------------------

def _rebuild_platforms_index(coins: Iterable[dict]) -> None:
    """Rebuild _BY_CONTRACT, _BY_ID, _BY_SYMBOL from a /coins/list dump."""
    global _BY_CONTRACT, _BY_ID, _BY_SYMBOL

    by_contract: dict[tuple[str, str], str] = {}
    by_id: dict[str, dict] = {}
    by_symbol: dict[str, list[str]] = {}

    for coin in coins:
        if not isinstance(coin, dict):
            continue
        cg_id = coin.get("id")
        if not cg_id:
            continue

        symbol = (coin.get("symbol") or "").upper()
        name = coin.get("name") or ""
        platforms_raw = coin.get("platforms") or {}

        # only keep platforms we can map to one of our chains
        platforms: dict[str, str] = {}
        for plat_cg, addr in platforms_raw.items():
            chain = PLATFORM_TO_CHAIN.get((plat_cg or "").lower())
            if not chain:
                continue
            if not addr or not isinstance(addr, str):
                continue
            addr_l = addr.strip().lower()
            if not addr_l:
                continue
            platforms[chain] = addr_l
            by_contract[(chain, addr_l)] = cg_id

        by_id[cg_id] = {"symbol": symbol, "name": name, "platforms": platforms}

        if symbol:
            by_symbol.setdefault(symbol, []).append(cg_id)

    _BY_CONTRACT = by_contract
    _BY_ID = by_id
    _BY_SYMBOL = by_symbol


def _rebuild_kraken_index(tickers: Iterable[dict]) -> None:
    """Rebuild _KRAKEN_VERIFIED_IDS from a /exchanges/kraken/tickers dump."""
    global _KRAKEN_VERIFIED_IDS

    ids: set[str] = set()
    for t in tickers:
        if not isinstance(t, dict):
            continue
        cg_id = t.get("coin_id")
        if cg_id:
            ids.add(cg_id)

    _KRAKEN_VERIFIED_IDS = frozenset(ids)


# --------------------------------------------------------------------------
# public init / lookup
# --------------------------------------------------------------------------

def init_cache() -> None:
    """Load both JSON files from disk and rebuild in-memory indices.

    Safe to call multiple times. Missing/corrupt files leave the indices
    empty — every lookup will then return None / False, which is fine.
    """
    plat = _load_json(PLATFORMS_FILE) or {}
    coins = plat.get("coins") if isinstance(plat, dict) else None
    if isinstance(coins, list):
        _rebuild_platforms_index(coins)

    kraken = _load_json(KRAKEN_PAIRS_FILE) or {}
    tickers = kraken.get("tickers") if isinstance(kraken, dict) else None
    if isinstance(tickers, list):
        _rebuild_kraken_index(tickers)

    log.info(
        "coingecko cache loaded: contracts=%d ids=%d symbols=%d kraken_verified=%d",
        len(_BY_CONTRACT), len(_BY_ID), len(_BY_SYMBOL), len(_KRAKEN_VERIFIED_IDS),
    )


def get_id_by_contract(chain: str, addr: str) -> Optional[str]:
    if not chain or not addr:
        return None
    return _BY_CONTRACT.get((chain.lower(), addr.lower()))


def get_info_by_id(cg_id: str) -> Optional[dict]:
    if not cg_id:
        return None
    return _BY_ID.get(cg_id)


def find_ids_by_symbol(symbol: str) -> list[str]:
    if not symbol:
        return []
    return list(_BY_SYMBOL.get(symbol.upper(), ()))


def is_traded_on_kraken(cg_id: str) -> bool:
    if not cg_id:
        return False
    return cg_id in _KRAKEN_VERIFIED_IDS


def kraken_verified_ids() -> frozenset[str]:
    return _KRAKEN_VERIFIED_IDS


# --------------------------------------------------------------------------
# HTTP fetchers (used by refresh_loop; safe to call manually too)
# --------------------------------------------------------------------------

async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    params: dict | None = None,
    max_retries: int = 4,
) -> Any:
    """GET with rate-limiter + 429 backoff. Returns parsed JSON or None.

    Free-tier CoinGecko routinely 429s for ~30-60s under load — we retry
    a few times with exponential backoff (capped at 60s) before giving
    up. Non-429 errors don't retry (probably persistent).
    """
    backoff = 15.0
    for attempt in range(max_retries):
        await rl_acquire("coingecko")
        try:
            async with session.get(url, params=params, headers=_http_headers(), timeout=60) as r:
                if r.status == 429:
                    log.warning(
                        "coingecko 429 on %s (attempt %d/%d) — sleeping %.0fs",
                        url, attempt + 1, max_retries, backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
                    continue
                if r.status != 200:
                    log.warning("coingecko %s -> %s", url, r.status)
                    return None
                return await r.json()
        except asyncio.TimeoutError:
            log.warning("coingecko %s timeout (attempt %d/%d)", url, attempt + 1, max_retries)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    log.warning("coingecko %s gave up after %d retries", url, max_retries)
    return None


async def refresh_platforms() -> bool:
    """Fetch /coins/list?include_platform=true and persist to disk.

    Returns True if cache was updated, False on failure (existing cache
    untouched).
    """
    url = f"{_http_base()}/coins/list"
    params = {"include_platform": "true"}

    async with aiohttp.ClientSession() as session:
        try:
            data = await _fetch_json(session, url, params=params)
        except Exception as e:
            log.warning("coingecko platforms fetch failed: %s", e)
            return False

    if not isinstance(data, list) or not data:
        log.warning("coingecko platforms response empty/unexpected, keeping old cache")
        return False

    payload = {"_meta": {"fetched_at": _now(), "count": len(data)}, "coins": data}
    atomic_write_json(PLATFORMS_FILE, payload)
    _rebuild_platforms_index(data)
    log.info("coingecko platforms refreshed: %d coins", len(data))
    return True


async def refresh_kraken_pairs() -> bool:
    """Fetch all pages of /exchanges/kraken/tickers and persist to disk.

    Returns True if cache was updated, False on failure.
    """
    url = f"{_http_base()}/exchanges/kraken/tickers"
    all_tickers: list[dict] = []

    async with aiohttp.ClientSession() as session:
        for page in range(1, KRAKEN_MAX_PAGES + 1):
            try:
                data = await _fetch_json(session, url, params={"page": str(page)})
            except Exception as e:
                log.warning("coingecko kraken page %d failed: %s", page, e)
                return False
            if not isinstance(data, dict):
                return False

            tickers = data.get("tickers")
            if not isinstance(tickers, list) or not tickers:
                # last page reached (empty)
                break
            all_tickers.extend(tickers)
            # stop early when CG returns less than a full page
            if len(tickers) < KRAKEN_PAGE_SIZE:
                break

    if not all_tickers:
        log.warning("coingecko kraken tickers came back empty, keeping old cache")
        return False

    payload = {"_meta": {"fetched_at": _now(), "count": len(all_tickers)}, "tickers": all_tickers}
    atomic_write_json(KRAKEN_PAIRS_FILE, payload)
    _rebuild_kraken_index(all_tickers)

    unique_ids = sum(1 for t in all_tickers if t.get("coin_id"))
    log.info(
        "coingecko kraken pairs refreshed: tickers=%d unique_coin_ids=%d",
        len(all_tickers), len(_KRAKEN_VERIFIED_IDS),
    )
    _ = unique_ids  # silence linter; real metric is _KRAKEN_VERIFIED_IDS
    return True


# --------------------------------------------------------------------------
# refresh loop
# --------------------------------------------------------------------------

def _platforms_age_sec() -> float:
    plat = _load_json(PLATFORMS_FILE) or {}
    fetched = (plat.get("_meta") or {}).get("fetched_at") if isinstance(plat, dict) else None
    if not fetched:
        return float("inf")
    return max(0.0, time.time() - float(fetched))


def _kraken_age_sec() -> float:
    krk = _load_json(KRAKEN_PAIRS_FILE) or {}
    fetched = (krk.get("_meta") or {}).get("fetched_at") if isinstance(krk, dict) else None
    if not fetched:
        return float("inf")
    return max(0.0, time.time() - float(fetched))


# --------------------------------------------------------------------------
# pending-listings queue (fresh Kraken symbols awaiting CG indexing)
# --------------------------------------------------------------------------

def pending_load() -> dict[str, dict]:
    data = _load_json(PENDING_FILE) or {}
    return data if isinstance(data, dict) else {}


def pending_save(data: dict) -> None:
    atomic_write_json(PENDING_FILE, data)


def pending_register(cache_key: str, kraken_symbol: str) -> None:
    """Add a freshly-listed Kraken entry to the retry queue."""
    p = pending_load()
    if cache_key in p:
        return
    p[cache_key] = {
        "first_seen": _now(),
        "last_try": _now(),
        "attempts": 1,
        "kraken_symbol": (kraken_symbol or "").upper(),
    }
    pending_save(p)
    log.info("pending: registered %s (%s) for CG retry", cache_key, kraken_symbol)


def pending_remove(cache_key: str) -> None:
    p = pending_load()
    if p.pop(cache_key, None) is not None:
        pending_save(p)


def pending_keys() -> set[str]:
    return set(pending_load().keys())


def _pending_due(p: dict[str, dict]) -> list[str]:
    now = _now()
    return [
        k for k, m in p.items()
        if (now - int(m.get("last_try", 0))) >= PENDING_RETRY_INTERVAL_SEC
    ]


async def kraken_pending_retry_loop(interval: float = 15 * 60.0) -> None:
    """
    Background task: every `interval` seconds, refresh CG (only if
    there is something to retry) and try to resolve fresh Kraken
    listings that previously had no CG match.

    Successful resolves → entry stays in cex_tokens, gets removed from
    queue. Aged-out entries (> PENDING_MAX_AGE_SEC) → removed from
    queue; `cleanup_kraken_pending` on next startup will drop them
    from cex_tokens.
    """
    if interval <= 0:
        log.info("kraken pending retry disabled (interval<=0)")
        return

    # Lazy import — cache_manager imports engine.coingecko at module
    # import time, so a top-level import here would create a cycle.
    from cex_part import cache_manager

    log.info("kraken pending retry loop started (interval=%.0fs)", interval)
    while True:
        try:
            await asyncio.sleep(interval)

            p = pending_load()
            if not p:
                continue

            due = _pending_due(p)
            if not due:
                continue

            log.info(
                "kraken pending retry: %d due (queue=%d) — refreshing CG",
                len(due), len(p),
            )
            await refresh_platforms()
            await refresh_kraken_pairs()

            now = _now()
            p = pending_load()  # reload in case it changed
            changed = False
            for key in due:
                meta = p.get(key)
                if meta is None:
                    continue
                age = now - int(meta.get("first_seen", now))
                if age > PENDING_MAX_AGE_SEC:
                    log.info(
                        "kraken pending: %s timed out (%.1fd) — dropping from queue",
                        key, age / 86400,
                    )
                    p.pop(key, None)
                    changed = True
                    continue

                resolved = await cache_manager.apply_coingecko_metadata_for_entry(key)
                if resolved:
                    log.info(
                        "kraken pending: %s RESOLVED after %d attempts (%.1fh)",
                        key, meta.get("attempts", 0) + 1, age / 3600,
                    )
                    p.pop(key, None)
                else:
                    p[key]["last_try"] = now
                    p[key]["attempts"] = int(meta.get("attempts", 0)) + 1
                changed = True

            if changed:
                pending_save(p)
        except Exception as e:
            log.error("kraken pending retry loop error: %s", e)


# --------------------------------------------------------------------------
# refresh loop (24h CG bulk refresh)
# --------------------------------------------------------------------------

async def refresh_loop(interval: float = COINGECKO_REFRESH_INTERVAL) -> None:
    """Background loop: refresh both caches every `interval` seconds.

    Honours the existing on-disk timestamps so a quick restart does not
    re-fetch immediately. Logs and continues on any error.
    """
    if interval <= 0:
        log.info("coingecko refresh disabled (interval<=0)")
        return

    log.info(
        "coingecko refresh loop started (interval=%.0fs, platforms_age=%.0fs, kraken_age=%.0fs)",
        interval, _platforms_age_sec(), _kraken_age_sec(),
    )

    while True:
        try:
            if _platforms_age_sec() >= interval:
                await refresh_platforms()
            if _kraken_age_sec() >= interval:
                await refresh_kraken_pairs()
        except Exception as e:
            log.error("coingecko refresh loop error: %s", e)

        # Sleep until the *earlier* of the two caches will be due again.
        # Worst-case both just refreshed → wait full interval.
        wait = max(60.0, min(
            interval - _platforms_age_sec(),
            interval - _kraken_age_sec(),
            interval,
        ))
        await asyncio.sleep(wait)
