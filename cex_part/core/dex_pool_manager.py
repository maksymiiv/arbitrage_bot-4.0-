"""
For every CEX-known token with on-chain contracts, ask DexScreener for the
highest-volume EVM pool on each chain and persist the (token, chain, pool)
link into both pools.json (DEX side) and cex_tokens.json (CEX side).
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import aiohttp

from dex_part.core.bad_pools import is_rejected as is_pool_rejected
from dex_part.core.pools_io import (
    load as pools_load,
    operation_lock as pools_operation_lock,
    save as pools_save,
)
from engine.atomic_io import atomic_write_json
from engine.config import DEXSCREENER_CONCURRENCY, NO_POOL_RETRY_TTL_SEC
from engine.logger import get_logger
from engine.rate_limiter import acquire as rl_acquire

from .. import cache_manager
from ..config.blacklist import is_blacklisted, is_chain_blacklisted


log = get_logger(__name__)

# Tiered retry for tokens that DexScreener didn't yet have a pool for.
# Driven by AGE since first detect — early (0-2h after CEX listing) the
# DEX pool may appear or get its initial liquidity any minute, so retry
# fast; later it almost never changes state, so back off to daily.
#
#   0-2h since first_seen   → 2 min      (hot phase: pool may appear any minute)
#   2-24h                   → NO_POOL_RETRY_TTL_SEC (default 10 min)
#   24h+                    → 24 h       (treat like every other stable token)
_HOT_PHASE_SEC = 2 * 3600         # boundary: hot → warm
_WARM_PHASE_SEC = 24 * 3600       # boundary: warm → cold
_HOT_RETRY_SEC = 2 * 60
_COLD_RETRY_SEC = 24 * 3600

# Back-compat alias — older code paths may still reference this.
RETRY_TTL_SEC = NO_POOL_RETRY_TTL_SEC


def _retry_cooldown(age_since_first_seen: float) -> int:
    """Return the cooldown that should apply at this age."""
    if age_since_first_seen < _HOT_PHASE_SEC:
        return _HOT_RETRY_SEC
    if age_since_first_seen < _WARM_PHASE_SEC:
        return NO_POOL_RETRY_TTL_SEC
    return _COLD_RETRY_SEC


def _read_no_pool_entry(entry) -> tuple[int, int] | None:
    """
    Return (first_seen, last_try) tuple from a no_dex_pools.json entry,
    handling both schemas:
      - legacy: bare int timestamp (treat as last_try, first_seen=last_try)
      - new:    {"first_seen": int, "last_try": int}
    Returns None if entry is malformed.
    """
    if isinstance(entry, int):
        return entry, entry
    if isinstance(entry, dict):
        try:
            return int(entry["first_seen"]), int(entry["last_try"])
        except (KeyError, TypeError, ValueError):
            return None
    return None

CEX_BASE_DIR = Path(__file__).resolve().parents[1]
NO_POOL_FILE = CEX_BASE_DIR / "cex_cache" / "no_dex_pools.json"

DEXSCREENER = "https://api.dexscreener.com/latest/dex/tokens/"

CHAIN_MAP = {
    "ethereum": "eth",
    "bsc": "bsc",
    "base": "base",
    "solana": "sol",
}

_semaphore = asyncio.Semaphore(DEXSCREENER_CONCURRENCY)


# --------------------------------------------------------------------------
# no_pools sidecar (kept here — purely a CEX-side throttle file)
# --------------------------------------------------------------------------

def _load_no_pools() -> dict:
    if not NO_POOL_FILE.exists():
        return {}
    try:
        text = NO_POOL_FILE.read_text(encoding="utf-8").strip()
        return json.loads(text) if text else {}
    except Exception:
        log.warning("no_dex_pools.json corrupted, resetting")
        return {}


def _write_no_pools(data: dict) -> None:
    atomic_write_json(NO_POOL_FILE, data)


def _is_evm_pool_address(addr: str) -> bool:
    if not isinstance(addr, str):
        return False
    addr = addr.lower()
    return addr.startswith("0x") and len(addr) == 42


# --------------------------------------------------------------------------
# DexScreener
# --------------------------------------------------------------------------

async def _fetch_best_pool(
    session: aiohttp.ClientSession,
    token_address: str,
    expected_chain: str,
) -> dict[str, Any] | None:
    """
    Single-token discovery query. Throttled through the shared
    `dexscreener` token bucket so we don't fight pool_refresh for the
    same 300/min IP budget. On 429 we silently return None — the caller
    records the symbol in `no_dex_pools` and retries later.
    """
    async with _semaphore:
        await rl_acquire("dexscreener")
        try:
            async with session.get(f"{DEXSCREENER}{token_address}", timeout=20) as r:
                if r.status == 429:
                    log.debug("DexScreener 429 for %s — will retry later", token_address)
                    return None
                if r.status != 200:
                    return None
                data = await r.json()

            pairs = data.get("pairs") or []
            pairs = [p for p in pairs if CHAIN_MAP.get(p.get("chainId")) == expected_chain]
            pairs = [p for p in pairs if _is_evm_pool_address(p.get("pairAddress"))]
            # Skip pools we've already rejected today for repeatedly
            # producing out-of-range prices — otherwise we'd just
            # re-attach the same broken pool DexScreener still ranks
            # first by volume.
            pairs = [p for p in pairs if not is_pool_rejected(expected_chain, p.get("pairAddress") or "")]
            if not pairs:
                return None

            return max(pairs, key=lambda x: x.get("volume", {}).get("h24", 0) or 0)

        except Exception as e:
            log.debug("DexScreener fetch failed for %s: %s", token_address, e)
            return None


async def _process_token(
    symbol: str,
    address: str,
    chain: str,
    pools: dict,
    existing_symbols: dict,
    no_pools: dict,
    session: aiohttp.ClientSession,
) -> None:
    now = int(time.time())
    symbol = (symbol or "").upper()
    chain = (chain or "").lower()

    if symbol in existing_symbols.get(chain, set()):
        return

    entry = no_pools.get(chain, {}).get(symbol)
    parsed = _read_no_pool_entry(entry) if entry is not None else None
    if parsed:
        first_seen, last_try = parsed
        cooldown = _retry_cooldown(now - first_seen)
        if now - last_try < cooldown:
            return
    else:
        # No prior entry — first detect for this (chain, symbol).
        first_seen = now

    best = await _fetch_best_pool(session, address, chain)
    if not best:
        no_pools.setdefault(chain, {})[symbol] = {
            "first_seen": first_seen,
            "last_try": now,
        }
        _write_no_pools(no_pools)
        log.debug("[DEX] retry later: %s %s", symbol, chain)
        return

    pool_addr = (best.get("pairAddress") or "").lower()
    if not pool_addr:
        return

    pools.setdefault(chain, []).append({
        "pool": pool_addr,
        "dex": best.get("dexId"),
        "symbol": symbol,
        "version": None,
    })
    existing_symbols.setdefault(chain, set()).add(symbol)
    await pools_save(pools)

    try:
        await cache_manager.add_pool(symbol=symbol, chain=chain, pool=pool_addr)
    except Exception as e:
        log.warning("[CEX] pool link failed %s %s: %s", symbol, chain, e)

    no_pools.get(chain, {}).pop(symbol, None)
    _write_no_pools(no_pools)
    log.info("[DEX] pool added: %s -> %s %s", symbol, chain.upper(), pool_addr)


async def sync_pools_with_cache() -> None:
    # Hold the high-level pools lock for the ENTIRE sweep so a concurrent
    # `pool_refresh.refresh_pools_once` can't load a stale snapshot, drop
    # entries, and overwrite our newly-added rows ("lost update" race —
    # this used to silently delete a third of the discovered pools).
    async with pools_operation_lock():
        await _sync_pools_with_cache_locked()


async def _sync_pools_with_cache_locked() -> None:
    pools = await pools_load()
    no_pools = _load_no_pools()

    existing_symbols: dict[str, set[str]] = {}
    for chain, items in pools.items():
        for p in items:
            if isinstance(p, dict):
                sym = (p.get("symbol") or "").upper()
                if sym:
                    existing_symbols.setdefault(chain, set()).add(sym)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for entry in cache_manager._CACHE.values():
            symbol = entry.get("symbol")
            if not symbol:
                continue
            # 🔒 never bind a pool to a stable/blacklisted symbol — that
            # used to corrupt USDC/USDT entries with the price of the
            # OTHER side of the pool (e.g. USDC/WETH would be stored as
            # USDC's price even though it's WETH's).
            if is_blacklisted(symbol):
                continue
            for chain, addr in (entry.get("contracts") or {}).items():
                chain = chain.lower()
                # Структурно неможлива пара (BNB на eth, SOL на bsc тощо):
                # на цьому чейні CEX не виводить токен, арбітраж нереальний.
                if is_chain_blacklisted(symbol, chain):
                    continue
                if symbol.upper() in existing_symbols.get(chain, set()):
                    continue
                tasks.append(
                    _process_token(symbol, addr, chain, pools, existing_symbols, no_pools, session)
                )

        if tasks:
            await asyncio.gather(*tasks)

    await pools_save(pools)
    _write_no_pools(no_pools)
