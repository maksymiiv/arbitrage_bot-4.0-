"""
Background maintenance for pools.json:

1) Drop pools that DexScreener no longer knows about (delisted / migrated).
2) Drop pools tied to blacklisted CEX symbols (legacy USDC/USDT/etc. that
   leaked in before the blacklist was added).
3) Re-bind every surviving pool to a cache_manager entry so the in-memory
   pool index stays consistent — this is what lets you delete
   cex_tokens.json on its own (without touching pools.json) and still
   recover full DEX routing after the next CEX metadata pass.

Pools are validated through `/latest/dex/pairs/{chainId}/{addr1,addr2,...}`
which accepts up to 30 addresses per request — much cheaper than
re-querying every token's full pair list. With INTER_BATCH_SLEEP=0.3 we
sit at ~120 req/min, well under DexScreener's 300/min IP cap, so the
refresh interval can safely go down to 1h even on tens of thousands of
pools.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Iterable, Optional

import aiohttp

from engine.atomic_io import atomic_write_json
from engine.logger import get_logger
from engine.rate_limiter import acquire as rl_acquire

from .. import cache_manager
from ..config.blacklist import is_blacklisted, is_chain_blacklisted
from dex_part.core.pools_io import (
    load as pools_load,
    operation_lock as pools_operation_lock,
    save as pools_save,
)


log = get_logger(__name__)

# DexScreener: chainId is one of "bsc", "ethereum", "base", "solana", ...
DEX_CHAIN_REVERSE = {
    "bsc": "bsc",
    "eth": "ethereum",
    "base": "base",
    "sol": "solana",
}

DEXSCREENER_PAIRS = "https://api.dexscreener.com/latest/dex/pairs"

# Persistent state — keeps the wall-clock timestamp of the last full
# refresh so a quick restart doesn't burn DexScreener quota repeating it.
_STATE_FILE = Path(__file__).resolve().parents[1] / "cex_cache" / "pool_refresh_state.json"


def _load_state() -> dict:
    if not _STATE_FILE.exists():
        return {}
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    atomic_write_json(_STATE_FILE, state)

# DexScreener accepts up to 30 addresses in one URL
BATCH_SIZE = 30
# When a 429 hits we back off this long before the next batch
RATE_LIMIT_BACKOFF_INITIAL = 5.0
RATE_LIMIT_BACKOFF_MAX = 60.0


async def _check_chain_pools(
    session: aiohttp.ClientSession,
    chain_local: str,
    pool_addrs: list[str],
) -> set[str]:
    """
    Return the subset of `pool_addrs` (lowercase) that DexScreener still
    knows about on `chain_local`. Uses the shared rate limiter so we
    don't fight `_dex_pool_loop` for the same 300/min IP budget; on 429
    we back off exponentially before the next batch (capped).
    """
    chain_remote = DEX_CHAIN_REVERSE.get(chain_local)
    if not chain_remote:
        log.debug("unknown chain for refresh: %s", chain_local)
        # be conservative: don't drop pools on chains we can't validate
        return set(pool_addrs)

    alive: set[str] = set()
    backoff = RATE_LIMIT_BACKOFF_INITIAL

    for i in range(0, len(pool_addrs), BATCH_SIZE):
        batch = pool_addrs[i:i + BATCH_SIZE]
        url = f"{DEXSCREENER_PAIRS}/{chain_remote}/{','.join(batch)}"

        await rl_acquire("dexscreener")

        try:
            async with session.get(url, timeout=20) as r:
                status = r.status
                if status == 429:
                    log.warning(
                        "DexScreener 429 (%s, %d addrs) — sleeping %.1fs",
                        chain_local, len(batch), backoff,
                    )
                    alive.update(batch)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, RATE_LIMIT_BACKOFF_MAX)
                    continue
                if status != 200:
                    log.warning("DexScreener %s/%s -> %s, keeping batch",
                                chain_local, batch[0], status)
                    alive.update(batch)
                    continue
                data = await r.json()
        except Exception as e:
            log.warning("DexScreener fetch failed (%s): %s — keeping batch", chain_local, e)
            alive.update(batch)
            continue

        # successful 200 -> reset backoff
        backoff = RATE_LIMIT_BACKOFF_INITIAL

        pairs = data.get("pairs") or []
        for p in pairs:
            addr = (p.get("pairAddress") or "").lower()
            if addr:
                alive.add(addr)

    return alive


def _drop_pool_from_cache(symbol: str, chain: str, pool: str) -> None:
    """Remove a pool from cache_manager._CACHE entries + pool index."""
    sym_u = (symbol or "").upper()
    chain_l = (chain or "").lower()
    pool_l = (pool or "").lower()

    keys: Iterable[str] = []
    keys_idx = cache_manager._INDEX["symbol"].get(sym_u)
    if isinstance(keys_idx, list):
        keys = list(keys_idx)
    elif isinstance(keys_idx, str):
        keys = [keys_idx]

    for key in keys:
        entry = cache_manager._CACHE.get(key)
        if not isinstance(entry, dict):
            continue
        chain_pools = (entry.get("pools") or {}).get(chain_l) or []
        if pool_l in chain_pools:
            chain_pools.remove(pool_l)

    cache_manager._INDEX["pool"].pop(f"{chain_l}:{pool_l}", None)


async def drop_pool_for_replacement(chain: str, pool: str) -> None:
    """
    Evict a known-bad pool from `pools.json` AND `cache_manager`, then
    fire `schedule_pool_sync()` so the next discovery pass picks a
    replacement from DexScreener (which will skip rejected addresses
    via `bad_pools.is_rejected`).

    Called by `chain_monitor` when a pool repeatedly emits out-of-range
    prices — see `dex_part.core.bad_pools` for the threshold logic.
    """
    from cex_part.background_task import schedule_pool_sync

    chain_l = (chain or "").lower()
    pool_l = (pool or "").lower()
    if not chain_l or not pool_l:
        return

    # Find the symbol via the existing pool index so we can also clean
    # up `cache_manager._CACHE[token]["pools"][chain]`. Best-effort —
    # if the index doesn't know the pool, just skip the cache cleanup.
    symbol = ""
    key = cache_manager._INDEX["pool"].get(f"{chain_l}:{pool_l}")
    if key:
        entry = cache_manager._CACHE.get(key, {})
        symbol = (entry.get("symbol") or "").upper()

    # 1) drop from pools.json — held under operation_lock so we don't
    #    race the periodic refresh / sync passes.
    async with pools_operation_lock():
        pools = await pools_load()
        items = pools.get(chain_l) or []
        before = len(items)
        items = [
            p for p in items
            if not (isinstance(p, dict) and (p.get("pool") or "").lower() == pool_l)
        ]
        if len(items) != before:
            pools[chain_l] = items
            await pools_save(pools)
            log.info("dropped bad pool from pools.json: %s %s", chain_l, pool_l)

    # 2) drop from cache_manager indexes (symbol→pool list, pool→key).
    if symbol:
        _drop_pool_from_cache(symbol, chain_l, pool_l)

    # 3) trigger a debounced re-discovery. If DexScreener has another
    #    pool for the same token, sync_pools_with_cache will pick it up;
    #    if the only candidate is the one we just rejected, the
    #    `bad_pools.is_rejected` filter blocks it for 24h.
    schedule_pool_sync()


async def replace_pool(
    chain: str,
    token_symbol: str,
    old_pool: str,
    new_pool: str,
    dex: str,
    version: Optional[str] = None,
    currency0: Optional[str] = None,
    currency1: Optional[str] = None,
    trigger_restart: bool = True,
) -> bool:
    """
    Atomic pool swap: drop `old_pool` and insert `new_pool` in pools.json
    + cache_manager for the same (chain, token_symbol). Used by the
    liquidity-filter's auto-upgrade flow when GeckoTerminal reveals a
    substantially better pool than the one DexScreener originally picked.

    For a Uniswap V4 replacement (`version == "v4"`) `new_pool` is a
    32-byte poolId and `currency0`/`currency1` MUST be supplied — there
    is no pool contract to read them from. The V4 pool metadata is built
    + cached up-front via `get_v4_pool_metadata`.

    Side effects:
      - Old pool gets added to `bad_pools._REJECTED` so the
        dex_pool_manager discovery pass won't immediately re-attach it.
      - When `trigger_restart` is True, sets the chain's restart event so
        chain_monitor reconnects with the updated subscription list.
        The bulk migration passes False and restarts once at the end.

    Returns True if a real swap happened, False on no-op / failure.
    """
    from dex_part.core.bad_pools import mark_rejected
    from dex_part.core.ws_subscriber import get_restart_event

    chain_l = (chain or "").lower()
    old_l = (old_pool or "").lower()
    new_l = (new_pool or "").lower()
    sym_u = (token_symbol or "").upper()
    is_v4 = (version == "v4")

    if not chain_l or not new_l or not sym_u:
        return False
    if old_l == new_l:
        return False

    # V4: build + cache the pool metadata BEFORE touching pools.json —
    # if this fails we don't want a half-applied state. There's no pool
    # contract; currencies come from the discovery source.
    if is_v4:
        c0 = (currency0 or "").lower()
        c1 = (currency1 or "").lower()
        if not c0 or not c1:
            log.warning("replace_pool: v4 pool %s missing currencies — skip", new_l)
            return False
        try:
            from web3 import Web3
            from dex_part.utils.token_metadata import get_v4_pool_metadata
            from engine.config import CHAINS
            rpc = (CHAINS.get(chain_l) or {}).get("rpc")
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
            get_v4_pool_metadata(w3, chain_l, new_l, c0, c1, dex or "uniswap")
        except Exception as e:
            log.warning("replace_pool: v4 metadata build failed for %s: %s", new_l, e)
            return False

    async with pools_operation_lock():
        pools = await pools_load()
        items = list(pools.get(chain_l) or [])

        # Strip out the old pool address; also strip any pre-existing
        # entry with the same NEW address so we don't end up with a dup.
        kept: list[dict] = []
        had_old = False
        for item in items:
            if not isinstance(item, dict):
                continue
            addr_l = (item.get("pool") or "").lower()
            if addr_l == old_l:
                had_old = True
                continue
            if addr_l == new_l:
                continue
            kept.append(item)

        new_entry = {
            "pool": new_l,
            "dex": dex or "unknown",
            "symbol": sym_u,
            "version": version,
            # `source=gt` tells the daily pool_refresh pass NOT to drop
            # this entry just because DexScreener doesn't index it.
            "source": "gt",
        }
        if is_v4:
            new_entry["currency0"] = (currency0 or "").lower()
            new_entry["currency1"] = (currency1 or "").lower()
        kept.append(new_entry)
        pools[chain_l] = kept
        await pools_save(pools)

    # Cache_manager cleanup + reattach.
    if had_old:
        _drop_pool_from_cache(sym_u, chain_l, old_l)
        # Blacklist old address so DS-driven discovery can't loop it back.
        mark_rejected(chain_l, old_l)
    try:
        # Indexes the pool/poolId -> token key so price_store can route
        # swaps. Works the same for a V4 poolId (just a longer string).
        await cache_manager.add_pool(symbol=sym_u, chain=chain_l, pool=new_l)
    except Exception as e:
        log.warning("replace_pool: add_pool failed for %s %s/%s: %s", chain_l, sym_u, new_l, e)

    # Force WS to re-init + re-subscribe so chain_monitor drops the old
    # V2/V3 subscription. V4 pools are picked up by v4_monitor's own
    # periodic reload, so a restart isn't strictly needed for them — but
    # the old pool still needs to leave chain_monitor's set. Restart only
    # the affected chain.
    if trigger_restart:
        get_restart_event(chain_l).set()

    log.info(
        "pool upgraded: %s %s %s -> %s (dex=%s%s)",
        chain_l, sym_u, old_l, new_l, dex,
        f", v={version}" if version else "",
    )
    return True


async def refresh_pools_once() -> None:
    """One-shot pass: validate every pool in pools.json, drop stale & blacklisted."""
    # Mutually exclusive with sync_pools_with_cache so neither can
    # overwrite the other's edits.
    async with pools_operation_lock():
        await _refresh_pools_once_locked()


async def _refresh_pools_once_locked() -> None:
    pools = await pools_load()
    if not pools:
        log.info("pool refresh: pools.json empty, nothing to do")
        return

    started = time.time()
    total = sum(len(items) for items in pools.values() if isinstance(items, list))
    log.info("pool refresh: validating %d pools across %d chains", total, len(pools))

    dropped_stale = 0
    dropped_blacklisted = 0
    new_pools: dict[str, list[dict]] = {}

    async with aiohttp.ClientSession() as session:
        for chain, items in pools.items():
            if not isinstance(items, list):
                continue

            # Phase 1: drop blacklisted symbols (no API call needed).
            # Глобальний symbol-blacklist (стейбли) + chain-level блекліст
            # (наприклад, BNB на eth — CEX туди не виводить).
            kept_after_bl: list[dict] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                sym = (item.get("symbol") or "").upper()
                if sym and (is_blacklisted(sym) or is_chain_blacklisted(sym, chain)):
                    dropped_blacklisted += 1
                    _drop_pool_from_cache(sym, chain, item.get("pool", ""))
                    continue
                kept_after_bl.append(item)

            if not kept_after_bl:
                new_pools[chain] = []
                continue

            # Phase 2: validate remaining pools against DexScreener.
            # Skip entries marked `source=gt` — those came from the
            # liquidity-filter's auto-upgrade flow and live on DEXes
            # DexScreener may not index (Aerodrome, Pancake V3 forks,
            # niche AMMs). Validating them via DS would drop perfectly
            # good pools and DS-discovery would then re-attach the
            # weaker pool we explicitly replaced.
            gt_pools = [it for it in kept_after_bl if (it.get("source") == "gt")]
            ds_pools = [it for it in kept_after_bl if (it.get("source") != "gt")]

            addrs = [str(it.get("pool", "")).lower() for it in ds_pools if it.get("pool")]
            alive = await _check_chain_pools(session, chain, addrs)

            kept: list[dict] = list(gt_pools)  # GT-sourced kept unconditionally
            for item in ds_pools:
                addr = str(item.get("pool", "")).lower()
                if addr and addr in alive:
                    kept.append(item)
                else:
                    dropped_stale += 1
                    _drop_pool_from_cache(item.get("symbol", ""), chain, addr)
                    log.info("pool refresh: drop stale %s %s %s",
                             chain, item.get("symbol"), addr)

            new_pools[chain] = kept

    # 3) Re-bind every surviving pool to a cache entry. Cheap (no API),
    #    only does work when the pool isn't already in the index — which
    #    happens after `cex_tokens.json` was wiped while pools.json kept
    #    its rows.
    rebound = await _rebind_pools(new_pools)

    await pools_save(new_pools)
    await cache_manager.flush(force=True)

    log.info(
        "pool refresh done in %.1fs: stale=%d blacklisted=%d rebound=%d kept=%d",
        time.time() - started, dropped_stale, dropped_blacklisted, rebound,
        sum(len(v) for v in new_pools.values()),
    )


async def _rebind_pools(pools: dict[str, list[dict]]) -> int:
    """
    Walk pools.json and call cache_manager.add_pool for any (chain, addr)
    that isn't yet in the in-memory pool index. Returns count of rebinds.
    """
    rebound = 0
    for chain, items in pools.items():
        if not isinstance(items, list):
            continue
        chain_l = chain.lower()
        for item in items:
            if not isinstance(item, dict):
                continue
            addr = str(item.get("pool", "")).lower()
            sym = (item.get("symbol") or "").upper()
            if not addr or not sym:
                continue
            if f"{chain_l}:{addr}" in cache_manager._INDEX["pool"]:
                continue
            try:
                ok = await cache_manager.add_pool(symbol=sym, chain=chain_l, pool=addr)
                if ok:
                    rebound += 1
            except Exception as e:
                log.debug("rebind failed for %s/%s/%s: %s", chain_l, sym, addr, e)
    return rebound


async def pool_refresh_loop(interval: float) -> None:
    if interval <= 0:
        log.info("pool refresh disabled (interval<=0)")
        return

    # If we refreshed recently (e.g. crash + quick restart), defer the
    # next pass to honor the wall-clock interval rather than burning
    # DexScreener quota on every cold start.
    last = float(_load_state().get("last_refresh_at", 0.0))
    now = time.time()
    initial_wait = max(0.0, last + interval - now)

    log.info(
        "pool refresh loop started (interval=%ss, initial_wait=%.0fs, last=%.0fs ago)",
        interval, initial_wait, max(0.0, now - last),
    )

    if initial_wait > 0:
        await asyncio.sleep(initial_wait)

    while True:
        try:
            await refresh_pools_once()
            _save_state({"last_refresh_at": time.time()})
        except Exception as e:
            log.error("pool refresh failed: %s", e)
        await asyncio.sleep(interval)
