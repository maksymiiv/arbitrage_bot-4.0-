"""
Resumable V4 pool migration.

Re-evaluates bound pools against GeckoTerminal — now including Uniswap
V4 — and swaps in the better pool where GT shows one. A GT query per
token is unavoidable (only GT indexes V4), so the full run is
RESUMABLE: progress is persisted to `migrate_progress.json`.

Two modes
---------
FULL SWEEP (no arguments)
    python -m cex_part.core.pool_migrate

    Walks every token that already has a pool on eth/base. Resumable —
    re-run until `remaining=0`. Run with the main bot STOPPED so the two
    processes don't both hammer the shared GeckoTerminal rate limit.

TARGETED (symbols as arguments)
    python -m cex_part.core.pool_migrate ROLL FIGHT REZ SAHARA

    Migrates only the named tokens — and unlike the full sweep it also
    handles tokens that have NO pool yet (it adds the best one fresh).
    Use this after removing tokens from CHAIN_BLACKLIST: the blacklist
    drops their pools, so they're invisible to the full sweep.

    IMPORTANT: remove a token from CHAIN_BLACKLIST *before* migrating it
    — otherwise the daily pool_refresh would just drop the new pool.
    Tokens still in the blacklist are skipped with a warning.

Everything is logged: progress lines + every `pool add/upgrade:` swap
go to the console AND to `logs/engine.log`.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

from engine.atomic_io import atomic_write_json
from engine.logger import get_logger

from .. import cache_manager
from ..config.blacklist import is_chain_blacklisted


log = get_logger(__name__)

_PROGRESS_FILE = (
    Path(__file__).resolve().parents[1] / "cex_cache" / "migrate_progress.json"
)

# Only chains we can fully monitor (V4 included). BSC has no Uniswap V4.
_MIGRATE_CHAINS = ("eth", "base")

_SAVE_EVERY = 5     # persist progress every N tokens (survives Ctrl+C)
_FLUSH_EVERY = 25   # flush cex_tokens.json (pool bindings) every N tokens


# --------------------------------------------------------------------------
# progress persistence (full-sweep mode only)
# --------------------------------------------------------------------------

def _load_progress() -> set[str]:
    if not _PROGRESS_FILE.exists():
        return set()
    try:
        data = json.loads(_PROGRESS_FILE.read_text(encoding="utf-8"))
        return set(data.get("done") or [])
    except Exception as e:
        log.warning("migrate: progress file unreadable (%s), starting fresh", e)
        return set()


def _save_progress(done: set[str]) -> None:
    atomic_write_json(
        _PROGRESS_FILE,
        {"done": sorted(done), "updated_at": int(time.time())},
    )


# --------------------------------------------------------------------------
# worklist
# --------------------------------------------------------------------------

def _build_worklist(only_symbols: set[str] | None = None) -> list[tuple[str, str, str]]:
    """
    [(chain, token_contract_lower, symbol), ...].

    Full sweep (`only_symbols` is None) — every token that ALREADY has a
    pool bound on a migrate-able chain.

    Targeted (`only_symbols` given) — exactly those tokens, on every
    chain where they have a contract, INCLUDING pool-less ones (so a
    freshly un-blacklisted token gets its first pool added).
    """
    work: list[tuple[str, str, str]] = []
    for entry in cache_manager._CACHE.values():
        if not isinstance(entry, dict):
            continue
        symbol = (entry.get("symbol") or "").upper()
        if only_symbols is not None and symbol not in only_symbols:
            continue
        contracts = entry.get("contracts") or {}
        pools = entry.get("pools") or {}
        for chain in _MIGRATE_CHAINS:
            addr = contracts.get(chain.upper()) or contracts.get(chain.lower())
            if not (isinstance(addr, str) and addr.strip()):
                continue
            # Full sweep only touches tokens that already have a pool.
            if only_symbols is None and not pools.get(chain):
                continue
            work.append((chain, addr.strip().lower(), symbol))
    return work


# --------------------------------------------------------------------------
# migration
# --------------------------------------------------------------------------

async def migrate_all_pools(only_symbols: set[str] | None = None) -> None:
    """Evaluate each work item; upgrade / add the best pool where GT shows
    one. Full-sweep mode is resumable; targeted mode always runs fully."""
    from engine.liquidity_filter import _fetch_aggregates, _maybe_upgrade_pool

    targeted = only_symbols is not None
    done: set[str] = set() if targeted else _load_progress()

    work = _build_worklist(only_symbols)
    remaining = [w for w in work if f"{w[0]}:{w[1]}" not in done]

    log.info(
        "migrate: mode=%s, %d items total, %d remaining this run",
        "targeted" if targeted else "full-sweep", len(work), len(remaining),
    )
    if not remaining:
        log.info("migrate: nothing to do")
        return

    upgraded = 0
    processed = 0
    deferred = 0  # tokens GT couldn't answer this run (429/timeout) — retried
    try:
        for i, (chain, addr, symbol) in enumerate(remaining, 1):
            wkey = f"{chain}:{addr}"

            # A blacklisted (symbol, chain) would just have its new pool
            # dropped by the daily pool_refresh — skip and tell the user.
            if is_chain_blacklisted(symbol, chain):
                log.warning(
                    "migrate: %s/%s is in CHAIN_BLACKLIST — remove it there "
                    "first, skipping", chain, symbol,
                )
                done.add(wkey)
                continue

            try:
                result = await _fetch_aggregates(chain, addr)
            except Exception as e:
                log.warning("migrate: %s/%s fetch error: %s", chain, symbol, e)
                result = None

            if result is None:
                # GT failed (429 / timeout) — DON'T mark done, retried
                # on the next run.
                deferred += 1
                log.debug("migrate: %s/%s GT unavailable, will retry", chain, symbol)
            else:
                _, _, pool_infos = result
                if pool_infos:
                    try:
                        did = await _maybe_upgrade_pool(
                            chain, addr, pool_infos, trigger_restart=False,
                        )
                        if did:
                            upgraded += 1
                    except Exception as e:
                        log.warning("migrate: %s/%s upgrade error: %s",
                                    chain, symbol, e)
                done.add(wkey)
                processed += 1

            if not targeted and i % _SAVE_EVERY == 0:
                _save_progress(done)
            if i % _FLUSH_EVERY == 0:
                await cache_manager.flush(force=True)
            if i % 10 == 0:
                log.info(
                    "migrate: %d/%d this run (upgraded=%d)",
                    i, len(remaining), upgraded,
                )
    except KeyboardInterrupt:
        log.info("migrate: interrupted — saving progress")
    finally:
        if not targeted:
            _save_progress(done)
        await cache_manager.flush(force=True)

    log.info(
        "migrate: run finished — processed=%d upgraded=%d deferred(429/err)=%d",
        processed, upgraded, deferred,
    )
    if targeted:
        log.info("migrate: targeted run complete")
    else:
        # Recompute from the actual worklist — `done` can hold stale keys
        # from tokens no longer in `work`, so `len(done) >= len(work)` used
        # to claim completion while 429-deferred tokens were still pending.
        still_remaining = [w for w in work if f"{w[0]}:{w[1]}" not in done]
        if not still_remaining:
            log.info("migrate: ALL tokens evaluated — full migration complete")
        else:
            log.info(
                "migrate: %d token(s) still pending (%d GT rate-limited this "
                "run) — re-run `python -m cex_part.core.pool_migrate` to finish",
                len(still_remaining), deferred,
            )


async def _main() -> None:
    cache_manager.init_cache()
    args = [a.strip().upper() for a in sys.argv[1:] if a.strip()]
    await migrate_all_pools(only_symbols=set(args) if args else None)


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
