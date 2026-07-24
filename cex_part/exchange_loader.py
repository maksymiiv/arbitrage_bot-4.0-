"""
Per-CEX poller loops:
- pull current spot symbol list
- detect new listings -> live-subscribe orderbook stream
- for any symbol missing metadata, request it once and merge into cache
- dispatch a (debounced) DEX pool discovery job
"""

import asyncio

from engine.logger import get_logger

from . import cache_manager
from .background_task import schedule_pool_sync
from .cex_metadata.bybit_metadata import get_bybit_coin_metadata, get_bybit_spot_coins
from .cex_metadata.gate_metadata import (
    GateMetadataCache,
    get_gate_spot_coins,
)
from .cex_metadata.kraken_metadata import (
    KrakenMetadataCache,
    get_kraken_spot_coins,
)


log = get_logger(__name__)


async def run_bybit(bybit_wss, poll_seconds: int = 60) -> None:
    while True:
        try:
            symbols = await get_bybit_spot_coins()
            new_syms, _ = await cache_manager.update_token_list("bybit", symbols)
            missing = cache_manager.get_missing_symbols_for_cex("bybit", symbols)

            if new_syms:
                log.info("[BYBIT] new listings: %s", new_syms[:10])
            if missing:
                log.info("[BYBIT] need metadata for %d symbols", len(missing))

            for sym in missing:
                meta = await get_bybit_coin_metadata(sym)
                if meta:
                    await cache_manager.merge_token("bybit", meta)
                    schedule_pool_sync()
                else:
                    await cache_manager.mark_symbol_ignored("bybit", sym)

            for sym in new_syms:
                await bybit_wss.subscribe_symbol(sym)

            await cache_manager.flush(force=True)

        except Exception as e:
            log.error("[BYBIT] loop error: %s", e)

        await asyncio.sleep(poll_seconds)


async def run_gate(gate_wss, poll_seconds: int = 60) -> None:
    while True:
        try:
            symbols = await get_gate_spot_coins()
            new_syms, _ = await cache_manager.update_token_list("gate", symbols)
            missing = cache_manager.get_missing_symbols_for_cex("gate", symbols)

            if new_syms:
                log.info("[GATE] new listings: %s", new_syms[:10])
            if missing:
                log.info("[GATE] need metadata for %d symbols", len(missing))

            # one REST call instead of one per symbol
            cache = await GateMetadataCache.fetch()
            for sym in missing:
                meta = cache.get_metadata(sym)
                if meta:
                    await cache_manager.merge_token("gate", meta)
                    schedule_pool_sync()
                    # Stream this token's BBO now that it has metadata —
                    # targeted (only DEX-relevant tokens) rather than every
                    # Gate spot pair, to keep price_store lean.
                    await gate_wss.subscribe_symbol(sym)
                else:
                    await cache_manager.mark_symbol_ignored("gate", sym)

            await cache_manager.flush(force=True)

        except Exception as e:
            log.error("[GATE] loop error: %s", e)

        await asyncio.sleep(poll_seconds)


async def run_kraken(kraken_wss, poll_seconds: int = 60) -> None:
    while True:
        try:
            symbols = await get_kraken_spot_coins()
            new_syms, _ = await cache_manager.update_token_list("kraken", symbols)
            missing = cache_manager.get_missing_symbols_for_cex("kraken", symbols)

            log.info(
                "[KRAKEN] known=%d unknown=%d", len(symbols) - len(missing), len(missing)
            )

            cache = await KrakenMetadataCache.fetch()
            for sym in missing:
                meta = cache.get_metadata(sym)
                if meta:
                    await cache_manager.merge_token("kraken", meta)
                    schedule_pool_sync()

                    # Try to resolve the new entry against CG immediately.
                    # If CG hasn't indexed it yet (typical for tokens
                    # listed in the last few hours), drop it into the
                    # pending retry queue — `kraken_pending_retry_loop`
                    # will keep checking every ~15 min.
                    cache_key = cache_manager.resolve_key_for_cex_symbol("kraken", sym)
                    if cache_key:
                        resolved = await cache_manager.apply_coingecko_metadata_for_entry(cache_key)
                        if not resolved:
                            from engine import coingecko
                            coingecko.pending_register(cache_key, sym)
                else:
                    await cache_manager.mark_symbol_ignored("kraken", sym)

            for sym in new_syms:
                log.info("[KRAKEN] new listing: %s", sym)
                try:
                    await kraken_wss.subscribe_symbol(sym)
                    await asyncio.sleep(0.03)
                except Exception as e:
                    log.warning("[KRAKEN] sub fail %s: %s", sym, e)

            cache_manager.save_kraken_unknown_symbols(missing)
            await cache_manager.flush(force=True)

        except Exception as e:
            log.error("[KRAKEN] loop error: %s", e)

        await asyncio.sleep(poll_seconds)


async def init_kraken_symbols(kraken_wss) -> None:
    initial = cache_manager.get_cex_symbols("kraken")
    log.info("[KRAKEN] initial subscribe: %d", len(initial))

    for sym in initial:
        try:
            await kraken_wss.subscribe_symbol(sym)
        except Exception as e:
            log.warning("[KRAKEN] init sub fail %s: %s", sym, e)
