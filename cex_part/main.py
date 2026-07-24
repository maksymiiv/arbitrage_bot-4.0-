import asyncio

from engine.logger import get_logger

from .cache_manager import apply_coingecko_metadata, cleanup_kraken_pending, init_cache
from .cex_orderbooks.bybit_orderbook import BybitOrderBookManager
from .cex_orderbooks.kraken_orderbook import KrakenOrderBookManager
from .core.dex_pool_manager import sync_pools_with_cache
from .exchange_loader import (
    init_kraken_symbols,
    run_bybit,
    run_gate,
    run_kraken,
)


log = get_logger(__name__)

# Loop period for the per-token DexScreener sweep. The per-symbol
# cooldown inside `_process_token` does the real throttling (tiered:
# 2min / 10min / 24h by age-since-first-seen), so this just needs to be
# short enough for the 2-min hot-phase cooldown to actually fire on time.
# 60s is a comfortable choice — full sweep walks ~1500 tokens but skips
# nearly all of them (already have pools or still in cooldown).
POOL_SYNC_INTERVAL = 60  # 1 min


async def _dex_pool_loop() -> None:
    while True:
        log.debug("DEX pool sync tick")
        try:
            await sync_pools_with_cache()
        except Exception as e:
            log.error("DEX pool sync failed: %s", e)
        await asyncio.sleep(POOL_SYNC_INTERVAL)


async def start_cex() -> None:
    log.info("CEX starting")
    init_cache()

    # One-shot pass over cex_tokens.json: fill `coingecko_id` and
    # `kraken_verified` on entries that don't have them yet. Cheap (no
    # HTTP — uses the in-memory CG indices loaded by main.py at startup),
    # safe even if CG cache is empty (function logs and returns).
    await apply_coingecko_metadata()

    # Drop entries that are still pending Kraken verification AND not on
    # the retry queue — they're not arbitrageable for current CEX-DEX
    # work. Fresh listings registered into the retry queue are skipped
    # here and handled by `kraken_pending_retry_loop`.
    await cleanup_kraken_pending()

    bybit_wss = BybitOrderBookManager()
    asyncio.create_task(bybit_wss.connect())

    kraken_wss = KrakenOrderBookManager()
    asyncio.create_task(kraken_wss.connect())

    await bybit_wss.ready.wait()
    await kraken_wss.ready.wait()

    log.info("orderbook managers ready")
    await init_kraken_symbols(kraken_wss)

    asyncio.create_task(run_bybit(bybit_wss))
    asyncio.create_task(run_gate())
    asyncio.create_task(run_kraken(kraken_wss))
    asyncio.create_task(_dex_pool_loop())
    log.info("CEX online")
