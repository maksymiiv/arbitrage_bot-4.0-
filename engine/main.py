"""
Top-level orchestrator: wires CEX, DEX, the spread runner, the periodic
price_store dumper and the daily stale-pool refresher.
"""

import asyncio

from engine.config import (
    COINGECKO_REFRESH_INTERVAL,
    ORDERBOOK_DEPTH_POLL_INTERVAL,
    POOL_REFRESH_INTERVAL,
    PRICE_STORE_DUMP_INTERVAL,
    SPREAD_INTERVAL,
    SPREAD_MIN_PCT,
)
from engine.logger import get_logger, install_print_bridge

# Bridge legacy `print(...)` calls into the rotating log files BEFORE we
# import any module that might print at import time.
install_print_bridge()

log = get_logger(__name__)

from cex_part.core.orderbook_depth import kraken_depth_loop  # noqa: E402
from cex_part.core.pool_refresh import pool_refresh_loop  # noqa: E402
from cex_part.main import start_cex  # noqa: E402
from cex_part.network_status import cex_network_loop  # noqa: E402
from dex_part.main import start_dex  # noqa: E402
from engine import coingecko, liquidity_filter  # noqa: E402
from engine.diagnostics import price_store_dumper  # noqa: E402
from engine.spread_logic import spread_runner  # noqa: E402
from engine.tasks import spawn  # noqa: E402


async def main() -> None:
    # CoinGecko: load existing cache files synchronously so any module
    # that reads them at startup (cache_manager, kraken_metadata) sees
    # populated indices. The actual HTTP refresh runs as a background
    # task so it never blocks startup if CG is slow / down.
    coingecko.init_cache()

    # Liquidity filter cache: persisted blacklist of low-liq tokens.
    # Loading is sync + instant; fresh fetches happen on-demand via
    # `is_low_liquidity` from the spread scanner.
    liquidity_filter.init_cache()

    log.info("orchestrator: starting CEX")
    await start_cex()

    log.info("orchestrator: starting DEX")
    spawn(start_dex(), name="dex")

    log.info(
        "orchestrator: starting spread runner (interval=%ss min_pct=%s)",
        SPREAD_INTERVAL, SPREAD_MIN_PCT,
    )
    spawn(
        spread_runner(interval=SPREAD_INTERVAL, min_spread_pct=SPREAD_MIN_PCT),
        name="spread_runner",
    )

    spawn(price_store_dumper(PRICE_STORE_DUMP_INTERVAL), name="price_store_dumper")
    spawn(pool_refresh_loop(POOL_REFRESH_INTERVAL), name="pool_refresh")
    spawn(coingecko.refresh_loop(COINGECKO_REFRESH_INTERVAL), name="cg_refresh")
    spawn(coingecko.kraken_pending_retry_loop(), name="cg_kraken_pending")

    # CEX deposit / withdraw status — adaptive cadence: ~5 min when
    # nothing is hot, ~8 s when at least one token has an active spread.
    # Kraken intentionally not polled (no per-chain D/W info exposed).
    spawn(cex_network_loop("bybit"), name="netstatus_bybit")
    spawn(cex_network_loop("gate"), name="netstatus_gate")

    # Orderbook-depth poller — REST-refreshes Kraken books for tokens
    # that currently have a spread (Bybit depth is already WS-live).
    spawn(kraken_depth_loop(ORDERBOOK_DEPTH_POLL_INTERVAL), name="kraken_depth")

    log.info("orchestrator: system online")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
