import asyncio

from engine.logger import get_logger

from .core.chain_monitor import monitor_chain
from .core.native_price_poller import native_price_loop
from .core.pool_reconcile import reconcile_loop
from .core.pools_watcher import pools_file_watcher
from .core.v4_monitor import monitor_v4_chain


log = get_logger(__name__)


async def start_dex() -> None:
    log.info("DEX starting")
    await asyncio.gather(
        monitor_chain("bsc"),
        monitor_chain("base"),
        monitor_chain("eth"),
        # Uniswap V4 — separate singleton-PoolManager monitor per chain.
        # `monitor_v4_chain` self-skips chains with no v4_pool_manager
        # configured (BSC), so it's safe to launch for all three.
        monitor_v4_chain("eth"),
        monitor_v4_chain("base"),
        # Native (WETH/WBNB) USD price via cheap RPC polling instead of a
        # WS firehose on the busiest pool. Self-idles until chain_monitor
        # registers a native<->stable pool.
        native_price_loop("bsc"),
        native_price_loop("eth"),
        native_price_loop("base"),
        # Multicall reconciliation backstop — refreshes every tracked
        # V2/V3 pool on a schedule so a WS per-pool drop can't freeze a
        # price indefinitely.
        reconcile_loop("bsc"),
        reconcile_loop("eth"),
        reconcile_loop("base"),
        pools_file_watcher(),
    )


if __name__ == "__main__":
    asyncio.run(start_dex())
