import asyncio

from engine.logger import get_logger

from .core.chain_monitor import monitor_chain
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
        pools_file_watcher(),
    )


if __name__ == "__main__":
    asyncio.run(start_dex())
