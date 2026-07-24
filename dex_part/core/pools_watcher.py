"""
Watch pools.json for changes and signal WS sessions to restart.
"""

import asyncio

from .pools_io import POOLS_FILE, normalized_snapshot
from .ws_subscriber import WS_RESTART_EVENT
from engine.logger import get_logger


log = get_logger(__name__)

POLL_INTERVAL = 10  # seconds


async def pools_file_watcher() -> None:
    known = normalized_snapshot()
    last_mtime = POOLS_FILE.stat().st_mtime if POOLS_FILE.exists() else 0

    while True:
        await asyncio.sleep(POLL_INTERVAL)

        if not POOLS_FILE.exists():
            continue

        mtime = POOLS_FILE.stat().st_mtime
        if mtime == last_mtime:
            continue

        last_mtime = mtime
        current = normalized_snapshot()

        if current != known:
            log.info("pools.json changed -> restarting WS subscriptions")
            known = current
            WS_RESTART_EVENT.set()
