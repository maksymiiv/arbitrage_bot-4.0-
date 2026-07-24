"""
Watch pools.json for changes and signal WS sessions to restart.
"""

import asyncio

from .pools_io import POOLS_FILE, normalized_snapshot
from .ws_subscriber import get_restart_event
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
            # Restart only the chains whose pool set actually changed —
            # a swap on eth must not force bsc/base to reconnect (which
            # would spam provider handshakes and risk 429s).
            changed = [
                chain for chain in (set(current) | set(known))
                if current.get(chain) != known.get(chain)
            ]
            if changed:
                log.info("pools.json changed on %s -> restarting those WS", changed)
                for chain in changed:
                    get_restart_event(chain).set()
            known = current
