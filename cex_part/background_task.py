"""
Debounced single-flight wrapper for `sync_pools_with_cache`.

If a sync is already running and another request arrives, we set a
"re-run" flag so the worker does another pass after it finishes —
otherwise new tokens added during the run would never be linked to pools
until the next periodic tick.
"""

import asyncio

from engine.logger import get_logger

from .core.dex_pool_manager import sync_pools_with_cache


log = get_logger(__name__)

_DEBOUNCE_SEC = 2.0

_lock = asyncio.Lock()
_rerun = False
_task: asyncio.Task | None = None


def schedule_pool_sync() -> None:
    """Fire-and-forget. Safe to call from anywhere in the asyncio loop."""
    global _task, _rerun

    if _task and not _task.done():
        _rerun = True
        return

    _rerun = False
    _task = asyncio.create_task(_runner())


async def _runner() -> None:
    global _rerun
    try:
        while True:
            await asyncio.sleep(_DEBOUNCE_SEC)
            async with _lock:
                _rerun = False
                try:
                    await sync_pools_with_cache()
                except Exception as e:
                    log.error("pool sync failed: %s", e)

            # if anyone called schedule_pool_sync while we were running,
            # do another pass so newly added tokens get processed
            if not _rerun:
                return
    finally:
        # leave _task referencing this coro until it exits
        pass
