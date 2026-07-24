"""
Supervised task spawning.

`asyncio.create_task()` has two footguns in a long-running bot:

  1. If nothing keeps a reference to the returned task, it can be
     garbage-collected mid-flight (documented CPython behaviour).
  2. If the coroutine dies with an exception, it's swallowed — the bot
     keeps running with, say, the spread runner silently dead, and the
     only trace is an "unretrieved exception" warning at interpreter
     shutdown.

`spawn()` fixes both: it holds a strong reference until the task
finishes and attaches a done-callback that logs any exception loudly.

It intentionally does NOT auto-restart: every long-lived loop here
already wraps its body in `while True: try/except`, so a task only dies
on a structural bug — which we want surfaced as a loud ERROR, not
papered over by a silent relaunch.
"""

import asyncio
from typing import Coroutine, Optional

from engine.logger import get_logger


log = get_logger(__name__)

# Strong references so tasks can't be GC'd while running.
_TASKS: set[asyncio.Task] = set()


def _log_if_failed(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error(
            "background task %s died: %r", task.get_name(), exc, exc_info=exc
        )


def spawn(coro: Coroutine, *, name: Optional[str] = None) -> asyncio.Task:
    """create_task that keeps a strong reference and logs uncaught
    exceptions instead of swallowing them."""
    task = asyncio.create_task(coro, name=name)
    _TASKS.add(task)
    task.add_done_callback(_TASKS.discard)
    task.add_done_callback(_log_if_failed)
    return task
