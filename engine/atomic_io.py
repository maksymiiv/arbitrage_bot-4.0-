"""
Cross-platform atomic JSON writer.

POSIX `os.rename` / `Path.replace` is atomic and works even if the
destination is open. On Windows, however, `MoveFileEx(REPLACE_EXISTING)`
will raise `PermissionError(WinError 5)` whenever some other process
holds *any* handle to the destination — that includes the IDE preview,
antivirus scanners, and our own `pools_watcher` while it's reading
mtime / contents.

This module wraps the rename in a short retry loop so transient handle
contention (which is what Windows file locking really is — it lasts
microseconds) doesn't tear down the calling task.
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any


# Tunables — tight enough that the worst case adds <2s to a write.
_RETRIES = 8
_BACKOFF_BASE = 0.05  # seconds: 50ms, 100ms, 200ms, ... up to ~6s total
_BACKOFF_MAX = 1.0


def _replace_with_retry(src: Path, dst: Path) -> None:
    last_exc: Exception | None = None
    delay = _BACKOFF_BASE
    for attempt in range(_RETRIES):
        try:
            os.replace(src, dst)
            return
        except PermissionError as e:
            # Windows ERROR_ACCESS_DENIED / ERROR_SHARING_VIOLATION
            last_exc = e
        except OSError as e:
            # ERROR_SHARING_VIOLATION shows up as winerror 32 too
            if getattr(e, "winerror", None) not in (5, 32):
                raise
            last_exc = e

        time.sleep(delay)
        delay = min(delay * 2, _BACKOFF_MAX)

    # Last resort — try one more time and let the caller see the error
    if last_exc is not None:
        raise last_exc


def _write_text_atomic(path: Path, text: str) -> None:
    """Write `text` to `path` via a temp file + atomic rename (with the
    Windows retry loop). Blocking — call from a worker thread if you're
    on the event loop."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        _replace_with_retry(tmp, path)
    except Exception:
        # Don't leave the .tmp lying around if we ultimately failed
        try:
            tmp.unlink()
        except Exception:
            pass
        raise


def atomic_write_json(path: Path, data: Any, *, indent: int = 2) -> None:
    """
    Write `data` as JSON to `path` atomically. Creates parent dirs if
    needed. Retries the rename step on Windows when another process
    momentarily holds a read handle. Blocking — prefer the async variant
    from coroutines.
    """
    _write_text_atomic(Path(path), json.dumps(data, indent=indent, ensure_ascii=False))


async def atomic_write_json_async(path: Path, data: Any, *, indent: int = 2) -> None:
    """
    Non-blocking atomic JSON write for use on the event loop.

    Serialization (json.dumps) happens in the CALLER's thread so `data`
    can't mutate underneath us mid-dump (asyncio is single-threaded; the
    caller must not await between building `data` and calling this). Only
    the disk write + the potentially slow Windows retry-rename are pushed
    to a worker thread, so the event loop keeps running.
    """
    text = json.dumps(data, indent=indent, ensure_ascii=False)
    await asyncio.to_thread(_write_text_atomic, Path(path), text)
