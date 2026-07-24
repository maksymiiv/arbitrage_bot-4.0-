"""
Single owner of dex_part/config/pools.json.

All readers / writers (pool discovery, version detector, watcher) MUST go
through here so concurrent writes don't corrupt the file.
"""

import asyncio
import json
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Union

from engine.atomic_io import atomic_write_json
from engine.logger import get_logger


log = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
POOLS_FILE = BASE_DIR / "config" / "pools.json"

_LOCK = asyncio.Lock()

# High-level lock for ENTIRE read-modify-write sequences
# (pool discovery sweep, refresh sweep). Without this, two concurrent
# sequences can each load a snapshot, diverge, then `save()` overwrites
# each other ("lost update"). Held for the whole duration of the
# operation — minutes, sometimes — but the only callers are
# `sync_pools_with_cache` and `refresh_pools_once`, which are explicitly
# meant to be mutually exclusive.
_OPERATION_LOCK = asyncio.Lock()


def operation_lock() -> asyncio.Lock:
    """Public accessor for the high-level operation lock."""
    return _OPERATION_LOCK


PoolMap = Dict[str, List[dict]]
Mutator = Callable[[PoolMap], Union[bool, Awaitable[bool]]]


def _atomic_write(path: Path, data: dict) -> None:
    """Thin wrapper — retry/locking lives in engine.atomic_io."""
    atomic_write_json(path, data)


def load_sync() -> PoolMap:
    """Best-effort sync read. Used by code paths that can't await."""
    if not POOLS_FILE.exists():
        return {}
    try:
        text = POOLS_FILE.read_text(encoding="utf-8").strip()
        if not text:
            return {}
        return json.loads(text)
    except Exception as e:
        log.warning("pools.json corrupted, ignoring: %s", e)
        return {}


async def load() -> PoolMap:
    async with _LOCK:
        return load_sync()


async def save(pools: PoolMap) -> None:
    async with _LOCK:
        _atomic_write(POOLS_FILE, pools)


async def mutate(mutator: Mutator) -> PoolMap:
    """
    Read-modify-write under a single lock.

    `mutator(pools)` may modify pools in-place. Return True to persist the
    result, False to discard. The returned pools dict is whatever is on
    disk after the call.
    """
    async with _LOCK:
        pools = load_sync()
        result = mutator(pools)
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            _atomic_write(POOLS_FILE, pools)
        return pools


def normalized_snapshot() -> PoolMap:
    """Deterministic shape for the pools_file_watcher diff."""
    raw = load_sync()
    snapshot: PoolMap = {}

    for chain, items in raw.items():
        if not isinstance(items, list):
            continue
        normalized = []
        for item in items:
            if not isinstance(item, dict):
                continue
            pool = str(item.get("pool", "")).lower().strip()
            if not pool:
                continue
            normalized.append({
                "pool": pool,
                "dex": str(item.get("dex", "")).lower().strip(),
                "version": item.get("version"),
            })
        snapshot[chain] = sorted(
            normalized,
            key=lambda x: (x["pool"], x["dex"], str(x["version"])),
        )

    return snapshot
