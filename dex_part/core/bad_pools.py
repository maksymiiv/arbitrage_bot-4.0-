"""
Reject list for DEX pools that repeatedly produce out-of-range prices.

A single absurd swap event isn't enough to evict a pool — V3 pools can
briefly hit MIN_SQRT_RATIO / MAX_SQRT_RATIO during a large arbitrage
and recover seconds later. But if the pool keeps emitting nonsense
prices (THRESHOLD consecutive hits), it's almost certainly drained or
rugged: we drop it from pools.json and tell `dex_pool_manager` to find
a replacement via DexScreener.

The reject state is kept in-memory only — on restart the bot re-syncs
pools and naturally re-evaluates them. A hit on a still-broken pool
will trigger another drop within seconds.

`is_rejected` is also consulted by `_fetch_best_pool` so DexScreener
candidates that we already evicted today don't get re-attached
immediately. After RETRY_AFTER_SEC the pool gets another chance.
"""

import time
from threading import Lock


# (chain_lower, pool_lower) -> consecutive bad-price hits not yet rejected
_BAD_COUNT: dict[tuple[str, str], int] = {}

# (chain_lower, pool_lower) -> ts of rejection
_REJECTED: dict[tuple[str, str], float] = {}

_LOCK = Lock()

# Number of out-of-range hits before we stop trusting the pool.
THRESHOLD = 3

# How long a rejected pool stays blacklisted from re-discovery.
RETRY_AFTER_SEC = 24 * 3600


def _key(chain: str, pool: str) -> tuple[str, str] | None:
    if not chain or not pool:
        return None
    return (chain.lower(), pool.lower())


def record_bad_price(chain: str, pool: str) -> bool:
    """
    Bump the bad-price counter for `(chain, pool)`. Returns True iff
    this hit just transitioned the pool from "watched" to "rejected"
    (so the caller can fire the drop+resync flow exactly once).
    """
    k = _key(chain, pool)
    if not k:
        return False
    with _LOCK:
        if k in _REJECTED:
            return False
        _BAD_COUNT[k] = _BAD_COUNT.get(k, 0) + 1
        if _BAD_COUNT[k] >= THRESHOLD:
            _REJECTED[k] = time.time()
            del _BAD_COUNT[k]
            return True
        return False


def is_rejected(chain: str, pool: str) -> bool:
    """True if this pool is currently on the reject list."""
    k = _key(chain, pool)
    if not k:
        return False
    with _LOCK:
        ts = _REJECTED.get(k)
        if ts is None:
            return False
        if (time.time() - ts) > RETRY_AFTER_SEC:
            del _REJECTED[k]
            return False
        return True


def clear(chain: str, pool: str) -> None:
    """Manual override — wipe state for one pool."""
    k = _key(chain, pool)
    if not k:
        return
    with _LOCK:
        _BAD_COUNT.pop(k, None)
        _REJECTED.pop(k, None)


def mark_rejected(chain: str, pool: str) -> None:
    """
    Immediately move a pool to the rejected set without going through
    the bad-price counter. Used by `liquidity_filter` when we deliberately
    swap a pool out for a better one — we don't want `dex_pool_manager`
    re-attaching the old address on its next discovery pass.
    """
    k = _key(chain, pool)
    if not k:
        return
    with _LOCK:
        _BAD_COUNT.pop(k, None)
        _REJECTED[k] = time.time()
