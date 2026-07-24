"""
Token-bucket rate limiter shared across all DexScreener call-sites.

DexScreener publishes a 300 req/min/IP cap on both `/latest/dex/tokens/`
and `/latest/dex/pairs/`. We cap ourselves at a lower number (default
240/min ≈ 4 req/sec) to stay well clear of bursts that trip 429.

`acquire(name)` blocks the caller until a token is available. Multiple
buckets can coexist (one per remote endpoint family) — fetched by name.
"""

import asyncio
import time
from typing import Dict


class _TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: float | None = None):
        self.rate = rate_per_sec
        self.capacity = capacity if capacity is not None else max(rate_per_sec, 1.0)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                self._last = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                # not enough — wait for the next token to arrive
                wait = (1.0 - self._tokens) / self.rate
                await asyncio.sleep(wait)


_BUCKETS: Dict[str, _TokenBucket] = {}
_BUCKETS_LOCK = asyncio.Lock()


def configure(name: str, rate_per_min: float, capacity: float | None = None) -> None:
    """(Re)configure a named bucket. Call this once at startup."""
    rate_per_sec = rate_per_min / 60.0
    _BUCKETS[name] = _TokenBucket(rate_per_sec, capacity)


async def acquire(name: str) -> None:
    """Block until a token is available in the named bucket."""
    bucket = _BUCKETS.get(name)
    if bucket is None:
        # default: very conservative 60/min — better to be slow than 429
        configure(name, 60.0)
        bucket = _BUCKETS[name]
    await bucket.acquire()


# DexScreener bucket — both endpoints share an IP-level 300/min cap.
# 240/min leaves a 20% safety margin for jitter; capacity=10 lets short
# bursts go through.
configure("dexscreener", rate_per_min=240.0, capacity=10.0)

# CoinGecko + GeckoTerminal SHARE an IP-level rate limit on the free
# tier (same parent company). Documented free-tier cap is 30/min,
# but in practice /tokens/{addr}/pools 429s well below that. We pick
# 15/min — 2.5x faster than the old 6/min while staying half the
# nominal cap, keeping cold-start GT bursts manageable. If 429s
# become frequent again, drop back to 6-8/min. Pro-tier users with
# an API key can safely raise this in their own configuration.
configure("gecko", rate_per_min=15.0, capacity=1.0)

# Back-compat aliases — older code paths that asked for "coingecko"
# or "geckoterminal" still work, but they all draw from the same
# underlying bucket. Implemented by sharing the bucket instance so
# every name accumulates against the same counter.
_BUCKETS["coingecko"] = _BUCKETS["gecko"]
_BUCKETS["geckoterminal"] = _BUCKETS["gecko"]

# Kraken public REST (/0/public/Depth for the orderbook-depth filter).
# Kraken's public endpoints share a counter that comfortably sustains
# ~1 req/sec; 60/min with a small burst capacity stays safely under it.
configure("kraken_public", rate_per_min=60.0, capacity=2.0)
