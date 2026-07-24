"""
Single source of truth for runtime configuration.

Reads .env (via python-dotenv) on import, exposes typed accessors.
All other modules MUST go through here — no os.getenv scattered around.
"""

import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load .env once. Silent if file is missing (CI / docker can inject env).
load_dotenv(PROJECT_ROOT / ".env")


def _str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# ---------- Bybit ----------
BYBIT_API_KEY = _str("BYBIT_API_KEY")
BYBIT_API_SECRET = _str("BYBIT_API_SECRET")


# ---------- Chain endpoints ----------
# Native token addresses are protocol-level constants → kept in tokens.py.
# Endpoints are infrastructure → live in env.
#
# `v4_pool_manager` — the Uniswap V4 singleton PoolManager for the chain.
# All V4 pools live inside this one contract; absence of the key means
# the chain has no Uniswap V4 (BSC — PancakeSwap Infinity instead, TBD).
# Endpoint defaults are PUBLIC, keyless fallbacks (publicnode / official
# dataseeds) — never embed API keys here. Real production endpoints
# (Alchemy / NodeReal / etc.) belong in .env; if set there, they win.
CHAINS = {
    "bsc": {
        "ws": _str("BSC_WS", "wss://bsc.publicnode.com"),
        "rpc": _str("BSC_RPC", "https://bsc-dataseed.binance.org"),
        "native": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
    },
    "eth": {
        "ws": _str("ETH_WS", "wss://ethereum.publicnode.com"),
        "rpc": _str("ETH_RPC", "https://ethereum.publicnode.com"),
        "native": "0xC02aaa39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        "v4_pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
    },
    "base": {
        "ws": _str("BASE_WS", "wss://base.publicnode.com"),
        "rpc": _str("BASE_RPC", "https://mainnet.base.org"),
        "native": "0x4200000000000000000000000000000000000006",  # WETH on Base
        "v4_pool_manager": "0x498581fF718922c3f8e6A244956aF099B2652b2b",
    },
}


# ---------- Spread runner ----------
SPREAD_INTERVAL = _float("SPREAD_INTERVAL", 1.0)
SPREAD_MIN_PCT = _float("SPREAD_MIN_PCT", 0.3)

# Staleness gates for the scanner: a price older than this many seconds
# has its side skipped. 0 = DISABLED (default). Enable with care —
# illiquid tokens legitimately go silent for long stretches, so an
# aggressive gate drops real (if thin) opportunities. The CEX gate is
# the safer one to turn on first (a stale CEX price usually means the WS
# feed dropped); keep any DEX gate generous (hours, not minutes).
MAX_CEX_AGE_SEC = _float("MAX_CEX_AGE_SEC", 0.0)
MAX_DEX_AGE_SEC = _float("MAX_DEX_AGE_SEC", 0.0)


# ---------- Diagnostics ----------
# How often to dump a full price_store snapshot to logs. 0 disables.
PRICE_STORE_DUMP_INTERVAL = _float("PRICE_STORE_DUMP_INTERVAL", 300.0)


# ---------- Pool maintenance ----------
# How often to validate pools.json against DexScreener (drop stale pools).
# Default 6h: ~120 req/min batched ⇒ comfortably under the 300/min cap and
# fast enough to clear delisted pools before they pollute spreads.
POOL_REFRESH_INTERVAL = _float("POOL_REFRESH_INTERVAL", 6 * 3600.0)


# ---------- Logging ----------
LOG_LEVEL = _str("LOG_LEVEL", "INFO").upper()
LOG_DIR = _str("LOG_DIR", "logs")
LOG_MAX_BYTES = _int("LOG_MAX_BYTES", 20 * 1024 * 1024)
LOG_BACKUP_COUNT = _int("LOG_BACKUP_COUNT", 5)


# ---------- Misc ----------
DEXSCREENER_CONCURRENCY = _int("DEXSCREENER_CONCURRENCY", 4)

# Retry cooldown for (chain, symbol) pairs that DexScreener doesn't yet
# index a pool for. This is the WARM-PHASE value (2-24h since first
# detect) — it's tiered in `cex_part.core.dex_pool_manager`:
#     0-2h since first_seen → 2 min   (hot: pool may appear any minute)
#     2-24h                 → this value (default 10 min)
#     24h+                  → 24 h    (cold: same cadence as stable tokens)
# Lower = more aggressive retries (eats DexScreener quota); higher =
# less load but newly-listed pools take longer to be picked up.
NO_POOL_RETRY_TTL_SEC = _int("NO_POOL_RETRY_TTL_SEC", 10 * 60)


# ---------- CoinGecko ----------
# How often to refresh the bulk platform list + Kraken tickers list.
# 24h is more than enough; CG data changes slowly. 0 disables refresh
# (cache-only mode — useful for offline dev / when CG is down).
COINGECKO_REFRESH_INTERVAL = _float("COINGECKO_REFRESH_INTERVAL", 24 * 3600.0)

# Optional Pro-tier API key. Empty = free public access (we self-throttle
# to ~8 req/min in rate_limiter.py). When set, the key is sent as
# `x-cg-pro-api-key` header and much higher rate limits become available.
COINGECKO_API_KEY = _str("COINGECKO_API_KEY")


# ---------- Liquidity filter (GeckoTerminal) ----------
# Tokens whose aggregated DEX liquidity across ALL pools on a chain is
# below this threshold get blacklisted from the spread runner — no point
# emitting an arbitrage opportunity that can't realistically be traded.
# 5000 USD excludes most scams / dead pools while keeping legitimate
# small-caps in scope.
LIQUIDITY_MIN_USD = _float("LIQUIDITY_MIN_USD", 5000.0)

# Additional active-trading filter — sum of 24h volume across all pools
# must clear this threshold. Catches "honeypot" tokens where dozens of
# dust pools add up to a respectable liquidity number but no one ever
# trades them (vol_24h == $0). $1000 is reasonable: any realistic
# arbitrage candidate has at least $1k of organic 24h activity.
LIQUIDITY_MIN_VOL_USD = _float("LIQUIDITY_MIN_VOL_USD", 1000.0)

# Per-bound-pool floor — even if the AGGREGATE numbers above pass, the
# specific pool we subscribe to must individually clear this threshold.
# Without this, a token can have e.g. $13k aggregate spread across one
# dust V3 ($1.8k) + 15 zero-volume V4 pools, look "ok" on aggregate,
# but the bound pool's swaps are so rare the DEX price freezes →
# phantom spreads against live CEX. $5000 means "the single pool we
# trade against must individually be a real venue, not just one piece
# of a barely-significant aggregate".
BOUND_POOL_MIN_USD = _float("BOUND_POOL_MIN_USD", 5000.0)


# ---------- Pool auto-upgrade (GeckoTerminal-driven) ----------
# DexScreener's pool discovery often picks a low-liquidity pool when a
# much better one exists on a less-indexed DEX. Since we already query
# GeckoTerminal during the liquidity check, we can compare its top pool
# against the one in pools.json and swap it in-place when GT's pick is
# substantially better. Replacement is atomic: drop old → add new →
# trigger WS reconnect → blacklist old so DexScreener can't re-pick it.
POOL_AUTO_UPGRADE = _str("POOL_AUTO_UPGRADE", "true").lower() in ("1", "true", "yes")
# Ratio gate is now TIERED — implemented inline in
# `engine.liquidity_filter._maybe_upgrade_pool`. As the current pool
# gets richer, smaller relative gains still represent big absolute
# value, so the required multiplier drops:
#     current < $10k → 2.0x
#     current ≥ $10k → 1.5x
#     current ≥ $50k → just any improvement (best > current)
# The replacement candidate must also out-trade the current pool by
# 24h volume — see `_maybe_upgrade_pool` for the full logic.
# Volume floor on the replacement candidate — refuse to swap to a pool
# that GT calls "high-liquidity" but nobody actually trades. $500/24h
# is a minimum-viable-sign-of-life.
POOL_UPGRADE_MIN_VOL = _float("POOL_UPGRADE_MIN_VOL", 500.0)


# ---------- Orderbook depth filter ----------
# A spread computed from top-of-book is a phantom unless the CEX side
# actually holds enough cumulative volume between its top-of-book and
# the DEX price. We require at least this much USD of depth in the
# profitable price band, otherwise the spread is dropped.
ORDERBOOK_MIN_DEPTH_USD = _float("ORDERBOOK_MIN_DEPTH_USD", 500.0)
# How often the depth poller REST-refreshes the orderbook for Kraken
# tokens that currently have an active spread (Bybit is WS-live).
ORDERBOOK_DEPTH_POLL_INTERVAL = _float("ORDERBOOK_DEPTH_POLL_INTERVAL", 8.0)
