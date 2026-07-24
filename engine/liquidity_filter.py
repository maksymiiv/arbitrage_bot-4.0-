"""
Aggregated DEX-liquidity filter, fed by GeckoTerminal.

Why
---
DexScreener-attached pools cover most volume but miss niche DEXs;
worse, they don't tell us the *aggregated* liquidity for a token across
every pool on a chain. A scam token might trade on 5 dead pools that
each look "normal" individually but together hold $300 of liquidity —
no real arbitrage will clear there. We don't want to fire spread
alerts on those.

How it's used
-------------
The spread scanner calls `is_low_liquidity(chain, token_addr,
threshold)` on every candidate opportunity. The function:

  - returns immediately from an in-memory cache when there's a fresh
    entry (sync, zero cost);
  - on a cache miss, schedules a background fetch via
    `asyncio.create_task` and returns False (let the spread through
    this tick — the next scan tick will see the cached verdict).

The fetch goes through the shared "geckoterminal" rate-limiter bucket
(25 req/min cap), so even when 100+ tokens come in simultaneously at
cold-start, we don't burst past the free-tier limit. Tasks queue,
process at ~25/min, populate the cache; after a few minutes the
filter is warm and runs on cache only.

Cache lives in `cex_part/cex_cache/liquidity_status.json`. Persistent
across restarts, atomic writes via `engine.atomic_io`.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Optional

import aiohttp

from engine.atomic_io import atomic_write_json
from engine.config import (
    BOUND_POOL_MIN_USD,
    LIQUIDITY_MIN_USD,
    LIQUIDITY_MIN_VOL_USD,
    POOL_AUTO_UPGRADE,
    POOL_UPGRADE_MIN_VOL,
)
from engine.logger import get_logger
from engine.rate_limiter import acquire as rl_acquire


log = get_logger(__name__)


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------

CACHE_FILE = Path(__file__).resolve().parents[1] / "cex_part" / "cex_cache" / "liquidity_status.json"

GT_URL = "https://api.geckoterminal.com/api/v2/networks/{network}/tokens/{addr}/pools"

# Internal chain keys -> GeckoTerminal network slugs.
NETWORK_MAP = {
    "eth": "eth",
    "bsc": "bsc",
    "base": "base",
    "sol": "solana",
}

# Low and ok verdicts are both sticky for 24h — illiquid tokens
# almost never become liquid fast, and ok tokens almost never drain
# across ALL aggregated pools fast either. `pool_refresh` separately
# re-discovers new (potentially higher-liquidity) pools via DexScreener,
# so this filter doesn't need to re-poll often.
TTL_LOW_SEC = 24 * 60 * 60.0
TTL_OK_SEC = 24 * 60 * 60.0
# Errors (429 / timeout / 5xx) are transient by nature — keep the retry
# window short so a temporary GeckoTerminal hiccup doesn't lock a real
# token into a fake "we don't know" state for the whole day. Note: 404
# is NOT mapped to `error` — it's treated as zero liquidity (status="low").
TTL_ERROR_SEC = 5 * 60.0

# GeckoTerminal pool-list pagination
MAX_PAGES = 5
PAGE_SIZE = 20


# --------------------------------------------------------------------------
# state
# --------------------------------------------------------------------------

# (chain_lower, addr_lower) -> {"status": "ok"|"low"|"error",
#                                "liq_usd": float | None,
#                                "checked_at": int,
#                                "threshold": float}
_CACHE: dict[tuple[str, str], dict] = {}

# Tokens currently being fetched — dedupes concurrent calls.
_PENDING: set[tuple[str, str]] = set()

# Serializes JSON file writes; in-memory dict mutations don't need it
# because asyncio is single-threaded.
_FILE_LOCK = asyncio.Lock()


# --------------------------------------------------------------------------
# JSON persistence
# --------------------------------------------------------------------------

def _load_from_disk() -> dict[tuple[str, str], dict]:
    if not CACHE_FILE.exists():
        return {}
    try:
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("liquidity cache unreadable: %s", e)
        return {}
    out: dict[tuple[str, str], dict] = {}
    for k, v in raw.items():
        # JSON keys are strings: "chain|addr"
        parts = k.split("|", 1)
        if len(parts) != 2 or not isinstance(v, dict):
            continue
        out[(parts[0], parts[1])] = v
    return out


async def _persist() -> None:
    async with _FILE_LOCK:
        serializable = {f"{k[0]}|{k[1]}": v for k, v in _CACHE.items()}
        atomic_write_json(CACHE_FILE, serializable)


def init_cache() -> None:
    """Load persisted cache from disk. Safe to call multiple times."""
    global _CACHE
    _CACHE = _load_from_disk()
    log.info("liquidity_filter cache loaded: %d entries", len(_CACHE))


# --------------------------------------------------------------------------
# core
# --------------------------------------------------------------------------

def _is_fresh(entry: dict, threshold: float, vol_threshold: float) -> bool:
    """An entry is fresh if it's recent AND was computed against the
    same thresholds (changing either threshold invalidates the verdict)."""
    if not entry:
        return False
    if entry.get("threshold") != threshold:
        return False
    if entry.get("vol_threshold") != vol_threshold:
        return False
    age = time.time() - float(entry.get("checked_at") or 0)
    status = entry.get("status")
    if status == "low":
        return age < TTL_LOW_SEC
    if status == "ok":
        return age < TTL_OK_SEC
    if status == "error":
        return age < TTL_ERROR_SEC
    return False


_HTTP_HEADERS = {
    "Accept": "application/json",
    # Some free-tier APIs throttle/block bare aiohttp clients; set an
    # explicit UA so we don't get caught in generic bot filters.
    "User-Agent": "arbitrage-bot/4.0 (+liquidity-filter)",
}


# DEXs whose pool layouts our decoders DO NOT understand. If GT ranks
# one of these as the "best" pool we must skip it for the auto-upgrade
# flow — replacing a working uniswap pool with a Curve pool would just
# stop the bot from getting any prices for that token.
UNSUPPORTED_UPGRADE_DEXES = {"curve", "balancer", "kyberswap", "wombat"}

# Standard EVM contract address — 0x + 40 hex chars (V2/V3 pools).
# Uniswap V4 pools have NO contract address — they're keyed by a 32-byte
# poolId (0x + 64 hex) inside the singleton PoolManager.
import re as _re
_EVM_ADDR_RE = _re.compile(r"^0x[0-9a-f]{40}$")
_V4_POOLID_RE = _re.compile(r"^0x[0-9a-f]{64}$")


def _is_subscribable_pool(address: str, dex_id: str) -> bool:
    """
    True iff this pool can be decoded + monitored by our pipeline.

    V2/V3 — standard 20-byte EVM address, monitored by chain_monitor.
    V4    — 32-byte poolId, monitored by v4_monitor (Uniswap only;
            PancakeSwap Infinity not supported yet).
    Curve / Balancer / etc. — decoder doesn't understand the layout.
    """
    dex_name, version = _parse_gt_dex_id(dex_id)
    if dex_name in UNSUPPORTED_UPGRADE_DEXES:
        return False
    if version == "v4":
        return dex_name == "uniswap" and bool(_V4_POOLID_RE.match(address or ""))
    return bool(_EVM_ADDR_RE.match(address or ""))


def _is_priceable_pool(chain: str, token_a: Optional[str], token_b: Optional[str]) -> bool:
    """
    True iff the pool's pair can be valued by our price engine — one
    side MUST be native or a stable. A token/token pool (e.g.
    1INCH/wstETH) has no USD anchor: `compute_price` returns 0 and the
    monitor would silently drop it, so we must never upgrade onto one.
    """
    from dex_part.core.stable_native import is_native, is_stable

    for t in (token_a, token_b):
        if not t:
            continue
        if is_native(chain, t) or is_stable(chain, t):
            return True
    return False


def _parse_gt_dex_id(dex_id: str) -> tuple[str, Optional[str]]:
    """
    GT names DEXes like "uniswap_v3", "pancakeswap_v2", "aerodrome-v1"
    — fold into our pools.json fields `(dex, version)`. Falls back to
    ("unknown", None) when GT doesn't tell us the family.
    """
    if not dex_id:
        return "unknown", None
    raw = dex_id.lower().replace("-", "_")
    parts = raw.split("_")
    base = parts[0] if parts else "unknown"

    # Normalize common families
    if base.startswith("pancake"):
        base = "pancake"
    elif base.startswith("uniswap"):
        base = "uniswap"
    elif base.startswith("aerodrome"):
        base = "aerodrome"
    elif base.startswith("sushi"):
        base = "sushi"

    version: Optional[str] = None
    for p in parts[1:]:
        if p in {"v2", "v3", "v4"}:
            version = p
            break
    return base, version


def _gt_token_addr(token_rel: Optional[dict]) -> Optional[str]:
    """
    Extract a token address from a GeckoTerminal relationship object.
    GT ids look like "eth_0xabc..." — strip the chain prefix.
    """
    data = (token_rel or {}).get("data") or {}
    tid = data.get("id") or ""
    if "_" in tid:
        tid = tid.split("_", 1)[1]
    tid = tid.strip().lower()
    return tid or None


async def _fetch_aggregates(
    chain: str, addr: str,
) -> Optional[tuple[float, float, list[dict]]]:
    """Sum `reserve_in_usd` AND `volume_usd.h24` across every pool
    GeckoTerminal indexes for `(chain, addr)`, and also return the
    raw per-pool list so the pool-upgrade flow can pick the best.

    Returns:
        (total_liquidity_usd, total_24h_volume_usd, pool_infos) on success.
        pool_infos = [{"address", "reserve_usd", "vol_24h", "dex_id"}, ...]
        (0.0, 0.0, []) when GT doesn't index the token (HTTP 404)
        None on irrecoverable error (429 / 5xx / timeout / network)

    No retries inside this call — on 429 we return None immediately and
    let `TTL_ERROR_SEC` schedule a fresh attempt a few minutes later.
    """
    network = NETWORK_MAP.get(chain.lower(), chain.lower())
    url = GT_URL.format(network=network, addr=addr.lower())

    pool_infos: list[dict] = []
    async with aiohttp.ClientSession(headers=_HTTP_HEADERS) as session:
        for page in range(1, MAX_PAGES + 1):
            await rl_acquire("gecko")
            try:
                async with session.get(url, params={"page": page}, timeout=20) as r:
                    if r.status == 404:
                        return (0.0, 0.0, [])
                    if r.status == 429:
                        log.warning(
                            "GT 429 %s/%s page=%d — error TTL (%.0fmin); "
                            "consider lowering rate_limiter.configure('gecko', ...)",
                            chain, addr, page, TTL_ERROR_SEC / 60,
                        )
                        return None
                    if r.status != 200:
                        log.warning("GT %s/%s page=%d -> HTTP %d", chain, addr, page, r.status)
                        return None
                    data = await r.json()
            except asyncio.TimeoutError:
                log.warning("GT %s/%s page=%d timeout", chain, addr, page)
                return None
            except Exception as e:
                log.warning("GT %s/%s page=%d failed: %s", chain, addr, page, e)
                return None

            pools = data.get("data") or []
            if not pools:
                break
            for p in pools:
                attr = p.get("attributes") or {}
                pool_addr = (attr.get("address") or "").strip().lower()
                if not pool_addr:
                    continue
                try:
                    liq = float(attr.get("reserve_in_usd") or 0)
                except (TypeError, ValueError):
                    liq = 0.0
                try:
                    vol = float((attr.get("volume_usd") or {}).get("h24") or 0)
                except (TypeError, ValueError, AttributeError):
                    vol = 0.0
                rel = p.get("relationships") or {}
                dex_id = ((rel.get("dex") or {}).get("data") or {}).get("id", "")
                # Token pair addresses — required for V4 (no pool
                # contract to read currency0/1 from on-chain).
                token_a = _gt_token_addr(rel.get("base_token"))
                token_b = _gt_token_addr(rel.get("quote_token"))
                pool_infos.append({
                    "address": pool_addr,
                    "reserve_usd": liq,
                    "vol_24h": vol,
                    "dex_id": dex_id or "",
                    "token_a": token_a,
                    "token_b": token_b,
                })
            if len(pools) < PAGE_SIZE:
                break

    total_liq = sum(p["reserve_usd"] for p in pool_infos)
    total_vol = sum(p["vol_24h"] for p in pool_infos)
    return (total_liq, total_vol, pool_infos)


async def _fetch_and_cache(
    chain: str,
    addr: str,
    threshold: float,
    vol_threshold: float,
    key: tuple[str, str],
) -> None:
    try:
        result = await _fetch_aggregates(chain, addr)
        now = int(time.time())
        if result is None:
            _CACHE[key] = {
                "status": "error",
                "liq_usd": None,
                "vol_usd": None,
                "checked_at": now,
                "threshold": threshold,
                "vol_threshold": vol_threshold,
            }
        else:
            liq, vol, pool_infos = result
            # Token is "low" if EITHER aggregate is below its threshold.
            # Volume catches honeypot tokens (lots of pools with dust
            # liquidity but $0 actual trading) — liquidity alone wouldn't.
            low_liq = liq < threshold
            low_vol = vol < vol_threshold
            if low_liq or low_vol:
                _CACHE[key] = {
                    "status": "low",
                    "liq_usd": liq,
                    "vol_usd": vol,
                    "checked_at": now,
                    "threshold": threshold,
                    "vol_threshold": vol_threshold,
                }
                reason = []
                if low_liq:
                    reason.append(f"liq=${liq:.0f}<${threshold:.0f}")
                if low_vol:
                    reason.append(f"vol=${vol:.0f}<${vol_threshold:.0f}")
                log.info(
                    "liquidity: %s/%s %s → blacklisted for %.0fmin",
                    chain, addr, " AND ".join(reason), TTL_LOW_SEC / 60,
                )
            else:
                # Aggregate passes. Try upgrade FIRST so the bound-pool
                # floor below evaluates against the (possibly new) best
                # pool, not the stale dust one.
                try:
                    await _maybe_upgrade_pool(chain, addr, pool_infos)
                except Exception as e:
                    log.warning("pool upgrade check failed for %s/%s: %s", chain, addr, e)

                # Per-bound-pool floor: the specific pool we listen to
                # must individually clear BOUND_POOL_MIN_USD. Catches the
                # "many dust pools sum to OK aggregate, but our actual
                # bound pool is too small to price reliably" case (FUEL:
                # $13k aggregate but bound V3 has only $1.8k → frozen
                # price → phantom spreads).
                bound_pool = _get_current_pool(chain, addr)[0]
                bound_pool_liq = 0.0
                if bound_pool:
                    for p in pool_infos:
                        if p["address"] == bound_pool:
                            bound_pool_liq = p["reserve_usd"]
                            break

                if bound_pool_liq < BOUND_POOL_MIN_USD:
                    _CACHE[key] = {
                        "status": "low",
                        "liq_usd": liq,
                        "vol_usd": vol,
                        "checked_at": now,
                        "threshold": threshold,
                        "vol_threshold": vol_threshold,
                    }
                    if not bound_pool:
                        reason = "no priceable pool bound"
                    else:
                        reason = (
                            f"bound pool {bound_pool[:10]}... "
                            f"liq=${bound_pool_liq:.0f}<${BOUND_POOL_MIN_USD:.0f}"
                        )
                    log.info(
                        "liquidity: %s/%s %s → blacklisted for %.0fmin",
                        chain, addr, reason, TTL_LOW_SEC / 60,
                    )
                else:
                    _CACHE[key] = {
                        "status": "ok",
                        "liq_usd": liq,
                        "vol_usd": vol,
                        "checked_at": now,
                        "threshold": threshold,
                        "vol_threshold": vol_threshold,
                    }
                    log.debug(
                        "liquidity: %s/%s liq=$%.0f vol=$%.0f bound=$%.0f → ok",
                        chain, addr, liq, vol, bound_pool_liq,
                    )
        await _persist()
    except Exception as e:
        log.warning("liquidity fetch error for %s/%s: %s", chain, addr, e)
        _CACHE[key] = {
            "status": "error",
            "liq_usd": None,
            "vol_usd": None,
            "checked_at": int(time.time()),
            "threshold": threshold,
            "vol_threshold": vol_threshold,
        }
    finally:
        _PENDING.discard(key)


# --------------------------------------------------------------------------
# pool auto-upgrade
# --------------------------------------------------------------------------

def _get_current_pool(chain: str, token_addr: str) -> tuple[Optional[str], Optional[str]]:
    """Look up the pool currently bound to (chain, token) via the
    cache_manager indexes. Returns (pool_address_lower, symbol_upper)
    or (None, None) when nothing is bound yet.

    `_INDEX["contract"]` is keyed with whatever case the contracts dict
    uses in cex_tokens.json (currently UPPERCASE chain), so we probe
    both casings to be robust against future normalization changes.
    """
    from cex_part import cache_manager

    chain_l = chain.lower()
    addr_l = token_addr.lower()
    contract_idx = cache_manager._INDEX.get("contract") or {}
    token_key = (
        contract_idx.get(f"{chain.upper()}:{addr_l}")
        or contract_idx.get(f"{chain_l}:{addr_l}")
    )
    if not token_key:
        return None, None
    entry = cache_manager._CACHE.get(token_key)
    if not isinstance(entry, dict):
        return None, None
    pools = ((entry.get("pools") or {}).get(chain_l)) or []
    symbol = (entry.get("symbol") or "").upper()
    current = pools[0].lower() if pools else None
    return current, (symbol or None)


def _v4_currencies(best: dict) -> Optional[tuple[str, str]]:
    """
    Return (currency0, currency1) for a V4 candidate, ordered by address
    (V4's canonical ordering: currency0 < currency1). None if the GT
    response didn't carry both token addresses.
    """
    ta = best.get("token_a")
    tb = best.get("token_b")
    if not ta or not tb:
        return None
    try:
        pair = sorted([ta.lower(), tb.lower()], key=lambda a: int(a, 16))
    except ValueError:
        return None
    return pair[0], pair[1]


async def _maybe_upgrade_pool(
    chain: str,
    token_addr: str,
    pool_infos: list[dict],
    trigger_restart: bool = True,
) -> bool:
    """
    When GT's top pool is substantially better than the one currently
    bound in pools.json (originally picked by DexScreener), swap it.

    Conditions for an upgrade:
      - POOL_AUTO_UPGRADE is enabled
      - Token has a current pool we can compare against
      - GT's best candidate is on a DEX our decoders understand
        (V2 / V3 / Uniswap V4)
      - Best candidate has >= POOL_UPGRADE_MIN_VOL of 24h volume
      - Either current pool is not in GT's response at all, OR best
        clears the tiered ratio gate (2.0x / 1.5x / >1.0x by current
        liquidity) AND best's 24h volume > current's 24h volume

    `trigger_restart` is forwarded to `replace_pool` — the bulk migration
    passes False and issues a single WS restart at the end.

    Returns True if a pool was actually swapped, False otherwise.
    """
    if not POOL_AUTO_UPGRADE or not pool_infos:
        return False

    current_pool, token_symbol = _get_current_pool(chain, token_addr)
    if not token_symbol:
        return False
    # `current_pool` may be None — the token has no pool bound yet
    # (e.g. just un-blacklisted). We still proceed: the best candidate
    # is then ADDED fresh (replace_pool treats old_pool="" as an add).

    # Candidates: decoder-supported pool (V2/V3 address or V4 poolId) on
    # a known DEX, with real 24h volume, AND priceable — one side native
    # or stable (a token/token pool has no USD anchor, compute_price
    # would return 0).
    candidates = [
        p for p in pool_infos
        if p["vol_24h"] >= POOL_UPGRADE_MIN_VOL
        and _is_subscribable_pool(p["address"], p["dex_id"])
        and _is_priceable_pool(chain, p.get("token_a"), p.get("token_b"))
    ]
    if not candidates:
        return False

    best = max(candidates, key=lambda p: p["reserve_usd"])
    if best["address"] == current_pool:
        return False  # already the best pool

    dex_name, version = _parse_gt_dex_id(best["dex_id"])

    # V4 candidate — only valid on chains that run a V4 monitor, and we
    # need both currency addresses (no pool contract to read them from).
    currency0 = currency1 = None
    if version == "v4":
        from engine.config import CHAINS
        if not (CHAINS.get(chain) or {}).get("v4_pool_manager"):
            log.debug("upgrade skip: %s has no V4 monitor (best is v4)", chain)
            return False
        cur = _v4_currencies(best)
        if not cur:
            log.debug("upgrade skip: v4 best %s missing token addrs", best["address"])
            return False
        currency0, currency1 = cur

    # Locate the current pool inside GT's response — gives us both its
    # liquidity and whether it's even priceable.
    current_info = None
    for p in pool_infos:
        if p["address"] == current_pool:
            current_info = p
            break
    current_liq = current_info["reserve_usd"] if current_info else 0.0
    current_priceable = (
        _is_priceable_pool(chain, current_info.get("token_a"), current_info.get("token_b"))
        if current_info is not None else True
    )

    # Tiered ratio gate — as the current pool gets richer, smaller
    # *relative* gains still represent big absolute value, so we
    # require LESS relative improvement to upgrade:
    #   current < $10k  → need 2.0x   (low liq: avoid flip-flop on noise)
    #   current ≥ $10k  → need 1.5x
    #   current ≥ $50k  → just need > current_liq (any improvement)
    # Skip the gate entirely when the current pool is unpriceable
    # (token/token): ANY priceable pool is strictly better than one we
    # can't value at all.
    if current_priceable and current_liq > 0:
        if current_liq >= 50_000:
            min_ratio = 1.0
        elif current_liq >= 10_000:
            min_ratio = 1.5
        else:
            min_ratio = 2.0
        # strict > for the ≥$50k tier ("any improvement"), >= for tiered
        if min_ratio == 1.0:
            ratio_ok = best["reserve_usd"] > current_liq
        else:
            ratio_ok = best["reserve_usd"] >= current_liq * min_ratio
        if not ratio_ok:
            log.debug(
                "upgrade skip: %s/%s best=$%.0f not %.1fx >= current=$%.0f",
                chain, token_symbol, best["reserve_usd"],
                min_ratio, current_liq,
            )
            return False

        # Volume must also improve — a fatter pool with dust volume is
        # often a stale LP position, not a tradable venue.
        current_vol = float(current_info.get("vol_24h") or 0.0) if current_info else 0.0
        if best["vol_24h"] <= current_vol:
            log.debug(
                "upgrade skip: %s/%s best vol=$%.0f not > current vol=$%.0f",
                chain, token_symbol, best["vol_24h"], current_vol,
            )
            return False

    # NOTE: no LIQUIDITY_MIN_USD floor on the per-pool best — token-level
    # total already cleared the threshold; this is purely "which pool".

    log.info(
        "pool %s: %s %s %s ($%.0f) -> %s ($%.0f, dex=%s%s, vol_24h=$%.0f)",
        "add" if current_pool is None else "upgrade",
        chain, token_symbol, current_pool or "(new)", current_liq,
        best["address"], best["reserve_usd"], dex_name,
        f" {version}" if version else "", best["vol_24h"],
    )

    # Lazy import to avoid a CEX <-> DEX import cycle at module load.
    from cex_part.core.pool_refresh import replace_pool
    return await replace_pool(
        chain=chain,
        token_symbol=token_symbol,
        old_pool=current_pool,
        new_pool=best["address"],
        dex=dex_name,
        version=version,
        currency0=currency0,
        currency1=currency1,
        trigger_restart=trigger_restart,
    )


def is_low_liquidity(
    chain: str,
    addr: str,
    threshold: float = LIQUIDITY_MIN_USD,
    vol_threshold: float = LIQUIDITY_MIN_VOL_USD,
) -> bool:
    """
    Return True iff this token is *known* to have aggregated DEX
    liquidity below `threshold` OR 24h volume below `vol_threshold`.
    False otherwise (cache says ok, cache says error, or no cache yet —
    let the caller decide).

    Side effect: when there's no fresh cache entry, schedules a
    fire-and-forget async fetch. The current call returns False (=
    don't filter); the next scan tick sees the cached verdict.

    Designed to be called from synchronous code that runs inside an
    asyncio event loop (e.g. the spread scanner). Outside an event
    loop the side-effect is a no-op (still returns False).
    """
    if not chain or not addr:
        return False
    key = (chain.lower(), addr.lower())

    entry = _CACHE.get(key)
    if _is_fresh(entry, threshold, vol_threshold):
        return entry.get("status") == "low"

    if key in _PENDING:
        return False

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False

    _PENDING.add(key)
    loop.create_task(_fetch_and_cache(chain, addr, threshold, vol_threshold, key))
    return False
