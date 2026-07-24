"""
Per-chain WebSocket session:
- robust reconnect loop
- concurrent metadata init with timeouts
- bootstrap (read state of all pools) when DEX side has no prices yet
- consumes swap logs and feeds price_store
"""

import asyncio
import json
import time
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import websockets
from web3 import Web3
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from engine import price_store
from engine.logger import get_logger, setup_chain_logger

from ..config.chains import CHAINS
from ..utils.native_price import update_native_price
from ..utils.token_metadata import get_pool_metadata
from .bad_pools import THRESHOLD as BAD_POOL_THRESHOLD, record_bad_price
from .current_price import bootstrap_dex_prices
from .dex_resolve import get_dex_handler_by_version
from .pool_version_detect import detect_pool_version, save_pool_version
from .pools_io import load_sync as load_pools_sync
from .stable_native import is_native, is_stable
from .ws_subscriber import WS_RESTART_EVENT, subscribe_all


log = get_logger(__name__)


# ---------------------------------------------------------------------------
# bootstrap throttle
# ---------------------------------------------------------------------------
_BOOTSTRAP_LAST_TRY: dict[str, float] = {}
_BOOTSTRAP_COOLDOWN_SEC = 60.0


# Optional: only allow DEX updates for symbols already populated by CEX.
# Default False — DEX may seed new symbols.
REQUIRE_SYMBOL_EXISTS_IN_STORE = False


async def _to_thread_timeout(fn, timeout: float = 12):
    return await asyncio.wait_for(asyncio.to_thread(fn), timeout=timeout)


def _has_any_dex_prices_for_chain(chain_name: str) -> bool:
    snap = price_store.snapshot()
    for _, data in (snap or {}).items():
        if chain_name in ((data or {}).get("dex") or {}):
            return True
    return False


def _should_bootstrap(chain_name: str) -> bool:
    if _has_any_dex_prices_for_chain(chain_name):
        return False
    last = _BOOTSTRAP_LAST_TRY.get(chain_name, 0.0)
    return (time.time() - last) >= _BOOTSTRAP_COOLDOWN_SEC


def _resolve_asset_symbol(meta: dict) -> Optional[str]:
    if meta.get("token0_is_native") or meta.get("token0_is_stable"):
        return meta.get("symbol1")
    if meta.get("token1_is_native") or meta.get("token1_is_stable"):
        return meta.get("symbol0")
    return None


def _is_unsupported_pool(dex: str) -> bool:
    if not dex:
        return False
    if dex in {"curve", "balancer"}:
        return True
    if dex.startswith("0x"):
        return True
    return False


# ---------------------------------------------------------------------------
# entry
# ---------------------------------------------------------------------------

async def monitor_chain(chain_name: str) -> None:
    setup_chain_logger(chain_name)

    # Adaptive reconnect backoff: a fast 1-2s retry on a clean WS close
    # is fine, but providers (especially free-tier Alchemy / publicnode)
    # 429 the *WebSocket handshake itself* when we reconnect aggressively
    # — tighter loops then make it worse. We track consecutive failures
    # and grow the sleep exponentially, with a hard 5 min cap so we still
    # recover after the rate-limit window clears.
    consecutive_errors = 0
    base_backoff = 2.0
    max_backoff = 5 * 60.0

    while True:
        WS_RESTART_EVENT.clear()
        try:
            await _run_ws_session(chain_name)
            consecutive_errors = 0  # clean exit / WS_RESTART_EVENT → reset
        except (ConnectionClosedError, ConnectionClosedOK) as e:
            log.warning("[%s] WS closed: %s", chain_name, type(e).__name__)
            await asyncio.sleep(1)
            consecutive_errors = 0  # normal close — don't escalate
        except Exception as e:
            consecutive_errors += 1
            # HTTP 429 on the WS handshake means the provider's rate
            # limiter rejected the *connection attempt*; retrying within
            # seconds will just get rejected again. Sleep at least 60s
            # in that case before retrying.
            msg = str(e).lower()
            is_429 = "429" in msg or "too many" in msg

            if is_429:
                wait = max(60.0, min(max_backoff, base_backoff * (2 ** consecutive_errors)))
                log.error(
                    "[%s] WS handshake 429 (provider rate-limit) — sleeping %.0fs before retry #%d",
                    chain_name, wait, consecutive_errors,
                )
            else:
                wait = min(max_backoff, base_backoff * (2 ** (consecutive_errors - 1)))
                log.error(
                    "[%s] session error (#%d, sleeping %.0fs): %s\n%s",
                    chain_name, consecutive_errors, wait, e, traceback.format_exc(),
                )

            await asyncio.sleep(wait)


async def _run_ws_session(chain_name: str) -> None:
    chain = CHAINS[chain_name]
    ws_url = chain["ws"]
    rpc = Web3(Web3.HTTPProvider(chain["rpc"], request_kwargs={"timeout": 10}))

    pools_cfg = load_pools_sync().get(chain_name, [])
    if not pools_cfg:
        log.info("[%s] no pools configured, sleeping", chain_name)
        await asyncio.sleep(5)
        return

    metadata, pools = await _init_metadata(chain_name, rpc, pools_cfg)
    if not pools:
        log.info("[%s] no pools initialized", chain_name)
        await asyncio.sleep(5)
        return

    if _should_bootstrap(chain_name):
        _BOOTSTRAP_LAST_TRY[chain_name] = time.time()
        log.info("[%s] bootstrap start (dex empty)", chain_name)
        try:
            await bootstrap_dex_prices(rpc, chain_name, metadata)
        except Exception as e:
            log.error("[%s] bootstrap error: %s", chain_name, e)
        log.info("[%s] bootstrap done", chain_name)

    async with websockets.connect(ws_url, ping_interval=20) as ws:
        await subscribe_all(ws, pools)
        log.info("[%s] subscribed %d pools", chain_name, len(pools))
        await _recv_loop(ws, chain_name, metadata)


async def _init_metadata(
    chain_name: str,
    rpc: Web3,
    pools_cfg: List[dict],
) -> tuple[Dict[str, Dict[str, Any]], List[str]]:
    metadata: Dict[str, Dict[str, Any]] = {}
    pools: List[str] = []
    # Per-chain init concurrency. Each metadata fetch performs up to 8
    # RPC calls (token0, token1, and decimals/symbol/name for each
    # token). Public free endpoints (publicnode, mainnet.base.org)
    # 429 around 30+ req/sec — keep this conservative so cold-starts
    # don't trigger rate-limit storms.
    sem = asyncio.Semaphore(3)

    async def init_one(item: dict) -> None:
        # Uniswap V4 pools are keyed by a 32-byte poolId (no contract
        # address) and live in the singleton PoolManager — they're
        # handled by the separate v4_monitor, skip them here.
        if item.get("version") == "v4":
            return

        pool = Web3.to_checksum_address(item["pool"])
        dex = (item.get("dex") or "").strip()
        version = item.get("version")

        if _is_unsupported_pool(dex):
            return

        async with sem:
            try:
                meta = await _to_thread_timeout(lambda: get_pool_metadata(rpc, pool), timeout=12)
            except Exception as e:
                log.warning("[%s] init meta fail %s: %s", chain_name, pool, e)
                return

        meta.update({"dex": dex, "version": version, "chain": chain_name})

        meta["token0_is_native"] = is_native(chain_name, meta["token0"])
        meta["token1_is_native"] = is_native(chain_name, meta["token1"])
        meta["token0_is_stable"] = is_stable(chain_name, meta["token0"])
        meta["token1_is_stable"] = is_stable(chain_name, meta["token1"])

        meta["is_native_stable_pool"] = (
            (meta["token0_is_native"] and meta["token1_is_stable"])
            or (meta["token1_is_native"] and meta["token0_is_stable"])
        )
        meta["last_swap"] = {"block": None, "msg": None}

        metadata[pool.lower()] = meta
        pools.append(pool)

    tasks = [init_one(item) for item in pools_cfg]
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=90,
        )
    except asyncio.TimeoutError:
        log.warning("[%s] init meta global timeout", chain_name)

    return metadata, pools


async def _recv_loop(ws, chain_name: str, metadata: dict) -> None:
    while not WS_RESTART_EVENT.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
        except asyncio.TimeoutError:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        if data.get("method") != "eth_subscription":
            continue

        params = data.get("params")
        if not params or "result" not in params:
            continue

        log_entry = params["result"]
        pool = (log_entry.get("address") or "").lower()
        meta = metadata.get(pool)
        if not meta:
            continue

        topics = log_entry.get("topics") or []
        if not topics:
            continue
        topic0 = (topics[0] or "").lower()

        if not meta.get("version"):
            detected = detect_pool_version(topic0)
            if not detected:
                continue
            meta["version"] = detected
            try:
                await save_pool_version(chain_name, pool, detected)
            except Exception as e:
                log.debug("save_pool_version failed: %s", e)

        handler = get_dex_handler_by_version(meta["version"])
        if not handler:
            continue

        decoded = handler.decode_swap(log_entry, meta)
        if not decoded:
            continue

        usd_price = handler.compute_price(decoded, meta)
        if not usd_price or usd_price <= 0:
            continue

        # Sanity-filter: V3 pools that get drained or manipulated end up
        # at MIN_SQRT_RATIO / MAX_SQRT_RATIO, which decode to absurd
        # prices (e.g. MAX_SQRT_RATIO + 10^12 decimals scale → 3.4e+50).
        # We don't reject the pool on a single bad hit — a brief boundary
        # excursion can recover within seconds. Instead we count hits and
        # only when the pool keeps producing nonsense (THRESHOLD events
        # in `bad_pools`) we drop it from pools.json and trigger a
        # DexScreener re-discovery for the same token.
        if usd_price > Decimal("1e9") or usd_price < Decimal("1e-12"):
            pair = f"{meta.get('symbol0','?')}/{meta.get('symbol1','?')}"
            just_rejected = record_bad_price(chain_name, pool)
            if just_rejected:
                log.warning(
                    "[%s] pool %s (%s) rejected after repeated out-of-range "
                    "prices — scheduling re-discovery",
                    chain_name, pool, pair,
                )
                # Lazy import to avoid a CEX <-> DEX startup cycle.
                from cex_part.core.pool_refresh import drop_pool_for_replacement
                asyncio.create_task(drop_pool_for_replacement(chain_name, pool))
            else:
                log.warning(
                    "[%s] out-of-range price %.3e for %s pool=%s (rejects after %d hits)",
                    chain_name, float(usd_price), pair, pool, BAD_POOL_THRESHOLD,
                )
            continue

        if meta.get("is_native_stable_pool"):
            native_usd = usd_price if meta.get("token0_is_native") else (Decimal(1) / usd_price)
            update_native_price(chain_name, native_usd)
            continue

        symbol = _resolve_asset_symbol(meta)
        if not symbol:
            continue

        if REQUIRE_SYMBOL_EXISTS_IN_STORE:
            if symbol not in (price_store.snapshot() or {}):
                continue

        price_store.update_dex(
            symbol=symbol,
            chain=chain_name,
            price=float(usd_price),
            pool=pool,
            protocol=meta["version"],
        )

        try:
            block = int(log_entry["blockNumber"], 16)
        except Exception:
            block = None

        now = datetime.now().strftime("%H:%M:%S")
        pair = f"{meta.get('symbol0')}/{meta.get('symbol1')}"
        ver = str(meta.get("version", "")).upper()
        msg = f"[{now}] [{chain_name.upper()}] [{pair}] [{ver}] price={usd_price} USD | block={block}"

        last = meta["last_swap"]
        if last.get("block") != block:
            if last.get("msg"):
                log.info(last["msg"])
            last["block"] = block
            last["msg"] = msg
        else:
            last["msg"] = msg
