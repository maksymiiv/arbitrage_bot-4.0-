"""
Uniswap V4 monitor.

Every V4 pool lives inside one singleton `PoolManager` per chain, so we
keep a SINGLE WebSocket subscription per chain — `logs` on the
PoolManager address filtered to the V4 Swap topic — and demultiplex by
`poolId` (carried in `topics[1]`).

Pool metadata (currency addresses, decimals, native/stable flags) is
prepared at discovery time by `token_metadata.get_v4_pool_metadata` and
cached in `token_cache.json` under the poolId key. This monitor reloads
the set of `version=v4` entries for its chain every `_RELOAD_INTERVAL`
seconds, so newly-discovered V4 pools start streaming without a
reconnect.

Price math is identical to V3 (same sqrtPriceX96 representation) — see
`dex.uniswap_v4.price`.
"""

import asyncio
import json
import time
from decimal import Decimal

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from engine import price_store
from engine.config import CHAINS
from engine.logger import get_logger, setup_chain_logger

from ..config.swap_topics import SWAP_TOPIC_V4
from ..dex.uniswap_v4.decoder import decode_swap
from ..dex.uniswap_v4.price import compute_price
from ..utils.native_price import update_native_price
from ..utils.token_metadata import CACHE_FILE


log = get_logger(__name__)

# How often to reload the v4 poolId→meta map from token_cache.json.
_RELOAD_INTERVAL = 60.0

# Reconnect backoff (mirrors chain_monitor): exponential, capped.
_BACKOFF_BASE = 2.0
_BACKOFF_MAX = 5 * 60.0

# Same sanity band as chain_monitor — drop absurd prices (drained /
# manipulated pools at the V4 tick boundary).
_PRICE_MIN = Decimal("1e-12")
_PRICE_MAX = Decimal("1e9")


def _load_v4_pools(chain: str) -> dict[str, dict]:
    """Return {poolId_lower: meta} for every version=v4 entry on `chain`."""
    try:
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, dict] = {}
    for key, meta in raw.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("version") != "v4" or meta.get("chain") != chain:
            continue
        out[key.lower()] = meta
    return out


def _resolve_asset_symbol(meta: dict):
    """The non-native / non-stable side is the asset we price."""
    if meta.get("token0_is_native") or meta.get("token0_is_stable"):
        return meta.get("symbol1")
    if meta.get("token1_is_native") or meta.get("token1_is_stable"):
        return meta.get("symbol0")
    return None


async def monitor_v4_chain(chain: str) -> None:
    """Top-level per-chain V4 monitor with a resilient reconnect loop."""
    manager = (CHAINS.get(chain) or {}).get("v4_pool_manager")
    if not manager:
        return  # this chain has no Uniswap V4

    setup_chain_logger(chain)
    consecutive_errors = 0

    while True:
        try:
            await _run_v4_session(chain, manager)
            consecutive_errors = 0
        except (ConnectionClosedError, ConnectionClosedOK):
            log.warning("[%s/v4] WS closed", chain)
            await asyncio.sleep(1)
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            msg = str(e).lower()
            is_429 = "429" in msg or "too many" in msg
            if is_429:
                wait = max(60.0, min(_BACKOFF_MAX, _BACKOFF_BASE * (2 ** consecutive_errors)))
            else:
                wait = min(_BACKOFF_MAX, _BACKOFF_BASE * (2 ** (consecutive_errors - 1)))
            log.error(
                "[%s/v4] session error (#%d, sleep %.0fs): %s",
                chain, consecutive_errors, wait, e,
            )
            await asyncio.sleep(wait)


async def _run_v4_session(chain: str, manager: str) -> None:
    pools = _load_v4_pools(chain)
    if not pools:
        # Nothing to monitor yet — don't open a WS that would just
        # receive every V4 swap on the chain and discard it (wastes
        # provider compute units). Re-check after a short wait.
        log.info("[%s/v4] no v4 pools tracked yet — idle", chain)
        await asyncio.sleep(_RELOAD_INTERVAL)
        return

    ws_url = CHAINS[chain]["ws"]
    log.info("[%s/v4] connecting, %d v4 pools tracked", chain, len(pools))

    last_reload = time.time()

    async with websockets.connect(ws_url, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
            "params": ["logs", {"address": manager, "topics": [SWAP_TOPIC_V4]}],
        }))
        ack = json.loads(await ws.recv())
        if ack.get("error"):
            raise RuntimeError(f"v4 subscribe rejected: {ack['error']}")
        log.info("[%s/v4] subscribed to PoolManager %s", chain, manager)

        while True:
            # Periodic reload picks up newly-discovered V4 pools. Do it
            # BEFORE recv so a quiet chain (recv repeatedly timing out)
            # still reloads on schedule instead of stalling until the
            # next swap arrives.
            if time.time() - last_reload > _RELOAD_INTERVAL:
                pools = _load_v4_pools(chain)
                last_reload = time.time()

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
            except asyncio.TimeoutError:
                # quiet period — ping_interval keeps the socket alive
                continue

            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("method") != "eth_subscription":
                continue
            result = (msg.get("params") or {}).get("result") or {}

            decoded = decode_swap(result)
            if not decoded:
                continue

            meta = pools.get(decoded["poolId"])
            if not meta:
                continue  # a V4 pool we don't track

            try:
                price = compute_price(
                    {"sqrtPriceX96": decoded["sqrtPriceX96"]}, meta,
                )
            except Exception as e:
                log.debug("[%s/v4] price calc failed for %s: %s",
                          chain, decoded["poolId"], e)
                continue
            if not price or price <= 0:
                continue

            # native<->stable pool feeds the chain's native USD price
            if meta.get("is_native_stable_pool"):
                native_usd = (
                    price if meta.get("token0_is_native")
                    else (Decimal(1) / price)
                )
                update_native_price(chain, native_usd)
                continue

            if price > _PRICE_MAX or price < _PRICE_MIN:
                pair = f"{meta.get('symbol0','?')}/{meta.get('symbol1','?')}"
                log.warning(
                    "[%s/v4] out-of-range price %.3e for %s pool=%s",
                    chain, float(price), pair, decoded["poolId"],
                )
                continue

            symbol = _resolve_asset_symbol(meta)
            if not symbol:
                continue

            log.info(
                "[%s/v4] %s price=%s USD pool=%s",
                chain, symbol, float(price), decoded["poolId"],
            )
            price_store.update_dex(
                symbol=symbol,
                chain=chain,
                price=float(price),
                pool=decoded["poolId"],
                protocol="v4",
            )
