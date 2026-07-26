"""
Periodic RPC reconciliation of DEX pool prices via Multicall3.

WS log subscriptions aren't reliable per-address: a large
`eth_subscribe logs` filter can silently stop delivering SOME pools'
swaps over time (a pool's price freezes while it's still trading on
chain). The chain-level stall watchdog can't catch this — the
subscription still delivers OTHER pools, so it looks healthy.

This loop is the backstop. Every POOL_RECONCILE_INTERVAL it reads the
current state of EVERY tracked V2/V3 pool on a chain in one Multicall3
`eth_call` per chunk (cheap — a single call reads hundreds of pools) and
refreshes price_store. So no pool's price can freeze longer than the
interval, whatever the WS feed does.

Native<->stable pools are skipped (the native-price poller owns those);
V4 pools live in the singleton PoolManager and are handled by v4_monitor.
"""

import asyncio
from decimal import Decimal

from web3 import Web3

from engine import price_store
from engine.config import CHAINS, POOL_RECONCILE_INTERVAL
from engine.logger import get_logger

from .dex_resolve import get_dex_handler_by_version
from .pool_registry import get_pools


log = get_logger(__name__)

# Multicall3 — same address on every EVM chain.
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
_MC_ABI = [{
    "inputs": [{
        "components": [
            {"name": "target", "type": "address"},
            {"name": "allowFailure", "type": "bool"},
            {"name": "callData", "type": "bytes"},
        ],
        "name": "calls", "type": "tuple[]",
    }],
    "name": "aggregate3",
    "outputs": [{
        "components": [
            {"name": "success", "type": "bool"},
            {"name": "returnData", "type": "bytes"},
        ],
        "name": "returnData", "type": "tuple[]",
    }],
    "stateMutability": "payable", "type": "function",
}]

_SLOT0_SEL = Web3.keccak(text="slot0()")[:4]
_GETRESERVES_SEL = Web3.keccak(text="getReserves()")[:4]

# Max calls per multicall eth_call — gas headroom on stricter nodes.
_CHUNK = 250

# Same sanity band as chain_monitor — reject drained/manipulated pools.
_PRICE_MAX = Decimal("1e9")
_PRICE_MIN = Decimal("1e-12")


def _asset_symbol(meta: dict):
    if meta.get("token0_is_native") or meta.get("token0_is_stable"):
        return meta.get("symbol1")
    if meta.get("token1_is_native") or meta.get("token1_is_stable"):
        return meta.get("symbol0")
    return None


def _reconcile_once(w3: Web3, chain: str) -> int:
    """One synchronous multicall sweep of the chain's V2/V3 token pools.
    Returns the number of pools refreshed."""
    pools = get_pools(chain)
    items = [
        (pool, meta) for pool, meta in pools.items()
        if meta.get("version") in ("v2", "v3")
        and not meta.get("is_native_stable_pool")
    ]
    if not items:
        return 0

    mc = w3.eth.contract(address=Web3.to_checksum_address(MULTICALL3), abi=_MC_ABI)
    updated = 0

    for i in range(0, len(items), _CHUNK):
        chunk = items[i:i + _CHUNK]
        calls = [
            (
                Web3.to_checksum_address(pool),
                True,  # allowFailure — a wrong-version pool just reverts
                _SLOT0_SEL if meta["version"] == "v3" else _GETRESERVES_SEL,
            )
            for pool, meta in chunk
        ]

        try:
            results = mc.functions.aggregate3(calls).call()
        except Exception as e:
            log.debug("[%s] reconcile multicall failed: %s", chain, e)
            continue

        for (pool, meta), res in zip(chunk, results):
            success, ret = res[0], res[1]
            if not success or not ret or len(ret) < 32:
                continue
            ver = meta["version"]
            handler = get_dex_handler_by_version(ver)
            if not handler:
                continue
            if ver == "v3":
                decoded = {"sqrtPriceX96": int.from_bytes(ret[0:32], "big")}
            else:
                if len(ret) < 64:
                    continue
                decoded = {
                    "reserve0": int.from_bytes(ret[0:32], "big"),
                    "reserve1": int.from_bytes(ret[32:64], "big"),
                }
            try:
                price = handler.compute_price(decoded, meta)
            except Exception:
                continue
            if not price or price <= 0 or price > _PRICE_MAX or price < _PRICE_MIN:
                continue
            symbol = _asset_symbol(meta)
            if not symbol:
                continue
            price_store.update_dex(
                symbol=symbol, chain=chain, price=float(price),
                pool=pool, protocol=ver,
            )
            updated += 1

    return updated


async def reconcile_loop(chain: str, interval: float | None = None) -> None:
    interval = interval if interval is not None else POOL_RECONCILE_INTERVAL
    if interval <= 0:
        log.info("[%s] pool reconcile disabled (interval<=0)", chain)
        return
    rpc_url = (CHAINS.get(chain) or {}).get("rpc")
    if not rpc_url:
        log.warning("[%s] no RPC configured — pool reconcile disabled", chain)
        return

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 15}))
    log.info("[%s] pool reconcile loop started (interval=%.0fs)", chain, interval)

    while True:
        await asyncio.sleep(interval)
        try:
            n = await asyncio.to_thread(_reconcile_once, w3, chain)
            if n:
                log.debug("[%s] reconcile refreshed %d pools", chain, n)
        except Exception as e:
            log.debug("[%s] reconcile loop error: %s", chain, e)
