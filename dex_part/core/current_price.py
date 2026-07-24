"""
One-shot DEX price bootstrap:
1) discover native USD price from a native<->stable pool
2) read every other pool's current state and seed price_store
"""

import asyncio
from decimal import Decimal
from typing import Optional

from web3 import Web3

from engine import price_store
from engine.logger import get_logger

from ..utils.native_price import update_native_price
from .dex_resolve import get_dex_handler_by_version


log = get_logger(__name__)

V2_POOL_ABI = [
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"name": "_reserve0", "type": "uint112"},
            {"name": "_reserve1", "type": "uint112"},
            {"name": "_blockTimestampLast", "type": "uint32"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

V3_POOL_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"internalType": "int24", "name": "tick", "type": "int24"},
            {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
            {"internalType": "uint32", "name": "feeProtocol", "type": "uint32"},
            {"internalType": "bool", "name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

V3_AERODROME_POOL_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"internalType": "int24", "name": "tick", "type": "int24"},
            {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
            {"internalType": "bool", "name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]


def _try_read_v2(rpc: Web3, pool: str) -> Optional[dict]:
    try:
        pool_cs = Web3.to_checksum_address(pool)
        contract = rpc.eth.contract(address=pool_cs, abi=V2_POOL_ABI)
        r0, r1, _ = contract.functions.getReserves().call()
        return {"reserve0": int(r0), "reserve1": int(r1)}
    except Exception:
        return None


def _try_read_v3(w3: Web3, pool: str) -> Optional[dict]:
    pool_cs = Web3.to_checksum_address(pool)

    for abi in (V3_POOL_ABI, V3_AERODROME_POOL_ABI):
        try:
            c = w3.eth.contract(address=pool_cs, abi=abi)
            slot0 = c.functions.slot0().call()
            return {"sqrtPriceX96": slot0[0]}
        except Exception:
            continue
    return None


async def _read_pool_state(rpc: Web3, pool: str, preferred_version: str | None):
    """
    Try preferred version first, fall back to the other.
    Returns (decoded, handler, version) or (None, None, None).
    """
    v2_handler = get_dex_handler_by_version("v2")
    v3_handler = get_dex_handler_by_version("v3")

    pref = (preferred_version or "").lower().strip()

    async def read_v2():
        decoded = await asyncio.to_thread(_try_read_v2, rpc, pool)
        if decoded and v2_handler:
            return decoded, v2_handler, "v2"
        return None, None, None

    async def read_v3():
        decoded = await asyncio.to_thread(_try_read_v3, rpc, pool)
        if decoded and v3_handler:
            return decoded, v3_handler, "v3"
        return None, None, None

    if pref == "v2":
        result = await read_v2()
        return result if result[0] else await read_v3()
    if pref == "v3":
        result = await read_v3()
        return result if result[0] else await read_v2()

    result = await read_v2()
    return result if result[0] else await read_v3()


async def bootstrap_dex_prices(rpc: Web3, chain_name: str, metadata: dict) -> None:
    log.info("[%s] bootstrap %d pools", chain_name, len(metadata))

    if not get_dex_handler_by_version("v2") and not get_dex_handler_by_version("v3"):
        log.error("[%s] no v2/v3 handlers", chain_name)
        return

    stats = {"native_ok": 0, "v2_ok": 0, "v3_ok": 0, "asset_ok": 0, "skipped": {}}

    def skip(reason: str, pool: str) -> None:
        stats["skipped"][reason] = stats["skipped"].get(reason, 0) + 1
        log.debug("[%s] skip %s: %s", chain_name, reason, pool)

    # 1) native USD via the first viable native<->stable pool
    native_found = False
    for pool, meta in metadata.items():
        if not meta.get("is_native_stable_pool"):
            continue
        try:
            decoded, handler, ver = await _read_pool_state(rpc, pool, meta.get("version"))
            if not decoded or not handler:
                skip("native_no_v2_no_v3", pool)
                continue
            usd_price = handler.compute_price(decoded, meta)
            if not usd_price or usd_price <= 0:
                skip("native_zero_price", pool)
                continue
            native_usd = usd_price if meta.get("token0_is_native") else (Decimal(1) / usd_price)
            update_native_price(chain_name, native_usd)
            stats["native_ok"] += 1
            native_found = True
            log.info(
                "[%s] native_usd=%s (raw=%s) pool=%s",
                chain_name.upper(), round(float(native_usd), 6), usd_price, pool,
            )
            break
        except Exception as e:
            skip(f"native_exception:{type(e).__name__}", pool)

    if not native_found:
        log.warning("[%s] native price not found — bootstrap aborted", chain_name)
        return

    # 2) every other pool
    for pool, meta in metadata.items():
        try:
            decoded, handler, ver = await _read_pool_state(rpc, pool, meta.get("version"))
            if not decoded or not handler or not ver:
                skip("no_v2_no_v3", pool)
                continue

            price = handler.compute_price(decoded, meta)
            if not price or price <= 0:
                skip(f"{ver}_zero_price", pool)
                continue

            # Sanity range — same reason as in chain_monitor: V3 pools
            # at MIN/MAX sqrt-ratio decode to absurd prices (e.g. 3.4e+50)
            # and would poison price_store at bootstrap.
            if price > Decimal("1e9") or price < Decimal("1e-12"):
                skip(f"{ver}_out_of_range", pool)
                continue

            symbol = None
            if meta.get("token0_is_stable") or meta.get("token0_is_native"):
                symbol = meta.get("symbol1")
            elif meta.get("token1_is_stable") or meta.get("token1_is_native"):
                symbol = meta.get("symbol0")
            if not symbol:
                skip(f"{ver}_asset_asset", pool)
                continue

            stats["asset_ok"] += 1
            stats["v2_ok" if ver == "v2" else "v3_ok"] += 1

            log.debug("[%s] %s [%s] %s USD pool=%s", chain_name.upper(), symbol, ver.upper(), price, pool)
            price_store.update_dex(
                symbol=symbol,
                chain=chain_name,
                price=float(price),
                pool=pool,
                protocol=ver,
            )

        except Exception as e:
            skip(f"exception:{type(e).__name__}", pool)

    log.info(
        "[%s] bootstrap summary native=%d v2=%d v3=%d assets=%d skipped=%s",
        chain_name, stats["native_ok"], stats["v2_ok"], stats["v3_ok"], stats["asset_ok"], stats["skipped"],
    )
