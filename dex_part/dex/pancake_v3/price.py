"""
USD price of the non-stable / non-native side of a V3 pool from sqrtPriceX96.
"""

from decimal import Decimal

from ...utils.native_price import get_native_usd
from ...utils.price_engine import raw_price_from_sqrt


def compute_price(decoded: dict, meta: dict) -> Decimal:
    """
    raw = token1 / token0 (from sqrtPriceX96 with decimal scaling).
    """
    if not decoded:
        return Decimal("0")

    sqrt_p = decoded.get("sqrtPriceX96")
    if not sqrt_p:
        return Decimal("0")

    raw = raw_price_from_sqrt(sqrt_p, meta["decimals0"], meta["decimals1"])
    if raw <= 0:
        return Decimal("0")

    t0_stable = meta["token0_is_stable"]
    t1_stable = meta["token1_is_stable"]
    t0_native = meta["token0_is_native"]
    t1_native = meta["token1_is_native"]

    # native<->stable pool: chain_monitor expects raw and inverts itself
    # depending on which side is native.
    if meta.get("is_native_stable_pool"):
        return raw

    # stable / token
    if t1_stable and not t0_stable:
        return raw  # token0 in USD
    if t0_stable and not t1_stable:
        return Decimal(1) / raw  # token1 in USD

    # native / token
    native_usd = get_native_usd(meta["chain"])
    if not native_usd or native_usd <= 0:
        return Decimal("0")

    if t1_native and not t0_native:
        return raw * native_usd
    if t0_native and not t1_native:
        return native_usd / raw

    return Decimal("0")
