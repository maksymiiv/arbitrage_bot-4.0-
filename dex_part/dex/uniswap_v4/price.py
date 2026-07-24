"""
USD price for a Uniswap V4 pool.

V4 concentrated-liquidity pools use the exact same `sqrtPriceX96` price
representation as V3 — the only V4-specific part is the Swap event
layout (handled in `decoder.py`). The price math — decimal scaling,
native/stable side resolution — is therefore identical, so we delegate
to the shared V3 implementation unchanged.

The `meta` dict must carry the same fields the V3 path expects:
    decimals0, decimals1,
    token0_is_native, token1_is_native,
    token0_is_stable, token1_is_stable,
    is_native_stable_pool, chain
"""

from ..pancake_v3.price import compute_price


__all__ = ["compute_price"]
