"""
Decode a V3 Swap log.

Standard Uniswap V3 layout — sender/recipient are indexed (topics[1..2]),
amounts/sqrtPriceX96/liquidity/tick live in data:

    data[0..32]   amount0   (int256)
    data[32..64]  amount1   (int256)
    data[64..96]  sqrtPriceX96 (uint160)
    data[96..128] liquidity (uint128)
    data[128..]   tick (int24) [+ extras for Pancake V3]

We only need sqrtPriceX96 to compute the spot price.
"""


def decode_swap(log: dict, meta: dict) -> dict:
    data = log.get("data") or ""
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 192:  # need 3 words (amount0, amount1, sqrtPriceX96)
        return {}
    sqrt_px96 = int(data[128:192], 16)  # 3rd 32-byte word
    return {"sqrtPriceX96": sqrt_px96}
