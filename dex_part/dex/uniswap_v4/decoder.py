"""
Decode a Uniswap V4 Swap log.

V4 emits all swaps from the singleton PoolManager, so a log is only
useful together with its `poolId` (which pool inside the singleton it
belongs to). Unlike V2/V3 the pool is NOT identified by the log's
`address` — that's always the PoolManager — but by `topics[1]`.

Event:
    Swap(PoolId indexed id, address indexed sender,
         int128 amount0, int128 amount1, uint160 sqrtPriceX96,
         uint128 liquidity, int24 tick, uint24 fee)

  topics[0] = event signature
  topics[1] = poolId            (bytes32)
  topics[2] = sender            (address, unused)

  data words (each 32 bytes):
    0  amount0       int128
    1  amount1       int128
    2  sqrtPriceX96  uint160   -> hex data[128:192]
    3  liquidity     uint128
    4  tick          int24
    5  fee           uint24

Only `poolId` + `sqrtPriceX96` are needed to derive the spot price —
the sqrt slot sits at the same offset our V3 decoder already reads.
"""


def decode_swap(log: dict) -> dict:
    """
    Return {"poolId": <0x..bytes32>, "sqrtPriceX96": int} or {} when the
    log is malformed (too-short data / missing poolId topic).
    """
    topics = log.get("topics") or []
    if len(topics) < 2:
        return {}

    pool_id = (topics[1] or "").lower()
    if not pool_id:
        return {}

    data = log.get("data") or ""
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 192:  # need at least 3 full 32-byte words
        return {}

    sqrt_px96 = int(data[128:192], 16)  # 3rd word
    return {"poolId": pool_id, "sqrtPriceX96": sqrt_px96}
