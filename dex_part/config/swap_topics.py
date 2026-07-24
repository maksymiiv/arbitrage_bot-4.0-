# Standard Uniswap/PancakeSwap V3 swap event
SWAP_TOPIC_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

# PancakeSwap V3 EXTENDED swap event (BSC only)
PANCAKE_SWAP_TOPIC = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"

# V2-style pools are tracked via their Sync (reserves) event.
# Uniswap V2 / PancakeSwap V2 — Sync(uint112,uint112).
ALL_SWAP_V2_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

# Aerodrome / Velodrome / Solidly v2 pools emit Sync(uint256,uint256) —
# a DIFFERENT topic. Without subscribing to it, Base's Aerodrome v2
# pools never receive reserve updates (price freezes at bootstrap).
# The data layout is identical (two 32-byte words: reserve0, reserve1),
# so the existing V2 decoder handles it unchanged.
AERODROME_SYNC_TOPIC = "0xcf2aa50876cdfbb541206f89af0ee78d44a2abf8d328e37fa4917f982149848a"

# Uniswap V4 events — emitted by the singleton PoolManager (one contract
# per chain holds every pool). Unlike V2/V3 there is no per-pool address;
# the pool is identified by `id` (bytes32 poolId) carried in topics[1].
#   Swap(PoolId indexed id, address indexed sender, int128 amount0,
#        int128 amount1, uint160 sqrtPriceX96, uint128 liquidity,
#        int24 tick, uint24 fee)
SWAP_TOPIC_V4 = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
#   Initialize(PoolId indexed id, Currency indexed currency0,
#              Currency indexed currency1, uint24 fee, int24 tickSpacing,
#              address hooks)  — kept for future auto-discovery of new pools.
INITIALIZE_TOPIC_V4 = "0x3fd553db44f207b1f41348cfc4d251860814af9eadc470e8e7895e4d120511f4"

# Загальний список топіків (щоб легко додавати нові DEX)
SWAP_TOPICS = {
    "uniswap_v3": [
        SWAP_TOPIC_V3,
    ],
    "uniswap_v2": [
        ALL_SWAP_V2_TOPIC,
    ],
    "uniswap_v4": [
        SWAP_TOPIC_V4,
    ],
    "pancake_v3": [
        SWAP_TOPIC_V3,
        PANCAKE_SWAP_TOPIC,
    ],
    "pancake_v2": [
        ALL_SWAP_V2_TOPIC,
    ],
    "aerodrome_v2": [
        AERODROME_SYNC_TOPIC,
    ],
}

# Every V2-style Sync topic — used as a single OR-filter in the WS
# subscription so one request covers Uniswap-V2 and Aerodrome-V2 pools.
ALL_V2_SYNC_TOPICS = [ALL_SWAP_V2_TOPIC, AERODROME_SYNC_TOPIC]