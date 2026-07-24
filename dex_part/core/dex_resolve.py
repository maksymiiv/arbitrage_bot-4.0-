"""
Maps protocol version -> swap decoder + price computer.

The pancake_v2/v3 modules are used as the canonical V2/V3 implementations
for all Uniswap-style forks (Pancake, Uniswap, ApeSwap, Biswap, Aerodrome,
etc.) — they read the same event layout.
"""

from dataclasses import dataclass
from typing import Callable, Dict

from ..config.swap_topics import ALL_SWAP_V2_TOPIC, PANCAKE_SWAP_TOPIC, SWAP_TOPIC_V3
from ..dex.pancake_v2.decoder import decode_swap as v2_decode
from ..dex.pancake_v2.price import compute_price as v2_price
from ..dex.pancake_v3.decoder import decode_swap as v3_decode
from ..dex.pancake_v3.price import compute_price as v3_price


@dataclass
class DexHandler:
    decode_swap: Callable
    compute_price: Callable


DEX_HANDLERS: Dict[str, DexHandler] = {
    "v3": DexHandler(v3_decode, v3_price),
    "v2": DexHandler(v2_decode, v2_price),
}


def get_dex_handler_by_topic(topic0: str) -> DexHandler | None:
    topic0 = (topic0 or "").lower()
    if topic0 in {SWAP_TOPIC_V3.lower(), PANCAKE_SWAP_TOPIC.lower()}:
        return DEX_HANDLERS["v3"]
    if topic0 == ALL_SWAP_V2_TOPIC.lower():
        return DEX_HANDLERS["v2"]
    return None


def get_dex_handler_by_version(version: str) -> DexHandler | None:
    return DEX_HANDLERS.get(version)
