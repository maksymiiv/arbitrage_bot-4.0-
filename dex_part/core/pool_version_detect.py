"""
Detect a pool's protocol version from a swap log topic and persist the
detection back to pools.json.
"""

from ..config.swap_topics import (
    ALL_V2_SYNC_TOPICS,
    PANCAKE_SWAP_TOPIC,
    SWAP_TOPIC_V3,
)
from .pools_io import mutate
from engine.logger import get_logger


log = get_logger(__name__)

# Both Uniswap-V2 and Aerodrome-V2 Sync topics map to "v2".
_V2_SYNC_SET = {t.lower() for t in ALL_V2_SYNC_TOPICS}


def detect_pool_version(topic0: str) -> str | None:
    topic0 = (topic0 or "").lower()
    if topic0 in _V2_SYNC_SET:
        return "v2"
    if topic0 in {SWAP_TOPIC_V3.lower(), PANCAKE_SWAP_TOPIC.lower()}:
        return "v3"
    return None


async def save_pool_version(chain: str, pool: str, version: str) -> None:
    if not chain or not pool or not version:
        return

    chain = chain.lower()
    pool = pool.lower()

    def mutator(pools: dict) -> bool:
        if not pools:
            return False
        for item in pools.get(chain, []):
            if str(item.get("pool", "")).lower() == pool:
                if item.get("version") != version:
                    item["version"] = version
                    return True
                return False
        return False

    await mutate(mutator)
