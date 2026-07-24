import asyncio
import json

from ..config.swap_topics import ALL_V2_SYNC_TOPICS, PANCAKE_SWAP_TOPIC, SWAP_TOPIC_V3
from engine.logger import get_logger


log = get_logger(__name__)

WS_RESTART_EVENT = asyncio.Event()


async def subscribe_all(ws, pools) -> None:
    """
    Subscribe to all relevant swap topics for the supplied pools.
    Caller (chain_monitor) owns the recv loop.
    """
    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["logs", {"address": pools, "topics": [SWAP_TOPIC_V3]}],
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_subscribe",
            "params": ["logs", {"address": pools, "topics": [PANCAKE_SWAP_TOPIC]}],
        },
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "eth_subscribe",
            # OR-filter ([[a, b]]) — one request covers Uniswap-V2 Sync
            # AND Aerodrome-V2 Sync, so Base's Aerodrome v2 pools also
            # get live reserve updates.
            "params": ["logs", {"address": pools, "topics": [ALL_V2_SYNC_TOPICS]}],
        },
    ]

    for req in requests:
        await ws.send(json.dumps(req))
        await asyncio.sleep(0.1)

    log.debug("subscribe requests sent for %d pools", len(pools))
