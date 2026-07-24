import asyncio
import json

from ..config.swap_topics import ALL_V2_SYNC_TOPICS, PANCAKE_SWAP_TOPIC, SWAP_TOPIC_V3
from engine.logger import get_logger


log = get_logger(__name__)

# Per-chain WS restart signals. Each chain owns its own event so a
# pools.json change (or pool swap) on ONE chain restarts only that
# chain's WS session — not every chain — and each chain clears its own
# flag without racing the others over a single shared event.
WS_RESTART_EVENTS: dict[str, asyncio.Event] = {}


def get_restart_event(chain: str) -> asyncio.Event:
    """Return (creating on first use) the restart event for `chain`."""
    ev = WS_RESTART_EVENTS.get(chain)
    if ev is None:
        ev = asyncio.Event()
        WS_RESTART_EVENTS[chain] = ev
    return ev


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
