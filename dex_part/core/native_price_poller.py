"""
Native (WETH/WBNB/...) USD price poller.

The native<->stable pool (e.g. WETH/USDC) is one of the busiest contracts
on a chain, so WS-subscribing to it just to read the current native price
delivers a firehose of swaps we discard — expensive on a metered provider
(~40 CU per event). Instead we RPC-poll that pool's state every
NATIVE_PRICE_POLL_INTERVAL seconds: a single slot0/getReserves call is
cheap and 15s freshness is more than enough for a slow-moving ETH/BNB USD
price.

chain_monitor registers pools into the shared `pool_registry` while
building metadata (and no longer WS-subscribes to native<->stable ones).
This loop reads the native<->stable pools from that registry and updates
the shared native-price cache. Startup seeding still comes from
`current_price.bootstrap_dex_prices`, so token pricing works before the
first poll.
"""

import asyncio
from decimal import Decimal

from web3 import Web3

from engine.config import CHAINS, NATIVE_PRICE_POLL_INTERVAL
from engine.logger import get_logger

from ..utils.native_price import update_native_price
from .current_price import _read_pool_state
from .pool_registry import get_pools


log = get_logger(__name__)


async def native_price_loop(chain: str, interval: float | None = None) -> None:
    interval = interval or NATIVE_PRICE_POLL_INTERVAL
    rpc_url = (CHAINS.get(chain) or {}).get("rpc")
    if not rpc_url:
        log.warning("[%s] no RPC configured — native price poller disabled", chain)
        return

    rpc = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
    log.info("[%s] native price poller started (interval=%.0fs)", chain, interval)

    warned_empty = False
    while True:
        try:
            pools = {
                p: m for p, m in get_pools(chain).items()
                if m.get("is_native_stable_pool")
            }
            if not pools:
                if not warned_empty:
                    log.info(
                        "[%s] native price poller: no native<->stable pool "
                        "registered yet — waiting", chain,
                    )
                    warned_empty = True
            else:
                warned_empty = False
                for pool, meta in pools.items():
                    decoded, handler, _ = await _read_pool_state(
                        rpc, pool, meta.get("version")
                    )
                    if not decoded or not handler:
                        continue
                    price = handler.compute_price(decoded, meta)
                    if price and price > 0:
                        native_usd = (
                            price if meta.get("token0_is_native")
                            else (Decimal(1) / price)
                        )
                        update_native_price(chain, native_usd)
                        break  # one good native pool is enough
        except Exception as e:
            log.debug("[%s] native price poll error: %s", chain, e)

        await asyncio.sleep(interval)
