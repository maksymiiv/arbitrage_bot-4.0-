"""
In-memory cache of native-token USD prices (BNB/ETH/...).

Updated by chain_monitor whenever a native<->stable pool emits a swap.
Read by price calculators of token<->native pools.
"""

from decimal import Decimal


_native_price_store: dict[str, Decimal] = {
    "bsc": Decimal(0),
    "eth": Decimal(0),
    "polygon": Decimal(0),
    "base": Decimal(0),
    "arbitrum": Decimal(0),
}

# Native<->stable pools per chain, registered by chain_monitor when it
# builds pool metadata: {chain: {pool_lower: meta}}. The native-price
# poller reads these (via RPC) instead of the WS firehose.
NATIVE_POOLS: dict[str, dict[str, dict]] = {}


def register_native_pool(chain: str, pool: str, meta: dict) -> None:
    """Record a native<->stable pool so the price poller can read it."""
    NATIVE_POOLS.setdefault(chain, {})[pool.lower()] = meta


def update_native_price(chain: str, price) -> None:
    # Coerce to Decimal so callers passing a float (or int) don't cause
    # `float / Decimal` TypeErrors downstream in the price calculators.
    if not isinstance(price, Decimal):
        try:
            price = Decimal(str(price))
        except Exception:
            return
    _native_price_store[chain] = price


def get_native_usd(chain: str) -> Decimal:
    return _native_price_store.get(chain, Decimal(0))
