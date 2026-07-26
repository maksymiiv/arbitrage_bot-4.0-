"""
Shared registry of tracked DEX pools per chain.

`chain_monitor` populates it while building pool metadata, so downstream
consumers (the native-price poller and the reconciliation loop) don't
have to rebuild metadata independently. Structure:

    POOLS[chain][pool_lower] = meta   # same meta dict chain_monitor uses
"""

POOLS: dict[str, dict[str, dict]] = {}


def register_pool(chain: str, pool: str, meta: dict) -> None:
    POOLS.setdefault(chain, {})[pool.lower()] = meta


def get_pools(chain: str) -> dict[str, dict]:
    return POOLS.get(chain, {})
