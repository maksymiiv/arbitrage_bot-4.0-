"""
Map vendor-specific chain names (Bybit "Ethereum", Gate "BNB Smart Chain",
etc.) into the canonical short codes we use elsewhere.
"""

ALLOWED_CHAINS = {"ETH", "BSC", "BASE", "SOL"}

CHAIN_MAP = {
    "Ethereum": "ETH",
    "ETH": "ETH",
    "BSC": "BSC",
    "BNB Smart Chain": "BSC",
    "Base Mainnet": "BASE",
    "BASE": "BASE",
    "BASEEVM": "BASE",
    "SOL": "SOL",
    "Solana": "SOL",
}


def normalize_chain(chain: str | None) -> str | None:
    return CHAIN_MAP.get(chain) if chain else None


def filter_networks(networks: dict) -> dict:
    return {
        chain: status
        for chain, status in networks.items()
        if chain.upper() in ALLOWED_CHAINS
    }
