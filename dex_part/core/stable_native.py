from ..config.tokens import NATIVE_NAMES, STABLES, SWAP_STABLES


def is_stable(chain: str, token_addr: str) -> bool:
    token_l = token_addr.lower()
    return token_l in (addr.lower() for addr in STABLES.get(chain, []))


def is_native(chain: str, token_addr: str) -> bool:
    token_l = token_addr.lower()
    return token_l in (addr.lower() for addr in NATIVE_NAMES.get(chain, []))


def get_stable(chain: str) -> str:
    try:
        return SWAP_STABLES[chain]
    except KeyError as e:
        raise ValueError(f"No stable token configured for chain {chain}") from e
