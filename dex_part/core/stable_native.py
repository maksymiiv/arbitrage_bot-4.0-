from ..config.tokens import NATIVE_NAMES, STABLES


def is_stable(chain: str, token_addr: str) -> bool:
    token_l = token_addr.lower()
    return token_l in (addr.lower() for addr in STABLES.get(chain, []))


def is_native(chain: str, token_addr: str) -> bool:
    token_l = token_addr.lower()
    return token_l in (addr.lower() for addr in NATIVE_NAMES.get(chain, []))
