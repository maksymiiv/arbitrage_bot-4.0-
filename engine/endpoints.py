"""
Per-chain endpoint pools with round-robin failover.

Each chain's `ws_list` / `rpc_list` (from config, comma-separated env)
holds one or more provider URLs. Connection code reads the CURRENT
endpoint via `current_ws`/`current_rpc`; when an endpoint keeps failing
(a free provider rate-limiting our IP, a node restart, a hard ban) the
caller calls `rotate_ws`/`rotate_rpc` to advance to the next one. A
single dead endpoint therefore can't darken a chain — we move on.

With one endpoint configured, rotate is a no-op (nothing to move to).
"""

from engine.config import CHAINS
from engine.logger import get_logger


log = get_logger(__name__)

_ws_cursor: dict[str, int] = {}
_rpc_cursor: dict[str, int] = {}


def _current(chain: str, key: str, cursor: dict[str, int]) -> str | None:
    lst = (CHAINS.get(chain) or {}).get(key) or []
    if not lst:
        return None
    return lst[cursor.get(chain, 0) % len(lst)]


def _rotate(chain: str, key: str, cursor: dict[str, int], kind: str) -> str | None:
    lst = (CHAINS.get(chain) or {}).get(key) or []
    if len(lst) > 1:
        cursor[chain] = (cursor.get(chain, 0) + 1) % len(lst)
        nxt = lst[cursor[chain]]
        log.warning("[%s] rotating %s endpoint -> %s", chain, kind, nxt)
    return _current(chain, key, cursor)


def current_ws(chain: str) -> str | None:
    return _current(chain, "ws_list", _ws_cursor)


def rotate_ws(chain: str) -> str | None:
    return _rotate(chain, "ws_list", _ws_cursor, "WS")


def current_rpc(chain: str) -> str | None:
    return _current(chain, "rpc_list", _rpc_cursor)


def rotate_rpc(chain: str) -> str | None:
    return _rotate(chain, "rpc_list", _rpc_cursor, "RPC")
