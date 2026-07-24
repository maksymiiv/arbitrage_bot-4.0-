"""
On-chain ERC20 + pool metadata reader with a JSON cache on disk.
"""

import json
import threading
import time
from pathlib import Path

from web3 import Web3

from engine.atomic_io import atomic_write_json
from engine.logger import get_logger


log = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "token_cache.json"

_cache_lock = threading.Lock()
_token_cache: dict | None = None


# Public RPC endpoints (publicnode, mainnet.base.org) routinely throttle
# bursts with HTTP 429. Retry briefly before giving up so a transient
# rate-limit doesn't cause us to skip a perfectly good pool.
_RPC_RETRIES = 4
_RPC_BACKOFF_INITIAL_SEC = 1.0
_RPC_BACKOFF_MAX_SEC = 8.0


def _rpc_call_with_retry(fn):
    """
    Run a synchronous web3 `.call()` with exponential backoff on
    transient errors (429, timeouts, connection resets). Re-raises
    immediately on errors that look like contract-level issues
    (missing function, revert) — those won't recover from a sleep.
    """
    backoff = _RPC_BACKOFF_INITIAL_SEC
    last_exc = None
    for _ in range(_RPC_RETRIES):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            transient = (
                "429" in msg
                or "too many" in msg
                or "timeout" in msg
                or "connection" in msg
                or "temporarily" in msg
                or "rate limit" in msg
            )
            if not transient:
                raise
            last_exc = e
            time.sleep(backoff)
            backoff = min(backoff * 2, _RPC_BACKOFF_MAX_SEC)
    if last_exc:
        raise last_exc


ERC20_ABI = [
    {"name": "symbol", "outputs": [{"type": "string"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "name", "outputs": [{"type": "string"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
]

POOL_ABI_MIN = [
    {"name": "token0", "outputs": [{"type": "address"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "token1", "outputs": [{"type": "address"}], "inputs": [], "stateMutability": "view", "type": "function"},
]


def _load_cache_once() -> None:
    global _token_cache
    if _token_cache is not None:
        return

    with _cache_lock:
        if _token_cache is not None:
            return
        try:
            _token_cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            _token_cache = {}


def _save_cache() -> None:
    atomic_write_json(CACHE_FILE, _token_cache)


def get_token_info(w3: Web3, token_address: str) -> dict:
    _load_cache_once()
    token_address = Web3.to_checksum_address(token_address)

    cached = _token_cache.get(token_address)
    # Auto-heal: an earlier `symbol()` failure caused us to cache
    # `symbol="UNKNOWN"` AND a guessed `decimals=18`. A wrong `decimals`
    # multiplies V2/V3 prices by 10^(diff) — e.g. AUSD's real 6 decimals
    # vs guessed 18 → 10^12 spread. Force a re-fetch when we see UNKNOWN.
    if cached and cached.get("symbol") and cached.get("symbol") != "UNKNOWN":
        return cached

    token = w3.eth.contract(address=token_address, abi=ERC20_ABI)

    # decimals() is the critical field for price math — refuse to cache
    # a guess if the call fails. The caller (chain_monitor._init_metadata)
    # logs a warning and skips this pool; we'll retry on next session.
    try:
        decimals = int(_rpc_call_with_retry(lambda: token.functions.decimals().call()))
    except Exception as e:
        raise RuntimeError(f"decimals() failed for {token_address}: {e}")

    try:
        symbol = _rpc_call_with_retry(lambda: token.functions.symbol().call())
    except Exception:
        symbol = "UNKNOWN"
    try:
        name = _rpc_call_with_retry(lambda: token.functions.name().call())
    except Exception:
        name = symbol

    info = {"symbol": symbol, "name": name, "decimals": decimals}

    with _cache_lock:
        _token_cache[token_address] = info
        _save_cache()

    return info


def get_pool_metadata(w3: Web3, pool_address: str) -> dict:
    _load_cache_once()
    pool_address = Web3.to_checksum_address(pool_address)

    cached = _token_cache.get(pool_address)
    if cached and "token0" in cached:
        # Same auto-heal: a stored `symbol0/1="UNKNOWN"` means the pool
        # was indexed with 18-decimal fallbacks for one of its tokens,
        # so the cached `decimals0/1` and downstream prices are wrong.
        if (
            cached.get("symbol0") != "UNKNOWN"
            and cached.get("symbol1") != "UNKNOWN"
        ):
            return cached

    pool = w3.eth.contract(address=pool_address, abi=POOL_ABI_MIN)

    try:
        token0 = _rpc_call_with_retry(lambda: pool.functions.token0().call()).lower()
        token1 = _rpc_call_with_retry(lambda: pool.functions.token1().call()).lower()
    except Exception as e:
        raise RuntimeError(f"token0/token1 not found for {pool_address}: {e}")

    info0 = get_token_info(w3, token0)
    info1 = get_token_info(w3, token1)

    entry = {
        "token0": token0,
        "token1": token1,
        "decimals0": info0["decimals"],
        "decimals1": info1["decimals"],
        "symbol0": info0["symbol"],
        "symbol1": info1["symbol"],
    }
    # Preserve fields added by callers later (chain, dex, version,
    # token0_is_native, last_swap, ...) so re-fetching doesn't drop them.
    if cached:
        for k, v in cached.items():
            if k not in entry:
                entry[k] = v

    with _cache_lock:
        _token_cache[pool_address] = entry
        _save_cache()

    return entry


# --------------------------------------------------------------------------
# Uniswap V4
# --------------------------------------------------------------------------

# Native currency in V4 is the zero address (a pool can hold native ETH
# directly, not WETH). decimals()/symbol() cannot be called on it.
_NATIVE_ZERO = "0x0000000000000000000000000000000000000000"
_NATIVE_INFO = {"symbol": "ETH", "name": "Ether", "decimals": 18}


def _currency_info(w3: Web3, addr: str) -> dict:
    """
    Token info for a V4 currency. The zero address (native ETH) is
    resolved to a static 18-decimal stub; everything else is a normal
    ERC20 and goes through `get_token_info` (cache-backed).
    """
    addr_l = (addr or "").lower()
    if not addr_l or addr_l == _NATIVE_ZERO:
        return dict(_NATIVE_INFO)
    return get_token_info(w3, Web3.to_checksum_address(addr_l))


def get_v4_pool_metadata(
    w3: Web3,
    chain: str,
    pool_id: str,
    currency0: str,
    currency1: str,
    dex: str = "uniswap",
) -> dict:
    """
    Build + cache metadata for a Uniswap V4 pool.

    V4 has no per-pool contract — the pool lives inside the singleton
    PoolManager and is keyed by `pool_id` (bytes32). The currency
    addresses therefore can't be read on-chain; they come from the
    discovery source (DexScreener / GeckoTerminal) and are passed in.

    We only touch the chain to read `decimals()` on each non-native
    currency (cheap, usually already cached for stables / natives).

    The resulting entry has the SAME shape as a V2/V3 pool entry, so
    `compute_price` and the asset-symbol resolver work unchanged. It's
    stored under the `pool_id` key (not an address).
    """
    from ..core.stable_native import is_native, is_stable

    _load_cache_once()
    pid = (pool_id or "").lower()
    c0 = (currency0 or "").lower()
    c1 = (currency1 or "").lower()

    cached = _token_cache.get(pid)
    if (
        cached and "token0" in cached
        and cached.get("symbol0") != "UNKNOWN"
        and cached.get("symbol1") != "UNKNOWN"
    ):
        return cached

    info0 = _currency_info(w3, c0)
    info1 = _currency_info(w3, c1)

    entry = {
        "token0": c0,
        "token1": c1,
        "decimals0": info0["decimals"],
        "decimals1": info1["decimals"],
        "symbol0": info0["symbol"],
        "symbol1": info1["symbol"],
        "dex": dex,
        "version": "v4",
        "chain": chain,
        "token0_is_native": is_native(chain, c0),
        "token1_is_native": is_native(chain, c1),
        "token0_is_stable": is_stable(chain, c0),
        "token1_is_stable": is_stable(chain, c1),
        "last_swap": {"block": None, "msg": None},
    }
    entry["is_native_stable_pool"] = (
        (entry["token0_is_native"] and entry["token1_is_stable"])
        or (entry["token1_is_native"] and entry["token0_is_stable"])
    )

    with _cache_lock:
        _token_cache[pid] = entry
        _save_cache()

    return entry
