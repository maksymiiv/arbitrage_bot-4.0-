"""
Persistent CEX-token metadata cache.

Each entry models ONE token (not one ticker). Two listings with the same
ticker but different chains/contracts produce TWO entries with distinct
canonical keys, e.g.:

    "HOLO"               <- Bybit's HOLO  (BSC + SOL contracts)
    "HOLO#eth.4c4d4144"  <- Gate's HOLO   (ETH contract)

A single ticker can therefore map to MULTIPLE keys, so `_INDEX["symbol"]`
holds a list[str].

Resolution priority for `merge_token`:

    1) `symbol_overrides` (manual disambiguation)
    2) any (chain, contract) match
    3) token_id match
    4) symbol match — but only if contracts are compatible (same address
       on at least one shared chain, or one side has no contracts)

If nothing matches, a brand-new entry is created with a stable
disambiguated key derived from the first contract.
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from cex_part.config.blacklist import filter_blacklisted_symbols, is_blacklisted
from cex_part.config.symbol_overrides import lookup as override_lookup
from engine.atomic_io import atomic_write_json_async
from engine.logger import get_logger


log = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "cex_cache"
TOKENS_FILE = CACHE_DIR / "cex_tokens.json"
TOKEN_LIST_FILE = CACHE_DIR / "token_list.json"


# --------------------------------------------------------------------------
# In-memory state
# --------------------------------------------------------------------------

_CACHE: Dict[str, Dict] = {}
_TOKEN_LIST: Dict[str, Dict] = {}

# Indexes (rebuilt on init_cache, updated on merge_token / add_pool)
_INDEX = {
    "symbol": {},     # SYMBOL.upper -> list[canonical_key]   (multi-valued)
    "token_id": {},   # TOKEN_ID.upper -> canonical_key
    "contract": {},   # f"{chain}:{addr.lower()}" -> canonical_key
    "pool": {},       # f"{chain.lower()}:{pool.lower()}" -> canonical_key
}

_PENDING_META = 0
_PENDING_LIST = 0
_LAST_FLUSH = 0.0
_FILE_LOCK = asyncio.Lock()

FLUSH_INTERVAL = 5
FLUSH_BATCH_SIZE = 25


# --------------------------------------------------------------------------
# init / flush
# --------------------------------------------------------------------------

def init_cache() -> None:
    global _CACHE, _TOKEN_LIST
    _CACHE = _load_json(TOKENS_FILE, default={})
    _TOKEN_LIST = _load_json(TOKEN_LIST_FILE, default={})

    for cex, v in list(_TOKEN_LIST.items()):
        if not isinstance(v, dict):
            _TOKEN_LIST[cex] = {"symbols": [], "ignored": [], "last_update": 0}
        else:
            v.setdefault("symbols", [])
            v.setdefault("ignored", [])
            v.setdefault("last_update", 0)
            _TOKEN_LIST[cex] = v

    _rebuild_indexes()
    log.info("cache initialized: tokens=%d cex_lists=%d", len(_CACHE), len(_TOKEN_LIST))


async def flush(force: bool = False) -> None:
    global _LAST_FLUSH, _PENDING_META, _PENDING_LIST

    async with _FILE_LOCK:
        now = time.time()
        pending = _PENDING_META + _PENDING_LIST

        if not force:
            if pending == 0:
                return
            if pending < FLUSH_BATCH_SIZE and (now - _LAST_FLUSH) < FLUSH_INTERVAL:
                return

        # Serialize under the lock (no await between build + dump, so the
        # dicts can't mutate mid-dump); the slow disk write is offloaded.
        await atomic_write_json_async(TOKENS_FILE, _CACHE)
        await atomic_write_json_async(TOKEN_LIST_FILE, _TOKEN_LIST)

        _PENDING_META = 0
        _PENDING_LIST = 0
        _LAST_FLUSH = now


def _load_json(path: Path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return default
            return json.loads(raw)
    except Exception as e:
        log.warning("failed to load %s: %s — falling back to default", path, e)
        return default


# --------------------------------------------------------------------------
# indexes
# --------------------------------------------------------------------------

def _rebuild_indexes() -> None:
    global _INDEX
    _INDEX = {"symbol": {}, "token_id": {}, "contract": {}, "pool": {}}

    for key, entry in _CACHE.items():
        if not isinstance(entry, dict):
            continue
        _index_entry(key, entry)


def _add_symbol_index(sym: str, key: str) -> None:
    s = sym.upper()
    keys = _INDEX["symbol"].setdefault(s, [])
    if key not in keys:
        keys.append(key)


def _index_entry(key: str, entry: Dict) -> None:
    sym = entry.get("symbol")
    if sym:
        _add_symbol_index(sym, key)

    for a in entry.get("aliases") or []:
        if a:
            _add_symbol_index(a, key)

    tid = entry.get("token_id")
    if tid:
        _INDEX["token_id"][tid.upper()] = key

    for chain, addr in (entry.get("contracts") or {}).items():
        if addr:
            _INDEX["contract"][f"{chain}:{addr.lower()}"] = key

    for chain, pools in (entry.get("pools") or {}).items():
        if not isinstance(pools, list):
            continue
        c = chain.lower()
        for p in pools:
            if p:
                _INDEX["pool"][f"{c}:{str(p).lower()}"] = key


def _keys_for_symbol(symbol: str) -> List[str]:
    keys = _INDEX["symbol"].get(symbol.upper(), [])
    if isinstance(keys, str):
        return [keys]
    return list(keys)


def _contracts_compatible(existing: Dict, new_contracts: Dict[str, str]) -> bool:
    """
    Two tokens with the same ticker are considered the SAME token iff:
    - either side has no contracts, or
    - they share at least one (chain, addr) pair (case-insensitive).

    Disjoint chains with both sides having contracts → treat as DIFFERENT
    (e.g. HOLO Bybit BSC/SOL vs HOLO Gate ETH).
    """
    existing_contracts = existing.get("contracts") or {}
    if not existing_contracts or not new_contracts:
        return True

    existing_norm = {c.upper(): a.lower() for c, a in existing_contracts.items() if a}
    new_norm = {c.upper(): a.lower() for c, a in new_contracts.items() if a}

    shared = set(existing_norm) & set(new_norm)
    if not shared:
        return False  # disjoint, treat as different

    return any(existing_norm[c] == new_norm[c] for c in shared)


def _make_disambig_key(symbol: str, contracts: Dict[str, str]) -> str:
    """
    Deterministic key when the bare symbol is already taken.

    Format: SYMBOL#CHAIN.PREFIX  — e.g. "HOLO#eth.4c4d4144".
    Falls back to a numeric suffix if no contract is available (rare).
    """
    if contracts:
        chain, addr = sorted(contracts.items())[0]
        prefix = addr.lower().lstrip("0x")[:8] or "0"
        return f"{symbol}#{chain.lower()}.{prefix}"

    # last resort: increment until unique
    n = 2
    while f"{symbol}#{n}" in _CACHE:
        n += 1
    return f"{symbol}#{n}"


# --------------------------------------------------------------------------
# Public lookups
# --------------------------------------------------------------------------

def resolve_key_by_pool(chain: str, pool: str) -> Optional[str]:
    """
    O(1) lookup used by price_store on every DEX swap log.
    Returns the canonical cache KEY (token identifier) or None.
    """
    if not chain or not pool:
        return None
    return _INDEX["pool"].get(f"{chain.lower()}:{pool.lower()}")


def resolve_key_for_cex_symbol(cex: str, symbol: str) -> Optional[str]:
    """
    Map (CEX, ticker) -> canonical token key.

    Order:
      1) explicit manual override (cex_part.config.symbol_overrides)
      2) the entry that has BOTH this ticker AND this CEX in networks_cex
      3) any entry that has this ticker (last-resort, only when there's
         exactly one such entry — otherwise it's ambiguous)

    Kraken extra: among the candidates from step 2, prefer those with
    `kraken_verified=True` and silently drop those with
    `kraken_verified=False`. This prevents a Kraken-listed price from
    landing on a different token that happens to share the ticker
    (the Litentry/Lit-Protocol class of bug).
    """
    if not cex or not symbol:
        return None
    cex_l = cex.lower()
    sym_u = symbol.upper()

    pinned = override_lookup(cex_l, sym_u)
    if pinned and pinned in _CACHE:
        return pinned

    keys = _keys_for_symbol(sym_u)
    if not keys:
        return None

    # Kraken-only safety net: never return an entry that CG has disproved
    # (`kraken_verified=False`). Applied to the candidate pool BEFORE the
    # networks_cex filter and BEFORE the unambiguous-fallback below — so
    # the rule covers all three resolution branches.
    if cex_l == "kraken":
        keys = [
            k for k in keys
            if _CACHE.get(k, {}).get("kraken_verified") is not False
        ]
        if not keys:
            return None

    matches_for_cex = [
        k for k in keys
        if cex_l in (_CACHE.get(k, {}).get("networks_cex") or {})
    ]

    if cex_l == "kraken" and matches_for_cex:
        # Among multiple candidates that have kraken in networks_cex,
        # prefer the verified ones (skip `None` if any `True` exists).
        verified_true = [
            k for k in matches_for_cex
            if _CACHE.get(k, {}).get("kraken_verified") is True
        ]
        if verified_true:
            matches_for_cex = verified_true

    if matches_for_cex:
        if len(matches_for_cex) > 1:
            log.warning(
                "ambiguous (cex=%s, symbol=%s) -> %d matches; using %s. "
                "Consider pinning via SYMBOL_OVERRIDES.",
                cex_l, sym_u, len(matches_for_cex), matches_for_cex[0],
            )
        return matches_for_cex[0]

    # CEX listed the symbol but never wrote metadata (Kraken case before
    # any merge ran). Use the only entry if unambiguous.
    if len(keys) == 1:
        return keys[0]

    log.warning(
        "no metadata for (cex=%s, symbol=%s); %d candidate token entries",
        cex_l, sym_u, len(keys),
    )
    return None


def get_display_symbol(key: str) -> str:
    """Return the human-readable ticker for a cache key, or the key itself."""
    entry = _CACHE.get(key)
    if isinstance(entry, dict):
        sym = entry.get("symbol")
        if sym:
            return sym
    return key


# --------------------------------------------------------------------------
# token list API (new listings)
# --------------------------------------------------------------------------

async def update_token_list(cex: str, symbols: List[str]) -> Tuple[List[str], List[str]]:
    global _PENDING_LIST

    cex = cex.lower()
    symbols_norm = sorted(filter_blacklisted_symbols(symbols))

    prev_symbols = set(_TOKEN_LIST.get(cex, {}).get("symbols", []))
    ignored = _TOKEN_LIST.get(cex, {}).get("ignored", [])

    new_set = set(symbols_norm)
    new_symbols = sorted(new_set - prev_symbols)
    removed_symbols = sorted(prev_symbols - new_set)

    _TOKEN_LIST[cex] = {
        "symbols": symbols_norm,
        "ignored": ignored,
        "last_update": int(time.time()),
    }

    _PENDING_LIST += 1
    await flush()
    return new_symbols, removed_symbols


async def mark_symbol_ignored(cex: str, symbol: str) -> None:
    global _PENDING_LIST

    cex = cex.lower()
    symbol = symbol.upper()

    entry = _TOKEN_LIST.setdefault(cex, {"symbols": [], "ignored": [], "last_update": 0})
    ignored = set(entry.get("ignored", []))
    if symbol not in ignored:
        ignored.add(symbol)
        entry["ignored"] = sorted(ignored)
        entry["last_update"] = int(time.time())
        _PENDING_LIST += 1
        await flush()


# --------------------------------------------------------------------------
# missing calculation
# --------------------------------------------------------------------------

def get_missing_symbols_for_cex(cex: str, symbols: List[str]) -> List[str]:
    """
    Return symbols that DON'T have metadata for this cex in any cache entry.
    """
    cex_l = cex.lower()
    ignored = set(_TOKEN_LIST.get(cex_l, {}).get("ignored", []))
    missing: List[str] = []

    for sym in symbols:
        sym_u = sym.upper()
        if is_blacklisted(sym_u) or sym_u in ignored:
            continue

        keys = _keys_for_symbol(sym_u)
        if not keys:
            missing.append(sym_u)
            continue

        if not any(cex_l in (_CACHE.get(k, {}).get("networks_cex") or {}) for k in keys):
            missing.append(sym_u)

    return missing


# --------------------------------------------------------------------------
# metadata merge API (UPSERT + aliases + indexes)
# --------------------------------------------------------------------------

def _resolve_for_merge(
    cex: str,
    symbol: str,
    token_id: Optional[str],
    contracts: Dict[str, str],
) -> Optional[str]:
    """
    Find an existing key compatible with the new metadata, or None to
    signal "create a fresh entry".
    """
    # 1) manual override
    pinned = override_lookup(cex, symbol)
    if pinned and pinned in _CACHE:
        return pinned

    # 2) any contract match
    for chain, addr in (contracts or {}).items():
        if not addr:
            continue
        k = f"{chain}:{addr.lower()}"
        if k in _INDEX["contract"]:
            return _INDEX["contract"][k]

    # 3) token_id match — sometimes the only signal between two CEXes that
    #    both expose a CMC/CoinGecko-ish name even when contracts differ
    if token_id:
        tid = token_id.upper()
        if tid in _INDEX["token_id"]:
            existing_key = _INDEX["token_id"][tid]
            existing = _CACHE.get(existing_key, {})
            if _contracts_compatible(existing, contracts):
                return existing_key

    # 4) symbol match, only if contracts agree
    for ek in _keys_for_symbol(symbol):
        existing = _CACHE.get(ek, {})
        if _contracts_compatible(existing, contracts):
            return ek

    return None


async def merge_token(cex: str, token_data: Dict) -> str:
    global _PENDING_META

    cex = cex.lower()

    symbol = (token_data.get("symbol") or "").upper()
    if not symbol:
        raise ValueError("token_data.symbol is required")

    token_id = token_data.get("token_id")
    contracts = token_data.get("contracts", {}) or {}
    networks = token_data.get("networks", {}) or {}

    key = _resolve_for_merge(cex, symbol, token_id, contracts)

    if not key:
        # need a brand-new entry; pick a key that's free
        if symbol not in _CACHE:
            key = symbol
        else:
            key = _make_disambig_key(symbol, contracts)
            log.info(
                "new token entry for %s (cex=%s) under key=%s — distinct from existing %s",
                symbol, cex, key, _keys_for_symbol(symbol),
            )

        _CACHE[key] = {
            "symbol": symbol,
            "token_id": token_id,
            "aliases": [],
            "contracts": {},
            "networks_cex": {},
            "last_update": int(time.time()),
        }

    entry = _CACHE[key]

    # alias when the canonical entry's symbol differs from the one we got
    canonical_sym = (entry.get("symbol") or "").upper()
    if symbol and symbol != canonical_sym:
        entry.setdefault("aliases", [])
        if symbol not in entry["aliases"]:
            entry["aliases"].append(symbol)

    if token_id and not entry.get("token_id"):
        entry["token_id"] = token_id

    entry.setdefault("contracts", {}).update(contracts)
    entry.setdefault("networks_cex", {})[cex] = networks
    entry["last_update"] = int(time.time())

    _index_entry(key, entry)

    _PENDING_META += 1
    await flush()

    return key


async def add_pool(symbol: str, chain: str, pool: str) -> bool:
    """
    Link a discovered DEX pool to a CEX-known token.

    When multiple cache entries share the same ticker (collision), pick
    the entry that has a contract on this chain — that's the only one
    where the pool can really belong.
    """
    global _PENDING_META

    sym_u = symbol.upper()
    chain_l = chain.lower()
    pool_l = pool.lower()

    async with _FILE_LOCK:
        candidates = _keys_for_symbol(sym_u)
        if not candidates:
            return False

        # prefer entry with a contract on this chain
        chosen = None
        for k in candidates:
            entry = _CACHE.get(k, {})
            contracts = entry.get("contracts") or {}
            if any(c.lower() == chain_l for c in contracts):
                chosen = k
                break
        if not chosen:
            # fall back to the only candidate; otherwise refuse to guess
            if len(candidates) == 1:
                chosen = candidates[0]
            else:
                log.warning(
                    "add_pool: ambiguous %s on %s — %d candidates without "
                    "a chain match; skipping pool=%s",
                    sym_u, chain_l, len(candidates), pool_l,
                )
                return False

        entry = _CACHE[chosen]
        pools = entry.setdefault("pools", {})
        chain_pools = pools.setdefault(chain_l, [])

        if pool_l in chain_pools:
            return False

        chain_pools.append(pool_l)
        entry["last_update"] = int(time.time())

        _index_entry(chosen, entry)
        _PENDING_META += 1

    await flush()
    log.info("pool linked: %s/%s %s %s", chosen, sym_u, chain_l, pool_l)
    return True


# --------------------------------------------------------------------------
# Read helpers
# --------------------------------------------------------------------------

def get_cache_size() -> int:
    return len(_CACHE)


def get_token_by_symbol(symbol: str) -> Optional[Dict]:
    """First entry indexed under `symbol`, or None."""
    keys = _keys_for_symbol(symbol)
    return _CACHE.get(keys[0]) if keys else None


def has_symbol(symbol: str) -> bool:
    return bool(_keys_for_symbol(symbol))


def get_cex_symbols(cex: str, active_only: bool = True) -> List[str]:
    """
    Symbols this CEX has metadata for in cache.

    `active_only=True` (default) intersects with the latest spot listing
    captured by `update_token_list` — that drops tokens the exchange has
    delisted since metadata was collected, so callers don't try to
    subscribe to ghost pairs (Bybit returns "Invalid symbol" for those).

    Pass `active_only=False` to ignore the listing filter (useful for
    backfill / one-off pool discovery).
    """
    cex_l = cex.lower()
    result = set()

    listed = None
    if active_only:
        listed_raw = _TOKEN_LIST.get(cex_l, {}).get("symbols") or []
        if listed_raw:
            listed = {s.upper() for s in listed_raw}

    for entry in _CACHE.values():
        if not isinstance(entry, dict):
            continue
        symbol = (entry.get("symbol") or "").upper()
        if not symbol or is_blacklisted(symbol):
            continue
        if cex_l not in (entry.get("networks_cex") or {}):
            continue
        if listed is not None and symbol not in listed:
            continue
        result.add(symbol)

    return sorted(result)


# --------------------------------------------------------------------------
# CoinGecko metadata backfill — fills `coingecko_id` and `kraken_verified`
# fields on existing entries. Run once at startup; later restarts skip
# entries that are already marked (Kraken delistings are rare enough that
# re-checking every boot would waste CG quota for no real benefit).
# --------------------------------------------------------------------------

def _resolve_entry_with_coingecko(key: str, entry: Dict, scanned_chains: set) -> Dict[str, int]:
    """
    Apply CG resolution to ONE entry. Mutates `entry` in place; does
    NOT persist or rebuild indexes. Returns a delta-stats dict that
    the caller can fold into a larger run.

    Resolution priority for `coingecko_id`:
      A) any (chain, contract) on the entry that CG knows
      B) symbol-fallback (only when the entry has `networks_cex.kraken`):
         CG candidates filtered by "Kraken-traded" + "has EVM contract".
         If exactly one survives, adopt id and merge its CG contracts
         into the entry.

    Then for `kraken_verified`: True if cg_id is on Kraken; False
    otherwise (in which case `networks_cex.kraken` is dropped).
    Already-decided entries are not touched.
    """
    from engine import coingecko

    delta = {
        "id_added_contract": 0,
        "id_added_symbol": 0,
        "contracts_merged": 0,
        "kraken_true": 0,
        "kraken_false": 0,
        "kraken_removed": 0,
    }

    cg_id = entry.get("coingecko_id")
    has_kraken = "kraken" in (entry.get("networks_cex") or {})
    prev_verified = entry.get("kraken_verified")

    # ---------- A) contract-based resolution ----------
    if not cg_id:
        for chain, addr in (entry.get("contracts") or {}).items():
            cand = coingecko.get_id_by_contract(chain, addr)
            if cand:
                entry["coingecko_id"] = cand
                cg_id = cand
                delta["id_added_contract"] += 1
                break

    # ---------- B) symbol-based fallback ----------
    if not cg_id and has_kraken and prev_verified is None:
        sym = (entry.get("symbol") or "").upper()
        cands = coingecko.find_ids_by_symbol(sym) if sym else []
        kraken_cands = []
        for cid in cands:
            if not coingecko.is_traded_on_kraken(cid):
                continue
            info = coingecko.get_info_by_id(cid) or {}
            plats = info.get("platforms") or {}
            if any(ch in scanned_chains for ch in plats):
                kraken_cands.append((cid, plats))

        if len(kraken_cands) == 1:
            cid, plats = kraken_cands[0]
            entry["coingecko_id"] = cid
            cg_id = cid
            delta["id_added_symbol"] += 1

            contracts = entry.setdefault("contracts", {})
            existing_upper = {c.upper() for c in contracts}
            added_any = False
            for chain_internal, addr in plats.items():
                if chain_internal not in scanned_chains:
                    continue
                if not isinstance(addr, str) or not addr.strip():
                    continue
                chain_key = chain_internal.upper()
                if chain_key in existing_upper:
                    continue
                contracts[chain_key] = addr.strip().lower()
                added_any = True
            if added_any:
                delta["contracts_merged"] += 1
                log.info(
                    "symbol-fallback rescue: %s -> cg_id=%s, added contracts",
                    key, cid,
                )

    # ---------- 2) kraken_verified ----------
    if has_kraken and prev_verified is None and cg_id:
        if coingecko.is_traded_on_kraken(cg_id):
            entry["kraken_verified"] = True
            delta["kraken_true"] += 1
        else:
            entry["kraken_verified"] = False
            entry["networks_cex"].pop("kraken", None)
            delta["kraken_false"] += 1
            delta["kraken_removed"] += 1
            log.info(
                "kraken not verified for %s (cg_id=%s) — removed networks_cex.kraken",
                key, cg_id,
            )

    return delta


async def apply_coingecko_metadata_for_entry(key: str) -> bool:
    """
    Single-entry version used by the pending-retry loop.

    Returns True if the entry was conclusively resolved
    (`kraken_verified` became True or False). False otherwise — caller
    keeps the entry on the retry queue.
    """
    from engine.coingecko import PLATFORM_TO_CHAIN

    entry = _CACHE.get(key)
    if not isinstance(entry, dict):
        return False

    delta = _resolve_entry_with_coingecko(key, entry, set(PLATFORM_TO_CHAIN.values()))

    if any(delta.values()):
        _rebuild_indexes()
        await flush(force=True)

    return entry.get("kraken_verified") in (True, False)


async def apply_coingecko_metadata() -> None:
    """
    Fill missing `coingecko_id` and `kraken_verified` on every entry.

    Resolution priority for `coingecko_id`:
      A) contract-based — any (chain, contract) on the entry that CG knows
      B) symbol-based fallback — only when (A) failed AND the entry has
         `networks_cex.kraken`. Looks up CG by ticker, filters by
         "actually traded on Kraken" (kraken_verified set), keeps only
         candidates that have a contract on a chain we currently scan
         (eth/bsc/base/sol). If exactly one survives, we adopt its CG id
         and merge its contracts into the entry — this rescues entries
         that Kraken ships without contract info (their main pain point).

    Already-marked entries are left untouched — per project policy,
    Kraken rarely delists, so a stale `True/False` is acceptable.

    Side effect: when `kraken_verified` resolves to False we drop
    `networks_cex.kraken` from the entry (kills cross-token mis-attributions
    like Kraken-LIT [Litentry] being merged into a Lit-Protocol entry).
    """
    from engine import coingecko
    from engine.coingecko import PLATFORM_TO_CHAIN

    if not coingecko._BY_CONTRACT and not coingecko._KRAKEN_VERIFIED_IDS:
        log.info("coingecko cache empty, skipping metadata backfill")
        return

    SCANNED_CHAINS = set(PLATFORM_TO_CHAIN.values())

    totals = {
        "id_resolved": 0,
        "id_added_contract": 0,
        "id_added_symbol": 0,
        "contracts_merged": 0,
        "kraken_true": 0,
        "kraken_false": 0,
        "kraken_removed": 0,
        "kraken_pending": 0,
    }
    changed = False

    for key, entry in _CACHE.items():
        if not isinstance(entry, dict):
            continue

        prev_verified = entry.get("kraken_verified")

        delta = _resolve_entry_with_coingecko(key, entry, SCANNED_CHAINS)
        for k, v in delta.items():
            if v:
                totals[k] += v
                changed = True

        if entry.get("coingecko_id"):
            totals["id_resolved"] += 1

        # Reflect the after-state in pending count: "kraken in networks_cex
        # AND verified is None" means we still couldn't decide.
        if (
            "kraken" in (entry.get("networks_cex") or {})
            and entry.get("kraken_verified") is None
        ):
            totals["kraken_pending"] += 1
        elif prev_verified is True and entry.get("kraken_verified") is True:
            # already counted as True in a previous run; bookkeeping only
            totals["kraken_true"] += 1
        elif prev_verified is False and entry.get("kraken_verified") is False:
            totals["kraken_false"] += 1

    if changed:
        _rebuild_indexes()
        await flush(force=True)

    log.info(
        "coingecko backfill: scanned=%d id_resolved=%d "
        "(via_contract=%d via_symbol=%d contracts_merged=%d) "
        "kraken_true=%d kraken_false=%d kraken_removed=%d kraken_pending=%d",
        len(_CACHE), totals["id_resolved"], totals["id_added_contract"],
        totals["id_added_symbol"], totals["contracts_merged"],
        totals["kraken_true"], totals["kraken_false"],
        totals["kraken_removed"], totals["kraken_pending"],
    )


def get_contract_address(key: str, chain: str) -> Optional[str]:
    """
    Return the on-chain contract address for `key` on `chain`, or None.
    Stored chain keys are uppercase ("ETH", "BSC", ...) but accept any
    case from callers.
    """
    if not key or not chain:
        return None
    entry = _CACHE.get(key)
    if not isinstance(entry, dict):
        return None
    contracts = entry.get("contracts") or {}
    addr = contracts.get(chain.upper()) or contracts.get(chain.lower())
    if isinstance(addr, str) and addr.strip():
        return addr.strip().lower()
    return None


def is_route_open(key: str, cex: str, chain: str, direction: str) -> Optional[bool]:
    """
    Tri-state route check from `networks_cex`:
        True   — exchange currently allows this direction on this chain
        False  — explicitly disabled
        None   — unknown (no data; caller MUST treat as "don't filter")

    `direction` is "deposit" or "withdraw". Chain is the internal short
    key ("eth", "bsc", "base", "sol") — accepted in any case, networks_cex
    stores them uppercase under each cex.
    """
    if not key or not cex or not chain:
        return None
    entry = _CACHE.get(key)
    if not isinstance(entry, dict):
        return None
    cex_block = (entry.get("networks_cex") or {}).get(cex.lower())
    if not isinstance(cex_block, dict):
        return None
    chain_status = cex_block.get(chain.upper()) or cex_block.get(chain.lower())
    if not isinstance(chain_status, list) or len(chain_status) < 2:
        return None
    idx = 0 if direction == "deposit" else 1
    return bool(chain_status[idx])


async def update_networks_for(cex: str, networks_by_symbol: dict[str, dict[str, list]]) -> int:
    """
    Refresh `networks_cex[<cex>]` flags on entries already in the cache.

    Used by the network-status background loop. Walks each (symbol -> chain
    flags) pair and updates entries whose ticker matches AND that already
    have a `networks_cex.<cex>` block — never CREATES a new cex attachment,
    just refreshes flags on known ones.

    Returns count of entries actually changed (so the caller can decide
    whether to log).
    """
    global _PENDING_META

    cex_l = cex.lower()
    changed = 0
    for sym, networks in networks_by_symbol.items():
        if not isinstance(networks, dict) or not networks:
            continue
        sym_u = (sym or "").upper()
        if not sym_u:
            continue
        for k in _keys_for_symbol(sym_u):
            entry = _CACHE.get(k)
            if not isinstance(entry, dict):
                continue
            nc = entry.get("networks_cex") or {}
            if cex_l not in nc:
                continue
            # Normalize both sides to upper-case chain keys so equality
            # check (and the eventual stored value) matches the existing
            # `cex_tokens.json` convention.
            new_block = {ch.upper(): list(flags) for ch, flags in networks.items()}
            old_block = nc.get(cex_l) or {}
            if old_block != new_block:
                nc[cex_l] = new_block
                changed += 1

    if changed:
        _PENDING_META += 1
        await flush(force=True)
    return changed


async def cleanup_kraken_pending() -> None:
    """
    Drop entries that are stuck in "kraken pending" (have
    `networks_cex.kraken` and `kraken_verified=None`) but are NOT
    actively being retried by the kraken_pending queue.

    These are tokens CG can't link to anything we can price on DEX —
    they'd otherwise sit in the cache forever, eating WS subscriptions
    and producing CEX prices that never match a DEX side.

    Two cases:
      - kraken-only entry → drop the whole record from `_CACHE`
      - multi-CEX entry (kraken + bybit/gate) → just strip the kraken
        block, mark `kraken_verified=False`, keep the rest of the entry

    Both cases also add the ticker to the kraken `ignored` list so
    `run_kraken` doesn't immediately re-add the entry on the next
    polling cycle (otherwise the cleanup would be undone in seconds).

    Entries currently registered for retry (fresh listings whose CG
    page might appear in the next few days) are skipped here — the
    retry loop owns their lifecycle.
    """
    from engine import coingecko

    actively_retrying = coingecko.pending_keys()

    dropped = 0
    cleaned = 0
    ignored_to_add: set[str] = set()

    for key in list(_CACHE.keys()):
        entry = _CACHE.get(key)
        if not isinstance(entry, dict):
            continue
        nc = entry.get("networks_cex") or {}
        if "kraken" not in nc:
            continue
        if key in actively_retrying:
            continue

        verified = entry.get("kraken_verified")
        sym = (entry.get("symbol") or "").upper()

        # `verified=False` means CG already disproved this entry's
        # kraken attribution. If `run_kraken` later re-added the kraken
        # block (because the symbol re-appeared in missing-list before
        # we marked it ignored), strip it here too.
        if verified is False:
            nc.pop("kraken", None)
            cleaned += 1
            log.info("cleanup_kraken_pending: stripped re-added kraken from %s (verified=False)", key)
            if sym:
                ignored_to_add.add(sym)
            continue

        # `verified=True` → keep as-is
        if verified is True:
            continue

        # `verified is None` — undecided, treat as pending
        if len(nc) == 1:
            del _CACHE[key]
            dropped += 1
            log.info("cleanup_kraken_pending: dropped kraken-only entry %s", key)
            if sym:
                ignored_to_add.add(sym)
        else:
            nc.pop("kraken", None)
            entry["kraken_verified"] = False
            cleaned += 1
            log.info("cleanup_kraken_pending: stripped kraken from multi-cex entry %s", key)
            if sym:
                ignored_to_add.add(sym)

    # Bulk-merge into the kraken `ignored` list so `run_kraken` won't
    # re-discover and re-add these symbols on its next polling tick.
    if ignored_to_add:
        token_list_entry = _TOKEN_LIST.setdefault(
            "kraken", {"symbols": [], "ignored": [], "last_update": 0}
        )
        existing = set(token_list_entry.get("ignored") or [])
        merged = existing | ignored_to_add
        if len(merged) > len(existing):
            token_list_entry["ignored"] = sorted(merged)
            token_list_entry["last_update"] = int(time.time())

    if dropped or cleaned or ignored_to_add:
        _rebuild_indexes()
        await flush(force=True)

    log.info(
        "cleanup_kraken_pending: dropped=%d cleaned=%d newly_ignored=%d",
        dropped, cleaned, len(ignored_to_add),
    )
