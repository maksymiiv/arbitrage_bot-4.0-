"""
In-memory store for live CEX/DEX prices.

KEY: canonical cache key from cex_part.cache_manager (one entry per
distinct TOKEN — not per ticker). Two tokens that share a ticker live
under different keys (e.g. "HOLO" vs "HOLO#eth.4c4d4144"). The
`display_symbol` field carries the human-readable ticker for output.

Structure:
    PRICE_STORE[key] = {
        "display_symbol": str,                    # e.g. "HOLO"
        "cex": {exchange: {bid, ask, mid, ts}},
        "dex": {chain: {price, pool, protocol, ts}},
        "source": "cex" | "dex",
    }
"""

import copy
import time
from typing import Any, Dict, Optional


class PriceStore:
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] = {}

    # ----------------------------------------------------------------------
    # internal
    # ----------------------------------------------------------------------

    @staticmethod
    def _norm_symbol(symbol: str) -> str:
        return str(symbol).upper().strip()

    @staticmethod
    def _norm_exchange(exchange: str) -> str:
        return str(exchange).lower().strip()

    @staticmethod
    def _norm_chain(chain: str) -> str:
        return str(chain).lower().strip()

    @staticmethod
    def _norm_pool(pool: str) -> str:
        return str(pool).lower().strip()

    def _ensure_entry(self, key: str, display_symbol: str, source: str) -> None:
        if key not in self._data:
            self._data[key] = {
                "display_symbol": display_symbol,
                "cex": {},
                "dex": {},
                "source": source,
            }
            return

        entry = self._data[key]
        if not entry.get("display_symbol"):
            entry["display_symbol"] = display_symbol
        if source == "cex":
            entry["source"] = "cex"

    # cache_manager helpers — lazy-imported to break the cex<->engine import cycle
    def _resolve_key_for_cex_symbol(self, cex: str, symbol: str) -> Optional[str]:
        from cex_part import cache_manager
        try:
            return cache_manager.resolve_key_for_cex_symbol(cex, symbol)
        except Exception:
            return None

    def _resolve_key_by_pool(self, chain: str, pool: str) -> Optional[str]:
        from cex_part import cache_manager
        try:
            return cache_manager.resolve_key_by_pool(chain, pool)
        except Exception:
            return None

    def _display_for(self, key: str) -> str:
        from cex_part import cache_manager
        try:
            return cache_manager.get_display_symbol(key) or key
        except Exception:
            return key

    # ----------------------------------------------------------------------
    # CEX update
    # ----------------------------------------------------------------------

    def update_cex(self, symbol: str, exchange: str, bid: float, ask: float) -> None:
        try:
            symbol = self._norm_symbol(symbol)
            exchange = self._norm_exchange(exchange)
            bid = float(bid)
            ask = float(ask)
        except Exception:
            return

        if not symbol or not exchange:
            return
        if bid <= 0 or ask <= 0 or bid > ask:
            return

        # disambiguate via cache_manager — same ticker on two CEXes may map
        # to two different tokens. If we can't resolve a single token,
        # fall back to using the ticker itself as the key (safe for the
        # common case where the ticker is unique).
        key = self._resolve_key_for_cex_symbol(exchange, symbol) or symbol
        display = self._display_for(key) or symbol

        self._ensure_entry(key, display, source="cex")
        self._data[key]["cex"][exchange] = {
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "ts": time.time(),
        }

    # ----------------------------------------------------------------------
    # DEX update
    # ----------------------------------------------------------------------

    def update_dex(
        self,
        symbol: str,        # symbol from DEX side — NOT trusted for routing
        chain: str,
        price: float,
        pool: str,
        protocol: str,
    ) -> None:
        try:
            chain = self._norm_chain(chain)
            pool = self._norm_pool(pool)
            protocol = str(protocol).lower().strip()
            price = float(price)
        except Exception:
            return

        if price <= 0 or not chain or not pool or not protocol:
            return

        # canonical token comes from the pool->token mapping; the symbol
        # parameter is ignored for routing.
        key = self._resolve_key_by_pool(chain, pool)
        if not key:
            return

        display = self._display_for(key) or self._norm_symbol(symbol) or key
        self._ensure_entry(key, display, source="dex")
        self._data[key]["dex"][chain] = {
            "price": price,
            "pool": pool,
            "protocol": protocol,
            "ts": time.time(),
        }

    # ----------------------------------------------------------------------
    # read helpers — accept EITHER a token key OR a display ticker
    # ----------------------------------------------------------------------

    def _entry_by_key_or_symbol(self, key_or_symbol: str) -> Dict[str, Any]:
        if not key_or_symbol:
            return {}
        if key_or_symbol in self._data:
            return self._data[key_or_symbol]
        sym = self._norm_symbol(key_or_symbol)
        for entry in self._data.values():
            if (entry.get("display_symbol") or "").upper() == sym:
                return entry
        return {}

    def get_asset(self, key_or_symbol: str) -> Dict[str, Any]:
        return copy.deepcopy(self._entry_by_key_or_symbol(key_or_symbol))

    def get_cex_prices(self, key_or_symbol: str) -> Dict[str, Any]:
        return copy.deepcopy(self._entry_by_key_or_symbol(key_or_symbol).get("cex", {}))

    def get_dex_prices(self, key_or_symbol: str) -> Dict[str, Any]:
        return copy.deepcopy(self._entry_by_key_or_symbol(key_or_symbol).get("dex", {}))

    def snapshot(self) -> Dict[str, Any]:
        return copy.deepcopy(self._data)

    # ----------------------------------------------------------------------
    # filter helpers
    # ----------------------------------------------------------------------

    def cex_keys(self):
        return [k for k, d in self._data.items() if d.get("cex")]

    def dex_only_keys(self):
        return [k for k, d in self._data.items() if d.get("dex") and not d.get("cex")]

    def ready_keys(self):
        return [k for k, d in self._data.items() if d.get("cex") and d.get("dex")]

    # ----------------------------------------------------------------------
    # maintenance
    # ----------------------------------------------------------------------

    def cleanup(self, max_age: float = 5.0) -> None:
        """
        Remove price entries older than max_age seconds. Currently unused
        because illiquid tokens are expected to update infrequently — keep
        the implementation around for future use.
        """
        now = time.time()
        for key in list(self._data.keys()):
            data = self._data.get(key)
            if not data:
                continue

            for ex in list((data.get("cex") or {}).keys()):
                item = data["cex"].get(ex)
                if not item or item.get("ts") is None:
                    del data["cex"][ex]
                    continue
                if now - item["ts"] > max_age:
                    del data["cex"][ex]

            for chain in list((data.get("dex") or {}).keys()):
                item = data["dex"].get(chain)
                if not item or item.get("ts") is None:
                    del data["dex"][chain]
                    continue
                if now - item["ts"] > max_age:
                    del data["dex"][chain]

            if not data.get("cex") and not data.get("dex"):
                del self._data[key]
