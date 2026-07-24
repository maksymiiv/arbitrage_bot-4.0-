"""
Gate.io metadata client.

`GateMetadataCache.fetch()` pulls the entire currencies list once; callers
then resolve metadata for any symbol against the cached payload.
"""

import aiohttp

from engine.logger import get_logger

from ..utils.chains_filter import ALLOWED_CHAINS, normalize_chain


log = get_logger(__name__)

GATE_SPOT_PAIRS = "https://api.gateio.ws/api/v4/spot/currency_pairs"
GATE_CURRENCIES = "https://api.gateio.ws/api/v4/spot/currencies"


async def get_gate_spot_coins() -> list[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(GATE_SPOT_PAIRS, timeout=20) as r:
            data = await r.json()

    tokens = set()
    for p in data:
        base = p.get("base")
        if base:
            tokens.add(base.upper())
    return list(tokens)


class GateMetadataCache:
    """Snapshot of Gate's currencies list, indexed by uppercase ticker."""

    def __init__(self, by_currency: dict[str, dict]):
        self._by_currency = by_currency

    @classmethod
    async def fetch(cls) -> "GateMetadataCache":
        async with aiohttp.ClientSession() as session:
            async with session.get(GATE_CURRENCIES, timeout=30) as r:
                data = await r.json()

        by_currency = {
            (coin.get("currency") or "").upper(): coin
            for coin in data
            if coin.get("currency")
        }
        return cls(by_currency)

    def all_networks(self) -> dict[str, dict[str, list[bool]]]:
        """
        Network-only snapshot for the network-status refresh loop:
        SYMBOL_UPPER -> {chain: [deposit_open, withdraw_open]}.

        Same chain filter as `get_metadata`, but trims the contracts /
        token_id fields so the caller doesn't accidentally overwrite
        them on entries that already had richer data.
        """
        out: dict[str, dict[str, list[bool]]] = {}
        for sym, coin in self._by_currency.items():
            if not isinstance(coin, dict):
                continue
            networks: dict[str, list[bool]] = {}
            for chain in coin.get("chains", []) or []:
                raw_chain = chain.get("name")
                norm = normalize_chain(raw_chain)
                if not norm or norm not in ALLOWED_CHAINS:
                    continue
                networks[norm] = [
                    not chain.get("deposit_disabled", True),
                    not chain.get("withdraw_disabled", True),
                ]
            if networks:
                out[sym] = networks
        return out

    def get_metadata(self, symbol: str) -> dict | None:
        symbol = symbol.upper()

        # Gate leveraged-token tickers (3L/3S/5L/5S) — never DEX-tradable.
        if symbol.endswith(("3L", "3S", "5L", "5S")):
            return None

        coin = self._by_currency.get(symbol)
        if not coin:
            return None
        if coin.get("delisted") or coin.get("trade_disabled"):
            return None

        networks: dict[str, list[bool]] = {}
        contracts: dict[str, str] = {}

        for chain in coin.get("chains", []):
            raw_chain = chain.get("name")
            norm_chain = normalize_chain(raw_chain)
            if not norm_chain or norm_chain not in ALLOWED_CHAINS:
                continue

            networks[norm_chain] = [
                not chain.get("deposit_disabled", True),
                not chain.get("withdraw_disabled", True),
            ]
            if chain.get("addr"):
                contracts[norm_chain] = chain["addr"]

        if not networks:
            return None

        return {
            "symbol": symbol,
            "token_id": coin.get("name"),
            "contracts": contracts,
            "networks": networks,
        }
