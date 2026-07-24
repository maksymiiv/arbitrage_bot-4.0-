"""
Kraken metadata client.

Kraken's public API does NOT expose contracts / deposit networks, so
`get_metadata` returns minimal stubs. The single REST call is shared
across all symbols via `KrakenMetadataCache.fetch()`.
"""

import aiohttp

from engine.logger import get_logger


log = get_logger(__name__)

KRAKEN_ASSETS = "https://api.kraken.com/0/public/Assets"


async def _fetch_kraken_assets() -> dict[str, dict]:
    async with aiohttp.ClientSession() as session:
        async with session.get(KRAKEN_ASSETS, timeout=15) as r:
            data = await r.json()

    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")

    return data.get("result", {}) or {}


async def get_kraken_spot_coins() -> list[str]:
    assets = await _fetch_kraken_assets()
    symbols = {asset["altname"].upper() for asset in assets.values() if asset.get("altname")}
    return sorted(symbols)


class KrakenMetadataCache:
    def __init__(self, by_altname: dict[str, dict]):
        self._by_altname = by_altname

    @classmethod
    async def fetch(cls) -> "KrakenMetadataCache":
        assets = await _fetch_kraken_assets()
        by_altname = {
            asset["altname"].upper(): asset
            for asset in assets.values()
            if asset.get("altname")
        }
        return cls(by_altname)

    def get_metadata(self, symbol: str) -> dict | None:
        sym = symbol.upper()
        if sym not in self._by_altname:
            return None
        return {
            "symbol": sym,
            "token_id": None,
            "contracts": {},
            "networks": {},
        }
