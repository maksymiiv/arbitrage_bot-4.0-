"""
Bybit metadata client.

Reads API credentials from engine.config (sourced from .env) — never
embed them in source.
"""

import hashlib
import hmac
import time
from urllib.parse import urlencode

import aiohttp

from engine.config import BYBIT_API_KEY, BYBIT_API_SECRET
from engine.logger import get_logger

from ..utils.chains_filter import ALLOWED_CHAINS, normalize_chain


log = get_logger(__name__)

BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_ASSET = "https://api.bybit.com/v5/asset/coin/query-info"


def _sign(payload: str) -> str:
    return hmac.new(
        BYBIT_API_SECRET.encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()


async def get_bybit_spot_coins() -> list[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(BYBIT_INSTRUMENTS, params={"category": "spot"}) as r:
            data = await r.json()
    return list({i["baseCoin"] for i in data["result"]["list"]})


async def get_bybit_coin_metadata(symbol: str) -> dict | None:
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        log.warning("BYBIT_API_KEY/SECRET not set — skipping metadata")
        return None

    symbol = symbol.upper()
    ts = str(int(time.time() * 1000))
    recv = "5000"
    params = {"coin": symbol}
    q = urlencode(sorted(params.items()))
    sig = _sign(ts + BYBIT_API_KEY + recv + q)

    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": sig,
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(BYBIT_ASSET, params=params, headers=headers, timeout=20) as r:
            data = await r.json()

    if data.get("retCode") != 0:
        return None

    rows = data.get("result", {}).get("rows", [])
    if not rows:
        return None

    row = rows[0]
    networks: dict[str, list[bool]] = {}
    contracts: dict[str, str] = {}

    for c in row.get("chains", []):
        raw_chain = c.get("chain") or c.get("chainType")
        norm_chain = normalize_chain(raw_chain)
        if not norm_chain or norm_chain not in ALLOWED_CHAINS:
            continue

        networks[norm_chain] = [
            c.get("chainDeposit") == "1",
            c.get("chainWithdraw") == "1",
        ]
        addr = c.get("contractAddress")
        if addr:
            contracts[norm_chain] = addr

    if not networks:
        return None

    return {
        "symbol": symbol,
        "token_id": row.get("name"),
        "contracts": contracts,
        "networks": networks,
    }


async def fetch_bybit_all_currencies() -> dict[str, dict]:
    """
    Bulk variant: a single signed call returns deposit / withdraw flags
    for every Bybit coin on every chain. Used by the network-status
    refresh loop so we don't hit the per-coin endpoint N times.

    Returns: SYMBOL_UPPER -> {"contracts": {chain: addr}, "networks": {chain: [dep, wd]}}.
    Empty dict on auth/network failure.
    """
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        log.warning("BYBIT_API_KEY/SECRET not set — can't bulk-fetch network status")
        return {}

    ts = str(int(time.time() * 1000))
    recv = "5000"
    q = ""  # no params -> all coins
    sig = _sign(ts + BYBIT_API_KEY + recv + q)

    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": sig,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BYBIT_ASSET, headers=headers, timeout=30) as r:
                data = await r.json()
    except Exception as e:
        log.warning("bybit bulk fetch failed: %s", e)
        return {}

    if data.get("retCode") != 0:
        log.warning("bybit bulk fetch retCode=%s msg=%s", data.get("retCode"), data.get("retMsg"))
        return {}

    out: dict[str, dict] = {}
    for row in data.get("result", {}).get("rows", []):
        sym = (row.get("coin") or "").upper()
        if not sym:
            continue
        networks: dict[str, list[bool]] = {}
        contracts: dict[str, str] = {}
        for c in row.get("chains", []):
            raw_chain = c.get("chain") or c.get("chainType")
            norm_chain = normalize_chain(raw_chain)
            if not norm_chain or norm_chain not in ALLOWED_CHAINS:
                continue
            networks[norm_chain] = [
                c.get("chainDeposit") == "1",
                c.get("chainWithdraw") == "1",
            ]
            addr = c.get("contractAddress")
            if addr:
                contracts[norm_chain] = addr
        if networks:
            out[sym] = {"contracts": contracts, "networks": networks}
    return out
