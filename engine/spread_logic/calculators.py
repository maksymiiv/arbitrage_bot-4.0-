import time
from typing import Optional

from .models import SpreadOpportunity


def _to_float(value) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def calc_dex_to_cex(
    symbol: str,
    cex_exchange: str,
    cex_data: dict,
    dex_chain: str,
    dex_data: dict,
) -> Optional[SpreadOpportunity]:
    """
    Buy on DEX at dex.price
    Sell on CEX at cex.bid

    VALID ONLY IF:
        dex_price < cex_bid
    """
    now = time.time()

    bid = _to_float(cex_data.get("bid"))
    ask = _to_float(cex_data.get("ask"))
    mid = _to_float(cex_data.get("mid"))
    cex_ts = _to_float(cex_data.get("ts"))

    dex_price = _to_float(dex_data.get("price"))
    dex_ts = _to_float(dex_data.get("ts"))

    if bid is None or ask is None or dex_price is None:
        return None

    if bid <= 0 or ask <= 0 or dex_price <= 0:
        return None

    if bid > ask:
        return None

    # opportunity exists only here
    if dex_price >= bid:
        return None

    spread_abs = bid - dex_price
    spread_pct = (spread_abs / dex_price) * 100.0

    return SpreadOpportunity(
        symbol=symbol,
        direction="dex->cex",
        cex_exchange=cex_exchange,
        cex_bid=bid,
        cex_ask=ask,
        cex_mid=mid if mid is not None else (bid + ask) / 2.0,
        cex_ts=cex_ts or 0.0,
        cex_age=(now - cex_ts) if cex_ts else -1.0,
        dex_chain=dex_chain,
        dex_price=dex_price,
        dex_pool=str(dex_data.get("pool") or ""),
        dex_protocol=str(dex_data.get("protocol") or ""),
        dex_ts=dex_ts or 0.0,
        dex_age=(now - dex_ts) if dex_ts else -1.0,
        spread_abs=spread_abs,
        spread_pct=spread_pct,
    )


def calc_cex_to_dex(
    symbol: str,
    cex_exchange: str,
    cex_data: dict,
    dex_chain: str,
    dex_data: dict,
) -> Optional[SpreadOpportunity]:
    """
    Buy on CEX at cex.ask
    Sell on DEX at dex.price

    VALID ONLY IF:
        dex_price > cex_ask
    """
    now = time.time()

    bid = _to_float(cex_data.get("bid"))
    ask = _to_float(cex_data.get("ask"))
    mid = _to_float(cex_data.get("mid"))
    cex_ts = _to_float(cex_data.get("ts"))

    dex_price = _to_float(dex_data.get("price"))
    dex_ts = _to_float(dex_data.get("ts"))

    if bid is None or ask is None or dex_price is None:
        return None

    if bid <= 0 or ask <= 0 or dex_price <= 0:
        return None

    if bid > ask:
        return None

    # opportunity exists only here
    if dex_price <= ask:
        return None

    spread_abs = dex_price - ask
    spread_pct = (spread_abs / ask) * 100.0

    return SpreadOpportunity(
        symbol=symbol,
        direction="cex->dex",
        cex_exchange=cex_exchange,
        cex_bid=bid,
        cex_ask=ask,
        cex_mid=mid if mid is not None else (bid + ask) / 2.0,
        cex_ts=cex_ts or 0.0,
        cex_age=(now - cex_ts) if cex_ts else -1.0,
        dex_chain=dex_chain,
        dex_price=dex_price,
        dex_pool=str(dex_data.get("pool") or ""),
        dex_protocol=str(dex_data.get("protocol") or ""),
        dex_ts=dex_ts or 0.0,
        dex_age=(now - dex_ts) if dex_ts else -1.0,
        spread_abs=spread_abs,
        spread_pct=spread_pct,
    )