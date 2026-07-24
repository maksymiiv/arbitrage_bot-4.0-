"""
In-memory CEX orderbook storage. Bybit feeds snapshots+deltas into here;
each update derives best bid/ask and pushes them to price_store.
"""

import time

from engine import price_store


ORDERBOOKS: dict[str, dict] = {}

STABLE_QUOTES = ("USDT", "USDC", "USD")


def _empty_book() -> dict:
    return {"bids": {}, "asks": {}, "ts": 0, "ready": False}


def init_book(key: str) -> None:
    ORDERBOOKS[key] = _empty_book()


def apply_snapshot(key: str, bids: list, asks: list) -> None:
    if key not in ORDERBOOKS:
        init_book(key)

    book = ORDERBOOKS[key]
    book["bids"].clear()
    book["asks"].clear()

    for p, s in bids:
        book["bids"][float(p)] = float(s)
    for p, s in asks:
        book["asks"][float(p)] = float(s)

    book["ts"] = int(time.time() * 1000)
    book["ready"] = bool(book["bids"] and book["asks"])

    if book["ready"]:
        _update_price_store_from_book(key)


def apply_delta(key: str, bids: list, asks: list) -> None:
    if key not in ORDERBOOKS:
        return

    book = ORDERBOOKS[key]

    for p, s in bids:
        p = float(p)
        s = float(s)
        if s == 0:
            book["bids"].pop(p, None)
        else:
            book["bids"][p] = s

    for p, s in asks:
        p = float(p)
        s = float(s)
        if s == 0:
            book["asks"].pop(p, None)
        else:
            book["asks"][p] = s

    book["ts"] = int(time.time() * 1000)

    if book["bids"] and book["asks"]:
        book["ready"] = True
        _update_price_store_from_book(key)
    else:
        book["ready"] = False


def get_book(key: str) -> dict | None:
    return ORDERBOOKS.get(key)


def is_ready(key: str) -> bool:
    return ORDERBOOKS.get(key, {}).get("ready", False)


def _update_price_store_from_book(key: str) -> None:
    book = ORDERBOOKS.get(key)
    if not book or not book["ready"]:
        return
    if not book["bids"] or not book["asks"]:
        return

    best_bid = max(book["bids"].keys())
    best_ask = min(book["asks"].keys())
    if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask:
        return

    try:
        exchange, pair = key.split(":", 1)
        symbol = extract_base_symbol(pair)
        if not symbol:
            return
    except Exception:
        return

    price_store.update_cex(
        symbol=symbol,
        exchange=exchange.lower(),
        bid=best_bid,
        ask=best_ask,
    )


def extract_base_symbol(pair: str) -> str | None:
    pair = pair.upper()

    if "/" in pair:
        base, quote = pair.split("/", 1)
        return base if quote in STABLE_QUOTES else None

    for q in STABLE_QUOTES:
        if pair.endswith(q):
            return pair[:-len(q)]
    return None
