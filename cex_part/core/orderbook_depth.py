"""
Orderbook-depth filter for confirmed spreads.

A spread computed from top-of-book can be a phantom: the CEX side may
only have a few dollars of size between its top-of-book and the DEX
price. Any real trade would walk the book straight past the point
where it's still profitable.

For every spread the scanner detects we sum the CEX-side cumulative
USD volume in the profitable price band and require it to clear
ORDERBOOK_MIN_DEPTH_USD.

Data sources per exchange
-------------------------
Bybit  — the full 50-level book is already streamed into
         `cex_part.core.orderbooks.ORDERBOOKS` by BybitOrderBookManager.
         Always live; we just read it (no polling).
Kraken — the WS feed is BBO-only (no depth). We REST-poll
         `/0/public/Depth` for tokens that currently have a spread.
Gate   — the WS feed (spot.book_ticker) is BBO-only too. We REST-poll
         `/spot/order_book` for tokens that currently have a spread.

Polling lifecycle
-----------------
The scanner calls `register_depth_watch()` on every tick a token shows
a spread. A brand-new Kraken token also triggers an immediate fetch so
the next scan tick already has data. The background `depth_poll_loop`
refreshes each watched token every few seconds and drops any token not
re-registered within `_WATCH_TTL_SEC` — i.e. its spread is gone, stop
polling.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from engine import price_store
from engine.logger import get_logger
from engine.rate_limiter import acquire as rl_acquire

from .orderbooks import ORDERBOOKS


log = get_logger(__name__)

KRAKEN_DEPTH_URL = "https://api.kraken.com/0/public/Depth"
KRAKEN_DEPTH_COUNT = 50

# Gate WS is BBO-only (spot.book_ticker), so — like Kraken — we REST-poll
# the order book for tokens that currently have a spread.
GATE_DEPTH_URL = "https://api.gateio.ws/api/v4/spot/order_book"
GATE_DEPTH_LIMIT = 50

# Drop a token from the poll set when no spread re-registered it within
# this window. Short on purpose: "spread gone → stop polling promptly".
# Slightly longer than a couple of scan ticks so a spread momentarily
# dipping below the threshold doesn't kill the watch.
_WATCH_TTL_SEC = 25.0

# (cex_lower, SYMBOL_UPPER) -> last time the scanner saw a spread.
_WATCH: dict[tuple[str, str], float] = {}

# Kraken REST-polled books:
#   SYMBOL_UPPER -> {"bids": {price: size}, "asks": {price: size},
#                    "ts": ms, "ready": bool}
_KRAKEN_BOOKS: dict[str, dict] = {}

# Symbols with an in-flight Kraken fetch — dedupes concurrent calls.
_KRAKEN_PENDING: set[str] = set()

# Gate REST-polled books (same shape as _KRAKEN_BOOKS) + in-flight set.
_GATE_BOOKS: dict[str, dict] = {}
_GATE_PENDING: set[str] = set()


# --------------------------------------------------------------------------
# CEX price refresh from a REST book
# --------------------------------------------------------------------------

def _push_cex_price(exchange: str, symbol: str, bids: dict, asks: dict) -> None:
    """Push best bid/ask from a freshly-fetched REST book into price_store.

    Kraken/Gate stream BBO over WS, but a WS feed can silently stop
    delivering a symbol while the socket stays alive (heartbeats) — the
    price then freezes for hours and fabricates phantom spreads. The depth
    poller already REST-fetches the book for every symbol that has a live
    spread (i.e. exactly the frozen ones), so refreshing the price from
    that same fetch self-corrects the freeze at no extra request.
    """
    if not bids or not asks:
        return
    best_bid = max(bids)
    best_ask = min(asks)
    if best_bid > 0 and best_ask > 0 and best_bid <= best_ask:
        price_store.update_cex(
            symbol=symbol, exchange=exchange, bid=best_bid, ask=best_ask
        )


# --------------------------------------------------------------------------
# watch registration
# --------------------------------------------------------------------------

def register_depth_watch(cex: str, symbol: str) -> None:
    """
    Mark (cex, symbol) as having an active spread right now.

    Bybit is a no-op — its book is already streamed live over WS, so
    there's nothing to poll. Kraken and Gate are BBO-only over WS, so a
    brand-new token kicks off an immediate REST fetch and the very next
    scan tick has depth.
    """
    cex_l = (cex or "").lower()
    sym_u = (symbol or "").upper()
    if not cex_l or not sym_u:
        return
    if cex_l == "bybit":
        return  # WS-live, nothing to poll

    k = (cex_l, sym_u)
    is_new = k not in _WATCH
    _WATCH[k] = time.time()

    if is_new and cex_l in ("kraken", "gate"):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if cex_l == "kraken":
            loop.create_task(_fetch_kraken_depth(sym_u))
        else:
            loop.create_task(_fetch_gate_depth(sym_u))


# --------------------------------------------------------------------------
# depth query
# --------------------------------------------------------------------------

def cumulative_usd_in_range(
    cex: str, symbol: str, side: str, low: float, high: float,
) -> Optional[float]:
    """
    Sum (price * size) of `side` ('bid' | 'ask') orderbook levels whose
    price falls within [low, high]. Returns the USD figure, or None when
    no orderbook is available yet for this (cex, symbol).

    None MUST be treated by the caller as "unknown — don't filter": a
    freshly-seen Kraken token has no REST snapshot until the poll loop
    fetches one.
    """
    cex_l = (cex or "").lower()
    sym_u = (symbol or "").upper()
    if not sym_u or low is None or high is None or low > high:
        return None

    book = None
    if cex_l == "bybit":
        book = ORDERBOOKS.get(f"BYBIT:{sym_u}USDT")
    elif cex_l == "kraken":
        book = _KRAKEN_BOOKS.get(sym_u)
    elif cex_l == "gate":
        book = _GATE_BOOKS.get(sym_u)

    if not book or not book.get("ready"):
        return None

    levels = book.get("bids" if side == "bid" else "asks") or {}
    total = 0.0
    for p, s in levels.items():
        if low <= p <= high:
            total += p * s
    return total


# --------------------------------------------------------------------------
# Kraken REST depth
# --------------------------------------------------------------------------

async def _fetch_kraken_depth(symbol: str) -> None:
    """Fetch /0/public/Depth for one Kraken symbol and store the book."""
    sym_u = symbol.upper()
    if sym_u in _KRAKEN_PENDING:
        return
    _KRAKEN_PENDING.add(sym_u)
    try:
        # Our Kraken symbols already ARE Kraken altnames, so
        # f"{symbol}USD" is a valid pair (XBTUSD, ETHUSD, ...).
        pair = f"{sym_u}USD"
        await rl_acquire("kraken_public")
        async with aiohttp.ClientSession() as session:
            async with session.get(
                KRAKEN_DEPTH_URL,
                params={"pair": pair, "count": KRAKEN_DEPTH_COUNT},
                timeout=10,
            ) as r:
                if r.status != 200:
                    log.debug("kraken depth %s -> HTTP %d", pair, r.status)
                    return
                data = await r.json()

        if data.get("error"):
            log.debug("kraken depth %s error: %s", pair, data["error"])
            return
        result = data.get("result") or {}
        if not result:
            return

        # `result` has one key — the resolved pair name. Take its value.
        book_data = next(iter(result.values()))
        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        for row in book_data.get("bids", []):
            try:
                bids[float(row[0])] = float(row[1])
            except (TypeError, ValueError, IndexError):
                pass
        for row in book_data.get("asks", []):
            try:
                asks[float(row[0])] = float(row[1])
            except (TypeError, ValueError, IndexError):
                pass

        _KRAKEN_BOOKS[sym_u] = {
            "bids": bids,
            "asks": asks,
            "ts": int(time.time() * 1000),
            "ready": bool(bids and asks),
        }
        # Un-freeze a stale WS price using this fresh REST book.
        _push_cex_price("kraken", sym_u, bids, asks)
    except Exception as e:
        log.debug("kraken depth fetch failed for %s: %s", symbol, e)
    finally:
        _KRAKEN_PENDING.discard(sym_u)


async def _fetch_gate_depth(symbol: str) -> None:
    """Fetch /spot/order_book for one Gate symbol and store the book."""
    sym_u = symbol.upper()
    if sym_u in _GATE_PENDING:
        return
    _GATE_PENDING.add(sym_u)
    try:
        pair = f"{sym_u}_USDT"
        await rl_acquire("gate_public")
        async with aiohttp.ClientSession() as session:
            async with session.get(
                GATE_DEPTH_URL,
                params={"currency_pair": pair, "limit": GATE_DEPTH_LIMIT},
                timeout=10,
            ) as r:
                if r.status != 200:
                    log.debug("gate depth %s -> HTTP %d", pair, r.status)
                    return
                data = await r.json()

        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        for row in data.get("bids", []):
            try:
                bids[float(row[0])] = float(row[1])
            except (TypeError, ValueError, IndexError):
                pass
        for row in data.get("asks", []):
            try:
                asks[float(row[0])] = float(row[1])
            except (TypeError, ValueError, IndexError):
                pass

        _GATE_BOOKS[sym_u] = {
            "bids": bids,
            "asks": asks,
            "ts": int(time.time() * 1000),
            "ready": bool(bids and asks),
        }
        # Un-freeze a stale WS price using this fresh REST book.
        _push_cex_price("gate", sym_u, bids, asks)
    except Exception as e:
        log.debug("gate depth fetch failed for %s: %s", symbol, e)
    finally:
        _GATE_PENDING.discard(sym_u)


async def depth_poll_loop(interval: float) -> None:
    """
    Background task: REST-refresh orderbook depth for every Kraken/Gate
    token that currently has an active spread (Bybit is WS-live and never
    enters the watch set). Tokens leave the poll set once their spread has
    been gone for `_WATCH_TTL_SEC`.
    """
    if interval <= 0:
        log.info("depth poll loop disabled (interval<=0)")
        return

    log.info(
        "depth poll loop started (interval=%.0fs, watch_ttl=%.0fs)",
        interval, _WATCH_TTL_SEC,
    )
    while True:
        await asyncio.sleep(interval)
        try:
            now = time.time()
            active: list[tuple[str, str]] = []
            for k, ts in list(_WATCH.items()):
                cex_l, sym_u = k
                if now - ts > _WATCH_TTL_SEC:
                    # spread gone — stop following this token
                    del _WATCH[k]
                    if cex_l == "kraken":
                        _KRAKEN_BOOKS.pop(sym_u, None)
                    elif cex_l == "gate":
                        _GATE_BOOKS.pop(sym_u, None)
                    continue
                if cex_l in ("kraken", "gate"):
                    active.append((cex_l, sym_u))

            for cex_l, sym in active:
                if cex_l == "kraken":
                    await _fetch_kraken_depth(sym)
                else:
                    await _fetch_gate_depth(sym)
        except Exception as e:
            log.error("depth poll loop error: %s", e)
