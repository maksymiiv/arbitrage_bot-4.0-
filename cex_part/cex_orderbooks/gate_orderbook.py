"""
Gate.io v4 spot book_ticker (BBO) consumer.

Gate's `spot.book_ticker` channel streams best bid/ask per pair — no
snapshot/delta merge needed, so this is much simpler than the Bybit
depth book: take b/a straight off each update and push to price_store.

One WS connection carries every desired symbol; the subscription set
persists across reconnects (so symbols live-subscribed for fresh
listings survive a drop). Pairs are `<BASE>_USDT`.
"""

import asyncio
import json
import time

import websockets

from engine import fastjson, price_store
from engine.logger import get_logger

from ..cache_manager import get_cex_symbols


log = get_logger(__name__)

WSS_URL = "wss://api.gateio.ws/ws/v4/"
QUOTE = "USDT"
RECONNECT_DELAY = 3
RECV_TIMEOUT = 45
SUB_BATCH = 100  # symbols per subscribe message


class GateOrderBookManager:
    def __init__(self):
        self.ws = None
        self.ready = asyncio.Event()
        self.desired_symbols: set[str] = set()
        self.send_lock = asyncio.Lock()

    @staticmethod
    def _pair(symbol: str) -> str:
        return f"{symbol.upper()}_{QUOTE}"

    async def _send(self, payload: dict) -> None:
        async with self.send_lock:
            if self.ws is None:
                raise RuntimeError("Gate WS not connected")
            await self.ws.send(json.dumps(payload))

    async def _subscribe(self, symbols: list[str]) -> None:
        pairs = [self._pair(s) for s in symbols if s]
        for i in range(0, len(pairs), SUB_BATCH):
            batch = pairs[i:i + SUB_BATCH]
            await self._send({
                "time": int(time.time()),
                "channel": "spot.book_ticker",
                "event": "subscribe",
                "payload": batch,
            })
            await asyncio.sleep(0.05)

    def _handle(self, msg: dict) -> None:
        if msg.get("channel") != "spot.book_ticker":
            return

        event = msg.get("event")
        if event != "update":
            # subscribe/unsubscribe ack — surface only errors
            err = msg.get("error")
            if err:
                log.warning("Gate book_ticker sub error: %s", err)
            return

        r = msg.get("result") or {}
        s = r.get("s")
        b = r.get("b")
        a = r.get("a")
        if not s or b is None or a is None:
            return
        try:
            bid = float(b)
            ask = float(a)
        except (TypeError, ValueError):
            return
        if bid <= 0 or ask <= 0 or bid > ask:
            return

        base = s.split("_", 1)[0].upper()
        if not base:
            return
        price_store.update_cex(symbol=base, exchange="gate", bid=bid, ask=ask)

    async def connect(self) -> None:
        if not self.desired_symbols:
            self.desired_symbols = set(get_cex_symbols("gate"))

        while True:
            try:
                async with websockets.connect(
                    WSS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self.ws = ws
                    log.info("Gate connected, symbols=%d", len(self.desired_symbols))
                    await self._subscribe(sorted(self.desired_symbols))
                    self.ready.set()

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                        except asyncio.TimeoutError:
                            raise RuntimeError(f"Gate recv timeout {RECV_TIMEOUT}s")

                        try:
                            msg = fastjson.loads(raw)
                        except Exception:
                            continue
                        if isinstance(msg, dict):
                            self._handle(msg)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Gate reconnect: %s", e)
                self.ready.clear()
                self.ws = None
                await asyncio.sleep(RECONNECT_DELAY)

    async def subscribe_symbol(self, symbol: str) -> None:
        symbol = symbol.upper().strip()
        if not symbol or symbol in self.desired_symbols:
            return
        self.desired_symbols.add(symbol)
        # Live-subscribe if we're connected; otherwise it's already in
        # desired_symbols and will be picked up on the next (re)connect.
        if self.ws is not None:
            try:
                await self._subscribe([symbol])
                log.info("Gate live subscribed: %s", self._pair(symbol))
            except Exception as e:
                log.warning("Gate live sub fail %s: %s", symbol, e)
