import asyncio
import json

import websockets

from engine import fastjson
from engine.logger import get_logger

from ..cache_manager import get_cex_symbols
from ..core.orderbooks import apply_delta, apply_snapshot, init_book


log = get_logger(__name__)

WSS_URL = "wss://stream.bybit.com/v5/public/spot"
DEPTH = 50
PING_INTERVAL = 15
RECONNECT_DELAY = 3


class BybitOrderBookManager:
    def __init__(self, symbols_slice=None):
        self.symbols_slice = symbols_slice

        self.ws = None
        self.ready = asyncio.Event()

        self.subscribed: set[str] = set()
        self.desired_symbols: set[str] = set()

        self.keys: list[str] = []
        self.topics: list[str] = []

        self.ping_task: asyncio.Task | None = None
        self.send_lock = asyncio.Lock()

    # ----------------------------------------------------------------------
    # Build initial state
    # ----------------------------------------------------------------------

    def build_topics(self) -> None:
        self.keys.clear()
        self.topics.clear()
        self.subscribed.clear()
        self.desired_symbols.clear()

        symbols = get_cex_symbols("bybit")
        if self.symbols_slice:
            symbols = symbols[self.symbols_slice]

        for sym in symbols:
            sym = str(sym).upper().strip()
            if not sym:
                continue

            self.desired_symbols.add(sym)

            pair = f"{sym}USDT"
            key = f"BYBIT:{pair}"
            topic = f"orderbook.{DEPTH}.{pair}"

            self.keys.append(key)
            self.topics.append(topic)
            init_book(key)

    # ----------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------

    async def _send(self, payload: dict) -> None:
        async with self.send_lock:
            if self.ws is None:
                raise RuntimeError("Bybit WS is not connected")
            await self.ws.send(json.dumps(payload))

    async def _subscribe_topic(self, topic: str) -> None:
        if topic in self.subscribed:
            return
        await self._send({"op": "subscribe", "args": [topic]})
        self.subscribed.add(topic)

    async def _subscribe_symbol_now(self, symbol: str) -> None:
        symbol = str(symbol).upper().strip()
        if not symbol:
            return
        pair = f"{symbol}USDT"
        topic = f"orderbook.{DEPTH}.{pair}"
        key = f"BYBIT:{pair}"
        init_book(key)
        await self._subscribe_topic(topic)

    async def _cancel_ping_task(self) -> None:
        if self.ping_task is None:
            return
        self.ping_task.cancel()
        try:
            await self.ping_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("ping cancel error: %s", e)
        finally:
            self.ping_task = None

    async def _ping_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(PING_INTERVAL)
                await self._send({"op": "ping"})
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("ping error: %s", e)
                return

    # ----------------------------------------------------------------------
    # Main loop
    # ----------------------------------------------------------------------

    async def connect(self) -> None:
        self.build_topics()

        while True:
            try:
                async with websockets.connect(WSS_URL, ping_interval=None) as ws:
                    self.ws = ws
                    self.subscribed.clear()
                    log.info("Bybit connected, topics=%d", len(self.topics))

                    await self._cancel_ping_task()
                    self.ping_task = asyncio.create_task(self._ping_loop())

                    for topic in self.topics:
                        await self._subscribe_topic(topic)
                        await asyncio.sleep(0.03)

                    self.ready.set()

                    async for raw in ws:
                        msg = fastjson.loads(raw)

                        if "success" in msg or "ret_msg" in msg or msg.get("op"):
                            if msg.get("success") is False:
                                log.warning("Bybit WS error: %s", msg)
                            continue

                        msg_type = msg.get("type")
                        if msg_type not in ("snapshot", "delta"):
                            continue

                        topic = msg.get("topic")
                        if not topic:
                            continue

                        pair = topic.split(".")[-1]
                        key = f"BYBIT:{pair}"

                        data = msg.get("data", {})
                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        u = data.get("u")

                        if msg_type == "snapshot":
                            apply_snapshot(key, bids, asks, u)
                        else:
                            apply_delta(key, bids, asks, u)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Bybit reconnect: %s", e)
                self.ready.clear()
                self.ws = None
                await self._cancel_ping_task()
                await asyncio.sleep(RECONNECT_DELAY)

    # ----------------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------------

    async def subscribe_symbol(self, symbol: str) -> None:
        symbol = str(symbol).upper().strip()
        if not symbol:
            return

        is_new = symbol not in self.desired_symbols
        self.desired_symbols.add(symbol)

        pair = f"{symbol}USDT"
        topic = f"orderbook.{DEPTH}.{pair}"

        if topic not in self.topics:
            self.topics.append(topic)

        if is_new:
            log.debug("Bybit new desired symbol: %s", symbol)

        await self.ready.wait()

        if topic not in self.subscribed:
            await self._subscribe_symbol_now(symbol)
            log.info("Bybit live subscribed: %s", pair)
