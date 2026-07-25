"""
Kraken v2 ticker (BBO-only) consumer.

The supervisor watches `desired_symbols`, debounces fast bursts of new
listings, and rebuilds shards (one WS connection per chunk) when the
symbol set changes.
"""

import asyncio
import json

import websockets

from engine import fastjson, price_store
from engine.logger import get_logger


log = get_logger(__name__)

WSS_URL = "wss://ws.kraken.com/v2"

MAX_SYMBOLS_PER_CONN = 80
RECV_TIMEOUT = 45
RECONNECT_DELAY = 3
REBUILD_DEBOUNCE = 2.0


class KrakenOrderBookManager:
    def __init__(self):
        self.ready = asyncio.Event()
        self.desired_symbols: set[str] = set()

        self._runner_task: asyncio.Task | None = None
        self._started = False
        self._symbols_changed = asyncio.Event()

    # ----------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------

    @staticmethod
    def _pair(symbol: str) -> str:
        return f"{symbol.upper()}/USD"

    @staticmethod
    def _chunked(items: list[str], size: int) -> list[list[str]]:
        return [items[i:i + size] for i in range(0, len(items), size)]

    async def _subscribe_batch(self, ws, pairs: list[str], req_id: int) -> None:
        payload = {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": pairs,
                "snapshot": True,
                "event_trigger": "bbo",
            },
            "req_id": req_id,
        }
        await ws.send(json.dumps(payload))
        log.info("Kraken subscribe req_id=%d pairs=%d", req_id, len(pairs))

    def _handle_ticker_message(self, msg: dict) -> None:
        data = msg.get("data")
        if not isinstance(data, list):
            return

        for item in data:
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol")
            bid = item.get("bid")
            ask = item.get("ask")
            if not symbol or bid is None or ask is None:
                continue

            try:
                bid = float(bid)
                ask = float(ask)
            except Exception:
                continue

            if bid <= 0 or ask <= 0 or bid > ask:
                continue

            base = symbol.split("/", 1)[0].upper()
            price_store.update_cex(symbol=base, exchange="kraken", bid=bid, ask=ask)

    # ----------------------------------------------------------------------
    # Shard
    # ----------------------------------------------------------------------

    async def _run_shard(self, shard_id: int, pairs: list[str]) -> None:
        while True:
            try:
                async with websockets.connect(
                    WSS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    log.info("Kraken shard=%d connected pairs=%d", shard_id, len(pairs))
                    await self._subscribe_batch(ws, pairs, req_id=shard_id)

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                        except asyncio.TimeoutError:
                            raise RuntimeError(
                                f"Kraken shard={shard_id} recv timeout {RECV_TIMEOUT}s"
                            )

                        msg = fastjson.loads(raw)
                        if not isinstance(msg, dict):
                            continue

                        if msg.get("method") == "subscribe":
                            # A rejected subscription is silent otherwise —
                            # surface it (WARNING) so altname/wsname
                            # mismatches don't just quietly drop a symbol.
                            if msg.get("success") is False:
                                log.warning(
                                    "Kraken subscribe rejected shard=%d: %s",
                                    shard_id, msg,
                                )
                            else:
                                log.debug(
                                    "Kraken sub ack shard=%d success=%s",
                                    shard_id, msg.get("success"),
                                )
                            continue

                        channel = msg.get("channel")
                        if channel in {"heartbeat", "status"}:
                            continue
                        if channel == "ticker":
                            self._handle_ticker_message(msg)

            except asyncio.CancelledError:
                log.debug("Kraken shard=%d cancelled", shard_id)
                raise
            except Exception as e:
                log.warning("Kraken shard=%d reconnect: %s", shard_id, e)
                await asyncio.sleep(RECONNECT_DELAY)

    # ----------------------------------------------------------------------
    # Supervisor
    # ----------------------------------------------------------------------

    async def _supervisor(self) -> None:
        current_signature = None
        shard_tasks: list[asyncio.Task] = []

        while True:
            await self._symbols_changed.wait()
            await asyncio.sleep(REBUILD_DEBOUNCE)
            self._symbols_changed.clear()

            symbols = sorted(self.desired_symbols)
            signature = tuple(symbols)

            if not symbols or signature == current_signature:
                continue

            for task in shard_tasks:
                task.cancel()
            for task in shard_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
            shard_tasks.clear()

            pairs = [self._pair(s) for s in symbols]
            chunks = self._chunked(pairs, MAX_SYMBOLS_PER_CONN)

            log.info(
                "Kraken rebuilding shards: symbols=%d shards=%d chunk_size=%d",
                len(symbols), len(chunks), MAX_SYMBOLS_PER_CONN,
            )

            for i, chunk in enumerate(chunks, start=1):
                shard_tasks.append(asyncio.create_task(self._run_shard(i, chunk)))

            current_signature = signature

    # ----------------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------------

    async def connect(self) -> None:
        if not self._started:
            self._started = True
            self._runner_task = asyncio.create_task(self._supervisor())
            log.info("Kraken supervisor started")

        self.ready.set()

        # keep the coroutine alive — orchestrator awaits self.ready
        while True:
            await asyncio.sleep(3600)

    async def subscribe_symbol(self, symbol: str) -> None:
        symbol = symbol.upper().strip()
        if not symbol:
            return
        if symbol not in self.desired_symbols:
            self.desired_symbols.add(symbol)
            self._symbols_changed.set()
