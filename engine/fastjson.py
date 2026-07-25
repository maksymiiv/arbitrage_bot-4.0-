"""
Fast JSON decode with a graceful fallback to the stdlib.

orjson parses ~3-6x faster than the stdlib `json`, which matters on the
WebSocket hot path (thousands of orderbook/swap messages per second).
It's an OPTIONAL dependency: if it isn't installed we transparently fall
back to `json`, so the bot runs either way.

Only `loads` is provided — that's the hot path (inbound WS frames).
Outbound sends and file writes still use the stdlib `json` (they're
low-frequency and orjson.dumps returns bytes, which would complicate
those callers for no real gain).
"""

try:
    import orjson

    def loads(data):
        # orjson accepts both str and bytes/bytearray.
        return orjson.loads(data)

    BACKEND = "orjson"
except Exception:  # pragma: no cover - fallback path
    import json

    def loads(data):
        return json.loads(data)

    BACKEND = "json"
