"""
RPC / WS connectivity probe — independent of the bot.

Reads BSC_*, ETH_*, BASE_* URLs from .env and tests each:
  - HTTP RPC: eth_blockNumber call
  - WebSocket: handshake + eth_blockNumber

Each result is one of:
  OK          — endpoint works
  CAPACITY    — Alchemy / provider says monthly limit hit
  RATE_LIMIT  — generic 429 (transient)
  AUTH_FAIL   — bad API key
  CONN_FAIL   — network / DNS / TLS issue

If everything is OK here but the bot still fails — the bug is in the bot,
not the RPC.

Run:  python test_rpc.py
"""

import asyncio
import json
import os
import sys

import aiohttp
import websockets
from dotenv import load_dotenv


load_dotenv()


def classify(status: int, body: str) -> tuple[str, str]:
    """Return (label, detail) from HTTP status + body."""
    b = (body or "").lower()
    if status == 200:
        return "OK", ""
    if status == 429:
        if "monthly" in b or "capacity" in b:
            return "CAPACITY", "monthly CU exhausted — pay/wait/switch provider"
        return "RATE_LIMIT", "429 — try again in a minute"
    if status in (401, 403):
        return "AUTH_FAIL", "bad / revoked API key"
    return "ERROR", f"HTTP {status}"


async def probe_http(name: str, url: str) -> None:
    if not url:
        print(f"  [HTTP] {name:6} - no URL set in .env")
        return
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=10) as r:
                txt = await r.text()
                label, detail = classify(r.status, txt)
                if label == "OK":
                    blk = json.loads(txt).get("result", "?")
                    print(f"  [HTTP] {name:6} OK         block={int(blk, 16) if isinstance(blk, str) and blk.startswith('0x') else blk}")
                else:
                    print(f"  [HTTP] {name:6} FAIL  {label:10} {detail}")
                    print(f"    body[:200]: {txt[:200]}")
    except Exception as e:
        print(f"  [HTTP] {name:6} FAIL  CONN_FAIL  {type(e).__name__}: {e}")


async def probe_ws(name: str, url: str) -> None:
    if not url:
        print(f"  [ WS ] {name:6} - no URL set in .env")
        return
    try:
        async with websockets.connect(url, ping_interval=20, open_timeout=10) as ws:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}))
            resp = await asyncio.wait_for(ws.recv(), timeout=5)
            blk = json.loads(resp).get("result", "?")
            print(f"  [ WS ] {name:6} OK         block={int(blk, 16) if isinstance(blk, str) and blk.startswith('0x') else blk}")
    except websockets.exceptions.InvalidStatus as e:
        try:
            status = e.response.status_code
            body = e.response.body.decode("utf-8", errors="ignore") if e.response.body else ""
        except Exception:
            status = 0
            body = str(e)
        label, detail = classify(status, body)
        print(f"  [ WS ] {name:6} FAIL  {label:10} {detail}")
        if body:
            print(f"    body[:200]: {body[:200]}")
    except Exception as e:
        print(f"  [ WS ] {name:6} FAIL  CONN_FAIL  {type(e).__name__}: {e}")


async def main():
    chains = ["BSC", "ETH", "BASE"]
    print(f"\nTesting {len(chains)} chains x 2 protocols (HTTP + WS)\n")

    any_failure = False
    for chain in chains:
        rpc_url = os.environ.get(f"{chain}_RPC", "")
        ws_url = os.environ.get(f"{chain}_WS", "")
        print(f"-- {chain} --")
        print(f"  RPC: {rpc_url}")
        print(f"  WS : {ws_url}")
        await probe_http(chain, rpc_url)
        await probe_ws(chain, ws_url)
        print()

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
