"""
Uniswap V4 monitor — proof of concept.

Subscribes to the V4 PoolManager singleton, decodes every Swap event,
and computes the USD price for one target poolId.

V4 Swap event:
    Swap(PoolId indexed id, address indexed sender,
         int128 amount0, int128 amount1, uint160 sqrtPriceX96,
         uint128 liquidity, int24 tick, uint24 fee)

  topics[1] = poolId (bytes32)
  data words: 0 amount0 | 1 amount1 | 2 sqrtPriceX96 | 3 liquidity
              4 tick | 5 fee
  -> sqrtPriceX96 sits at hex data[128:192], same offset as our V3 decoder.

Run:  python _test_v4.py
"""

import asyncio
import json

import websockets
from web3 import Web3


# ---- config -------------------------------------------------------------
# Public node — independent of the (possibly exhausted) Alchemy key.
ETH_WS = "wss://ethereum-rpc.publicnode.com"
ETH_RPC = "https://ethereum-rpc.publicnode.com"

POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
SWAP_TOPIC_V4 = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"

TARGET_POOL_ID = "0xdb4c4d91f12ce76f5c9ac0eae193cf3b4d6684cd5f09bf35d03dd9ae6d8a43b1"
TOKEN_ADDR = "0xc8Fb80fCc03f699C70ff0CC08C09106288888888"
ETH_USD = 2140.0  # static for the test

# currency0 = ETH (0x000...000 — smallest address), currency1 = TOKEN
ETH_DECIMALS = 18

# Stop after this many events so the test run terminates on its own.
MAX_EVENTS = 40

ERC20_DECIMALS_ABI = [{
    "name": "decimals", "outputs": [{"type": "uint8"}],
    "inputs": [], "stateMutability": "view", "type": "function",
}]


def decode_sqrt_price(data_hex: str) -> int:
    """sqrtPriceX96 = 3rd 32-byte word of the Swap event data."""
    data = data_hex[2:] if data_hex.startswith("0x") else data_hex
    return int(data[128:192], 16)


def price_token_usd(sqrt_x96: int, dec0: int, dec1: int) -> float:
    """
    token1 USD price for a currency0=ETH / currency1=TOKEN pool.

    raw = (sqrt/2^96)^2 * 10^(dec0-dec1)  -> TOKEN per ETH (decimal-adj)
    token_usd = ETH_USD / raw
    """
    ratio = (sqrt_x96 / (2 ** 96)) ** 2
    raw = ratio * (10 ** (dec0 - dec1))
    if raw <= 0:
        return 0.0
    return ETH_USD / raw


async def main() -> None:
    # one-time: read the token's decimals
    w3 = Web3(Web3.HTTPProvider(ETH_RPC, request_kwargs={"timeout": 10}))
    try:
        token = w3.eth.contract(
            address=Web3.to_checksum_address(TOKEN_ADDR), abi=ERC20_DECIMALS_ABI,
        )
        token_dec = int(token.functions.decimals().call())
    except Exception as e:
        print(f"decimals() failed ({e}), defaulting to 18")
        token_dec = 18
    print(f"token decimals = {token_dec}  (ETH decimals = {ETH_DECIMALS})")

    dec0, dec1 = ETH_DECIMALS, token_dec
    target = TARGET_POOL_ID.lower()

    print(f"connecting to {ETH_WS} ...")
    async with websockets.connect(ETH_WS, ping_interval=20) as ws:
        sub = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
            "params": ["logs", {
                "address": POOL_MANAGER,
                "topics": [SWAP_TOPIC_V4],
            }],
        }
        await ws.send(json.dumps(sub))
        ack = json.loads(await ws.recv())
        print(f"subscribe ack: {ack}")
        print(f"watching target poolId {target}")
        print("-" * 70)

        seen = 0
        target_hits = 0
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("method") != "eth_subscription":
                continue
            result = (msg.get("params") or {}).get("result") or {}
            topics = result.get("topics") or []
            if len(topics) < 2:
                continue

            pool_id = topics[1].lower()
            sqrt = decode_sqrt_price(result.get("data", ""))
            seen += 1

            if pool_id == target:
                target_hits += 1
                usd = price_token_usd(sqrt, dec0, dec1)
                print(f"#{seen}  *** TARGET HIT ***  sqrtPriceX96={sqrt}")
                print(f"        token USD price = ${usd:.10f}")
            else:
                print(f"#{seen}  pool={pool_id[:20]}...  sqrt={sqrt}")

            if seen >= MAX_EVENTS:
                break

        print("-" * 70)
        print(f"done: {seen} swaps seen, {target_hits} on the target pool")


asyncio.run(main())
