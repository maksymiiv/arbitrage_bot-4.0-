"""
GeckoTerminal aggregated-liquidity probe.

Usage:
    python test.py <chain> <token_address>

Examples:
    python test.py eth 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48   # USDC
    python test.py bsc 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c   # WBNB
    python test.py base 0x4200000000000000000000000000000000000006 # WETH on Base

Pulls every pool GeckoTerminal indexes for a given token on a given
chain, prints them one by one (with their per-pool liquidity and 24h
volume), and finally prints the SUM. That sum is what you'd compare
against your $2-3k threshold.

Free-tier rate limit: 30 requests / minute, no API key required.
"""

import asyncio
import sys

import aiohttp


GT_URL = "https://api.geckoterminal.com/api/v2/networks/{network}/tokens/{addr}/pools"

# Map our internal short keys -> GeckoTerminal network slugs.
NETWORK_MAP = {
    "eth": "eth",
    "bsc": "bsc",
    "base": "base",
    "sol": "solana",
}

# GT default page size is 20, max 5 pages here = up to 100 pools.
# Tokens with more than ~50 pools are extremely rare; capping is safe.
MAX_PAGES = 5
PAGE_SIZE = 20


async def fetch_all_pools(chain: str, addr: str) -> list[dict]:
    network = NETWORK_MAP.get(chain.lower(), chain.lower())
    pools: list[dict] = []

    async with aiohttp.ClientSession() as session:
        for page in range(1, MAX_PAGES + 1):
            url = GT_URL.format(network=network, addr=addr.lower())
            try:
                async with session.get(url, params={"page": page}, timeout=15) as r:
                    if r.status == 404:
                        print(f"  -> 404: token {addr} not indexed on {network}")
                        break
                    if r.status == 429:
                        print(f"  -> 429: rate-limited, try again in a minute")
                        break
                    if r.status != 200:
                        print(f"  -> page {page} HTTP {r.status}: {await r.text()}")
                        break
                    data = await r.json()
            except Exception as e:
                print(f"  -> request failed: {e}")
                break

            page_pools = data.get("data") or []
            if not page_pools:
                break
            pools.extend(page_pools)
            if len(page_pools) < PAGE_SIZE:
                break  # last page

    return pools


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    chain = sys.argv[1]
    addr = sys.argv[2]

    pools = asyncio.run(fetch_all_pools(chain, addr))

    if not pools:
        print("\nNo pools found.")
        return

    print(f"\n{len(pools)} pools for {addr} on {chain}:\n")

    total_liq = 0.0
    total_vol = 0.0

    for i, pool in enumerate(pools, 1):
        attr = pool.get("attributes") or {}
        name = attr.get("name") or "?"
        pool_addr = attr.get("address") or ""
        liq = float(attr.get("reserve_in_usd") or 0)
        vol = float((attr.get("volume_usd") or {}).get("h24") or 0)
        dex = attr.get("dex_id") or "?"

        print(
            f"  {i:>2}. {name:<28}  liq=${liq:>14,.0f}  "
            f"vol24h=${vol:>14,.0f}  dex={dex:<18}  {pool_addr}"
        )

        total_liq += liq
        total_vol += vol

    print()
    print(f"  TOTAL liquidity : ${total_liq:>14,.0f}")
    print(f"  TOTAL 24h volume: ${total_vol:>14,.0f}")


if __name__ == "__main__":
    main()
