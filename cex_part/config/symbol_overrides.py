"""
Manual disambiguation map for tickers that collide between CEXes when one
side does not expose contract addresses (Kraken).

Keyed by (cex_lower, symbol_upper). The value is the canonical cache key
that this CEX listing should be merged into.

Example: Kraken's "LIT" is a different token from Bybit's "LIT". After we
have entries for Bybit's LIT (key="LIT", BSC contract) and Litentry's LIT
(key="LIT#eth.0x...", ETH contract), pin Kraken's LIT explicitly:

    SYMBOL_OVERRIDES = {
        ("kraken", "LIT"): "LIT#eth.0x763fa6",
    }

Override is applied BEFORE the regular contract / symbol resolver, so it
short-circuits any wrong inference.
"""

from typing import Dict, Tuple


SYMBOL_OVERRIDES: Dict[Tuple[str, str], str] = {
    # ("kraken", "LIT"): "LIT#eth.0x763fa6",
}


def lookup(cex: str, symbol: str) -> str | None:
    if not cex or not symbol:
        return None
    return SYMBOL_OVERRIDES.get((cex.lower(), symbol.upper()))
