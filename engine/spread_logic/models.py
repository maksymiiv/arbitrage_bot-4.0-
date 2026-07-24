from dataclasses import dataclass
from typing import Optional


@dataclass
class SpreadOpportunity:
    symbol: str

    direction: str  # "dex->cex" | "cex->dex"

    cex_exchange: str
    cex_bid: float
    cex_ask: float
    cex_mid: float
    cex_ts: float
    cex_age: float

    dex_chain: str
    dex_price: float
    dex_pool: str
    dex_protocol: str
    dex_ts: float
    dex_age: float

    spread_abs: float
    spread_pct: float

    # Cumulative USD volume the orderbook-depth check summed in the
    # profitable price band on the CEX side. None = book not loaded yet
    # (typical for a fresh Kraken spread before the first REST poll).
    depth_usd: Optional[float] = None

    def short_line(self) -> str:
        return (
            f"{self.symbol:<10} | {self.direction:<8} | "
            f"CEX={self.cex_exchange:<8} bid={self.cex_bid:<12} ask={self.cex_ask:<12} | "
            f"DEX={self.dex_chain:<6} price={self.dex_price:<12} | "
            f"spread={round(self.spread_pct, 6):>12}% | "
            f"cex_age={round(self.cex_age, 2):>8}s | dex_age={round(self.dex_age, 2):>8}s"
        )

    def log_line(self) -> str:
        # `depth` is the cumulative USD volume the orderbook-depth check
        # found in the profitable band. "n/a" = book not loaded yet.
        depth_str = (
            f"${self.depth_usd:.0f}" if self.depth_usd is not None else "n/a"
        )
        return (
            f"symbol={self.symbol} | "
            f"dir={self.direction} | "
            f"cex={self.cex_exchange} bid={self.cex_bid} ask={self.cex_ask} depth={depth_str} age={round(self.cex_age, 2)}s | "
            f"dex={self.dex_chain} price={self.dex_price} pool={self.dex_pool} proto={self.dex_protocol} age={round(self.dex_age, 2)}s | "
            f"spread_abs={round(self.spread_abs, 10)} | "
            f"spread_pct={round(self.spread_pct, 10)}"
        )