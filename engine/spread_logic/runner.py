import asyncio
import hashlib

from engine import price_store
from engine.logger import get_logger

from .logger import SpreadLogger
from .scanner import scan_snapshot


log = get_logger(__name__)


def _make_fingerprint(opportunities) -> str:
    rows = [
        f"{o.symbol}|{o.direction}|{o.cex_exchange}|{o.dex_chain}|"
        f"{round(o.spread_pct, 8)}|{round(o.cex_age, 2)}|{round(o.dex_age, 2)}"
        for o in opportunities
    ]
    return hashlib.md5("\n".join(rows).encode("utf-8")).hexdigest()


async def spread_runner(interval: float = 1.0, min_spread_pct: float = 0.5) -> None:
    log.info("spread runner started (interval=%ss min_pct=%s)", interval, min_spread_pct)
    spread_log = SpreadLogger()
    last_fp = None

    while True:
        try:
            snapshot = price_store.snapshot()
            opportunities = scan_snapshot(snapshot, min_spread_pct=min_spread_pct)
            fp = _make_fingerprint(opportunities)

            if fp != last_fp:
                spread_log.log(
                    f"========== DEX-CEX SPREADS | min_spread={min_spread_pct}% =========="
                )
                if not opportunities:
                    spread_log.log("No opportunities found.")
                else:
                    for opp in opportunities:
                        spread_log.log(opp.log_line())
                spread_log.log("===================================================================")
                last_fp = fp

        except Exception as e:
            log.error("spread runner error: %s", e)

        await asyncio.sleep(interval)
