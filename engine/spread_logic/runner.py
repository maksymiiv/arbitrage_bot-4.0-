import asyncio
import hashlib

from engine import price_store
from engine.logger import get_logger

from .logger import SpreadLogger
from .scanner import scan_snapshot


log = get_logger(__name__)


def _make_fingerprint(opportunities) -> str:
    # Dedup on the SET of opportunities + spread rounded to 0.01%. Ages are
    # deliberately excluded — they tick every second, which made the old
    # fingerprint change every tick and defeated the dedup entirely (the
    # full block re-logged once per second). Now an unchanged/idle spread
    # logs once; only a real composition or >=0.01% spread change re-logs.
    rows = [
        f"{o.symbol}|{o.direction}|{o.cex_exchange}|{o.dex_chain}|{round(o.spread_pct, 2)}"
        for o in opportunities
    ]
    return hashlib.md5("\n".join(rows).encode("utf-8")).hexdigest()


async def spread_runner(interval: float = 1.0, min_spread_pct: float = 0.5) -> None:
    log.info("spread runner started (interval=%ss min_pct=%s)", interval, min_spread_pct)
    spread_log = SpreadLogger()
    last_fp = None

    while True:
        try:
            # scan_snapshot is fully synchronous (no await mid-iteration),
            # so a live read-only view is safe and skips a per-second
            # deepcopy of the whole store.
            snapshot = price_store.snapshot_view()
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
