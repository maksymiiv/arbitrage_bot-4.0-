"""
Periodic price_store snapshot dumper.

Logs the full state of price_store so you can eyeball what's getting
updated and how stale each side is. Goes through the same logger as
everything else (rotating engine.log + console).
"""

import asyncio
import time

from engine import price_store
from engine.logger import get_logger


log = get_logger(__name__)


def _format_snapshot(now: float) -> str:
    snapshot = price_store.snapshot()
    if not snapshot:
        return "PRICE STORE SNAPSHOT — empty"

    lines = ["", "====== PRICE STORE SNAPSHOT ======"]

    for key, data in sorted(snapshot.items()):
        display = data.get("display_symbol") or key
        source = (data.get("source") or "unknown").upper()
        lines.append(f"\n[{display}]  key={key}  ({source})")

        cex = data.get("cex") or {}
        if cex:
            for ex, c in cex.items():
                ts = c.get("ts")
                age = round(now - float(ts), 2) if ts else "?"
                lines.append(
                    f"  CEX {ex:<8} bid={c.get('bid'):<12} ask={c.get('ask'):<12} "
                    f"mid={round(float(c.get('mid', 0.0)), 6):<12} age={age}s"
                )
        else:
            lines.append("  CEX: —")

        dex = data.get("dex") or {}
        if dex:
            for chain, d in dex.items():
                ts = d.get("ts")
                age = round(now - float(ts), 2) if ts else "?"
                lines.append(
                    f"  DEX {chain:<8} price={round(float(d.get('price', 0.0)), 8):<14} "
                    f"pool={d.get('pool')} proto={d.get('protocol', 'NA')} age={age}s"
                )
        else:
            lines.append("  DEX: —")

    lines.append("==================================")
    return "\n".join(lines)


async def price_store_dumper(interval: float) -> None:
    """Dump the snapshot every `interval` seconds. interval<=0 disables."""
    if interval <= 0:
        log.info("price_store dumper disabled (interval<=0)")
        return

    log.info("price_store dumper started (interval=%ss)", interval)
    while True:
        await asyncio.sleep(interval)
        try:
            log.info(_format_snapshot(time.time()))
        except Exception as e:
            log.error("price_store dumper error: %s", e)
