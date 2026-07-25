"""
Per-day spread log file (logs/spreads/spreads_YYYY-MM-DD.log).
Independent from the engine's rotating logger because the audit trail
needs deterministic daily files.
"""

from datetime import datetime
from pathlib import Path

from engine.config import LOG_DIR, PROJECT_ROOT
from engine.logger import get_logger


_console_log = get_logger(__name__)


class SpreadLogger:
    def __init__(self, base_dir: str | None = None):
        target = base_dir or str(PROJECT_ROOT / LOG_DIR / "spreads")
        self.base_dir = Path(target)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._date: str | None = None
        self._fh = None

    def _handle(self):
        """Keep one open append handle, reopening only on date rollover —
        avoids an open()+close() syscall pair on every logged line."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        if date_str != self._date or self._fh is None:
            if self._fh is not None:
                try:
                    self._fh.close()
                except Exception:
                    pass
            self._fh = (self.base_dir / f"spreads_{date_str}.log").open(
                "a", encoding="utf-8"
            )
            self._date = date_str
        return self._fh

    def log(self, message: str) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _console_log.info(message)
        fh = self._handle()
        fh.write(f"[{now}] {message}\n")
        fh.flush()
