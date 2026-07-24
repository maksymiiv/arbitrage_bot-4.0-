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

    def _file_path(self) -> Path:
        date_str = datetime.now().strftime("%Y-%m-%d")
        return self.base_dir / f"spreads_{date_str}.log"

    def log(self, message: str) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{now}] {message}"

        _console_log.info(message)

        with self._file_path().open("a", encoding="utf-8") as f:
            f.write(line + "\n")
