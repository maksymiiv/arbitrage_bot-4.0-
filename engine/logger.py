"""
Project-wide logging setup.

- Single rotating root file (`logs/engine.log`)
- One rotating per-chain file written by chain-specific loggers
- Console mirror at the same level
- All `print(...)` callers can either:
    1. Migrate to `get_logger(__name__).info(...)` (preferred)
    2. Keep using print: stdout/stderr are still tee'd into the engine log

The rotating handler tolerates Windows file-lock races (your IDE keeps
the log open → `os.rename` raises PermissionError) by retrying the
rollover step a few times and, as a last resort, continuing to write to
the existing file rather than crashing the process.

The stream-to-logger bridge has a re-entrancy guard so that if the
logger itself fails (full disk, locked rollover) and the standard
library writes a traceback to stderr, we don't recurse back into the
broken logger and produce a `RecursionError` that kills async tasks.
"""

import logging
import os
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from typing import Optional

from engine.config import (
    LOG_BACKUP_COUNT,
    LOG_DIR,
    LOG_LEVEL,
    LOG_MAX_BYTES,
    PROJECT_ROOT,
)


_LOG_PATH = PROJECT_ROOT / LOG_DIR
_LOG_PATH.mkdir(parents=True, exist_ok=True)

_FORMATTER = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_CONFIGURED: set[str] = set()


# --------------------------------------------------------------------------
# Windows-tolerant rotating file handler
# --------------------------------------------------------------------------

class _SafeRotatingFileHandler(RotatingFileHandler):
    """
    RotatingFileHandler that doesn't crash when the rollover rename fails
    because the destination is held open (typical on Windows: IDE
    preview, antivirus, our own re-open).

    Strategy:
      1. Retry the rename a few times with short backoff.
      2. If still failing, log the issue to the original stderr and
         keep writing to the current file (it grows past the limit, but
         we don't lose data and we don't crash the process).
    """

    _ROLLOVER_RETRIES = 8
    _ROLLOVER_BACKOFF_BASE = 0.05

    def doRollover(self) -> None:
        last_exc: Exception | None = None
        delay = self._ROLLOVER_BACKOFF_BASE
        for _ in range(self._ROLLOVER_RETRIES):
            try:
                super().doRollover()
                return
            except (PermissionError, OSError) as e:
                # ERROR_ACCESS_DENIED (5) / ERROR_SHARING_VIOLATION (32)
                if isinstance(e, OSError) and getattr(e, "winerror", None) not in (None, 5, 32):
                    raise
                last_exc = e
                time.sleep(delay)
                delay = min(delay * 2, 1.0)

        # Could not rotate. Surface the issue without going through the
        # logger (which would re-enter this same handler and recurse).
        try:
            sys.__stderr__.write(
                f"[logger] rollover failed for {self.baseFilename} "
                f"after {self._ROLLOVER_RETRIES} retries: {last_exc}; "
                f"continuing without rotation\n"
            )
            sys.__stderr__.flush()
        except Exception:
            pass
        # Reopen the existing file so emit() can keep appending.
        if self.stream is None:
            self.stream = self._open()


def _file_handler(filename: str) -> _SafeRotatingFileHandler:
    handler = _SafeRotatingFileHandler(
        _LOG_PATH / filename,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(_FORMATTER)
    return handler


def _console_handler() -> logging.StreamHandler:
    handler = logging.StreamHandler(sys.__stdout__)
    handler.setFormatter(_FORMATTER)
    return handler


def _configure_root() -> None:
    if "__root__" in _CONFIGURED:
        return

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    # avoid duplicate handlers if reload happens
    for h in list(root.handlers):
        root.removeHandler(h)

    root.addHandler(_console_handler())
    root.addHandler(_file_handler("engine.log"))

    # Belt & braces: never let logging itself crash the process if a
    # handler raises. Default `raiseExceptions=True` would propagate.
    logging.raiseExceptions = False

    _CONFIGURED.add("__root__")


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger. Safe to call multiple times."""
    _configure_root()
    return logging.getLogger(name)


def setup_chain_logger(chain: str) -> logging.Logger:
    """
    Per-chain logger that ALSO writes to its own rotating file
    (e.g. logs/bsc.log) on top of the root engine log.
    """
    _configure_root()
    name = f"chain.{chain}"
    logger = logging.getLogger(name)

    if name in _CONFIGURED:
        return logger

    handler = _file_handler(f"{chain}.log")
    logger.addHandler(handler)
    _CONFIGURED.add(name)
    return logger


# --------------------------------------------------------------------------
# stdout / stderr -> logger bridge with re-entrancy guard
# --------------------------------------------------------------------------

class _StreamToLogger:
    """
    Bridge sys.stdout / sys.stderr into a logger so legacy `print()`
    calls land in the rotating log file.

    Re-entrancy guard: if the logger itself raises (full disk, locked
    rollover) and Python's logging machinery writes the resulting
    `--- Logging error ---` traceback to stderr, we MUST NOT recurse
    back into the same broken logger — that yields RecursionError and
    kills async tasks. While we're inside `write()`, any nested call
    just goes to the original console FD instead.
    """

    _local = threading.local()

    def __init__(self, logger: logging.Logger, level: int):
        self._logger = logger
        self._level = level
        self._buf = ""

    def _bypass_write(self, message: str) -> None:
        target = sys.__stderr__ if self._level >= logging.WARNING else sys.__stdout__
        try:
            target.write(message)
            target.flush()
        except Exception:
            pass

    def write(self, message: str) -> int:
        if not message:
            return 0

        # already inside this bridge → bypass logger entirely
        if getattr(self._local, "active", False):
            self._bypass_write(message)
            return len(message)

        self._local.active = True
        try:
            self._buf += message
            while "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                line = line.rstrip()
                if line:
                    try:
                        self._logger.log(self._level, line)
                    except Exception:
                        # logger blew up → fall back to direct console
                        self._bypass_write(line + "\n")
            return len(message)
        finally:
            self._local.active = False

    def flush(self) -> None:
        if not self._buf.strip():
            self._buf = ""
            return
        if getattr(self._local, "active", False):
            self._bypass_write(self._buf)
            self._buf = ""
            return
        self._local.active = True
        try:
            try:
                self._logger.log(self._level, self._buf.strip())
            except Exception:
                self._bypass_write(self._buf)
            self._buf = ""
        finally:
            self._local.active = False

    def isatty(self) -> bool:  # some libs probe this
        return False


def install_print_bridge(logger_name: Optional[str] = None) -> None:
    """
    Reroute `print` (and uncaught tracebacks) into the logging pipeline.
    Call once from the entrypoint.
    """
    _configure_root()
    bridge_logger = logging.getLogger(logger_name or "stdout")
    sys.stdout = _StreamToLogger(bridge_logger, logging.INFO)
    sys.stderr = _StreamToLogger(bridge_logger, logging.ERROR)
