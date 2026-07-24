"""Backwards-compat shim — chain logging now lives in engine.logger."""

from engine.logger import setup_chain_logger as setup_logger

__all__ = ["setup_logger"]
