"""
controller package
──────────────────
Orchestrator module that wires together pipeline, loader, and reporter.
"""

from .orchestrator import run, report, load_config

__all__ = ["run", "report", "load_config"]
