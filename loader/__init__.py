"""
loader package
──────────────
Database loader module for persisting ETL results.
"""

from .db_loader import (
    build_engine,
    init_schema,
    save_results,
    fetch_run,
    fetch_all_runs,
)

__all__ = [
    "build_engine",
    "init_schema",
    "save_results",
    "fetch_run",
    "fetch_all_runs",
]
