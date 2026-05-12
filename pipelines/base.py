"""
pipelines/base.py
─────────────────
Abstract base class that every execution pipeline must subclass.

Defines:
• RunMetadata   – statistics collected during a run
• BatchRecord   – per-batch record and malformed counts
• QueryResult   – container for one query's output rows
• BasePipeline  – abstract class with the contract every pipeline must honour
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple


# ─── Sentinel: all three mandatory query names ────────────────────────────────

ALL_QUERIES: Set[str] = {
    "query1_daily_traffic",
    "query2_top_resources",
    "query3_hourly_errors",
}


# ─── Data containers ──────────────────────────────────────────────────────────

@dataclass
class RunMetadata:
    """Statistics gathered during a single ETL run."""
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str = ""
    batch_size: int = 0
    total_records: int = 0
    malformed_records: int = 0
    num_batches: int = 0
    avg_batch_size: float = 0.0
    runtime_seconds: float = 0.0
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Per-query runtimes (query_name -> seconds)
    query_runtimes: Dict[str, float] = field(default_factory=dict)

    def compute_derived(self) -> None:
        """Re-compute avg_batch_size from raw counters."""
        if self.num_batches > 0:
            self.avg_batch_size = self.total_records / self.num_batches
        else:
            self.avg_batch_size = 0.0


@dataclass
class BatchRecord:
    """Per-batch statistics stored in the batch_metadata table."""
    run_id: str
    batch_id: int
    records_in_batch: int       # total lines attempted in this batch
    malformed_in_batch: int     # lines that failed parsing in this batch


@dataclass
class QueryResult:
    """Output from a single query execution."""
    run_id: str
    query_name: str
    pipeline_name: str
    batch_id: int               # The last batch_id at time of query execution
    executed_at: datetime
    data: List[Dict[str, Any]]
    runtime_secs: float = 0.0


# ─── Abstract pipeline ────────────────────────────────────────────────────────

class BasePipeline(ABC):
    """
    Contract every execution pipeline must implement.

    Subclasses receive the full parsed config dict at construction time.
    An optional batch_size_override lets the CLI override config.yaml.
    The single public entry point is ``execute()``.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        batch_size_override: Optional[int] = None,
    ) -> None:
        self.config = config
        self.batch_size = (
            batch_size_override
            if batch_size_override is not None
            else config.get("etl", {}).get("batch_size", 10_000)
        )
        self.log_files = config.get("etl", {}).get("log_files", [])

    # ── Mandatory interface ────────────────────────────────────────────────

    @property
    @abstractmethod
    def pipeline_name(self) -> str:
        """Short label used in DB records and reports (e.g. 'mapreduce')."""
        ...

    @abstractmethod
    def execute(
        self,
        engine=None,
        queries: Optional[Set[str]] = None,
    ) -> Tuple[RunMetadata, List[QueryResult], List[BatchRecord], Dict[str, int]]:
        """
        Run the full ETL pipeline.

        Parameters
        ----------
        engine : SQLAlchemy Engine, optional
            If provided, results are saved to the relational DB and the
            DB save time is included in the measured runtime.
        queries : set of query name strings, optional
            Subset of ALL_QUERIES to execute. Defaults to all three when None.

        Returns
        -------
        (RunMetadata, List[QueryResult], List[BatchRecord], error_type_counts)

        • RunMetadata         – aggregate run statistics
        • List[QueryResult]   – one entry per requested query
        • List[BatchRecord]   – one entry per processed batch
        • error_type_counts   – parser error code -> count mapping
        """
        ...

    # ── Shared helpers available to all subclasses ─────────────────────────

    @staticmethod
    def resolve_queries(queries: Optional[Set[str]]) -> Set[str]:
        """Normalise the queries argument; None means all three."""
        if queries is None:
            return set(ALL_QUERIES)
        return {q for q in queries if q in ALL_QUERIES}

    @staticmethod
    def make_run_metadata(
        pipeline_name: str,
        batch_size: int,
        total_records: int,
        malformed: int,
        num_batches: int,
        runtime_secs: float,
        query_runtimes: Optional[Dict[str, float]] = None,
    ) -> RunMetadata:
        meta = RunMetadata(
            pipeline_name=pipeline_name,
            batch_size=batch_size,
            total_records=total_records,
            malformed_records=malformed,
            num_batches=num_batches,
            runtime_seconds=runtime_secs,
            executed_at=datetime.now(timezone.utc),
            query_runtimes=query_runtimes or {},
        )
        meta.compute_derived()
        return meta
