"""
loader/db_loader.py
───────────────────
Loads ETL results into a relational database (SQLite / MySQL / PostgreSQL)
using SQLAlchemy Core (no ORM).

Schema
──────
run_metadata           – one row per pipeline run
batch_metadata         – one row per batch within a run
malformed_record_summary – per-error-type counts for each run
query_results_q1       – Query 1: Daily Traffic Summary
query_results_q2       – Query 2: Top Requested Resources
query_results_q3       – Query 3: Hourly Error Analysis

Every result row carries: run_id, pipeline_name, batch_id, executed_at.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger, Column, DateTime, Float, Integer,
    MetaData, String, Table, Text, create_engine, insert, text,
)
from sqlalchemy.engine import Engine

from pipelines.base import BatchRecord, QueryResult, RunMetadata


# ─── Schema definitions ───────────────────────────────────────────────────────

_metadata = MetaData()

run_metadata_table = Table(
    "run_metadata", _metadata,
    Column("run_id", String(64), primary_key=True),
    Column("pipeline_name", String(32), nullable=False),
    Column("batch_size", Integer, nullable=False),
    Column("total_records", BigInteger, nullable=False),
    Column("malformed_records", BigInteger, nullable=False),
    Column("num_batches", Integer, nullable=False),
    Column("avg_batch_size", Float, nullable=False),
    Column("runtime_seconds", Float, nullable=False),
    Column("executed_at", DateTime, nullable=False),
    Column("query_runtimes", Text, nullable=True),  # JSON blob
)

batch_metadata_table = Table(
    "batch_metadata", _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("batch_id", Integer, nullable=False),
    Column("records_in_batch", BigInteger, nullable=False),
    Column("malformed_in_batch", BigInteger, nullable=False),
)

malformed_summary_table = Table(
    "malformed_record_summary", _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("error_type", String(64), nullable=False),
    Column("count", BigInteger, nullable=False),
)

q1_table = Table(
    "query_results_q1", _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("pipeline_name", String(32), nullable=False),
    Column("batch_id", Integer, nullable=False),
    Column("executed_at", DateTime, nullable=False),
    Column("log_date", String(16), nullable=False),
    Column("status_code", Integer, nullable=False),
    Column("request_count", BigInteger, nullable=False),
    Column("total_bytes", BigInteger, nullable=False),
)

q2_table = Table(
    "query_results_q2", _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("pipeline_name", String(32), nullable=False),
    Column("batch_id", Integer, nullable=False),
    Column("executed_at", DateTime, nullable=False),
    Column("resource_path", Text, nullable=False),
    Column("request_count", BigInteger, nullable=False),
    Column("total_bytes", BigInteger, nullable=False),
    Column("distinct_host_count", Integer, nullable=False),
)

q3_table = Table(
    "query_results_q3", _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("pipeline_name", String(32), nullable=False),
    Column("batch_id", Integer, nullable=False),
    Column("executed_at", DateTime, nullable=False),
    Column("log_date", String(16), nullable=False),
    Column("log_hour", Integer, nullable=False),
    Column("error_request_count", BigInteger, nullable=False),
    Column("total_request_count", BigInteger, nullable=False),
    Column("error_rate", Float, nullable=False),
    Column("distinct_error_hosts", Integer, nullable=False),
)

_QUERY_TABLE_MAP = {
    "query1_daily_traffic": q1_table,
    "query2_top_resources": q2_table,
    "query3_hourly_errors": q3_table,
}


# ─── Public API ───────────────────────────────────────────────────────────────

def build_engine(config: Dict[str, Any]) -> Engine:
    """Create a SQLAlchemy engine from the project config dict."""
    db_cfg = config.get("database", {})
    db_type = db_cfg.get("type", "sqlite").lower()

    if db_type == "sqlite":
        path = db_cfg.get("sqlite", {}).get("path", "./etl_results.db")
        url = f"sqlite:///{path}"

    elif db_type == "mysql":
        c = db_cfg["mysql"]
        url = (
            f"mysql+pymysql://{c['username']}:{c['password']}"
            f"@{c['host']}:{c['port']}/{c['database']}"
        )

    elif db_type in ("postgresql", "postgres"):
        c = db_cfg["postgresql"]
        url = (
            f"postgresql+psycopg2://{c['username']}:{c['password']}"
            f"@{c['host']}:{c['port']}/{c['database']}"
        )

    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    return create_engine(url, future=True)


def init_schema(engine: Engine) -> None:
    """Create all tables if they do not yet exist."""
    _metadata.create_all(engine)


def save_results(
    engine: Engine,
    metadata: RunMetadata,
    results: List[QueryResult],
    batch_records: Optional[List[BatchRecord]] = None,
    error_type_counts: Optional[Dict[str, int]] = None,
) -> None:
    """
    Persist one RunMetadata and its associated rows.

    Parameters
    ----------
    engine           : SQLAlchemy Engine
    metadata         : run-level statistics
    results          : list of QueryResult objects (one per executed query)
    batch_records    : per-batch statistics for batch_metadata table
    error_type_counts: parser error code → count for malformed_record_summary
    """
    with engine.begin() as conn:
        # ── run_metadata ──────────────────────────────────────────────────
        conn.execute(
            insert(run_metadata_table),
            {
                "run_id": metadata.run_id,
                "pipeline_name": metadata.pipeline_name,
                "batch_size": metadata.batch_size,
                "total_records": metadata.total_records,
                "malformed_records": metadata.malformed_records,
                "num_batches": metadata.num_batches,
                "avg_batch_size": metadata.avg_batch_size,
                "runtime_seconds": metadata.runtime_seconds,
                "executed_at": metadata.executed_at,
                "query_runtimes": json.dumps(metadata.query_runtimes),
            },
        )

        # ── batch_metadata ────────────────────────────────────────────────
        if batch_records:
            batch_rows = [
                {
                    "run_id": br.run_id,
                    "batch_id": br.batch_id,
                    "records_in_batch": br.records_in_batch,
                    "malformed_in_batch": br.malformed_in_batch,
                }
                for br in batch_records
            ]
            conn.execute(insert(batch_metadata_table), batch_rows)

        # ── malformed_record_summary ───────────────────────────────────────
        if error_type_counts:
            summary_rows = [
                {
                    "run_id": metadata.run_id,
                    "error_type": err_type,
                    "count": count,
                }
                for err_type, count in error_type_counts.items()
                if count > 0
            ]
            if summary_rows:
                conn.execute(insert(malformed_summary_table), summary_rows)

        # ── query result rows ─────────────────────────────────────────────
        for qr in results:
            tbl = _QUERY_TABLE_MAP.get(qr.query_name)
            if tbl is None:
                continue

            base = {
                "run_id": qr.run_id,
                "pipeline_name": qr.pipeline_name,
                "batch_id": qr.batch_id,
                "executed_at": qr.executed_at,
            }

            rows_to_insert = [{**base, **row} for row in qr.data]

            if rows_to_insert:
                # Insert in chunks of 500 to avoid parameter-binding limits
                chunk_size = 500
                for i in range(0, len(rows_to_insert), chunk_size):
                    conn.execute(insert(tbl), rows_to_insert[i:i + chunk_size])


def fetch_run(engine: Engine, run_id: str) -> Dict[str, Any]:
    """
    Retrieve a complete run from the DB and return it as a plain dict.
    Used by the reporter.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM run_metadata WHERE run_id = :rid"),
            {"rid": run_id},
        ).fetchone()
        if row is None:
            return {}

        meta = dict(row._mapping)

        results: Dict[str, List] = {}
        for qname, tbl in _QUERY_TABLE_MAP.items():
            rows = conn.execute(
                text(f"SELECT * FROM {tbl.name} WHERE run_id = :rid"),
                {"rid": run_id},
            ).fetchall()
            results[qname] = [dict(r._mapping) for r in rows]

        # Fetch batch metadata
        batch_rows = conn.execute(
            text("SELECT * FROM batch_metadata WHERE run_id = :rid ORDER BY batch_id"),
            {"rid": run_id},
        ).fetchall()
        batches = [dict(r._mapping) for r in batch_rows]

        # Fetch malformed summary
        malformed_rows = conn.execute(
            text("SELECT * FROM malformed_record_summary WHERE run_id = :rid"),
            {"rid": run_id},
        ).fetchall()
        malformed_summary = [dict(r._mapping) for r in malformed_rows]

        return {
            "metadata": meta,
            "results": results,
            "batches": batches,
            "malformed_summary": malformed_summary,
        }


def fetch_all_runs(engine: Engine) -> List[Dict[str, Any]]:
    """Return a summary list of all runs stored in the DB."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM run_metadata ORDER BY executed_at DESC")
        ).fetchall()
        return [dict(r._mapping) for r in rows]
