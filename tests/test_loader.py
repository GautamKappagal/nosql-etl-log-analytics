"""
tests/test_loader.py
────────────────────
Tests for loader/db_loader.py using an in-memory SQLite database.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from sqlalchemy import create_engine

from loader.db_loader import init_schema, save_results, fetch_run, fetch_all_runs
from pipelines.base import BatchRecord, QueryResult, RunMetadata


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:", future=True)
    init_schema(eng)
    return eng


@pytest.fixture
def sample_run():
    meta = RunMetadata(
        run_id="test-run-001",
        pipeline_name="mapreduce",
        batch_size=1000,
        total_records=5000,
        malformed_records=10,
        num_batches=5,
        runtime_seconds=12.34,
        executed_at=datetime(2025, 1, 1, 12, 0, 0),
        query_runtimes={"query1_daily_traffic": 1.1,
                       "query2_top_resources": 0.8,
                       "query3_hourly_errors": 0.9},
    )
    meta.compute_derived()

    q1 = QueryResult(
        run_id="test-run-001",
        query_name="query1_daily_traffic",
        pipeline_name="mapreduce",
        batch_id=5,
        executed_at=datetime(2025, 1, 1, 12, 0, 1),
        data=[
            {"log_date": "1995-07-01", "status_code": 200,
             "request_count": 100, "total_bytes": 50000},
            {"log_date": "1995-07-01", "status_code": 404,
             "request_count": 5, "total_bytes": 0},
        ],
    )

    q2 = QueryResult(
        run_id="test-run-001",
        query_name="query2_top_resources",
        pipeline_name="mapreduce",
        batch_id=5,
        executed_at=datetime(2025, 1, 1, 12, 0, 2),
        data=[
            {"resource_path": "/index.html", "request_count": 200,
             "total_bytes": 100000, "distinct_host_count": 50},
        ],
    )

    q3 = QueryResult(
        run_id="test-run-001",
        query_name="query3_hourly_errors",
        pipeline_name="mapreduce",
        batch_id=5,
        executed_at=datetime(2025, 1, 1, 12, 0, 3),
        data=[
            {"log_date": "1995-07-01", "log_hour": 12,
             "error_request_count": 10, "total_request_count": 100,
             "error_rate": 0.1, "distinct_error_hosts": 3},
        ],
    )

    batch_records = [
        BatchRecord(run_id="test-run-001", batch_id=1,
                    records_in_batch=1000, malformed_in_batch=2),
        BatchRecord(run_id="test-run-001", batch_id=2,
                    records_in_batch=1000, malformed_in_batch=3),
    ]

    error_counts = {"REGEX_NO_MATCH": 8, "BAD_TIMESTAMP": 2}

    return meta, [q1, q2, q3], batch_records, error_counts


def test_save_and_fetch_run(engine, sample_run):
    meta, results, batch_records, error_counts = sample_run
    save_results(engine, meta, results, batch_records, error_counts)
    data = fetch_run(engine, "test-run-001")

    assert data["metadata"]["run_id"] == "test-run-001"
    assert data["metadata"]["pipeline_name"] == "mapreduce"
    assert data["metadata"]["total_records"] == 5000


def test_q1_rows_persisted(engine, sample_run):
    meta, results, batch_records, error_counts = sample_run
    save_results(engine, meta, results, batch_records, error_counts)
    data = fetch_run(engine, "test-run-001")
    assert len(data["results"]["query1_daily_traffic"]) == 2


def test_q2_rows_persisted(engine, sample_run):
    meta, results, batch_records, error_counts = sample_run
    save_results(engine, meta, results, batch_records, error_counts)
    data = fetch_run(engine, "test-run-001")
    assert len(data["results"]["query2_top_resources"]) == 1


def test_q3_rows_persisted(engine, sample_run):
    meta, results, batch_records, error_counts = sample_run
    save_results(engine, meta, results, batch_records, error_counts)
    data = fetch_run(engine, "test-run-001")
    assert len(data["results"]["query3_hourly_errors"]) == 1


def test_batch_metadata_persisted(engine, sample_run):
    """batch_metadata table should contain one row per batch."""
    meta, results, batch_records, error_counts = sample_run
    save_results(engine, meta, results, batch_records, error_counts)
    data = fetch_run(engine, "test-run-001")
    assert len(data["batches"]) == 2
    assert data["batches"][0]["batch_id"] == 1
    assert data["batches"][1]["records_in_batch"] == 1000


def test_malformed_summary_persisted(engine, sample_run):
    """malformed_record_summary should have one row per error type."""
    meta, results, batch_records, error_counts = sample_run
    save_results(engine, meta, results, batch_records, error_counts)
    data = fetch_run(engine, "test-run-001")
    summary = {row["error_type"]: row["count"] for row in data["malformed_summary"]}
    assert summary["REGEX_NO_MATCH"] == 8
    assert summary["BAD_TIMESTAMP"] == 2


def test_fetch_all_runs(engine, sample_run):
    meta, results, batch_records, error_counts = sample_run
    save_results(engine, meta, results, batch_records, error_counts)
    all_runs = fetch_all_runs(engine)
    assert len(all_runs) == 1
    assert all_runs[0]["run_id"] == "test-run-001"


def test_unknown_run_returns_empty(engine):
    data = fetch_run(engine, "nonexistent-id")
    assert data == {}


def test_save_without_optional_params(engine, sample_run):
    """save_results must work without batch_records or error_counts (backward compat)."""
    meta, results, _, _ = sample_run
    # Override run_id so it doesn't clash with other tests
    meta.run_id = "test-run-compat"
    for r in results:
        r.run_id = "test-run-compat"
    save_results(engine, meta, results)   # no batch_records, no error_counts
    data = fetch_run(engine, "test-run-compat")
    assert data["metadata"]["run_id"] == "test-run-compat"
    assert data["batches"] == []
    assert data["malformed_summary"] == []
