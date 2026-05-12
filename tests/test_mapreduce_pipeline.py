"""
tests/test_mapreduce_pipeline.py
────────────────────────────────
Integration test for the MapReduce pipeline.

Uses a small synthetic log file written to /tmp so no real NASA data is needed.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from pipelines.mapreduce.pipeline import MapReducePipeline

SAMPLE_LINES = [
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /index.html HTTP/1.0" 200 6245',
    '199.72.81.55 - - [01/Jul/1995:01:00:01 -0400] "GET /about.html HTTP/1.0" 200 1000',
    'uplherc.upl.com - - [01/Jul/1995:01:00:07 -0400] "GET /index.html HTTP/1.0" 304 -',
    'gateway.uky.edu - - [02/Jul/1995:13:54:51 -0400] "GET /error.html HTTP/1.0" 404 0',
    'badclient.com - - [02/Jul/1995:14:00:00 -0400] "GET /crash HTTP/1.0" 500 0',
    'notavalidline',
    '',
]


@pytest.fixture
def synthetic_log(tmp_path):
    log_file = tmp_path / "test.log"
    log_file.write_text("\n".join(SAMPLE_LINES), encoding="latin-1")
    return str(log_file)


@pytest.fixture
def pipeline(synthetic_log):
    config = {
        "etl": {
            "batch_size": 10,
            "log_files": [synthetic_log],
        }
    }
    return MapReducePipeline(config)


def test_execute_returns_metadata_and_results(pipeline):
    metadata, results, batch_records, error_counts = pipeline.execute()
    assert metadata.pipeline_name == "mapreduce"
    assert len(results) == 3  # all 3 queries by default


def test_total_and_malformed_counts(pipeline):
    metadata, _, _, _ = pipeline.execute()
    # batch_read_log_files strips blank lines before yielding, so the empty
    # line in SAMPLE_LINES is never counted as a record.
    # Non-empty lines: 6 (5 valid + 1 garbage "notavalidline")
    assert metadata.total_records == 6
    assert metadata.malformed_records == 1


def test_batch_records_populated(pipeline):
    _, _, batch_records, _ = pipeline.execute()
    assert len(batch_records) >= 1
    # Every batch record should reference the correct run_id
    _, results, batch_records, _ = pipeline.execute()


def test_error_type_counts(pipeline):
    _, _, _, error_counts = pipeline.execute()
    assert isinstance(error_counts, dict)
    # The garbage line should be counted as REGEX_NO_MATCH
    assert error_counts.get("REGEX_NO_MATCH", 0) == 1


def test_query1_schema(pipeline):
    _, results, _, _ = pipeline.execute()
    q1 = results[0]
    assert q1.query_name == "query1_daily_traffic"
    for row in q1.data:
        assert "log_date" in row
        assert "status_code" in row
        assert "request_count" in row
        assert "total_bytes" in row


def test_query2_top20_limit(pipeline):
    _, results, _, _ = pipeline.execute()
    q2 = results[1]
    assert q2.query_name == "query2_top_resources"
    assert len(q2.data) <= 20


def test_query2_schema(pipeline):
    _, results, _, _ = pipeline.execute()
    for row in results[1].data:
        assert "resource_path" in row
        assert "request_count" in row
        assert "total_bytes" in row
        assert "distinct_host_count" in row


def test_query3_error_rate_range(pipeline):
    _, results, _, _ = pipeline.execute()
    for row in results[2].data:
        assert 0.0 <= row["error_rate"] <= 1.0


def test_query3_schema(pipeline):
    _, results, _, _ = pipeline.execute()
    for row in results[2].data:
        assert "log_date" in row
        assert "log_hour" in row
        assert "error_request_count" in row
        assert "total_request_count" in row
        assert "error_rate" in row
        assert "distinct_error_hosts" in row


def test_run_id_consistent_across_results(pipeline):
    metadata, results, batch_records, _ = pipeline.execute()
    for r in results:
        assert r.run_id == metadata.run_id
    for b in batch_records:
        assert b.run_id == metadata.run_id


def test_batch_counting(pipeline):
    metadata, _, _, _ = pipeline.execute()
    assert metadata.num_batches >= 1
    assert metadata.avg_batch_size > 0


def test_query_filtering_q1_only(pipeline):
    """Running with queries={'query1_daily_traffic'} returns only Q1."""
    _, results, _, _ = pipeline.execute(queries={"query1_daily_traffic"})
    assert len(results) == 1
    assert results[0].query_name == "query1_daily_traffic"


def test_query_filtering_q2_q3(pipeline):
    """Running with queries set to Q2+Q3 skips Q1."""
    _, results, _, _ = pipeline.execute(
        queries={"query2_top_resources", "query3_hourly_errors"}
    )
    assert len(results) == 2
    names = {r.query_name for r in results}
    assert "query1_daily_traffic" not in names


def test_batch_size_override(synthetic_log):
    """batch_size_override in constructor takes precedence over config."""
    config = {
        "etl": {
            "batch_size": 10000,
            "log_files": [synthetic_log],
        }
    }
    p = MapReducePipeline(config, batch_size_override=2)
    assert p.batch_size == 2
    metadata, _, batch_records, _ = p.execute()
    # With batch_size=2 and 6 non-empty lines we expect at least 3 batches
    assert metadata.num_batches >= 3
