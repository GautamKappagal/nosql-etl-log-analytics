"""
pipelines/mapreduce/pipeline.py
-------------------------------
Pure-Python MapReduce pipeline.

This backend implements the classic Map -> Shuffle/Sort -> Reduce pattern
in-process. The data loading and cleaning stage is itself a MapReduce job:
raw batch lines are mapped into clean-record or malformed-record events, then
reduced into cleaned records, batch metadata, and malformed-record summaries.

The three mandatory analytical queries are separate downstream MapReduce jobs,
so the complete path for this backend is:

    raw batch source -> load/clean MapReduce -> query MapReduce -> DB loader
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set, Tuple, TYPE_CHECKING

from parser.log_parser import batch_read_log_files, parse_log_line
from pipelines.base import BasePipeline, BatchRecord, QueryResult, RunMetadata

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

MapItem = Tuple[Any, Any]
Mapper = Callable[[Any], Iterable[MapItem]]
Reducer = Callable[[Any, List[Any]], Iterable[MapItem]]


class MapReducePipeline(BasePipeline):
    """Executes ETL and analytics using an in-process MapReduce engine."""

    @property
    def pipeline_name(self) -> str:
        return "mapreduce"

    def execute(
        self,
        engine=None,
        queries: Optional[Set[str]] = None,
    ) -> Tuple[RunMetadata, List[QueryResult], List[BatchRecord], Dict[str, int]]:
        wall_start = time.perf_counter()
        active_queries = self.resolve_queries(queries)
        run_id = str(uuid.uuid4())

        cleaned_records, batch_records, error_type_counts, total_records, malformed_count, num_batches = (
            self._run_load_clean_job(run_id)
        )

        q_runtimes: Dict[str, float] = {}
        results: List[QueryResult] = []
        executed_at = datetime.now(timezone.utc)

        if "query1_daily_traffic" in active_queries:
            t0 = time.perf_counter()
            q1_data = self._query1_daily_traffic(cleaned_records)
            q_runtimes["query1_daily_traffic"] = time.perf_counter() - t0
            results.append(QueryResult(
                run_id=run_id,
                query_name="query1_daily_traffic",
                pipeline_name=self.pipeline_name,
                batch_id=num_batches,
                executed_at=executed_at,
                data=q1_data,
                runtime_secs=q_runtimes["query1_daily_traffic"],
            ))

        if "query2_top_resources" in active_queries:
            t0 = time.perf_counter()
            q2_data = self._query2_top_resources(cleaned_records)
            q_runtimes["query2_top_resources"] = time.perf_counter() - t0
            results.append(QueryResult(
                run_id=run_id,
                query_name="query2_top_resources",
                pipeline_name=self.pipeline_name,
                batch_id=num_batches,
                executed_at=executed_at,
                data=q2_data,
                runtime_secs=q_runtimes["query2_top_resources"],
            ))

        if "query3_hourly_errors" in active_queries:
            t0 = time.perf_counter()
            q3_data = self._query3_hourly_errors(cleaned_records)
            q_runtimes["query3_hourly_errors"] = time.perf_counter() - t0
            results.append(QueryResult(
                run_id=run_id,
                query_name="query3_hourly_errors",
                pipeline_name=self.pipeline_name,
                batch_id=num_batches,
                executed_at=executed_at,
                data=q3_data,
                runtime_secs=q_runtimes["query3_hourly_errors"],
            ))

        metadata = self.make_run_metadata(
            pipeline_name=self.pipeline_name,
            batch_size=self.batch_size,
            total_records=total_records,
            malformed=malformed_count,
            num_batches=num_batches,
            runtime_secs=0.0,
            query_runtimes=q_runtimes,
        )
        metadata.run_id = run_id

        if engine is not None:
            from loader.db_loader import save_results, update_run_runtime

            save_results(engine, metadata, results, batch_records, dict(error_type_counts))
            metadata.runtime_seconds = time.perf_counter() - wall_start
            update_run_runtime(engine, metadata.run_id, metadata.runtime_seconds)
        else:
            metadata.runtime_seconds = time.perf_counter() - wall_start

        return metadata, results, batch_records, dict(error_type_counts)

    # ------------------------------------------------------------------
    # Shared in-process MapReduce runner
    # ------------------------------------------------------------------

    @staticmethod
    def _run_map_reduce(inputs: Iterable[Any], mapper: Mapper, reducer: Reducer) -> Iterator[MapItem]:
        shuffled: Dict[Any, List[Any]] = defaultdict(list)
        for item in inputs:
            for key, value in mapper(item):
                shuffled[key].append(value)

        for key in sorted(shuffled, key=lambda value: str(value)):
            yield from reducer(key, shuffled[key])

    # ------------------------------------------------------------------
    # Job 0: load and clean raw batch records
    # ------------------------------------------------------------------

    def _raw_batch_items(self) -> Iterator[Dict[str, Any]]:
        for batch_id, raw_lines in batch_read_log_files(self.log_files, self.batch_size):
            for line_number, line in enumerate(raw_lines, start=1):
                yield {
                    "batch_id": batch_id,
                    "line_number": line_number,
                    "line": line,
                }

    @staticmethod
    def _load_clean_mapper(raw_item: Dict[str, Any]) -> Iterable[MapItem]:
        batch_id = raw_item["batch_id"]
        record, err = parse_log_line(raw_item["line"])

        yield ("batch_stats", batch_id), {
            "total": 1,
            "malformed": 1 if err else 0,
        }

        if err:
            yield ("error_type", err), 1
            yield ("malformed_record", batch_id), {
                "line_number": raw_item["line_number"],
                "error_type": err,
            }
            return

        yield ("clean_record", batch_id), record

    @staticmethod
    def _load_clean_reducer(key: Any, values: List[Any]) -> Iterable[MapItem]:
        key_type, key_value = key

        if key_type == "batch_stats":
            yield key, {
                "records_in_batch": sum(v["total"] for v in values),
                "malformed_in_batch": sum(v["malformed"] for v in values),
            }
            return

        if key_type == "error_type":
            yield key, sum(values)
            return

        for value in values:
            yield key, value

    def _run_load_clean_job(
        self,
        run_id: str,
    ) -> Tuple[List[Dict[str, Any]], List[BatchRecord], Dict[str, int], int, int, int]:
        cleaned_records: List[Dict[str, Any]] = []
        batch_records: List[BatchRecord] = []
        error_type_counts: Dict[str, int] = {}
        total_records = 0
        malformed_count = 0
        num_batches = 0

        for key, value in self._run_map_reduce(
            self._raw_batch_items(),
            self._load_clean_mapper,
            self._load_clean_reducer,
        ):
            key_type, key_value = key

            if key_type == "clean_record":
                cleaned_records.append(value)
            elif key_type == "batch_stats":
                batch_id = int(key_value)
                num_batches = max(num_batches, batch_id)
                total_records += value["records_in_batch"]
                malformed_count += value["malformed_in_batch"]
                batch_records.append(BatchRecord(
                    run_id=run_id,
                    batch_id=batch_id,
                    records_in_batch=value["records_in_batch"],
                    malformed_in_batch=value["malformed_in_batch"],
                ))
            elif key_type == "error_type":
                error_type_counts[str(key_value)] = int(value)

        batch_records.sort(key=lambda batch: batch.batch_id)
        return (
            cleaned_records,
            batch_records,
            error_type_counts,
            total_records,
            malformed_count,
            num_batches,
        )

    # ------------------------------------------------------------------
    # Query 1: Daily Traffic Summary
    # ------------------------------------------------------------------

    def _query1_daily_traffic(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def mapper(record: Dict[str, Any]) -> Iterable[MapItem]:
            yield (record["log_date"], record["status_code"]), (
                1,
                record["bytes_transferred"],
            )

        def reducer(key: Tuple[str, int], values: List[Tuple[int, int]]) -> Iterable[MapItem]:
            request_count = sum(v[0] for v in values)
            total_bytes = sum(v[1] for v in values)
            yield key, {
                "log_date": key[0],
                "status_code": key[1],
                "request_count": request_count,
                "total_bytes": total_bytes,
            }

        return [
            row
            for _, row in self._run_map_reduce(records, mapper, reducer)
        ]

    # ------------------------------------------------------------------
    # Query 2: Top 20 Requested Resources
    # ------------------------------------------------------------------

    def _query2_top_resources(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def mapper(record: Dict[str, Any]) -> Iterable[MapItem]:
            yield record["resource_path"], (
                1,
                record["bytes_transferred"],
                record["host"],
            )

        def reducer(path: str, values: List[Tuple[int, int, str]]) -> Iterable[MapItem]:
            request_count = sum(v[0] for v in values)
            total_bytes = sum(v[1] for v in values)
            distinct_hosts = {v[2] for v in values}
            yield path, {
                "resource_path": path,
                "request_count": request_count,
                "total_bytes": total_bytes,
                "distinct_host_count": len(distinct_hosts),
            }

        rows = [
            row
            for _, row in self._run_map_reduce(records, mapper, reducer)
        ]
        return sorted(rows, key=lambda row: row["request_count"], reverse=True)[:20]

    # ------------------------------------------------------------------
    # Query 3: Hourly Error Analysis
    # ------------------------------------------------------------------

    def _query3_hourly_errors(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def mapper(record: Dict[str, Any]) -> Iterable[MapItem]:
            status_code = record["status_code"]
            is_error = 400 <= status_code <= 599
            yield (record["log_date"], record["log_hour"]), (
                1 if is_error else 0,
                1,
                record["host"] if is_error else None,
            )

        def reducer(key: Tuple[str, int], values: List[Tuple[int, int, Optional[str]]]) -> Iterable[MapItem]:
            error_request_count = sum(v[0] for v in values)
            total_request_count = sum(v[1] for v in values)
            distinct_error_hosts = {v[2] for v in values if v[2] is not None}
            yield key, {
                "log_date": key[0],
                "log_hour": key[1],
                "error_request_count": error_request_count,
                "total_request_count": total_request_count,
                "error_rate": (
                    round(error_request_count / total_request_count, 6)
                    if total_request_count
                    else 0.0
                ),
                "distinct_error_hosts": len(distinct_error_hosts),
            }

        return [
            row
            for _, row in self._run_map_reduce(records, mapper, reducer)
        ]
