"""
pipelines/mongodb/pipeline.py
─────────────────────────────
MongoDB pipeline.

Strategy:
1. Parse raw log lines in Python (same shared parser as every other pipeline).
2. Bulk-insert parsed documents into a MongoDB collection in batches.
3. Run three server-side aggregation pipelines for the mandatory queries.
4. Return results in the standard QueryResult / RunMetadata format.

The raw collection is dropped and recreated on every run so that reruns are
idempotent. Indexes are created before queries run to ensure the aggregations
are efficient even on the full ~3.5M-record NASA dataset.
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

import pymongo
from pymongo import MongoClient

from parser.log_parser import parse_log_line, batch_read_log_files
from pipelines.base import (
    ALL_QUERIES, BasePipeline, BatchRecord, QueryResult, RunMetadata,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class MongoDBPipeline(BasePipeline):
    """ETL and analytics executed through MongoDB aggregation framework."""

    @property
    def pipeline_name(self) -> str:
        return "mongodb"

    # ─────────────────────────────────────────────────────────────────────────
    # Main entry point
    # ─────────────────────────────────────────────────────────────────────────

    def execute(
        self,
        engine=None,
        queries: Optional[Set[str]] = None,
    ) -> Tuple[RunMetadata, List[QueryResult], List[BatchRecord], Dict[str, int]]:
        wall_start = time.perf_counter()
        active_queries = self.resolve_queries(queries)

        cfg = self.config.get("mongodb", {})
        host = cfg.get("host", "localhost")
        port = int(cfg.get("port", 27017))
        db_name = cfg.get("database", "nosql_etl")
        coll_name = cfg.get("raw_collection", "web_logs")

        client = MongoClient(host, port, serverSelectionTimeoutMS=5000)
        db = client[db_name]
        coll = db[coll_name]

        # ── Drop + recreate for idempotency ───────────────────────────────
        coll.drop()

        # ── Phase 1: Parse + bulk-insert ──────────────────────────────────
        run_id = str(uuid.uuid4())
        total_records = 0
        malformed_count = 0
        num_batches = 0
        batch_records: List[BatchRecord] = []
        error_type_counts: Dict[str, int] = defaultdict(int)

        for batch_id, raw_lines in batch_read_log_files(self.log_files, self.batch_size):
            num_batches = batch_id
            batch_total = 0
            batch_malformed = 0
            batch_docs: List[Dict[str, Any]] = []

            for line in raw_lines:
                record, err = parse_log_line(line)
                total_records += 1
                batch_total += 1
                if err:
                    malformed_count += 1
                    batch_malformed += 1
                    error_type_counts[err] += 1
                else:
                    batch_docs.append(record)

            if batch_docs:
                coll.insert_many(batch_docs, ordered=False)

            batch_records.append(BatchRecord(
                run_id=run_id,
                batch_id=batch_id,
                records_in_batch=batch_total,
                malformed_in_batch=batch_malformed,
            ))

        # ── Create indexes for efficient aggregation ───────────────────────
        coll.create_index([("log_date", 1), ("status_code", 1)])
        coll.create_index([("resource_path", 1)])
        coll.create_index([("log_date", 1), ("log_hour", 1)])

        # ── Phase 2: Run aggregation queries ──────────────────────────────
        q_runtimes: Dict[str, float] = {}
        results: List[QueryResult] = []
        executed_at = datetime.now(timezone.utc)

        if "query1_daily_traffic" in active_queries:
            t0 = time.perf_counter()
            q1_data = self._query1_daily_traffic(coll)
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
            q2_data = self._query2_top_resources(coll)
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
            q3_data = self._query3_hourly_errors(coll)
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
            runtime_secs=0.0,  # filled after DB save
            query_runtimes=q_runtimes,
        )
        metadata.run_id = run_id

        # ── Phase 3: Save to database (included in runtime) ────────────────
        if engine is not None:
            from loader.db_loader import save_results, update_run_runtime
            save_results(engine, metadata, results, batch_records, dict(error_type_counts))
            metadata.runtime_seconds = time.perf_counter() - wall_start
            update_run_runtime(engine, metadata.run_id, metadata.runtime_seconds)
        else:
            # Runtime includes everything from reading input to DB save
            metadata.runtime_seconds = time.perf_counter() - wall_start

        client.close()
        return metadata, results, batch_records, dict(error_type_counts)

    # ─────────────────────────────────────────────────────────────────────────
    # Query 1 – Daily Traffic Summary
    # ─────────────────────────────────────────────────────────────────────────

    def _query1_daily_traffic(self, coll) -> List[Dict[str, Any]]:
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "log_date": "$log_date",
                        "status_code": "$status_code",
                    },
                    "request_count": {"$sum": 1},
                    "total_bytes": {"$sum": "$bytes_transferred"},
                }
            },
            {"$sort": {"_id.log_date": 1, "_id.status_code": 1}},
            {
                "$project": {
                    "_id": 0,
                    "log_date": "$_id.log_date",
                    "status_code": "$_id.status_code",
                    "request_count": 1,
                    "total_bytes": 1,
                }
            },
        ]
        return list(coll.aggregate(pipeline, allowDiskUse=True))

    # ─────────────────────────────────────────────────────────────────────────
    # Query 2 – Top 20 Requested Resources
    # ─────────────────────────────────────────────────────────────────────────

    def _query2_top_resources(self, coll) -> List[Dict[str, Any]]:
        pipeline = [
            {
                "$group": {
                    "_id": "$resource_path",
                    "request_count": {"$sum": 1},
                    "total_bytes": {"$sum": "$bytes_transferred"},
                    "hosts": {"$addToSet": "$host"},
                }
            },
            {"$sort": {"request_count": -1}},
            {"$limit": 20},
            {
                "$project": {
                    "_id": 0,
                    "resource_path": "$_id",
                    "request_count": 1,
                    "total_bytes": 1,
                    "distinct_host_count": {"$size": "$hosts"},
                }
            },
        ]
        return list(coll.aggregate(pipeline, allowDiskUse=True))

    # ─────────────────────────────────────────────────────────────────────────
    # Query 3 – Hourly Error Analysis
    # ─────────────────────────────────────────────────────────────────────────

    def _query3_hourly_errors(self, coll) -> List[Dict[str, Any]]:
        pipeline = [
            # Tag each document: is it an error request?
            {
                "$addFields": {
                    "is_error": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$status_code", 400]},
                                    {"$lte": ["$status_code", 599]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "log_date": "$log_date",
                        "log_hour": "$log_hour",
                    },
                    "error_request_count": {"$sum": "$is_error"},
                    "total_request_count": {"$sum": 1},
                    "error_hosts": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$is_error", 1]},
                                "$host",
                                "$$REMOVE",
                            ]
                        }
                    },
                }
            },
            {"$sort": {"_id.log_date": 1, "_id.log_hour": 1}},
            {
                "$project": {
                    "_id": 0,
                    "log_date": "$_id.log_date",
                    "log_hour": "$_id.log_hour",
                    "error_request_count": 1,
                    "total_request_count": 1,
                    "error_rate": {
                        "$cond": [
                            {"$gt": ["$total_request_count", 0]},
                            {"$divide": ["$error_request_count", "$total_request_count"]},
                            0,
                        ]
                    },
                    "distinct_error_hosts": {"$size": "$error_hosts"},
                }
            },
        ]
        return list(coll.aggregate(pipeline, allowDiskUse=True))
