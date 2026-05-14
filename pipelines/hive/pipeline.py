"""
pipelines/hive/pipeline.py
───────────────────────────
Hive pipeline.

The Python orchestrator:
1. Writes ALL raw log lines (including malformed ones) to a temp flat file so
   that the Hive external table and the parsed_logs view perform their own
   native filtering — satisfying the pipeline authenticity requirement.
   Python only scans the lines to compute per-batch statistics.
2. Invokes the HiveQL script (etl.hql) via subprocess, passing --hivevar
   substitutions.
3. Reads back result tables via a separate 'hive -e SELECT ...' invocation.
4. Returns the standard RunMetadata / QueryResult / BatchRecord objects.

Supports both the legacy Hive CLI (hive) and Beeline (beeline).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from parser.log_parser import parse_log_line, batch_read_log_files
from pipelines.base import (
    ALL_QUERIES, BasePipeline, BatchRecord, QueryResult, RunMetadata,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

_SCRIPT_DIR = os.path.join(os.path.dirname(__file__), "scripts")
_ETL_SCRIPT = os.path.join(_SCRIPT_DIR, "etl.hql")

_Q1_COLS = ["log_date", "status_code", "request_count", "total_bytes"]
_Q1_TYPES = [str, int, int, int]

_Q2_COLS = ["resource_path", "request_count", "total_bytes", "distinct_host_count"]
_Q2_TYPES = [str, int, int, int]

_Q3_COLS = ["log_date", "log_hour", "error_request_count",
            "total_request_count", "error_rate", "distinct_error_hosts"]
_Q3_TYPES = [str, int, int, int, float, int]


class HivePipeline(BasePipeline):
    """ETL and analytics executed through Apache Hive."""

    @property
    def pipeline_name(self) -> str:
        return "hive"

    # ─────────────────────────────────────────────────────────────────────────
    # Main entry point
    # ─────────────────────────────────────────────────────────────────────────

    def execute(
        self,
        engine=None,
        queries: Optional[Set[str]] = None,
    ) -> Tuple[RunMetadata, List[QueryResult], List[BatchRecord], Dict[str, int]]:
        cfg = self.config.get("hive", {})
        hive_exe = cfg.get("executable", "hive")

        if not shutil.which(hive_exe):
            raise RuntimeError(
                f"Hive executable '{hive_exe}' not found on PATH. "
                "Install Hive or update config/config.yaml → hive.executable."
            )

        active_queries = self.resolve_queries(queries)
        wall_start = time.perf_counter()

        run_id = str(uuid.uuid4())
        tmp_dir = tempfile.mkdtemp(prefix="hive_etl_")
        input_file = os.path.join(tmp_dir, "combined.log")
        output_db = "nosql_etl_hive"

        # ── Phase 1: Write ALL raw lines to flat file ─────────────────────
        # Every raw line (including malformed ones) goes to the file so that
        # Hive's WHERE clause and regex-based parsed_logs view perform the
        # filtering natively, satisfying the pipeline authenticity requirement.
        total_records = 0
        malformed_count = 0
        num_batches = 0
        batch_records: List[BatchRecord] = []
        error_type_counts: Dict[str, int] = defaultdict(int)

        with open(input_file, "w", encoding="latin-1") as fout:
            for batch_id, raw_lines in batch_read_log_files(
                self.log_files, self.batch_size
            ):
                num_batches = batch_id
                batch_total = 0
                batch_malformed = 0

                for line in raw_lines:
                    # Write ALL lines — Hive does its own filtering
                    fout.write(line + "\n")
                    # Parse only for statistics / error categorisation
                    _, err = parse_log_line(line)
                    total_records += 1
                    batch_total += 1
                    if err:
                        malformed_count += 1
                        batch_malformed += 1
                        error_type_counts[err] += 1

                batch_records.append(BatchRecord(
                    run_id=run_id,
                    batch_id=batch_id,
                    records_in_batch=batch_total,
                    malformed_in_batch=batch_malformed,
                ))

        # ── Phase 2: Invoke Hive ───────────────────────────────────────────
        hive_cmd = [
            hive_exe,
            "--hivevar", f"INPUT_LOCATION={tmp_dir}",
            "--hivevar", f"OUTPUT_DB={output_db}",
            "--hivevar", f"BATCH_SIZE={self.batch_size}",
            "-f", _ETL_SCRIPT,
        ]

        result = subprocess.run(hive_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise RuntimeError(
                f"Hive script failed (exit {result.returncode}).\n"
                f"STDERR:\n{result.stderr[-3000:]}"
            )

        # ── Phase 3: Read results back from Hive ──────────────────────────
        results: List[QueryResult] = []
        executed_at = datetime.now(timezone.utc)

        if "query1_daily_traffic" in active_queries:
            q1_data = self._hive_select(hive_exe, output_db,
                                        "q1_daily_traffic", _Q1_COLS, _Q1_TYPES)
            results.append(QueryResult(
                run_id=run_id,
                query_name="query1_daily_traffic",
                pipeline_name=self.pipeline_name,
                batch_id=num_batches,
                executed_at=executed_at,
                data=q1_data,
                runtime_secs=0.0,
            ))

        if "query2_top_resources" in active_queries:
            q2_data = self._hive_select(hive_exe, output_db,
                                        "q2_top_resources", _Q2_COLS, _Q2_TYPES)
            results.append(QueryResult(
                run_id=run_id,
                query_name="query2_top_resources",
                pipeline_name=self.pipeline_name,
                batch_id=num_batches,
                executed_at=executed_at,
                data=q2_data,
                runtime_secs=0.0,
            ))

        if "query3_hourly_errors" in active_queries:
            q3_data = self._hive_select(hive_exe, output_db,
                                        "q3_hourly_errors", _Q3_COLS, _Q3_TYPES)
            results.append(QueryResult(
                run_id=run_id,
                query_name="query3_hourly_errors",
                pipeline_name=self.pipeline_name,
                batch_id=num_batches,
                executed_at=executed_at,
                data=q3_data,
                runtime_secs=0.0,
            ))

        shutil.rmtree(tmp_dir, ignore_errors=True)

        meta = self.make_run_metadata(
            pipeline_name=self.pipeline_name,
            batch_size=self.batch_size,
            total_records=total_records,
            malformed=malformed_count,
            num_batches=num_batches,
            runtime_secs=0.0,  # filled after DB save
        )
        meta.run_id = run_id

        # ── Phase 4: Save to database (included in runtime) ────────────────
        if engine is not None:
            from loader.db_loader import save_results
            save_results(engine, meta, results, batch_records, dict(error_type_counts))

        # Runtime includes everything from reading input to DB save
        meta.runtime_seconds = time.perf_counter() - wall_start

        return meta, results, batch_records, dict(error_type_counts)

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _hive_select(
        hive_exe: str,
        db: str,
        table: str,
        columns: List[str],
        types: List[type],
    ) -> List[Dict[str, Any]]:
        """Run a SELECT * query via hive -e and parse tab-separated output."""
        query = f"USE {db}; SELECT * FROM {table};"
        result = subprocess.run(
            [hive_exe, "-e", query],
            capture_output=True,
            text=True,
        )
        rows: List[Dict[str, Any]] = []
        for line in result.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) < len(columns):
                continue
            record: Dict[str, Any] = {}
            for col, typ, val in zip(columns, types, parts):
                try:
                    record[col] = typ(val.strip())
                except (ValueError, TypeError):
                    record[col] = val.strip()
            rows.append(record)
        return rows
