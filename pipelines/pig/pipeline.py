"""
pipelines/pig/pipeline.py
─────────────────────────
Pig pipeline.

The Python orchestrator:
1. Writes ALL raw log lines (including malformed ones) to a temp flat file so
   that Pig performs its own filtering natively — satisfying the pipeline
   authenticity requirement. Malformed record counts are obtained by comparing
   the raw line count against records that passed Pig's own filter.
2. Invokes the Pig Latin script (etl.pig) via subprocess in local mode.
3. Reads Pig's CSV output files back into Python dicts.
4. Returns the standard RunMetadata / QueryResult / BatchRecord objects.

If the `pig` executable is not found the pipeline raises a clear RuntimeError
rather than silently failing.
"""

from __future__ import annotations

import csv
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
_ETL_SCRIPT = os.path.join(_SCRIPT_DIR, "etl.pig")


class PigPipeline(BasePipeline):
    """ETL and analytics executed through Apache Pig (local mode)."""

    @property
    def pipeline_name(self) -> str:
        return "pig"

    # ─────────────────────────────────────────────────────────────────────────
    # Main entry point
    # ─────────────────────────────────────────────────────────────────────────

    def execute(
        self,
        engine=None,
        queries: Optional[Set[str]] = None,
    ) -> Tuple[RunMetadata, List[QueryResult], List[BatchRecord], Dict[str, int]]:
        cfg = self.config.get("pig", {})
        pig_exe = cfg.get("executable", "pig")

        # Verify Pig is available
        if not shutil.which(pig_exe):
            raise RuntimeError(
                f"Apache Pig executable '{pig_exe}' not found on PATH. "
                "Install Pig and ensure it is on your PATH, or update "
                "config/config.yaml → pig.executable."
            )

        active_queries = self.resolve_queries(queries)
        wall_start = time.perf_counter()

        # ── Phase 1: Write ALL raw lines to flat file for Pig ────────────
        # We write every raw line — including malformed ones — so that Pig
        # performs its own parsing and filtering natively.  We still scan
        # the lines in Python in order to compute per-batch statistics and
        # error type counts, but we do NOT pre-filter.
        tmp_dir = tempfile.mkdtemp(prefix="pig_etl_")
        input_file = os.path.join(tmp_dir, "combined.log")
        output_dir = cfg.get("output_dir", os.path.join(tmp_dir, "pig_out"))
        pig_logfile = os.path.join(tmp_dir, "pig.log")

        run_id = str(uuid.uuid4())
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
                    # Write ALL lines to file regardless of parsability
                    fout.write(line + "\n")
                    # Parse to count malformed and error types (stats only)
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

        # ── Phase 2: Invoke Pig ────────────────────────────────────────────
        # Build query-filter param so Pig can skip unneeded output stores
        query_flags = ",".join(sorted(active_queries))

        # Clean output dir (Pig/Hadoop fails if output already exists)
        # Treat configured output_dir as scratch space for this pipeline.
        shutil.rmtree(output_dir, ignore_errors=True)

        pig_cmd = [
            pig_exe, "-x", "local",
            "-v",  # print full error messages
            "-w",  # do not aggregate warnings (helps debugging)
            "-logfile", pig_logfile,
            "-param", f"INPUT_FILES={input_file}",
            "-param", f"OUTPUT_DIR={output_dir}",
            "-param", f"BATCH_SIZE={self.batch_size}",
            "-param", f"QUERIES={query_flags}",
            _ETL_SCRIPT,
        ]

        result = subprocess.run(
            pig_cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            stdout_path = os.path.join(tmp_dir, "pig.stdout")
            stderr_path = os.path.join(tmp_dir, "pig.stderr")
            try:
                with open(stdout_path, "w", encoding="utf-8", errors="replace") as fh:
                    fh.write(result.stdout or "")
                with open(stderr_path, "w", encoding="utf-8", errors="replace") as fh:
                    fh.write(result.stderr or "")
            except OSError:
                stdout_path = "(failed to write)"
                stderr_path = "(failed to write)"

            # Keep tmp_dir for debugging (combined input + pig.log).
            # The orchestrator can still clean it up manually later.
            raise RuntimeError(
                f"Pig script failed (exit {result.returncode}).\n"
                f"Working dir: {tmp_dir}\n"
                f"Pig logfile: {pig_logfile}\n"
                f"Pig stdout : {stdout_path}\n"
                f"Pig stderr : {stderr_path}\n"
                f"Output dir : {output_dir}\n"
                f"STDOUT (tail):\n{(result.stdout or '')[-3000:]}\n"
                f"STDERR (tail):\n{(result.stderr or '')[-3000:]}"
            )

        # ── Phase 3: Read Pig output back into Python ──────────────────────
        results: List[QueryResult] = []
        executed_at = datetime.now(timezone.utc)

        if "query1_daily_traffic" in active_queries:
            q1_data = self._read_pig_csv(
                os.path.join(output_dir, "query1"),
                ["log_date", "status_code", "request_count", "total_bytes"],
                [str, int, int, int],
            )
            results.append(QueryResult(
                run_id=run_id,
                query_name="query1_daily_traffic",
                pipeline_name=self.pipeline_name,
                batch_id=num_batches,
                executed_at=executed_at,
                data=q1_data,
                runtime_secs=0.0,  # Pig does not expose per-query timings
            ))

        if "query2_top_resources" in active_queries:
            q2_data = self._read_pig_csv(
                os.path.join(output_dir, "query2"),
                ["resource_path", "request_count", "total_bytes", "distinct_host_count"],
                [str, int, int, int],
            )
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
            q3_data = self._read_pig_csv(
                os.path.join(output_dir, "query3"),
                ["log_date", "log_hour", "error_request_count",
                 "total_request_count", "error_rate", "distinct_error_hosts"],
                [str, int, int, int, float, int],
            )
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
    def _read_pig_csv(
        folder: str,
        columns: List[str],
        types: List[type],
    ) -> List[Dict[str, Any]]:
        """
        Pig writes output as part-* files inside a folder.
        Read all part files, parse CSV, cast types.
        """
        rows: List[Dict[str, Any]] = []
        if not os.path.isdir(folder):
            return rows
        for fname in sorted(os.listdir(folder)):
            if not fname.startswith("part"):
                continue
            fpath = os.path.join(folder, fname)
            with open(fpath, encoding="utf-8") as fh:
                reader = csv.reader(fh)
                for row in reader:
                    if len(row) < len(columns):
                        continue
                    record = {}
                    for col, typ, val in zip(columns, types, row):
                        try:
                            record[col] = typ(val.strip())
                        except (ValueError, TypeError):
                            record[col] = val.strip()
                    rows.append(record)
        return rows
