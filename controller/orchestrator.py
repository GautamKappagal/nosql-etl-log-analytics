"""
controller/orchestrator.py
──────────────────────────
Wires together the pipeline, loader and reporter into a single callable.

The orchestrator is the only place that knows about all three layers.
The CLI calls run() and report(); it never imports pipeline or loader directly.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Set

import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from loader.db_loader import build_engine, fetch_all_runs, init_schema, save_results
from pipelines import PIPELINE_REGISTRY
from pipelines.base import ALL_QUERIES
from reporter.report import print_comparison_report, print_run_report

console = Console()

# Friendly labels used in CLI help text and log messages
_QUERY_LABELS = {
    "query1_daily_traffic": "Q1 – Daily Traffic Summary",
    "query2_top_resources": "Q2 – Top 20 Requested Resources",
    "query3_hourly_errors": "Q3 – Hourly Error Analysis",
}


def load_config(path: str = "config/config.yaml") -> Dict[str, Any]:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def _resolve_query_set(query_arg: Optional[str]) -> Set[str]:
    """
    Convert the CLI --query string to a set of internal query names.

    Accepted values (case-insensitive):
      all | q1 | q2 | q3 | query1_daily_traffic | query2_top_resources | query3_hourly_errors
    Returns a set containing the matching ALL_QUERIES members.
    """
    if query_arg is None or query_arg.lower() == "all":
        return set(ALL_QUERIES)

    aliases = {
        "q1": "query1_daily_traffic",
        "q2": "query2_top_resources",
        "q3": "query3_hourly_errors",
        "query1_daily_traffic": "query1_daily_traffic",
        "query2_top_resources": "query2_top_resources",
        "query3_hourly_errors": "query3_hourly_errors",
    }
    resolved = aliases.get(query_arg.lower())
    if resolved is None:
        raise ValueError(
            f"Unknown query '{query_arg}'. "
            "Choose from: all, q1, q2, q3 (or the full query name)."
        )
    return {resolved}


def run(
    pipeline_name: str,
    config_path: str = "config/config.yaml",
    query: Optional[str] = None,
    batch_size: Optional[int] = None,
) -> str:
    """
    Execute the full ETL pipeline and persist results.

    Parameters
    ----------
    pipeline_name : one of mapreduce | mongodb | pig | hive
    config_path   : path to the YAML config file
    query         : 'all' | 'q1' | 'q2' | 'q3' (default: 'all')
    batch_size    : override the batch_size in config.yaml

    Returns
    -------
    run_id (str) – the UUID of the completed run, for use by the reporter
    """
    config = load_config(config_path)

    # ── Resolve pipeline class ─────────────────────────────────────────────
    key = pipeline_name.lower().strip()
    if key not in PIPELINE_REGISTRY:
        raise ValueError(
            f"Unknown pipeline '{pipeline_name}'. "
            f"Choose from: {', '.join(PIPELINE_REGISTRY)}"
        )

    # ── Resolve which queries to run ───────────────────────────────────────
    active_queries = _resolve_query_set(query)

    # ── Instantiate pipeline (batch_size override honoured here) ──────────
    PipelineClass = PIPELINE_REGISTRY[key]
    pipeline = PipelineClass(config, batch_size_override=batch_size)

    # ── Set up DB ──────────────────────────────────────────────────────────
    engine = build_engine(config)
    init_schema(engine)

    # ── Execute with a Rich progress spinner ──────────────────────────────
    effective_batch_size = pipeline.batch_size
    query_label = (
        "all queries"
        if active_queries == set(ALL_QUERIES)
        else ", ".join(_QUERY_LABELS.get(q, q) for q in sorted(active_queries))
    )
    console.print(f"\n[bold green]▶ Starting {pipeline_name.upper()} pipeline[/bold green]")
    console.print(
        f"  Batch size : {effective_batch_size:,}\n"
        f"  Queries    : {query_label}\n"
        f"  Input files: {', '.join(config['etl']['log_files'])}\n"
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task(f"Running {pipeline_name.upper()} ETL …", total=None)
        metadata, results, batch_records, error_counts = pipeline.execute(
            engine, queries=active_queries
        )

    console.print(
        f"\n[bold green]✔ Done[/bold green] "
        f"run_id=[cyan]{metadata.run_id}[/cyan] "
        f"elapsed={metadata.runtime_seconds:.2f}s\n"
    )
    return metadata.run_id


def report(run_id: Optional[str], config_path: str = "config/config.yaml") -> None:
    """
    Print the report for a specific run, or a comparison of all runs.

    Parameters
    ----------
    run_id      : UUID string from a previous run, or None for comparison view
    config_path : path to the YAML config file
    """
    config = load_config(config_path)
    engine = build_engine(config)
    init_schema(engine)  # create tables if they don't exist yet (no-op otherwise)

    if run_id:
        print_run_report(engine, run_id)
    else:
        # No run_id supplied → show all runs for comparison
        runs = fetch_all_runs(engine)
        if not runs:
            console.print("[yellow]No runs found. Execute a pipeline first.[/yellow]")
            return
        if len(runs) == 1:
            print_run_report(engine, runs[0]["run_id"])
        else:
            print_comparison_report(engine)
            console.print(
                "\n[dim]Tip: pass --run-id <id> to drill into a specific run.[/dim]\n"
            )
