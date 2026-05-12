"""
reporter/report.py
──────────────────
Terminal reporter built with the `rich` library.

Reads stored results from the relational DB and renders:
• Run summary card (pipeline, run_id, runtime, batch stats, record counts)
• Malformed record breakdown (by error type)
• Batch-level statistics table
• Query 1 table (Daily Traffic Summary)
• Query 2 table (Top 20 Requested Resources)
• Query 3 table (Hourly Error Analysis)
• Comparison table (if multiple runs exist)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from loader.db_loader import fetch_all_runs, fetch_run

console = Console()

# ─── Top-level functions ──────────────────────────────────────────────────────

def print_run_report(engine, run_id: str) -> None:
    """Print the full report for a single run."""
    data = fetch_run(engine, run_id)
    if not data:
        console.print(f"[red]Run {run_id} not found in database.[/red]")
        return

    _print_header(data["metadata"])
    _print_malformed_summary(data.get("malformed_summary", []))
    _print_batch_table(data.get("batches", []))

    results = data["results"]
    q1_rows = results.get("query1_daily_traffic", [])
    q2_rows = results.get("query2_top_resources", [])
    q3_rows = results.get("query3_hourly_errors", [])

    if q1_rows:
        _print_q1(q1_rows)
    if q2_rows:
        _print_q2(q2_rows)
    if q3_rows:
        _print_q3(q3_rows)

    if not q1_rows and not q2_rows and not q3_rows:
        console.print("[yellow]No query results found for this run.[/yellow]")


def print_comparison_report(engine) -> None:
    """Print a side-by-side comparison table of all stored runs."""
    runs = fetch_all_runs(engine)
    if not runs:
        console.print("[yellow]No runs found in the database.[/yellow]")
        return

    tbl = Table(
        title="Pipeline Run Comparison",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
    )
    for col in ["Run ID", "Pipeline", "Executed At",
                "Total Records", "Malformed", "Batches",
                "Avg Batch Size", "Runtime (s)"]:
        tbl.add_column(col, no_wrap=(col == "Run ID"))

    for run in runs:
        tbl.add_row(
            run["run_id"][:12] + "…",
            run["pipeline_name"].upper(),
            _fmt_dt(run["executed_at"]),
            f"{run['total_records']:,}",
            f"{run['malformed_records']:,}",
            str(run["num_batches"]),
            f"{run['avg_batch_size']:.1f}",
            f"{run['runtime_seconds']:.2f}",
        )

    console.print(tbl)


# ─── Private renderers ────────────────────────────────────────────────────────

def _print_header(meta: Dict[str, Any]) -> None:
    q_rts = json.loads(meta.get("query_runtimes") or "{}")

    lines = [
        f"[bold]Run ID       :[/bold] {meta['run_id']}",
        f"[bold]Pipeline     :[/bold] {meta['pipeline_name'].upper()}",
        f"[bold]Executed At  :[/bold] {_fmt_dt(meta['executed_at'])}",
        "",
        f"[bold]Total Records:[/bold] {meta['total_records']:>12,}",
        f"[bold]Malformed    :[/bold] {meta['malformed_records']:>12,}",
        f"[bold]Batches      :[/bold] {meta['num_batches']:>12,}",
        f"[bold]Batch Size   :[/bold] {meta['batch_size']:>12,}",
        f"[bold]Avg Batch Sz :[/bold] {meta['avg_batch_size']:>12.1f}",
        f"[bold]Total Runtime:[/bold] {meta['runtime_seconds']:>11.2f}s",
    ]

    if q_rts:
        lines.append("")
        lines.append("[bold]Per-Query Runtimes:[/bold]")
        for qname, secs in q_rts.items():
            lines.append(f"  {qname:<30} {secs:.3f}s")

    panel_text = "\n".join(lines)
    console.print(Panel(panel_text, title="[bold green]ETL Run Summary[/bold green]",
                       border_style="green"))


def _print_malformed_summary(rows: List[Dict[str, Any]]) -> None:
    """Print a breakdown of malformed records by error type."""
    if not rows:
        return

    tbl = Table(
        title="Malformed Record Summary",
        box=box.SIMPLE_HEAD,
        header_style="bold red",
    )
    tbl.add_column("Error Type", style="red")
    tbl.add_column("Count", justify="right")

    for r in rows:
        tbl.add_row(str(r["error_type"]), f"{int(r['count']):,}")

    console.print(tbl)


def _print_batch_table(batches: List[Dict[str, Any]]) -> None:
    """Print per-batch statistics."""
    if not batches:
        return

    tbl = Table(
        title="Batch Statistics",
        box=box.SIMPLE_HEAD,
        header_style="bold magenta",
    )
    tbl.add_column("Batch ID", justify="right", style="magenta")
    tbl.add_column("Records", justify="right")
    tbl.add_column("Malformed", justify="right", style="yellow")
    tbl.add_column("Valid", justify="right", style="green")

    for b in batches:
        total = int(b["records_in_batch"])
        mal = int(b["malformed_in_batch"])
        tbl.add_row(
            str(b["batch_id"]),
            f"{total:,}",
            f"{mal:,}",
            f"{total - mal:,}",
        )

    console.print(tbl)


def _print_q1(rows: List[Dict[str, Any]]) -> None:
    tbl = Table(
        title="Query 1 – Daily Traffic Summary",
        box=box.SIMPLE_HEAD,
        header_style="bold yellow",
    )
    tbl.add_column("Log Date", style="cyan")
    tbl.add_column("Status Code", justify="right")
    tbl.add_column("Request Count", justify="right")
    tbl.add_column("Total Bytes", justify="right")

    for r in rows:
        tbl.add_row(
            str(r["log_date"]),
            str(r["status_code"]),
            f"{int(r['request_count']):,}",
            f"{int(r['total_bytes']):,}",
        )

    console.print(tbl)


def _print_q2(rows: List[Dict[str, Any]]) -> None:
    tbl = Table(
        title="Query 2 – Top 20 Requested Resources",
        box=box.SIMPLE_HEAD,
        header_style="bold yellow",
    )
    tbl.add_column("#", justify="right", style="dim")
    tbl.add_column("Resource Path", style="cyan", no_wrap=False)
    tbl.add_column("Request Count", justify="right")
    tbl.add_column("Total Bytes", justify="right")
    tbl.add_column("Distinct Hosts", justify="right")

    for i, r in enumerate(rows, 1):
        tbl.add_row(
            str(i),
            str(r["resource_path"]),
            f"{int(r['request_count']):,}",
            f"{int(r['total_bytes']):,}",
            str(r["distinct_host_count"]),
        )

    console.print(tbl)


def _print_q3(rows: List[Dict[str, Any]]) -> None:
    tbl = Table(
        title="Query 3 – Hourly Error Analysis",
        box=box.SIMPLE_HEAD,
        header_style="bold yellow",
    )
    tbl.add_column("Log Date", style="cyan")
    tbl.add_column("Hour", justify="right")
    tbl.add_column("Error Reqs", justify="right")
    tbl.add_column("Total Reqs", justify="right")
    tbl.add_column("Error Rate", justify="right")
    tbl.add_column("Distinct Hosts", justify="right")

    for r in rows:
        rate = float(r.get("error_rate", 0))
        colour = "red" if rate > 0.10 else "yellow" if rate > 0.05 else "green"
        tbl.add_row(
            str(r["log_date"]),
            str(r["log_hour"]),
            f"{int(r['error_request_count']):,}",
            f"{int(r['total_request_count']):,}",
            f"[{colour}]{rate:.4f}[/{colour}]",
            str(r["distinct_error_hosts"]),
        )

    console.print(tbl)


def _fmt_dt(val: Any) -> str:
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S UTC")
    return str(val)
