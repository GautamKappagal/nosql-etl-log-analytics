#!/usr/bin/env python3
"""
main.py
────────
CLI entry point for the Multi-Pipeline ETL Framework.

Usage
─────
# Run the MapReduce pipeline (all 3 queries, batch size from config)
python main.py run --pipeline mapreduce

# Run only Query 1 with MongoDB
python main.py run --pipeline mongodb --query q1

# Override batch size at runtime
python main.py run --pipeline mapreduce --batch-size 5000

# Run a specific pipeline with a custom config
python main.py run --pipeline mongodb --config config/config.yaml

# Print the report for the most recent run (or comparison if multiple)
python main.py report

# Print the report for a specific run
python main.py report --run-id <uuid>

# Show all stored runs in a comparison table
python main.py compare
"""

import sys
import click
from rich.console import Console

console = Console()

# Ensure project root is on sys.path so absolute imports work when
# the script is invoked from any working directory.
import os
sys.path.insert(0, os.path.dirname(__file__))

from controller.orchestrator import report, run


# ─── CLI definition ───────────────────────────────────────────────────────────

@click.group()
def cli():
    """Multi-Pipeline ETL & Reporting Framework for Web Server Log Analytics."""
    pass


# ── run ───────────────────────────────────────────────────────────────────────

@cli.command("run")
@click.option(
    "--pipeline", "-p",
    required=True,
    type=click.Choice(["mapreduce", "mongodb", "pig", "hive"],
                      case_sensitive=False),
    help="Execution pipeline to use.",
)
@click.option(
    "--query", "-q",
    default="all",
    show_default=True,
    type=click.Choice(["all", "q1", "q2", "q3"], case_sensitive=False),
    help=(
        "Which query to execute. "
        "q1=Daily Traffic, q2=Top Resources, q3=Hourly Errors, all=run all three."
    ),
)
@click.option(
    "--batch-size", "-b",
    default=None,
    type=int,
    help=(
        "Override the batch_size in config.yaml. "
        "Must be kept identical across pipelines in a comparative experiment."
    ),
)
@click.option(
    "--config", "-c",
    default="config/config.yaml",
    show_default=True,
    help="Path to the YAML configuration file.",
)
def run_cmd(pipeline: str, query: str, batch_size: int, config: str) -> None:
    """Execute the ETL pipeline and store results in the relational DB."""
    try:
        run_id = run(
            pipeline_name=pipeline,
            config_path=config,
            query=query,
            batch_size=batch_size,
        )
        console.print(
            f"[bold]Results saved.[/bold] "
            f"Run [cyan]python main.py report --run-id {run_id}[/cyan] to view them.\n"
        )
    except Exception as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        sys.exit(1)


# ── report ────────────────────────────────────────────────────────────────────

@cli.command("report")
@click.option(
    "--run-id", "-r",
    default=None,
    help="UUID of the run to report on. Omit to see the comparison table.",
)
@click.option(
    "--config", "-c",
    default="config/config.yaml",
    show_default=True,
    help="Path to the YAML configuration file.",
)
def report_cmd(run_id: str, config: str) -> None:
    """Display query results and execution metadata from the database."""
    try:
        report(run_id=run_id, config_path=config)
    except Exception as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        sys.exit(1)


# ── compare ───────────────────────────────────────────────────────────────────

@cli.command("compare")
@click.option(
    "--config", "-c",
    default="config/config.yaml",
    show_default=True,
    help="Path to the YAML configuration file.",
)
def compare_cmd(config: str) -> None:
    """Show a comparison table of all stored pipeline runs."""
    try:
        report(run_id=None, config_path=config)
    except Exception as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        sys.exit(1)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
