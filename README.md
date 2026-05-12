# Multi-Pipeline ETL & Reporting Framework
### DAS 839 – NoSQL Systems | End Semester Project (Phase 1)

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Directory Structure](#3-directory-structure)
4. [Dependencies](#4-dependencies)
5. [Setup & Installation](#5-setup--installation)
6. [Downloading the Dataset](#6-downloading-the-dataset)
7. [Configuration](#7-configuration)
8. [Running the Tool](#8-running-the-tool)
9. [Pipeline Descriptions](#9-pipeline-descriptions)
10. [Mandatory Queries](#10-mandatory-queries)
11. [Batching & Runtime Reporting](#11-batching--runtime-reporting)
12. [Relational Schema](#12-relational-schema)
13. [Running the Tests](#13-running-the-tests)
14. [Phase 1 Status](#14-phase-1-status)
15. [Known Limitations & Phase 2 Plan](#15-known-limitations--phase-2-plan)

---

## 1. Project Overview

This tool is a **comparative multi-pipeline ETL and reporting framework** for
NASA HTTP web server log analytics. It allows the user to select one of four
execution backends — **MapReduce**, **MongoDB**, **Apache Pig**, or **Apache
Hive** — and run the same ETL workload through that backend. The results are
stored in a relational database and rendered in a terminal report.

The goal is to study how different data-processing paradigms handle the same
semi-structured log analytics problem and compare them across implementation
style, runtime, batching behaviour, and reporting suitability.

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          main.py  (CLI)                          │
│                    click group: run / report / compare           │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                ┌────────────▼────────────┐
                │  controller/            │
                │  orchestrator.py        │
                │  (wires all layers)     │
                └──┬──────────┬──────────┘
                   │          │
       ┌───────────▼──┐  ┌────▼──────────────┐
       │  pipelines/  │  │  loader/           │
       │  ┌─────────┐ │  │  db_loader.py      │
       │  │mapreduce│ │  │  SQLAlchemy Core   │
       │  │mongodb  │ │  │  SQLite/MySQL/PG   │
       │  │pig      │ │  └────────┬───────────┘
       │  │hive     │ │           │
       │  └────┬────┘ │  ┌────────▼───────────┐
       │       │      │  │  reporter/          │
       └───────┼──────┘  │  report.py          │
               │         │  Rich terminal UI   │
       ┌───────▼──────┐  └────────────────────┘
       │  parser/     │
       │  log_parser.py│
       │  (shared by  │
       │   all pipelines)
       └──────────────┘
```

**Key design decisions:**

- **Single shared parser** (`parser/log_parser.py`) is used by every pipeline
  so that parsing semantics are identical across all four backends.
- **Each pipeline** subclasses `BasePipeline` and must return the same
  `RunMetadata` + `List[QueryResult]` types regardless of backend.
- **The loader** is pipeline-agnostic — it only consumes the standard return
  types, so adding a new pipeline never requires touching the DB layer.
- **SQLite** is the default database (zero setup); MySQL and PostgreSQL are
  supported via a one-line config change.

---

## 3. Directory Structure

```
nosql_etl/
│
├── main.py                        # CLI entry point
├── requirements.txt
├── README.md
│
├── config/
│   └── config.yaml                # Central configuration
│
├── data/                          # Drop NASA .gz files here
│   └── .gitkeep
│
├── parser/
│   ├── __init__.py
│   └── log_parser.py              # NASA Combined Log Format parser
│
├── pipelines/
│   ├── __init__.py                # Pipeline registry
│   ├── base.py                    # RunMetadata, QueryResult, BasePipeline
│   │
│   ├── mapreduce/
│   │   ├── __init__.py
│   │   └── pipeline.py            # Pure-Python Map → Shuffle → Reduce
│   │
│   ├── mongodb/
│   │   ├── __init__.py
│   │   └── pipeline.py            # pymongo + aggregation framework
│   │
│   ├── pig/
│   │   ├── __init__.py
│   │   ├── pipeline.py            # Subprocess invocation of Pig CLI
│   │   └── scripts/
│   │       └── etl.pig            # Pig Latin ETL script
│   │
│   └── hive/
│       ├── __init__.py
│       ├── pipeline.py            # Subprocess invocation of Hive CLI
│       └── scripts/
│           └── etl.hql            # HiveQL ETL script
│
├── loader/
│   ├── __init__.py
│   └── db_loader.py               # SQLAlchemy Core – schema + CRUD
│
├── reporter/
│   ├── __init__.py
│   └── report.py                  # Rich terminal report renderer
│
├── controller/
│   ├── __init__.py
│   └── orchestrator.py            # Wires pipeline → loader → reporter
│
└── tests/
    ├── test_parser.py
    ├── test_mapreduce_pipeline.py
    └── test_loader.py
```

---

## 4. Dependencies

### Python version
Python **3.9 or later** is required.

### Python packages

| Package | Purpose |
|---|---|
| `click` | CLI argument parsing |
| `pyyaml` | Configuration file loading |
| `sqlalchemy` | Relational DB abstraction (SQLite, MySQL, PostgreSQL) |
| `pymongo` | MongoDB driver (only needed for the MongoDB pipeline) |
| `rich` | Terminal table and progress rendering |
| `tabulate` | Fallback table rendering |

### Optional drivers (install only if not using SQLite)

| Package | Purpose |
|---|---|
| `pymysql` | MySQL driver for SQLAlchemy |
| `psycopg2-binary` | PostgreSQL driver for SQLAlchemy |

### External systems (only needed for specific pipelines)

| Pipeline | External requirement |
|---|---|
| `mapreduce` | **None** — runs entirely in-process |
| `mongodb` | **MongoDB** server ≥ 4.4 running on `localhost:27017` |
| `pig` | **Apache Pig** ≥ 0.17 installed and on `$PATH` |
| `hive` | **Apache Hive** ≥ 2.3 (or Beeline) on `$PATH` |

For **Phase 1**, only `mapreduce` and `mongodb` are required.

---

## 5. Setup & Installation

### Step 1 – Clone the repository
```bash
git clone <your-repo-url>
cd nosql_etl
```

### Step 2 – Create a virtual environment (recommended)
```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows
```

### Step 3 – Install dependencies
```bash
pip install -r requirements.txt
```

If you intend to use **MySQL**:
```bash
pip install pymysql
```

If you intend to use **PostgreSQL**:
```bash
pip install psycopg2-binary
```

---

## 6. Downloading the Dataset

The project requires the official NASA Kennedy Space Center HTTP access logs
from the Internet Traffic Archive. Download them into the `data/` folder:

```bash
mkdir -p data
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz -P data/
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz -P data/
```

Or with curl:
```bash
curl -L https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz -o data/NASA_access_log_Jul95.gz
curl -L https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz -o data/NASA_access_log_Aug95.gz
```

> **Do not decompress, edit, or preprocess the files.** The tool reads `.gz`
> files directly. Decompression is handled in-process by the parser.

---

## 7. Configuration

All settings live in `config/config.yaml`. Edit this file before running.

### Key settings

```yaml
etl:
  batch_size: 10000          # Records per batch — keep consistent across pipelines
  log_files:
    - ./data/NASA_access_log_Jul95.gz
    - ./data/NASA_access_log_Aug95.gz

database:
  type: sqlite               # sqlite | mysql | postgresql

mongodb:
  host: localhost
  port: 27017
  database: nosql_etl
```

### Switching to MySQL
```yaml
database:
  type: mysql
  mysql:
    host: localhost
    port: 3306
    database: nosql_etl
    username: root
    password: yourpassword
```

---

## 8. Running the Tool

All commands are run from the `nosql_etl/` root directory.

### Execute a pipeline

```bash
# MapReduce (no external dependencies)
python main.py run --pipeline mapreduce

# MongoDB (requires MongoDB server running)
python main.py run --pipeline mongodb

# Apache Pig (requires Pig installed)
python main.py run --pipeline pig

# Apache Hive (requires Hive installed)
python main.py run --pipeline hive
```

After the run completes the tool prints the `run_id`.

### View the report for a specific run

```bash
python main.py report --run-id <run_id_printed_above>
```

### View the most recent run (or comparison if multiple runs exist)

```bash
python main.py report
```

### View a side-by-side comparison of all stored runs

```bash
python main.py compare
```

### Custom config path

```bash
python main.py run --pipeline mongodb --config /path/to/my_config.yaml
```

---

## 9. Pipeline Descriptions

### MapReduce (`--pipeline mapreduce`)
A pure-Python implementation of the classical Map → Shuffle/Sort → Reduce
pattern. Records are processed in batches using the shared parser. Three
separate map/reduce pairs implement the three mandatory queries. No Hadoop
installation is required — the pipeline runs entirely in-process.

**Suitable for**: development, testing, baseline comparison, environments
without Hadoop.

---

### MongoDB (`--pipeline mongodb`)
Records are bulk-inserted into a MongoDB collection in batches. Three
server-side aggregation pipelines (`$group`, `$sort`, `$limit`, `$project`,
`$addFields`) implement the mandatory queries. Indexes are created on
`log_date + status_code`, `resource_path`, and `log_date + log_hour` before
queries run. The raw collection is dropped and recreated on each run for
idempotency.

**Suitable for**: demonstrating document-store aggregation; fast on the full
~3.5M-record dataset.

---

### Apache Pig (`--pipeline pig`)
The Python orchestrator writes a combined flat log file, then invokes the Pig
Latin script (`pipelines/pig/scripts/etl.pig`) via subprocess in local mode
(`pig -x local`). A custom UDF handles log parsing. Pig writes CSV part-files
to an output directory; the orchestrator reads them back into Python dicts.

**Requires**: Apache Pig ≥ 0.17 on `$PATH`.

---

### Apache Hive (`--pipeline hive`)
The Python orchestrator writes a flat log file, creates an external Hive table
pointing at it, then runs the HiveQL script (`pipelines/hive/scripts/etl.hql`)
via subprocess. The script materialises a cleaned `etl_logs` table and runs
three `CREATE TABLE AS SELECT` queries. Results are read back via `hive -e`.

**Requires**: Apache Hive ≥ 2.3 (or Beeline) on `$PATH`.

---

## 10. Mandatory Queries

All four pipelines implement the exact same three queries with the same output
schema.

### Query 1 — Daily Traffic Summary
For each `(log_date, status_code)` pair, compute total request count and total
bytes transferred.

**Output columns**: `log_date`, `status_code`, `request_count`, `total_bytes`

---

### Query 2 — Top 20 Requested Resources
Rank resource paths by request count (descending), take the top 20. For each,
compute total requests, total bytes, and the number of distinct requesting
hosts.

**Output columns**: `resource_path`, `request_count`, `total_bytes`,
`distinct_host_count`

---

### Query 3 — Hourly Error Analysis
For each `(log_date, log_hour)` pair, compute the count of 4xx/5xx requests,
the total request count, the error rate, and the distinct host count among
error-generating requests.

**Output columns**: `log_date`, `log_hour`, `error_request_count`,
`total_request_count`, `error_rate`, `distinct_error_hosts`

---

## 11. Batching & Runtime Reporting

- `batch_size` is set in `config/config.yaml` and controls how many log
  records are processed per batch.
- Batch IDs start at **1** and increment sequentially.
- The final batch may contain fewer than `batch_size` records and is still
  counted as a valid batch.
- **Average batch size** = `total_records / num_batches`
- **Runtime** is measured from the moment the tool starts reading input files
  until the final aggregated results are written to the relational DB.
  Dataset download time, software installation time, and report rendering time
  are excluded.
- For fair cross-pipeline comparisons keep `batch_size` identical across runs.

---

## 12. Relational Schema

All results are persisted in a relational DB (SQLite by default).

### `run_metadata`
| Column | Type | Notes |
|---|---|---|
| `run_id` | VARCHAR(64) PK | UUID per run |
| `pipeline_name` | VARCHAR(32) | mapreduce / mongodb / pig / hive |
| `batch_size` | INT | configured batch size |
| `total_records` | BIGINT | including malformed |
| `malformed_records` | BIGINT | failed parse count |
| `num_batches` | INT | |
| `avg_batch_size` | FLOAT | total_records / num_batches |
| `runtime_seconds` | FLOAT | wall-clock ETL time |
| `executed_at` | DATETIME | UTC |
| `query_runtimes` | TEXT | JSON blob: {query_name → seconds} |

### `query_results_q1` — Daily Traffic Summary
| Column | Type |
|---|---|
| `run_id` | VARCHAR(64) FK |
| `pipeline_name` | VARCHAR(32) |
| `batch_id` | INT |
| `executed_at` | DATETIME |
| `log_date` | VARCHAR(16) |
| `status_code` | INT |
| `request_count` | BIGINT |
| `total_bytes` | BIGINT |

### `query_results_q2` — Top Requested Resources
| Column | Type |
|---|---|
| `run_id` | VARCHAR(64) FK |
| `pipeline_name` | VARCHAR(32) |
| `batch_id` | INT |
| `executed_at` | DATETIME |
| `resource_path` | TEXT |
| `request_count` | BIGINT |
| `total_bytes` | BIGINT |
| `distinct_host_count` | INT |

### `query_results_q3` — Hourly Error Analysis
| Column | Type |
|---|---|
| `run_id` | VARCHAR(64) FK |
| `pipeline_name` | VARCHAR(32) |
| `batch_id` | INT |
| `executed_at` | DATETIME |
| `log_date` | VARCHAR(16) |
| `log_hour` | INT |
| `error_request_count` | BIGINT |
| `total_request_count` | BIGINT |
| `error_rate` | FLOAT |
| `distinct_error_hosts` | INT |

---

## 13. Running the Tests

```bash
pip install pytest
pytest tests/ -v
```

The test suite covers:
- `test_parser.py` — 10 unit tests for the log parser (valid lines, malformed
  lines, edge cases like missing bytes, missing protocol).
- `test_mapreduce_pipeline.py` — 9 integration tests for the MapReduce
  pipeline on a small synthetic log file. No NASA data required.
- `test_loader.py` — 6 tests for the DB loader using an in-memory SQLite
  database.

All 25 tests pass without any external services running.

---

## 14. Phase 1 Status

| Component | Status |
|---|---|
| Shared log parser | ✅ Complete |
| `BasePipeline` contract | ✅ Complete |
| MapReduce pipeline | ✅ Complete — fully working, no external deps |
| MongoDB pipeline | ✅ Complete — requires MongoDB server |
| Pig pipeline | ✅ Script + orchestrator written; requires Pig installed |
| Hive pipeline | ✅ Script + orchestrator written; requires Hive installed |
| DB loader (SQLite / MySQL / PG) | ✅ Complete |
| Terminal reporter | ✅ Complete |
| CLI (`run` / `report` / `compare`) | ✅ Complete |
| Unit + integration tests | ✅ 25 tests passing |
| README | ✅ This file |

**For Phase 1 review**, the two fully working pipelines without any additional
infrastructure are **MapReduce** and **MongoDB**.

---

## 15. Known Limitations & Phase 2 Plan

- The **Pig UDF jar** (`com.etl.pig.LogParserUDF`) is declared in the Pig
  script but the Java source is not yet written. For Phase 2 the UDF will be
  implemented in Java, compiled, and bundled in `pipelines/pig/lib/`.
- The **Hive** pipeline's `log_date` derivation uses HiveQL string functions
  that behave correctly for the NASA log timestamp format but have not been
  tested against a live Hive cluster in Phase 1.
- **Parallel batch processing** (threading/multiprocessing) is not yet
  implemented for the MapReduce pipeline. Phase 2 will add an optional
  `--workers N` flag.
- **Error-rate colour coding** in the reporter uses a fixed threshold
  (>10% = red, >5% = yellow). This will be made configurable in Phase 2.
