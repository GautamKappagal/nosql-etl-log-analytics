# Multi-Pipeline ETL and Reporting Framework

DAS 839 - NoSQL Systems End Semester Project

This project is a complete command-line ETL and reporting framework for NASA
HTTP server log analytics. It runs the same three analytical queries through
four execution backends:

- MapReduce
- MongoDB
- Apache Pig
- Apache Hive

Each run records metadata, batch statistics, malformed-record summaries, query
outputs, and runtime information into a relational database. The project can use
SQLite for local testing and MySQL for the final demonstration.

---

## Table of Contents

1. Project Goal
2. Features
3. Architecture
4. Directory Structure
5. Query Definitions
6. Pipeline Design
7. Database Schema
8. Batching Strategy
9. Requirements
10. Setup
11. Dataset Setup
12. Configuration
13. Running the Pipelines
14. Reporting Commands
15. MySQL Setup for Final Demo
16. WSL Commands for Hadoop, Hive, Pig, and MongoDB
17. Testing with Small Data
18. Troubleshooting
19. Evaluation Checklist
20. GitHub Notes

---

## 1. Project Goal

The aim of this project is to demonstrate a structured data-management workflow
for semi-structured web log data. The system is not just a collection of
independent scripts. It is a single tool with:

- one CLI interface,
- four selectable execution pipelines,
- three common queries,
- configurable batching,
- pipeline-level loading and cleaning,
- relational result loading,
- runtime and metadata capture,
- and a common reporting layer.

The workload uses the NASA Kennedy Space Center HTTP access logs from July and
August 1995.

---

## 2. Features

- CLI pipeline selection: `mapreduce`, `mongodb`, `pig`, `hive`
- CLI query selection: `q1`, `q2`, `q3`, or `all`
- Configurable batch size
- Support for gzipped `.gz` log files
- Shared schema and result contract across all pipelines
- Batch metadata table
- Malformed-record summary table
- Query result tables
- Runtime capture
- SQLite, MySQL, and PostgreSQL support through SQLAlchemy
- Rich terminal reports
- Comparison report across multiple runs
- Small dataset config for safe demo/testing

---

## 3. Architecture

```text
main.py
  |
  |-- controller/orchestrator.py
  |     |-- loads YAML config
  |     |-- resolves selected pipeline
  |     |-- creates database engine
  |     |-- initializes result schema
  |     |-- executes selected query or all queries
  |     |-- calls common reporter
  |
  |-- pipelines/
  |     |-- mapreduce/
  |     |-- mongodb/
  |     |-- pig/
  |     |-- hive/
  |
  |-- parser/log_parser.py
  |
  |-- loader/db_loader.py
  |
  |-- reporter/report.py
```

All pipeline classes implement the common `BasePipeline` contract and return:

```text
RunMetadata
List[QueryResult]
List[BatchRecord]
Dict[str, int] malformed_error_counts
```

This is what allows every backend to use the same relational loader and the
same report generator.

---

## 4. Directory Structure

```text
nosql-endterm-project/
  main.py
  requirements.txt
  README.md

  config/
    config.yaml
    config_small.yaml

  data/
    NASA_access_log_Jul95.gz
    NASA_access_log_Aug95.gz
    small_test/
      NASA_access_log_small.gz

  controller/
    orchestrator.py

  parser/
    log_parser.py

  pipelines/
    base.py
    __init__.py

    mapreduce/
      pipeline.py

    mongodb/
      pipeline.py

    pig/
      pipeline.py
      scripts/
        etl.pig

    hive/
      pipeline.py
      scripts/
        etl.hql

  loader/
    db_loader.py

  reporter/
    report.py

  tests/
    test_parser.py
    test_mapreduce_pipeline.py
    test_loader.py
```

---

## 5. Query Definitions

The same three mandatory queries are implemented for all four pipelines.

### Query 1: Daily Traffic Summary

For each `(log_date, status_code)` pair:

- count requests,
- sum bytes transferred.

Output columns:

```text
log_date
status_code
request_count
total_bytes
```

### Query 2: Top 20 Requested Resources

For each resource path:

- count requests,
- sum bytes transferred,
- count distinct requesting hosts,
- return the top 20 by request count.

Output columns:

```text
resource_path
request_count
total_bytes
distinct_host_count
```

### Query 3: Hourly Error Analysis

For each `(log_date, log_hour)` pair:

- count 4xx/5xx requests,
- count total requests,
- compute error rate,
- count distinct error-generating hosts.

Output columns:

```text
log_date
log_hour
error_request_count
total_request_count
error_rate
distinct_error_hosts
```

---

## 6. Pipeline Design

### MapReduce Pipeline

File:

```text
pipelines/mapreduce/pipeline.py
```

The MapReduce pipeline runs in-process and requires no Hadoop installation.

Important design point: loading and cleaning are part of the MapReduce pipeline
itself. Raw batch records flow through a dedicated load/clean MapReduce job:

```text
raw batch records
  -> load_clean mapper
  -> shuffle/group
  -> load_clean reducer
  -> cleaned records + batch metadata + malformed summary
  -> query MapReduce jobs
```

This satisfies the requirement that loading and cleaning should not be a
separate single-threaded preprocessing script.

### MongoDB Pipeline

File:

```text
pipelines/mongodb/pipeline.py
```

The MongoDB pipeline:

1. reads raw log batches,
2. parses records into documents,
3. bulk inserts cleaned documents into MongoDB,
4. creates indexes,
5. executes the three mandatory queries using MongoDB aggregation pipelines,
6. saves standardized results to the relational database.

MongoDB must be running on `localhost:27017`.

### Pig Pipeline

Files:

```text
pipelines/pig/pipeline.py
pipelines/pig/scripts/etl.pig
```

The Pig pipeline:

1. writes all raw input lines to a temporary file,
2. sends the raw file to Pig,
3. performs parsing, filtering, cleaning, grouping, ordering, and aggregation in
   Pig Latin,
4. reads Pig part files back into the common result format,
5. saves results to the relational database.

Pig local mode is used:

```bash
pig -x local
```

### Hive Pipeline

Files:

```text
pipelines/hive/pipeline.py
pipelines/hive/scripts/etl.hql
```

The Hive pipeline:

1. writes all raw input lines to a temporary file,
2. uploads the file to HDFS,
3. creates an external `raw_logs` table,
4. creates a parsed view,
5. materializes a cleaned `etl_logs` table,
6. creates query result tables using HiveQL,
7. reads results back through Hive,
8. saves standardized results to the relational database.

The default Hive execution engine is MapReduce:

```yaml
hive:
  execution_engine: mr
```

This avoids Tez runtime issues on local WSL installations while still keeping
the pipeline inside Hive.

---

## 7. Database Schema

The loader creates the following relational tables.

### `run_metadata`

One row per pipeline run.

```text
run_id
pipeline_name
batch_size
total_records
malformed_records
num_batches
avg_batch_size
runtime_seconds
executed_at
query_runtimes
```

### `batch_metadata`

One row per processed batch.

```text
run_id
batch_id
records_in_batch
malformed_in_batch
```

### `malformed_record_summary`

One row per malformed record type per run.

```text
run_id
error_type
count
```

### `query_results_q1`

Stores Query 1 output.

```text
run_id
pipeline_name
batch_id
executed_at
log_date
status_code
request_count
total_bytes
```

### `query_results_q2`

Stores Query 2 output.

```text
run_id
pipeline_name
batch_id
executed_at
resource_path
request_count
total_bytes
distinct_host_count
```

### `query_results_q3`

Stores Query 3 output.

```text
run_id
pipeline_name
batch_id
executed_at
log_date
log_hour
error_request_count
total_request_count
error_rate
distinct_error_hosts
```

---

## 8. Batching Strategy

Batching is controlled by:

```yaml
etl:
  batch_size: 10000
```

For example, if `batch_size` is `10000`, the framework reads up to 10,000 log
records per batch. Batch IDs start at `1` and increase sequentially. The final
batch may contain fewer than `batch_size` records.

The framework records:

```text
records_in_batch
malformed_in_batch
num_batches
avg_batch_size = total_records / num_batches
```

For fair comparison, use the same batch size for all pipelines during a
comparative run.

---

## 9. Requirements

### Python

Python 3.9 or later.

Python packages:

```text
click
pyyaml
pymongo
sqlalchemy
rich
tabulate
```

Optional database drivers:

```text
pymysql
psycopg2-binary
```

### External Systems

| Pipeline | Requirement |
|---|---|
| MapReduce | No external service |
| MongoDB | MongoDB server on localhost:27017 |
| Pig | Apache Pig 0.17+ |
| Hive | Hadoop, HDFS, YARN, Hive |

---

## 10. Setup

### Linux / WSL setup

Go to the project:

```bash
cd ~/nosql-endterm-project
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

If using MySQL:

```bash
pip install pymysql
```

If using PostgreSQL:

```bash
pip install psycopg2-binary
```

### Windows PowerShell setup

Go to the project:

```powershell
cd C:\Users\raipr\Downloads\nosql-endterm-project
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

---

## 11. Dataset Setup

Create the data folder:

```bash
mkdir -p data
```

Download the NASA logs:

```bash
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz -P data/
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz -P data/
```

Or with `curl`:

```bash
curl -L https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz -o data/NASA_access_log_Jul95.gz
curl -L https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz -o data/NASA_access_log_Aug95.gz
```

Do not manually decompress the files for MapReduce or MongoDB. The parser reads
`.gz` files directly.

---

## 12. Configuration

Main config file:

```text
config/config.yaml
```

Small demo config:

```text
config/config_small.yaml
```

### Default SQLite config

SQLite is easiest for local testing:

```yaml
database:
  type: sqlite
  sqlite:
    path: ./etl_results.db
```

### MySQL config

For final evaluation, MySQL is recommended:

```yaml
database:
  type: mysql
  mysql:
    host: localhost
    port: 3306
    database: nosql_etl
    username: root
    password: your_password
```

### ETL config

Full dataset:

```yaml
etl:
  batch_size: 10000
  log_files:
    - ./data/NASA_access_log_Jul95.gz
    - ./data/NASA_access_log_Aug95.gz
```

Small test dataset:

```yaml
etl:
  batch_size: 10000
  log_files:
    - ./data/small_test/NASA_access_log_small.gz
```

### Hive config

```yaml
hive:
  executable: hive
  hdfs_executable: hdfs
  execution_engine: mr
  warehouse_dir: /user/hive/warehouse
  output_dir: /tmp/hive_etl_output
```

### Pig config

```yaml
pig:
  executable: pig
  output_dir: /tmp/pig_etl_output
```

### MongoDB config

```yaml
mongodb:
  host: localhost
  port: 27017
  database: nosql_etl
  raw_collection: web_logs
```

---

## 13. Running the Pipelines

All commands should be run from the project root.

```bash
cd ~/nosql-endterm-project
source .venv/bin/activate
```

On Windows:

```powershell
cd C:\Users\raipr\Downloads\nosql-endterm-project
.\.venv\Scripts\Activate.ps1
```

### MapReduce

Run all queries:

```bash
python main.py run --pipeline mapreduce --config config/config_small.yaml
```

Run only Query 1:

```bash
python main.py run --pipeline mapreduce --query q1 --config config/config_small.yaml
```

Run only Query 2:

```bash
python main.py run --pipeline mapreduce --query q2 --config config/config_small.yaml
```

Run only Query 3:

```bash
python main.py run --pipeline mapreduce --query q3 --config config/config_small.yaml
```

Override batch size:

```bash
python main.py run --pipeline mapreduce --batch-size 5000 --config config/config_small.yaml
```

### MongoDB

Start MongoDB in WSL if installed as a service:

```bash
sudo service mongod start
```

If WSL does not have a MongoDB service, start manually:

```bash
sudo mkdir -p /data/db
sudo chown -R "$USER":"$USER" /data/db
mongod --dbpath /data/db --bind_ip 127.0.0.1 --port 27017
```

Leave that terminal open. In a second terminal:

```bash
cd ~/nosql-endterm-project
source .venv/bin/activate
mongosh --eval "db.runCommand({ ping: 1 })"
python main.py run --pipeline mongodb --config config/config_small.yaml
```

Run one query:

```bash
python main.py run --pipeline mongodb --query q1 --config config/config_small.yaml
```

### Pig

Check Pig:

```bash
pig -version
```

Clean old Pig output and run:

```bash
rm -rf /tmp/pig_etl_output
python main.py run --pipeline pig --config config/config_small.yaml
```

Run one query:

```bash
rm -rf /tmp/pig_etl_output
python main.py run --pipeline pig --query q1 --config config/config_small.yaml
```

### Hive

Start Hadoop services:

```bash
start-dfs.sh
start-yarn.sh
jps
```

Expected important processes:

```text
NameNode
DataNode
ResourceManager
NodeManager
```

Clean old Hive input:

```bash
hdfs dfs -rm -r -f /tmp/hive_input
```

Run Hive:

```bash
python main.py run --pipeline hive --config config/config_small.yaml
```

Run one query:

```bash
hdfs dfs -rm -r -f /tmp/hive_input
python main.py run --pipeline hive --query q1 --config config/config_small.yaml
```

Monitor YARN:

```bash
yarn application -list
yarn node -list
```

Check a specific YARN application:

```bash
yarn application -status <application_id>
```

---

## 14. Reporting Commands

Show the latest run or comparison:

```bash
python main.py report --config config/config_small.yaml
```

Show a specific run:

```bash
python main.py report --run-id <run_id> --config config/config_small.yaml
```

Show comparison across all runs:

```bash
python main.py compare --config config/config_small.yaml
```

Example workflow:

```bash
python main.py run --pipeline mapreduce --query q1 --config config/config_small.yaml
python main.py run --pipeline mongodb --query q1 --config config/config_small.yaml
python main.py run --pipeline pig --query q1 --config config/config_small.yaml
python main.py run --pipeline hive --query q1 --config config/config_small.yaml
python main.py compare --config config/config_small.yaml
```

---

## 15. MySQL Setup for Final Demo

Install driver:

```bash
pip install pymysql
```

Create database:

```bash
mysql -u root -p
```

Inside MySQL:

```sql
CREATE DATABASE IF NOT EXISTS nosql_etl;
SHOW DATABASES;
EXIT;
```

Update `config/config.yaml`:

```yaml
database:
  type: mysql
  mysql:
    host: localhost
    port: 3306
    database: nosql_etl
    username: root
    password: your_password
```

Run a pipeline:

```bash
python main.py run --pipeline mapreduce --config config/config.yaml
```

Inspect tables:

```bash
mysql -u root -p nosql_etl
```

Inside MySQL:

```sql
SHOW TABLES;
SELECT * FROM run_metadata ORDER BY executed_at DESC LIMIT 5;
SELECT * FROM batch_metadata LIMIT 10;
SELECT * FROM malformed_record_summary LIMIT 10;
SELECT * FROM query_results_q1 LIMIT 10;
SELECT * FROM query_results_q2 LIMIT 10;
SELECT * FROM query_results_q3 LIMIT 10;
EXIT;
```

---

## 16. WSL Commands for Hadoop, Hive, Pig, and MongoDB

### Hadoop / YARN

Start:

```bash
start-dfs.sh
start-yarn.sh
jps
```

Stop:

```bash
stop-yarn.sh
stop-dfs.sh
```

Check HDFS:

```bash
hdfs dfs -ls /
hdfs dfs -ls /tmp
hdfs dfs -du -h /tmp/hive_input
```

Clean Hive input:

```bash
hdfs dfs -rm -r -f /tmp/hive_input
```

### Hive

Check version:

```bash
hive --version
```

Show project Hive tables:

```bash
hive --hiveconf hive.execution.engine=mr -e "USE nosql_etl_hive; SHOW TABLES;"
```

Check cleaned rows:

```bash
hive --hiveconf hive.execution.engine=mr -e "USE nosql_etl_hive; SELECT COUNT(*) FROM etl_logs;"
```

Preview Query 1:

```bash
hive --hiveconf hive.execution.engine=mr -e "USE nosql_etl_hive; SELECT * FROM q1_daily_traffic LIMIT 10;"
```

### Pig

Check version:

```bash
pig -version
```

Clean output:

```bash
rm -rf /tmp/pig_etl_output
```

### MongoDB

Check binaries:

```bash
which mongod
which mongosh
```

Start manually if service is unavailable:

```bash
sudo mkdir -p /data/db
sudo chown -R "$USER":"$USER" /data/db
mongod --dbpath /data/db --bind_ip 127.0.0.1 --port 27017
```

Ping:

```bash
mongosh --eval "db.runCommand({ ping: 1 })"
```

Inspect collection:

```bash
mongosh nosql_etl --eval "db.web_logs.countDocuments()"
mongosh nosql_etl --eval "db.web_logs.findOne()"
```

---

## 17. Testing with Small Data

Create a 10,000-line sample:

```bash
cd ~/nosql-endterm-project
mkdir -p data/small_test
zcat data/NASA_access_log_Jul95.gz | head -n 10000 | gzip > data/small_test/NASA_access_log_small.gz
```

Create small config:

```bash
cp config/config.yaml config/config_small.yaml
nano config/config_small.yaml
```

Set:

```yaml
etl:
  batch_size: 10000
  log_files:
    - ./data/small_test/NASA_access_log_small.gz
```

Run all pipelines on small data:

```bash
python main.py run --pipeline mapreduce --config config/config_small.yaml
python main.py run --pipeline mongodb --config config/config_small.yaml
rm -rf /tmp/pig_etl_output
python main.py run --pipeline pig --config config/config_small.yaml
hdfs dfs -rm -r -f /tmp/hive_input
python main.py run --pipeline hive --config config/config_small.yaml
```

Compare:

```bash
python main.py compare --config config/config_small.yaml
```

---

## 18. Troubleshooting

### SLF4J warnings

Messages such as the following are usually warnings, not the actual failure:

```text
SLF4J: Class path contains multiple SLF4J bindings.
```

Look for the real error after the warning block.

### Hive fails with TezTask

Error:

```text
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.tez.TezTask
```

Use MapReduce execution engine:

```yaml
hive:
  execution_engine: mr
```

The pipeline passes this to Hive:

```bash
--hiveconf hive.execution.engine=mr
```

### Hive stuck at 0 percent or 5 percent

Check YARN:

```bash
yarn application -list
yarn node -list
```

Check the application:

```bash
yarn application -status <application_id>
```

If no NodeManager is active:

```bash
stop-yarn.sh
stop-dfs.sh
start-dfs.sh
start-yarn.sh
jps
```

### Pig output already exists

Clean output:

```bash
rm -rf /tmp/pig_etl_output
```

The Python wrapper also clears this directory before running.

### MongoDB service not found

If this fails:

```bash
sudo service mongod start
```

Run MongoDB manually:

```bash
sudo mkdir -p /data/db
sudo chown -R "$USER":"$USER" /data/db
mongod --dbpath /data/db --bind_ip 127.0.0.1 --port 27017
```

### Python cannot import packages

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### MySQL driver missing

Install:

```bash
pip install pymysql
```

---

## 19. Evaluation Checklist

During the demonstration, show the following:

1. CLI pipeline selection:

```bash
python main.py run --pipeline mapreduce --query q1 --config config/config_small.yaml
```

2. Query selection:

```bash
python main.py run --pipeline hive --query q1 --config config/config_small.yaml
```

3. Batch size:

```bash
python main.py run --pipeline mapreduce --batch-size 5000 --config config/config_small.yaml
```

4. Batch metadata:

```sql
SELECT * FROM batch_metadata LIMIT 10;
```

5. Malformed summary:

```sql
SELECT * FROM malformed_record_summary LIMIT 10;
```

6. Run metadata:

```sql
SELECT run_id, pipeline_name, batch_size, total_records, malformed_records,
       num_batches, avg_batch_size, runtime_seconds
FROM run_metadata
ORDER BY executed_at DESC
LIMIT 10;
```

7. Query result tables:

```sql
SELECT * FROM query_results_q1 LIMIT 10;
SELECT * FROM query_results_q2 LIMIT 10;
SELECT * FROM query_results_q3 LIMIT 10;
```

8. Common reporting layer:

```bash
python main.py report --config config/config_small.yaml
python main.py compare --config config/config_small.yaml
```

9. All four pipelines:

```bash
python main.py run --pipeline mapreduce --query q1 --config config/config_small.yaml
python main.py run --pipeline mongodb --query q1 --config config/config_small.yaml
python main.py run --pipeline pig --query q1 --config config/config_small.yaml
python main.py run --pipeline hive --query q1 --config config/config_small.yaml
```

---

## 20. GitHub Notes

Commit source/config files:

```bash
git add README.md \
  config/config.yaml \
  config/config_small.yaml \
  loader/db_loader.py \
  pipelines/mapreduce/pipeline.py \
  pipelines/mongodb/pipeline.py \
  pipelines/pig/pipeline.py \
  pipelines/pig/scripts/etl.pig \
  pipelines/hive/pipeline.py \
  pipelines/hive/scripts/etl.hql
```

Do not commit generated files:

```text
etl_results.db
derby.log
__pycache__/
*.pyc
```

Commit:

```bash
git status
git commit -m "Complete multi-pipeline ETL framework"
git push
```

If the small dataset is allowed by the instructor and small enough, you may
also commit:

```bash
git add data/small_test/NASA_access_log_small.gz
```

Otherwise, keep datasets out of GitHub and document how to create them with the
commands above.
