-- ─────────────────────────────────────────────────────────────────────────────
-- etl.hql
-- HiveQL script for the Multi-Pipeline ETL Framework.
--
-- Variables substituted by the Python orchestrator via --hivevar:
-- INPUT_TABLE – name of the external table pointing at the log data
-- OUTPUT_DB – target Hive database for result tables
-- RUN_ID – unique UUID for this run (inserted into result tables)
-- PIPELINE_NAME – always 'hive'
-- BATCH_ID – last batch id reported by the Python layer
-- ─────────────────────────────────────────────────────────────────────────────

-- ─── Setup ────────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ${hivevar:OUTPUT_DB};
USE ${hivevar:OUTPUT_DB};

-- ─── External raw-data table ──────────────────────────────────────────────────
-- The Python orchestrator writes the combined flat log file to a known
-- HDFS (or local-FS) path before invoking this script.
DROP TABLE IF EXISTS raw_logs;
CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
    line STRING
)
STORED AS TEXTFILE
LOCATION '${hivevar:INPUT_LOCATION}';

-- ─── Parsed view ─────────────────────────────────────────────────────────────
-- Uses a Hive SerDe regex to extract the core fields in one pass.
-- The regex mirrors the Python parser's logic so semantics are identical.
DROP VIEW IF EXISTS parsed_logs;
CREATE VIEW parsed_logs AS
SELECT
    regexp_extract(line,
        '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
        1) AS host,
    -- timestamp as-is; date/hour extracted below
    regexp_extract(line,
        '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
        2) AS ts_raw,
    regexp_extract(
        regexp_extract(line,
            '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
            3),
        '^(\\S+)',1) AS http_method,
    regexp_extract(
        regexp_extract(line,
            '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
            3),
        '^\\S+\\s+(\\S+)',1) AS resource_path,
    CAST(
        regexp_extract(line,
            '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
            4)
        AS INT) AS status_code,
    CASE
        WHEN regexp_extract(line,
            '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
            5) = '-' THEN 0
        ELSE CAST(regexp_extract(line,
            '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
            5) AS BIGINT)
    END AS bytes_transferred
FROM raw_logs
WHERE regexp_extract(line,
    '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\S+)\\s+(\\S+)',
    1) != '';

-- Materialise with date and hour derived from ts_raw
DROP TABLE IF EXISTS etl_logs;
CREATE TABLE etl_logs AS
SELECT
    host,
    ts_raw,
    concat_ws('-',
        lpad(split(split(ts_raw,'/')[2],':')[0],4,'0'),
        CASE split(ts_raw,'/')[1]
            WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
            WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
            WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
            WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
            ELSE '00'
        END,
        lpad(split(ts_raw,'/')[0],2,'0')
    ) AS log_date,
    CAST(split(split(ts_raw,'/')[2],':')[1] AS INT) AS log_hour,
    http_method,
    resource_path,
    status_code,
    bytes_transferred
FROM parsed_logs
WHERE host IS NOT NULL AND host != '';

-- ─── Query 1: Daily Traffic Summary ──────────────────────────────────────────
DROP TABLE IF EXISTS q1_daily_traffic;
CREATE TABLE q1_daily_traffic AS
SELECT
    log_date,
    status_code,
    COUNT(*) AS request_count,
    SUM(bytes_transferred) AS total_bytes
FROM etl_logs
GROUP BY log_date, status_code
ORDER BY log_date, status_code;

-- ─── Query 2: Top 20 Requested Resources ─────────────────────────────────────
DROP TABLE IF EXISTS q2_top_resources;
CREATE TABLE q2_top_resources AS
SELECT
    resource_path,
    COUNT(*) AS request_count,
    SUM(bytes_transferred) AS total_bytes,
    COUNT(DISTINCT host) AS distinct_host_count
FROM etl_logs
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;

-- ─── Query 3: Hourly Error Analysis ──────────────────────────────────────────
DROP TABLE IF EXISTS q3_hourly_errors;
CREATE TABLE q3_hourly_errors AS
SELECT
    log_date,
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        AS error_request_count,
    COUNT(*) AS total_request_count,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        / COUNT(*) AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599
        THEN host ELSE NULL END) AS distinct_error_hosts
FROM etl_logs
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;
