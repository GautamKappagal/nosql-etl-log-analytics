-- ─────────────────────────────────────────────────────────────────────────────
-- etl.pig
-- Pig Latin script for the Multi-Pipeline ETL Framework.
--
-- Parameters injected by the Python orchestrator at runtime:
--   $INPUT_FILES  – path to the combined (raw, unfiltered) log file
--   $OUTPUT_DIR   – local base directory for query output folders
--   $BATCH_SIZE   – informational only (batching handled at Python level)
--   $QUERIES      – comma-separated subset of query names to run (optional)
--
-- Run in local mode (no Hadoop required):
--   pig -x local \
--       -param INPUT_FILES=/tmp/pig_etl_xxx/combined.log \
--       -param OUTPUT_DIR=/tmp/pig_out \
--       -param BATCH_SIZE=10000 \
--       -param QUERIES=query1_daily_traffic,query2_top_resources,query3_hourly_errors \
--       etl.pig
--
-- NOTE: This script receives ALL raw lines (including malformed ones).
-- Filtering is performed here natively so the pipeline satisfies the
-- authenticity requirement (no pre-filtering by the Python orchestrator).
-- ─────────────────────────────────────────────────────────────────────────────

-- ─── 1. Load raw log lines ────────────────────────────────────────────────────
raw_lines = LOAD '$INPUT_FILES' USING TextLoader() AS (line:chararray);

-- ─── 2. Extract fields via REGEX_EXTRACT ─────────────────────────────────────
-- Mirrors the Python parser's regex semantics.
-- Fields: host, ts_raw, http_method, resource_path, protocol_version,
--         status_code, bytes_transferred
parsed = FOREACH raw_lines GENERATE
    REGEX_EXTRACT(line, '^(\\S+)', 1)                             AS host:chararray,
    REGEX_EXTRACT(line, '\\[([^\\]]+)\\]', 1)                    AS ts_raw:chararray,
    REGEX_EXTRACT(line, '"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"', 1)  AS http_method:chararray,
    REGEX_EXTRACT(line, '"(?:\\S+)\\s+(\\S+)(?:\\s+\\S+)?"', 1)  AS resource_path:chararray,
    REGEX_EXTRACT(line, '"(?:\\S+)\\s+\\S+\\s+(\\S+)"', 1)       AS protocol_version_raw:chararray,
    (int)REGEX_EXTRACT(line, '\\s+(\\d{3})\\s+', 1)              AS status_code:int,
    (REGEX_EXTRACT(line, '\\s+(\\S+)$', 1) == '-' ? 0L :
        (long)REGEX_EXTRACT(line, '\\s+(\\d+)$', 1))             AS bytes_transferred:long;

-- ─── 3. Filter out records that failed parsing ────────────────────────────────
-- This is the native Pig filtering that enforces the same rules as the Python
-- parser: a record is valid only if host and ts_raw are non-null/non-empty.
parsed_clean = FILTER parsed BY
    host      IS NOT NULL AND host      != '' AND
    ts_raw    IS NOT NULL AND ts_raw    != '' AND
    status_code IS NOT NULL;

-- ─── 4. Derive log_date and log_hour from ts_raw ─────────────────────────────
-- ts_raw format: 01/Jul/1995:00:00:01 -0400
-- We use string splitting to avoid Joda-Time timezone format issues.
-- split(ts_raw, '/') → ['01', 'Jul', '1995:00:00:01 -0400']
-- split(day_year_time, ':') → ['1995', '00', '00', '01 -0400']  (for part [2])
parsed_final = FOREACH parsed_clean {
    -- Null-safe protocol version
    pv = (protocol_version_raw IS NULL ? '-' : protocol_version_raw);
    dt = ToDate(ts_raw, 'dd/MMM/yyyy:HH:mm:ss Z');
    GENERATE
        host,
        ts_raw,
        ToString(dt, 'yyyy-MM-dd')                 AS log_date:chararray,
        GetHour(dt)                                AS log_hour:int,
        http_method,
        resource_path,
        pv                                         AS protocol_version:chararray,
        status_code,
        bytes_transferred;
};

-- ─── Query 1: Daily Traffic Summary ──────────────────────────────────────────
grp_q1 = GROUP parsed_final BY (log_date, status_code);

q1 = FOREACH grp_q1 GENERATE
    FLATTEN(group) AS (log_date, status_code),
    COUNT(parsed_final)                AS request_count,
    SUM(parsed_final.bytes_transferred) AS total_bytes;

q1_sorted = ORDER q1 BY log_date ASC, status_code ASC;

STORE q1_sorted INTO '$OUTPUT_DIR/query1' USING PigStorage(',');

-- ─── Query 2: Top 20 Requested Resources ─────────────────────────────────────
-- NOTE: COUNT(DISTINCT bag.field) is NOT valid Pig syntax.
-- Use a nested FOREACH with DISTINCT instead.
grp_q2 = GROUP parsed_final BY resource_path;

q2_agg = FOREACH grp_q2 {
    uniq_hosts = DISTINCT parsed_final.host;
    GENERATE
        group AS resource_path,
        COUNT(parsed_final)                 AS request_count,
        SUM(parsed_final.bytes_transferred) AS total_bytes,
        COUNT(uniq_hosts)                   AS distinct_host_count;
};

q2_sorted = ORDER q2_agg BY request_count DESC;
q2 = LIMIT q2_sorted 20;

STORE q2 INTO '$OUTPUT_DIR/query2' USING PigStorage(',');

-- ─── Query 3: Hourly Error Analysis ──────────────────────────────────────────
errors_only = FILTER parsed_final BY (status_code >= 400 AND status_code <= 599);

grp_q3_total  = GROUP parsed_final BY (log_date, log_hour);
grp_q3_errors = GROUP errors_only  BY (log_date, log_hour);

q3_totals = FOREACH grp_q3_total GENERATE
    FLATTEN(group) AS (log_date, log_hour),
    COUNT(parsed_final) AS total_request_count;

-- Use nested FOREACH to count distinct error-generating hosts correctly.
q3_errors = FOREACH grp_q3_errors {
    uniq_err_hosts = DISTINCT errors_only.host;
    GENERATE
        FLATTEN(group) AS (log_date, log_hour),
        COUNT(errors_only)    AS error_request_count,
        COUNT(uniq_err_hosts) AS distinct_error_hosts;
};

-- LEFT OUTER JOIN so hours with zero errors still appear
q3_joined = JOIN q3_totals BY (log_date, log_hour)
    LEFT OUTER, q3_errors BY (log_date, log_hour);

q3 = FOREACH q3_joined GENERATE
    q3_totals::log_date AS log_date,
    q3_totals::log_hour AS log_hour,
    (q3_errors::error_request_count IS NULL ? 0L :
        q3_errors::error_request_count) AS error_request_count,
    q3_totals::total_request_count AS total_request_count,
    ((q3_errors::error_request_count IS NULL ? 0.0 :
        (double)q3_errors::error_request_count)
     / (double)q3_totals::total_request_count) AS error_rate,
    (q3_errors::distinct_error_hosts IS NULL ? 0L :
        q3_errors::distinct_error_hosts) AS distinct_error_hosts;

q3_sorted = ORDER q3 BY log_date ASC, log_hour ASC;

STORE q3_sorted INTO '$OUTPUT_DIR/query3' USING PigStorage(',');
