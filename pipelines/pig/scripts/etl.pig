-- ============================================================
-- etl.pig
-- Fully Pig-compatible ETL script
-- ============================================================

raw_lines = LOAD '$INPUT_FILES'
USING TextLoader()
AS (line:chararray);

-- ============================================================
-- Parse fields
-- ============================================================

parsed = FOREACH raw_lines GENERATE

    REGEX_EXTRACT(line, '^(\\S+)', 1)
        AS host:chararray,

    REGEX_EXTRACT(line, '\\[([^\\]]+)\\]', 1)
        AS ts_raw:chararray,

    REGEX_EXTRACT(
        line,
        '"(?:\\S+)\\s+(\\S+)',
        1
    ) AS resource_path:chararray,

    (int)REGEX_EXTRACT(
        line,
        '\\s+(\\d{3})\\s+',
        1
    ) AS status_code:int,

    (
        REGEX_EXTRACT(line, '\\s+(\\d+)$', 1) IS NULL
        ? 0L
        : (long)REGEX_EXTRACT(line, '\\s+(\\d+)$', 1)
    ) AS bytes_transferred:long;

-- ============================================================
-- Remove malformed rows
-- ============================================================

parsed_clean = FILTER parsed BY
    host IS NOT NULL AND
    ts_raw IS NOT NULL AND
    status_code IS NOT NULL AND
    resource_path IS NOT NULL;

-- ============================================================
-- Derive date + hour
-- ============================================================

parsed_final = FOREACH parsed_clean GENERATE

    host,

    CONCAT(
        CONCAT(
            SUBSTRING(
                STRSPLIT(
                    STRSPLIT(ts_raw, '/', 3).$2,
                    ':',
                    5
                ).$0,
                0,
                4
            ),
            '-'
        ),

        CONCAT(

            (CASE STRSPLIT(ts_raw, '/', 3).$1
                WHEN 'Jan' THEN '01'
                WHEN 'Feb' THEN '02'
                WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04'
                WHEN 'May' THEN '05'
                WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07'
                WHEN 'Aug' THEN '08'
                WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10'
                WHEN 'Nov' THEN '11'
                ELSE '12'
            END),

            CONCAT(
                '-',
                STRSPLIT(ts_raw, '/', 3).$0
            )
        )
    ) AS log_date:chararray,

    (int)STRSPLIT(
        STRSPLIT(ts_raw, '/', 3).$2,
        ':',
        5
    ).$1
    AS log_hour:int,

    resource_path,
    status_code,
    bytes_transferred;

-- ============================================================
-- QUERY 1 : Daily Traffic Summary
-- ============================================================

grp_q1 = GROUP parsed_final
BY (log_date, status_code);

q1 = FOREACH grp_q1 GENERATE

    group.log_date AS log_date,
    group.status_code AS status_code,

    COUNT(parsed_final)
        AS request_count,

    SUM(parsed_final.bytes_transferred)
        AS total_bytes;

q1_sorted = ORDER q1
BY log_date ASC,
   status_code ASC;

STORE q1_sorted
INTO '$OUTPUT_DIR/query1'
USING PigStorage(',');

-- ============================================================
-- QUERY 2 : Top Requested Resources
-- ============================================================

grp_q2 = GROUP parsed_final
BY resource_path;

q2_temp = FOREACH grp_q2 {

    uniq_hosts = DISTINCT parsed_final.host;

    GENERATE

        group AS resource_path,

        COUNT(parsed_final)
            AS request_count,

        SUM(parsed_final.bytes_transferred)
            AS total_bytes,

        COUNT(uniq_hosts)
            AS distinct_host_count;
};

-- Compute top-20 without a global ORDER (more stable in local mode)
q2_all = GROUP q2_temp ALL;
q2 = FOREACH q2_all {
    q2_sorted = ORDER q2_temp BY request_count DESC;
    q2_top = LIMIT q2_sorted 20;
    GENERATE FLATTEN(q2_top);
};

STORE q2
INTO '$OUTPUT_DIR/query2'
USING PigStorage(',');

-- ============================================================
-- QUERY 3 : Hourly Error Analysis
-- ============================================================

errors_only = FILTER parsed_final BY
    status_code >= 400 AND status_code <= 599;

grp_total = GROUP parsed_final
BY (log_date, log_hour);

totals = FOREACH grp_total GENERATE

    group.log_date AS log_date,
    group.log_hour AS log_hour,

    COUNT(parsed_final)
        AS total_request_count;

grp_errors = GROUP errors_only
BY (log_date, log_hour);

errors = FOREACH grp_errors {

    uniq_err_hosts = DISTINCT errors_only.host;

    GENERATE

        group.log_date AS log_date,
        group.log_hour AS log_hour,

        COUNT(errors_only)
            AS error_request_count,

        COUNT(uniq_err_hosts)
            AS distinct_error_hosts;
};

joined = JOIN
    totals BY (log_date, log_hour)
    LEFT OUTER,
    errors BY (log_date, log_hour);

q3 = FOREACH joined GENERATE

    totals::log_date
        AS log_date,

    totals::log_hour
        AS log_hour,

    (CASE
        WHEN errors::error_request_count IS NULL THEN 0L
        ELSE errors::error_request_count
    END) AS error_request_count,

    totals::total_request_count
        AS total_request_count,

    (
        (CASE
            WHEN errors::error_request_count IS NULL THEN 0.0
            ELSE (double)errors::error_request_count
        END)
        /
        (double)totals::total_request_count
    ) AS error_rate,

    (CASE
        WHEN errors::distinct_error_hosts IS NULL THEN 0L
        ELSE errors::distinct_error_hosts
    END) AS distinct_error_hosts;

q3_sorted = ORDER q3
BY log_date ASC,
   log_hour ASC;

STORE q3_sorted
INTO '$OUTPUT_DIR/query3'
USING PigStorage(',');
