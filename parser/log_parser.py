"""
parser/log_parser.py
────────────────────
Parses NASA Kennedy Space Center HTTP access logs (Combined Log Format).

Log line example:
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245

Extracted structured fields:
host, timestamp, log_date, log_hour, http_method, resource_path,
protocol_version, status_code, bytes_transferred

Rules (per project spec):
• bytes == '-' → treated as 0
• Any line that cannot be parsed → returned as (None, error_reason)
The caller is responsible for counting these; they must NOT be silently dropped.
"""

import re
import gzip
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

# ─── Regex patterns ──────────────────────────────────────────────────────────

# Matches the full Combined Log Format line.
# Group layout:
# 1 host
# 2 timestamp (inside [])
# 3 request (inside "")
# 4 status_code
# 5 bytes
_LOG_RE = re.compile(
    r'^(\S+)'                       # host
    r'\s+\S+\s+\S+'                # ident and authuser (always "-" in NASA logs)
    r'\s+\[([^\]]+)\]'             # [timestamp]
    r'\s+"([^"]*)"'                # "request"
    r'\s+(\S+)'                    # status_code
    r'\s+(\S+)'                    # bytes
)

# Matches the three parts of an HTTP request string: METHOD PATH PROTOCOL
_REQUEST_RE = re.compile(r'^(\S+)\s+(\S+)(?:\s+(\S+))?$')

# Timestamp formats seen in NASA logs
_TS_FORMATS = [
    '%d/%b/%Y:%H:%M:%S %z',       # with numeric offset e.g. -0400
    '%d/%b/%Y:%H:%M:%S %Z',       # with named zone e.g. EDT
    '%d/%b/%Y:%H:%M:%S',          # without tz
]


# ─── Public API ───────────────────────────────────────────────────────────────

def parse_log_line(line: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Parse a single raw log line.

    Returns
    -------
    (record, None) on success
    (None, error_reason) on failure
    """
    line = line.strip()
    if not line:
        return None, "EMPTY_LINE"

    m = _LOG_RE.match(line)
    if not m:
        return None, "REGEX_NO_MATCH"

    host = m.group(1)
    ts_raw = m.group(2)
    request_raw = m.group(3)
    status_raw = m.group(4)
    bytes_raw = m.group(5)

    # ── Timestamp ────────────────────────────────────────────────────────────
    dt = _parse_timestamp(ts_raw)
    if dt is None:
        return None, "BAD_TIMESTAMP"

    # ── Request string ───────────────────────────────────────────────────────
    rm = _REQUEST_RE.match(request_raw.strip())
    if rm:
        http_method = rm.group(1)
        resource_path = rm.group(2)
        protocol_version = rm.group(3) if rm.group(3) else "-"
    else:
        # Malformed request line – keep what we have rather than discard
        http_method = "-"
        resource_path = request_raw.strip() or "-"
        protocol_version = "-"

    # ── Status code ──────────────────────────────────────────────────────────
    try:
        status_code = int(status_raw)
    except ValueError:
        status_code = 0

    # ── Bytes ────────────────────────────────────────────────────────────────
    if bytes_raw == "-":
        bytes_transferred = 0
    else:
        try:
            bytes_transferred = int(bytes_raw)
        except ValueError:
            bytes_transferred = 0

    return {
        "host": host,
        "timestamp": dt.isoformat(),
        "log_date": dt.strftime("%Y-%m-%d"),
        "log_hour": dt.hour,
        "http_method": http_method,
        "resource_path": resource_path,
        "protocol_version": protocol_version,
        "status_code": status_code,
        "bytes_transferred": bytes_transferred,
    }, None


def read_log_file(filepath: str) -> Iterator[str]:
    """Yield raw lines from a plain or gzipped log file."""
    opener = gzip.open if filepath.endswith(".gz") else open
    with opener(filepath, "rt", encoding="latin-1", errors="replace") as fh:
        for line in fh:
            yield line


def batch_read_log_files(
    filepaths: List[str],
    batch_size: int,
) -> Iterator[Tuple[int, List[str]]]:
    """
    Read one or more log files and yield (batch_id, lines) tuples.

    batch_id starts at 1 and increments for every full or partial batch.
    The final batch may contain fewer than batch_size lines; it is still
    yielded and must be counted as a valid batch.
    """
    batch_id = 1
    current: List[str] = []

    for filepath in filepaths:
        for line in read_log_file(filepath):
            stripped = line.strip()
            if stripped:
                current.append(stripped)
            if len(current) >= batch_size:
                yield batch_id, current
                batch_id += 1
                current = []

    if current:
        yield batch_id, current


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_timestamp(ts_raw: str) -> Optional[datetime]:
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(ts_raw, fmt)
        except ValueError:
            continue
    return None
