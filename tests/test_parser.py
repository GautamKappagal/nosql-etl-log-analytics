"""
tests/test_parser.py
────────────────────
Unit tests for parser/log_parser.py.

Run with: pytest tests/
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from parser.log_parser import parse_log_line


# ─── Valid lines ──────────────────────────────────────────────────────────────

VALID_LINE = (
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] '
    '"GET /history/apollo/ HTTP/1.0" 200 6245'
)

VALID_LINE_NO_BYTES = (
    'uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] '
    '"GET / HTTP/1.0" 304 -'
)

VALID_LINE_NO_PROTOCOL = (
    'gateway.uky.edu - - [02/Jul/1995:13:54:51 -0400] '
    '"GET /shuttle/countdown/liftoff.html" 200 0'
)


def test_valid_line_fields():
    record, err = parse_log_line(VALID_LINE)
    assert err is None
    assert record["host"] == "199.72.81.55"
    assert record["log_date"] == "1995-07-01"
    assert record["log_hour"] == 0
    assert record["http_method"] == "GET"
    assert record["resource_path"] == "/history/apollo/"
    assert record["protocol_version"] == "HTTP/1.0"
    assert record["status_code"] == 200
    assert record["bytes_transferred"] == 6245


def test_missing_bytes_becomes_zero():
    record, err = parse_log_line(VALID_LINE_NO_BYTES)
    assert err is None
    assert record["bytes_transferred"] == 0
    assert record["status_code"] == 304


def test_no_protocol_in_request():
    record, err = parse_log_line(VALID_LINE_NO_PROTOCOL)
    assert err is None
    assert record["protocol_version"] == "-"
    assert record["resource_path"] == "/shuttle/countdown/liftoff.html"


def test_empty_line_returns_error():
    record, err = parse_log_line("")
    assert record is None
    assert err == "EMPTY_LINE"


def test_whitespace_line_returns_error():
    record, err = parse_log_line(" \t ")
    assert record is None
    assert err == "EMPTY_LINE"


def test_garbage_line_returns_error():
    record, err = parse_log_line("this is not a log line at all")
    assert record is None
    assert err == "REGEX_NO_MATCH"


def test_post_method():
    line = (
        'ftp.digex.com - - [01/Jul/1995:00:01:11 -0400] '
        '"POST /cgi-bin/upload HTTP/1.0" 404 0'
    )
    record, err = parse_log_line(line)
    assert err is None
    assert record["http_method"] == "POST"
    assert record["status_code"] == 404


def test_status_code_500():
    line = (
        'badserver.nasa.gov - - [15/Jul/1995:12:00:00 -0400] '
        '"GET /missing.html HTTP/1.0" 500 0'
    )
    record, err = parse_log_line(line)
    assert err is None
    assert record["status_code"] == 500


def test_log_hour_extraction():
    line = (
        '192.168.1.1 - - [10/Aug/1995:23:59:00 -0400] '
        '"GET / HTTP/1.0" 200 1024'
    )
    record, err = parse_log_line(line)
    assert err is None
    assert record["log_hour"] == 23
    assert record["log_date"] == "1995-08-10"
