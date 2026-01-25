from dev_health_ops.audit.perf import format_perf_report, run_perf_audit


def test_perf_audit_unsupported_backend() -> None:
    report = run_perf_audit("sqlite:///tmp/test.db")
    assert report["status"] == "unchecked"
    assert report["db_error"] == "unsupported backend: sqlite"


def test_format_perf_report_unchecked() -> None:
    report = {
        "status": "unchecked",
        "db_backend": "clickhouse",
        "db_error": "connection failed",
        "threshold_ms": 1000,
        "lookback_minutes": 60,
        "limit": 20,
        "log_settings": {},
        "slow_queries": [],
        "notes": [],
    }
    output = format_perf_report(report)
    assert "status: unchecked" in output
    assert "db_error: connection failed" in output
