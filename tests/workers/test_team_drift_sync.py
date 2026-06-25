from __future__ import annotations

from dev_health_ops.workers.team_drift_sync import _provider_scan_complete


def test_provider_scan_complete_accepts_plain_success() -> None:
    assert _provider_scan_complete({"status": "success"})


def test_provider_scan_complete_rejects_truncated_or_warning_results() -> None:
    assert not _provider_scan_complete({"status": "success", "complete": False})
    assert not _provider_scan_complete({"status": "success", "warnings": ["bounded"]})
    assert not _provider_scan_complete({"status": "skipped"})
