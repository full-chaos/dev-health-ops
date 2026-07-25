"""Cross-language drift guard for the report query metric registry."""

from __future__ import annotations

import json
from pathlib import Path

from dev_health_ops.reports.metric_registry import METRIC_REGISTRY
from scripts.export_report_metric_registry import build_payload

ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = ROOT / "internal/jobs/report/metric_registry.json"


def test_go_report_metric_registry_matches_every_python_definition() -> None:
    checked_in = json.loads(ARTIFACT.read_text(encoding="utf-8"))

    assert checked_in == build_payload()
    assert len(checked_in["metrics"]) == len(METRIC_REGISTRY)
    assert {metric["canonical_name"] for metric in checked_in["metrics"]} == set(
        METRIC_REGISTRY
    )

    for metric in checked_in["metrics"]:
        definition = METRIC_REGISTRY[metric["canonical_name"]]
        assert metric["display_name"] == definition.display_name
        assert metric["source_table"] == definition.source_table
        assert metric["unit"] == definition.unit
        assert tuple(metric["dimensions"]) == definition.dimensions
