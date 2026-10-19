"""CHAOS-6903: the Python billing-edge Deployment is gone from the chart.

Every other guard of this file moved to Go (internal/cigates/chartgates/workers_chart_test.go,
billingEdge*). This one test stays because contracts/jobs/v1/transitional-inventory.json names it as
the acceptance test of the billing row, and tests/workers/test_transitional_inventory_contract.py
resolves that id with ast.parse; it leaves with that inventory contract.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


def _template(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["helm", "template", "r", str(_CHART), *args],
        capture_output=True,
        text=True,
    )


def _docs(completed: subprocess.CompletedProcess[str]) -> list[dict]:
    assert completed.returncode == 0, completed.stderr
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


def test_no_workload_or_service_is_named_billing_edge() -> None:
    docs = _docs(
        _template(
            "--set",
            "ingress.enabled=true",
            "--set-json",
            'ingress.hosts=[{"host":"h","paths":[{"path":"/","pathType":"Prefix","service":"web"}]}]',
        )
    )
    named = [
        (doc["kind"], doc["metadata"]["name"])
        for doc in docs
        if "billing-edge" in doc["metadata"]["name"]
    ]
    assert named == [], named
