"""CHAOS-6903: the Python billing-edge Deployment is gone from the chart.

The billing host is served by the go-api billing-edge listener (CHAOS-6520),
routed by the deploy chart's `ingress.goApiHosts` entry with `billingEdge:
true`. This chart no longer renders a Python billing-edge workload, and it
must not go silent about it: a values file that still enables it, or still
routes an Ingress path to it, fails the render with a message that names the
replacement, instead of deploying nothing while the operator believes an edge
is up.
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
            'ingress.hosts=[{"host":"h","paths":[{"path":"/","pathType":"Prefix","service":"api"}]}]',
        )
    )
    named = [
        (doc["kind"], doc["metadata"]["name"])
        for doc in docs
        if "billing-edge" in doc["metadata"]["name"]
    ]
    assert named == [], named


def test_enabling_the_removed_python_edge_fails_the_render() -> None:
    completed = _template("--set", "billingEdge.enabled=true")
    assert completed.returncode != 0, completed.stdout[:400]
    assert "billingEdge" in completed.stderr, completed.stderr
    assert "goApiBillingEdge" in completed.stderr, completed.stderr


def test_disabled_billing_edge_values_still_render() -> None:
    # `billingEdge: {enabled: false}` is what older values files carry.
    _docs(_template("--set", "billingEdge.enabled=false"))


def test_routing_an_ingress_path_to_the_removed_edge_fails_the_render() -> None:
    completed = _template(
        "--set",
        "ingress.enabled=true",
        "--set-json",
        'ingress.hosts=[{"host":"h","paths":[{"path":"/","pathType":"Prefix","service":"billing-edge"}]}]',
    )
    assert completed.returncode != 0, completed.stdout[:400]
    assert "billing-edge" in completed.stderr, completed.stderr
    assert "goApiBillingEdge" in completed.stderr, completed.stderr
