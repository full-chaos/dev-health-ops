"""The chart renders no CronJob, and its values carry no `cronjobs` block.

Daily metrics and provider sync are owned by the Go scheduler. The two default
CronJobs the chart used to ship ran verbs that no longer exist in the image they
targeted, so they are gone; this pins that they stay gone. Rendering is the
behaviour that matters, so each value set below is rendered with helm and every
document's kind is read, rather than grepping the templates directory.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"
_RELEASE = "no-cronjobs"

_VALUE_SETS = {
    "default": [],
    "quickstart": ["-f", str(_CHART / "values-quickstart.yaml")],
    "go-workers-and-go-api": [
        "--set",
        "goWorkers.enabled=true",
        "--set",
        "goApi.enabled=true",
    ],
}


def _render(extra: list[str]) -> list[dict]:
    completed = subprocess.run(
        ["helm", "template", _RELEASE, str(_CHART), *extra],
        check=True,
        capture_output=True,
        text=True,
    )
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
@pytest.mark.parametrize("name", sorted(_VALUE_SETS))
def test_chart_renders_no_cronjob(name: str) -> None:
    documents = _render(_VALUE_SETS[name])
    assert documents, (
        f"the {name} value set rendered nothing; the test would pass vacuously"
    )
    cronjobs = [
        doc.get("metadata", {}).get("name")
        for doc in documents
        if doc.get("kind") == "CronJob"
    ]
    assert not cronjobs, f"the {name} value set renders CronJobs: {cronjobs}"


@pytest.mark.parametrize("filename", ["values.yaml", "values-quickstart.yaml"])
def test_chart_values_carry_no_cronjobs_block(filename: str) -> None:
    values = yaml.safe_load((_CHART / filename).read_text())
    assert "cronjobs" not in values, (
        f"{filename} still has a `cronjobs` block; no template reads it"
    )
