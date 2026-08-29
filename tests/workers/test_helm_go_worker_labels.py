"""Every rendered Go-worker label VALUE must be a legal Kubernetes label value.

This exists because the chart could not install its own defaults (CHAOS-4428).
`dev-health.io/queue-set` was built with `join ","`, and a comma is not in the
label-value grammar, so the API server rejected the `heavy` and `ops`
Deployments outright:

    Deployment.apps "…-go-heavy" is invalid: metadata.labels: Invalid value:
    "investment,metrics,reports,workgraph": a valid label must be an empty
    string or consist of alphanumeric characters, '-', '_' or '.', …

`helm template` renders that happily -- only the API server refuses it -- so a
rendering smoke test would not have caught it. This asserts the grammar itself,
against the chart's OWN default groups (the ones that were broken), on both the
Deployment metadata and the pod template, where the illegal value appeared
twice.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"
_RELEASE = "label-contract"

# k8s label-value grammar, verbatim from the apiserver's own error text:
# (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?  -- max 63 chars, may be empty.
_LABEL_VALUE = re.compile(r"^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$")

# The groups that carry more than one queue, i.e. the ones a "," join breaks.
# Both are chart DEFAULTS -- no values override is needed to hit the bug.
_MULTI_QUEUE_GROUPS = {"go-heavy", "go-ops"}


def _render() -> list[dict]:
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            _CHART,
            "--set",
            "goWorkers.enabled=true",
            "--set",
            "goWorkers.pgbouncer.enabled=true",
            "--set",
            "goWorkers.pgbouncer.postgres.host=postgres.internal",
            "--set-string",
            "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=d",
            "--set-string",
            "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=q",
            "--set-string",
            "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=c",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


def _go_worker_deployments() -> dict[str, dict]:
    found = {}
    for doc in _render():
        if doc.get("kind") != "Deployment":
            continue
        labels = doc.get("metadata", {}).get("labels", {})
        if labels.get("app.kubernetes.io/component") != "go-worker":
            continue
        name = doc["metadata"]["name"].removeprefix(f"{_RELEASE}-dev-health-")
        found[name] = doc
    return found


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_default_go_worker_groups_render_legal_label_values() -> None:
    deployments = _go_worker_deployments()
    assert _MULTI_QUEUE_GROUPS <= set(deployments), (
        "the chart's own multi-queue default groups must render; "
        f"got {sorted(deployments)}"
    )

    illegal: list[str] = []
    for name, doc in deployments.items():
        sites = {
            "metadata.labels": doc["metadata"].get("labels", {}),
            "spec.template.metadata.labels": doc["spec"]["template"]["metadata"].get(
                "labels", {}
            ),
        }
        for where, labels in sites.items():
            for key, value in labels.items():
                text = "" if value is None else str(value)
                if not _LABEL_VALUE.match(text) or len(text) > 63:
                    illegal.append(f"{name} {where}[{key}]={text!r}")
    assert not illegal, "illegal Kubernetes label values rendered: " + "; ".join(
        illegal
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_queue_set_label_encodes_every_queue_without_a_comma() -> None:
    """The label must stay informative, not just legal.

    A fix that dropped the label, or truncated it to one queue, would pass the
    grammar assertion above while losing what the label is for.
    """
    for name, doc in _go_worker_deployments().items():
        if name not in _MULTI_QUEUE_GROUPS:
            continue
        container = doc["spec"]["template"]["spec"]["containers"][0]
        selected = [
            argument.removeprefix("--queues=")
            for argument in container.get("args", [])
            if argument.startswith("--queues=")
        ]
        assert selected, f"{name} declares no --queues argument to compare against"
        queues = selected[0].split(",")
        assert len(queues) > 1, f"{name} is not a multi-queue group any more"

        value = doc["metadata"]["labels"]["dev-health.io/queue-set"]
        assert "," not in value, (
            f"{name} queue-set label reintroduced a comma: {value!r}"
        )
        for queue in queues:
            assert queue in value, (
                f"{name} queue-set label {value!r} dropped queue {queue!r}"
            )
