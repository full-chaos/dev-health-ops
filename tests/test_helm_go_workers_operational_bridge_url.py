"""`--operational-bridge-url` must render for every group whose HTTPDispatcher
needs it, not just the metrics-queue group (CHAOS-4984).

CHAOS-4351 originally scoped `--operational-bridge-url` to whichever group's
queue set includes `metrics` -- that group calls the metrics compatibility
bridge and needs the flag to reach it. kiac-workers traced a crash-loop in
the shipped `ops` group (queues: coverage, heartbeat, retention, webhooks)
to `buildOperationalWorker` in cmd/dev-health-worker: its own HTTPDispatcher
needs `--operational-bridge-url` too, and the flag was never emitted for it
because `ops`'s queue set has no `metrics` member.

The emission condition is now broadened to any of
{metrics, coverage, heartbeat, retention, webhooks} -- but the
metricsApi-vs-api DEFAULT-URL SELECTION stays gated on `metrics`
specifically, unchanged: an ops-shaped group with no `metrics` queue always
gets the `api` default (metricsApi exists to isolate metrics-bridge load,
not ops traffic), regardless of whether metricsApi is enabled.
"""

from __future__ import annotations

import shutil
import textwrap
import uuid
from pathlib import Path
from subprocess import run
from tempfile import TemporaryDirectory

import pytest
import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"
_RELEASE = "bridge-url-contract"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


def _group_values(*, name: str, queues: list[str]) -> str:
    """One goWorkers group as a values document -- a FILE, not `--set
    goWorkers.groups[0].x=y`, which replaces the whole list element and
    drops sibling keys the template dereferences (same trap documented in
    tests/workers/test_helm_worker_disruption_and_drain.py's `_group`
    helper). `terminationGracePeriodSeconds` is 7260 whenever `metrics` is
    among the queues -- this file's own separate CHAOS-4428 guard fails
    the render below that floor for a metrics-carrying group, and this
    helper only needs to satisfy that guard, not exercise it."""
    grace = 7260 if "metrics" in queues else 60
    return textwrap.dedent(f"""
        goWorkers:
          enabled: true
          groups:
            - name: {name}
              image: ghcr.io/full-chaos/dev-health-go-worker:latest
              queues: [{", ".join(queues)}]
              queueConcurrency: {{{", ".join(f"{q}: 1" for q in queues)}}}
              replicas: 1
              terminationGracePeriodSeconds: {grace}
              resources: {{requests: {{cpu: 250m, memory: 256Mi}}, limits: {{cpu: "1", memory: 1Gi}}}}
              autoscaling: {{enabled: false}}
              bridgeUrl: ""
        """)


def _container_args(values: str, extra_set: list[str] | None = None) -> list[str]:
    with TemporaryDirectory() as tmp:
        values_file = Path(tmp) / f"values-{uuid.uuid4().hex}.yaml"
        values_file.write_text(values, encoding="utf-8")
        argv = [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            "--show-only",
            "templates/go-workers.yaml",
            "-f",
            str(values_file),
        ]
        if extra_set:
            for value in extra_set:
                argv += ["--set", value]
        rendered = run(argv, check=True, capture_output=True, text=True)
    documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]
    (deployment,) = [d for d in documents if d.get("kind") == "Deployment"]
    return deployment["spec"]["template"]["spec"]["containers"][0]["args"]


def _bridge_url_arg(args: list[str]) -> str | None:
    for arg in args:
        if arg.startswith("--operational-bridge-url="):
            return arg
    return None


def test_ops_shaped_group_renders_the_flag() -> None:
    """The exact incident: coverage/heartbeat/retention/webhooks must get
    --operational-bridge-url, or the ops group's HTTPDispatcher
    crash-loops on boot."""
    args = _container_args(
        _group_values(
            name="ops", queues=["coverage", "heartbeat", "retention", "webhooks"]
        )
    )
    bridge = _bridge_url_arg(args)
    assert bridge is not None, (
        f"expected --operational-bridge-url on an ops-shaped group, got args={args}"
    )


def test_workgraph_only_group_does_not_render_the_flag() -> None:
    """The negative control CHAOS-4351 originally relied on: a group whose
    queues touch none of the bridge-needing set must not get the flag at
    all -- broadening emission must not become "always emit"."""
    args = _container_args(_group_values(name="sync", queues=["workgraph"]))
    assert _bridge_url_arg(args) is None, (
        f"expected no --operational-bridge-url on a workgraph-only group, got args={args}"
    )


def test_ops_shaped_group_defaults_to_api_not_metrics_api() -> None:
    """The metricsApi-vs-api selection stays gated on `metrics`
    specifically -- an ops-shaped group (no `metrics` queue) must default
    to `api`'s own Service, even with metricsApi enabled, since
    metricsApi exists to isolate metrics-bridge load, not ops traffic."""
    args = _container_args(
        _group_values(
            name="ops", queues=["coverage", "heartbeat", "retention", "webhooks"]
        ),
        extra_set=["metricsApi.enabled=true"],
    )
    bridge = _bridge_url_arg(args)
    assert bridge is not None
    assert "-api:" in bridge and "-metrics-api:" not in bridge, (
        f"expected the api default (not metrics-api), got {bridge!r}"
    )


def test_metrics_group_still_defaults_to_metrics_api_when_enabled() -> None:
    """Unchanged CHAOS-4351 behavior, asserted here so this file is a
    complete positive+negative control for the whole condition, not just
    the ops-group addition."""
    args = _container_args(
        _group_values(name="heavy", queues=["metrics"]),
        extra_set=["metricsApi.enabled=true"],
    )
    bridge = _bridge_url_arg(args)
    assert bridge is not None
    assert "-metrics-api:" in bridge, (
        f"expected the metrics-api default, got {bridge!r}"
    )
