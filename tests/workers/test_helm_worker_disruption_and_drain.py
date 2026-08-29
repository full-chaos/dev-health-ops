"""Chart-level guarantees for draining and rolling the worker family (CHAOS-4428).

Three separate defects, each caught only by rendering:

1. A `metrics` group whose terminationGracePeriodSeconds is below 7260s is
   SIGKILLed while holding a claimed job. The worker's own diagnosis names the
   queue coverage check, not the grace period, so the render must refuse it.
2. A PodDisruptionBudget over a single replica permits zero voluntary
   evictions and wedges `kubectl drain` permanently. The PDB must appear only
   at replicas >= 2.
3. The request-serving Deployments must not drop below current capacity during
   a rollout. Kubernetes' 25% default happens to round to maxUnavailable 0 at
   one replica; that is rounding, not a guarantee, and it stops holding at
   replicas: 4.
"""

from __future__ import annotations

import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"
_RELEASE = "drain-contract"
_METRICS_FLOOR = 7260

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


def _group(
    *,
    name: str,
    queues: list[str],
    replicas: int,
    grace: int,
    autoscaling: str = "{enabled: false}",
) -> str:
    """One goWorkers group as a values document.

    A values FILE, not `--set goWorkers.groups[0].x=y`: --set on a list index
    replaces the whole element, dropping the sibling keys the template
    dereferences, and the render then fails on `nil pointer evaluating
    interface {}.enabled` -- a failure that looks like the guard firing but
    is not. That false pass is exactly what this helper exists to avoid.
    """
    return textwrap.dedent(f"""
        goWorkers:
          enabled: true
          groups:
            - name: {name}
              image: ghcr.io/full-chaos/dev-health-go-worker:latest
              queues: [{", ".join(queues)}]
              queueConcurrency: {{{", ".join(f"{q}: 1" for q in queues)}}}
              replicas: {replicas}
              terminationGracePeriodSeconds: {grace}
              resources: {{requests: {{cpu: 250m, memory: 256Mi}}, limits: {{cpu: "1", memory: 1Gi}}}}
              autoscaling: {autoscaling}
              bridgeUrl: ""
        """)


def _render(values: str | None, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    argv = ["helm", "template", _RELEASE, str(_CHART)]
    if values is not None:
        values_file = tmp_path / "values.yaml"
        values_file.write_text(values, encoding="utf-8")
        argv += ["-f", str(values_file)]
    else:
        argv += ["--set", "goWorkers.enabled=true"]
    return subprocess.run(argv, capture_output=True, text=True)


def _docs(values: str | None, tmp_path: Path) -> list[dict]:
    completed = _render(values, tmp_path)
    assert completed.returncode == 0, completed.stderr
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


# --- 1. the metrics grace-period floor -------------------------------------


def test_metrics_group_below_the_floor_fails_the_render(tmp_path: Path) -> None:
    completed = _render(
        _group(name="heavy", queues=["metrics"], replicas=1, grace=960), tmp_path
    )
    assert completed.returncode != 0, "a 960s metrics group must not render"
    assert str(_METRICS_FLOOR) in completed.stderr, (
        f"the error must name the {_METRICS_FLOOR}s floor so the operator can "
        f"act on it: {completed.stderr}"
    )
    assert "--shutdown-timeout" in completed.stderr, (
        "the error must name the flag the value becomes, which is the "
        f"causal link an operator cannot otherwise see: {completed.stderr}"
    )


def test_metrics_group_at_the_floor_renders(tmp_path: Path) -> None:
    completed = _render(
        _group(name="heavy", queues=["metrics"], replicas=1, grace=_METRICS_FLOOR),
        tmp_path,
    )
    assert completed.returncode == 0, (
        f"{_METRICS_FLOOR}s is the floor itself and must be allowed: {completed.stderr}"
    )


def test_non_metrics_group_is_not_constrained(tmp_path: Path) -> None:
    """The floor is a property of the metrics queue, not of workers generally.

    Without this, tightening the guard to every group would still pass the two
    tests above while breaking every short-lived worker in the chart.
    """
    completed = _render(
        _group(name="ops", queues=["heartbeat"], replicas=1, grace=60), tmp_path
    )
    assert completed.returncode == 0, (
        f"a 60s non-metrics group is legitimate: {completed.stderr}"
    )


def test_chart_defaults_satisfy_their_own_floor(tmp_path: Path) -> None:
    """The shipped defaults must not be a configuration the chart rejects."""
    for doc in _docs(None, tmp_path):
        if doc.get("kind") != "Deployment":
            continue
        spec = doc["spec"]["template"]["spec"]
        containers = spec.get("containers", [])
        args = [arg for c in containers for arg in c.get("args", [])]
        if not any("--queues=" in a and "metrics" in a for a in args):
            continue
        assert spec["terminationGracePeriodSeconds"] >= _METRICS_FLOOR, (
            f"{doc['metadata']['name']} ships below its own floor"
        )


# --- 2. the PDB replica gate ------------------------------------------------


def _pdbs(values: str | None, tmp_path: Path) -> dict[str, dict]:
    return {
        doc["metadata"]["name"]: doc
        for doc in _docs(values, tmp_path)
        if doc.get("kind") == "PodDisruptionBudget"
    }


def test_single_replica_group_renders_no_pdb(tmp_path: Path) -> None:
    values = _group(name="heavy", queues=["metrics"], replicas=1, grace=_METRICS_FLOOR)
    assert _pdbs(values, tmp_path) == {}, (
        "a PDB with maxUnavailable: 1 over one replica allows zero evictions "
        "and blocks node drain forever"
    )


def test_multi_replica_group_renders_a_pdb_that_permits_a_drain(
    tmp_path: Path,
) -> None:
    values = _group(name="heavy", queues=["metrics"], replicas=2, grace=_METRICS_FLOOR)
    pdbs = _pdbs(values, tmp_path)
    assert len(pdbs) == 1, f"expected exactly one PDB, got {sorted(pdbs)}"
    (pdb,) = pdbs.values()

    assert pdb["spec"]["maxUnavailable"] == 1
    assert "minAvailable" not in pdb["spec"], (
        "minAvailable silently changes meaning when an HPA moves the replica "
        "count; maxUnavailable does not"
    )
    assert pdb["spec"]["selector"]["matchLabels"]["dev-health.io/worker-group"] == (
        "heavy"
    ), "a PDB whose selector does not target its own group budgets the wrong pods"


def test_pdb_selector_matches_the_deployment_it_budgets(tmp_path: Path) -> None:
    """A selector that matches nothing is a PDB that protects nothing.

    Asserting the label value alone would pass against a selector missing the
    component label, which would then also budget non-worker pods.
    """
    values = _group(name="heavy", queues=["metrics"], replicas=2, grace=_METRICS_FLOOR)
    docs = _docs(values, tmp_path)
    (pdb,) = [d for d in docs if d.get("kind") == "PodDisruptionBudget"]
    deployments = [
        d
        for d in docs
        if d.get("kind") == "Deployment"
        and d["metadata"]["labels"].get("app.kubernetes.io/component") == "go-worker"
    ]
    (deployment,) = deployments

    pod_labels = deployment["spec"]["template"]["metadata"]["labels"]
    for key, value in pdb["spec"]["selector"]["matchLabels"].items():
        assert pod_labels.get(key) == value, (
            f"PDB selector {key}={value} does not match the pod template "
            f"({pod_labels.get(key)!r}); the budget would apply to no pods"
        )


_AUTOSCALED = (
    "{enabled: true, minReplicas: 0, maxReplicas: 2, queueDepth: 1, "
    "oldestAgeSeconds: 60, saturationMilli: 800m}"
)
_AUTOSCALED_TO_ONE = (
    "{enabled: true, minReplicas: 0, maxReplicas: 1, queueDepth: 1, "
    "oldestAgeSeconds: 60, saturationMilli: 800m}"
)


def test_autoscaled_group_renders_a_pdb_at_zero_static_replicas(
    tmp_path: Path,
) -> None:
    """Static `replicas` is the HPA's starting point, not its running size.

    Every shipped worker group starts at replicas: 0 so a fresh install stays
    inert, and four of them scale to two pods under load. Gating on the static
    value alone renders no budget for exactly the groups that do run more than
    one pod, so a node drain can evict every co-located worker at once -- which
    is the disruption this file exists to prevent.
    """
    values = _group(
        name="heavy",
        queues=["metrics"],
        replicas=0,
        grace=_METRICS_FLOOR,
        autoscaling=_AUTOSCALED,
    )
    pdbs = _pdbs(values, tmp_path)
    assert len(pdbs) == 1, (
        f"an HPA group that reaches 2 pods has no budget: {sorted(pdbs)}"
    )
    (pdb,) = pdbs.values()
    assert pdb["spec"]["maxUnavailable"] == 1


def test_autoscaled_group_that_cannot_reach_two_renders_no_pdb(
    tmp_path: Path,
) -> None:
    """The wedge hazard is about the pod count, not about having an HPA.

    Without this, widening the gate to "autoscaling.enabled" alone would pass
    the test above while reintroducing the single-replica PDB that blocks
    `kubectl drain` forever.
    """
    values = _group(
        name="heavy",
        queues=["metrics"],
        replicas=0,
        grace=_METRICS_FLOOR,
        autoscaling=_AUTOSCALED_TO_ONE,
    )
    assert _pdbs(values, tmp_path) == {}, (
        "a group capped at one pod still permits zero voluntary evictions"
    )


def test_chart_defaults_budget_their_own_autoscaled_groups(tmp_path: Path) -> None:
    """The shipped values are the configuration this gate is judged on."""
    pdbs = _pdbs(None, tmp_path)
    budgeted = {
        doc["metadata"]["labels"]["dev-health.io/worker-group"] for doc in pdbs.values()
    }
    assert budgeted == {"heavy", "ops", "sync", "sync-provider"}, (
        "exactly the four groups whose HPA reaches maxReplicas: 2 must be "
        f"budgeted; the singletons must not be: {sorted(budgeted)}"
    )


# --- 3. rollout capacity for the serving Deployments ------------------------


@pytest.mark.parametrize("component", ["api", "metrics-api", "web"])
def test_serving_deployments_never_dip_below_capacity(
    component: str, tmp_path: Path
) -> None:
    values = "metricsApi:\n  enabled: true\n"
    matches = [
        doc
        for doc in _docs(values, tmp_path)
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["labels"].get("app.kubernetes.io/component") == component
    ]
    assert matches, f"no {component} Deployment rendered"
    for doc in matches:
        strategy = doc["spec"].get("strategy", {})
        assert strategy.get("type") == "RollingUpdate", (
            f"{component} must roll, not Recreate: {strategy}"
        )
        rolling = strategy.get("rollingUpdate", {})
        assert rolling.get("maxUnavailable") == 0, (
            f"{component} would drop below serving capacity mid-rollout: {rolling}"
        )
        assert rolling.get("maxSurge", 0) >= 1, (
            f"{component} with maxUnavailable 0 and no surge cannot roll at "
            f"all -- it would deadlock: {rolling}"
        )


def test_a_group_without_an_autoscaling_block_is_rejected_chart_wide(
    tmp_path: Path,
) -> None:
    """The PDB gate dereferences `$group.autoscaling` unguarded, and that is
    safe because the chart already requires the block on EVERY group:
    `go-workers.yaml:171` reads `$group.autoscaling.enabled` with no default,
    and it is the first such dereference in the chart.

    Pinned rather than guarded. A `| default dict` in the PDB template would be
    unreachable code that reads as defensiveness, and it would mask the render
    error an operator needs to see -- which names the template that actually
    owns the requirement.
    """
    values = textwrap.dedent("""
        goWorkers:
          enabled: true
          groups:
            - name: solo
              image: ghcr.io/full-chaos/dev-health-go-worker:latest
              queues: [heartbeat]
              queueConcurrency: {heartbeat: 1}
              replicas: 3
              terminationGracePeriodSeconds: 60
              resources: {requests: {cpu: 250m, memory: 256Mi}, limits: {cpu: "1", memory: 1Gi}}
        """)
    completed = _render(values, tmp_path)
    assert completed.returncode != 0, (
        "a group with no autoscaling block must not render silently"
    )
    assert "autoscaling" in completed.stderr, (
        f"the error must name the missing block: {completed.stderr}"
    )
