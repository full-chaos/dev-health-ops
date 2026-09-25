"""CHAOS-6767: a KEDA-scaled go-worker Deployment renders without spec.replicas.

KEDA's HPA owns the scale subresource. A manifest that also sets spec.replicas conflicts (server-side
apply, `conflict with "k3s" with subresource "scale"`) whenever KEDA has moved the count, and a forced
apply would reset it. Groups without autoscaling keep their fixed replicas.
"""

import json
from pathlib import Path
from subprocess import run

import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"


def _render(group0: dict | None = None) -> list[dict]:
    """Render; `group0` is merged into goWorkers.groups[0] (a list value replaces wholesale, so send all groups)."""
    args = ["helm", "template", "t", str(_CHART)]
    if group0:
        groups = yaml.safe_load((_CHART / "values.yaml").read_text())["goWorkers"][
            "groups"
        ]
        groups[0] = {**groups[0], **group0}
        args += ["--set-json", f"goWorkers.groups={json.dumps(groups)}"]
    out = run(args, check=True, capture_output=True, text=True).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def _workers(docs: list[dict]) -> dict[str, dict]:
    return {
        d["metadata"]["name"]: d
        for d in docs
        if d["kind"] == "Deployment"
        and "go-worker"
        in d["metadata"]["labels"].get("app.kubernetes.io/component", "")
    }


def _scaled_targets(docs: list[dict]) -> set[str]:
    return {
        d["spec"]["scaleTargetRef"]["name"] for d in docs if d["kind"] == "ScaledObject"
    }


def test_keda_scaled_groups_omit_replicas_and_others_keep_them() -> None:
    docs = _render()
    workers, scaled = _workers(docs), _scaled_targets(docs)
    assert scaled, "default values must render at least one ScaledObject"
    assert scaled <= set(workers), (scaled, set(workers))
    for name, dep in workers.items():
        if name in scaled:
            assert "replicas" not in dep["spec"], name
        else:
            assert dep["spec"].get("replicas", 0) >= 1, name
    assert set(workers) - scaled, "coordinator/stream groups stay fixed-replica"


def test_a_fixed_replicas_value_is_ignored_for_a_scaled_group() -> None:
    docs = _render({"replicas": 3})
    workers, scaled = _workers(docs), _scaled_targets(docs)
    first = next(n for n in workers if n in scaled)
    assert "replicas" not in workers[first]["spec"]


def test_disabling_autoscaling_restores_fixed_replicas() -> None:
    docs = _render({"autoscaling": {"enabled": False}, "replicas": 3})
    workers, scaled = _workers(docs), _scaled_targets(docs)
    unscaled_three = [
        n
        for n, d in workers.items()
        if n not in scaled and d["spec"].get("replicas") == 3
    ]
    assert unscaled_three, (workers.keys(), scaled)


def test_autoscaled_group_with_a_minimum_above_one_is_refused() -> None:
    """r1 P1: a fresh install of `replicas: 3, minReplicas: 3` started at 1 replica (below its minimum) until KEDA reconciled."""
    groups = yaml.safe_load((_CHART / "values.yaml").read_text())["goWorkers"]["groups"]
    groups[0] = {
        **groups[0],
        "replicas": 3,
        "autoscaling": {**groups[0]["autoscaling"], "minReplicas": 3, "maxReplicas": 4},
    }
    args = ["helm", "template", "t", str(_CHART), "--set-json"]
    args.append(f"goWorkers.groups={json.dumps(groups)}")
    result = run(args, capture_output=True, text=True)
    assert result.returncode != 0
    assert "would run below its own minimum" in result.stderr, result.stderr
