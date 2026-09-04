"""goWorkers containers must not inherit the top-level image.pullPolicy (CHAOS-4984).

The top-level `image.pullPolicy` exists for the commit-tagged APPLICATION
image a local kind/kiac profile side-loads into the cluster and therefore
sets to `Never` (docs/contribute/development/lane-isolation-kiac.md). Before
this fix, go-workers.yaml's container rendered `imagePullPolicy: {{
$.Values.image.pullPolicy }}` -- the same value -- so following that local
profile with `goWorkers.enabled=true` and untouched worker defaults set
`Never` on every published `ghcr.io/full-chaos/dev-health-go-*` image too,
none of which were ever side-loaded, failing every worker pod with
`ErrImageNeverPull`.

`goWorkers.imagePullPolicy` is now its own value, decoupled from the
top-level one and defaulting to `IfNotPresent` -- the same pattern the chart
already uses for `postgresql.image.pullPolicy` (a registry image with its
own dedicated policy, values.yaml).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from subprocess import run

import pytest
import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


def _go_worker_deployments(*extra_set: str) -> list[dict]:
    argv = [
        "helm",
        "template",
        "pull-policy-contract",
        str(_CHART),
        "--show-only",
        "templates/go-workers.yaml",
    ]
    for value in extra_set:
        argv += ["--set", value]
    rendered = run(argv, check=True, capture_output=True, text=True)
    documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]
    return [d for d in documents if d.get("kind") == "Deployment"]


def test_go_worker_default_pull_policy_is_ifnotpresent() -> None:
    deployments = _go_worker_deployments(
        "goWorkers.pgbouncer.postgres.host=postgres.example.com"
    )
    assert deployments, "expected at least one go-worker Deployment rendered"
    for deployment in deployments:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert container["imagePullPolicy"] == "IfNotPresent", (
            f"{deployment['metadata']['name']}: expected IfNotPresent, got "
            f"{container['imagePullPolicy']!r}"
        )


def test_go_worker_pull_policy_does_not_follow_the_local_kind_profile() -> None:
    """The exact incident this fix closes: a local profile sets the
    top-level image.pullPolicy to Never for its side-loaded application
    image. goWorkers containers must NOT pick that up -- they stay on
    goWorkers.imagePullPolicy, which that profile never touches."""
    deployments = _go_worker_deployments(
        "goWorkers.pgbouncer.postgres.host=postgres.example.com",
        "image.pullPolicy=Never",
    )
    assert deployments, "expected at least one go-worker Deployment rendered"
    for deployment in deployments:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert container["imagePullPolicy"] != "Never", (
            f"{deployment['metadata']['name']}: image.pullPolicy=Never "
            "leaked into a go-worker container's own imagePullPolicy -- "
            "this is the exact ErrImageNeverPull incident CHAOS-4984 fixes"
        )
        assert container["imagePullPolicy"] == "IfNotPresent"


def test_go_worker_pull_policy_is_overridable_independently() -> None:
    """goWorkers.imagePullPolicy is its own value -- an operator who
    genuinely needs a different policy for worker images specifically
    (e.g. Always, for a mutable :latest tag in a dev cluster) must be
    able to set it without touching the unrelated top-level image
    .pullPolicy."""
    deployments = _go_worker_deployments(
        "goWorkers.pgbouncer.postgres.host=postgres.example.com",
        "goWorkers.imagePullPolicy=Always",
    )
    assert deployments, "expected at least one go-worker Deployment rendered"
    for deployment in deployments:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert container["imagePullPolicy"] == "Always", (
            f"{deployment['metadata']['name']}: goWorkers.imagePullPolicy "
            f"override did not take effect, got "
            f"{container['imagePullPolicy']!r}"
        )
