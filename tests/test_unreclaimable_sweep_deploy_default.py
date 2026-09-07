"""Every reconciler deploy shape must ship the unreclaimable sweep ACTIVE.

CHAOS-4005's sweep defaults to ``shadow`` in code: it selects the units nothing
else in the system can reach and then refuses to write. That default was
justified as "every deployment gets would-terminalize observability at zero
write risk", and for the entire life of the feature it was only half true --
``rg SYNC_UNRECLAIMABLE_SWEEP`` over the whole repository hit definition sites
and nothing else. No compose file, no chart, no ConfigMap ever set a VALUE, so
every deployment ran in shadow by ACCIDENT rather than by choice.

The cost was measured: sync run ``115e6246`` held 17 provider units in
``dispatching`` for thirteen hours while the sweep logged them as candidates
every second and terminalized none of them.

The assertion ``active`` encodes -- "no Celery consumer serves provider units
for this deployment" -- is now unconditionally true, because CHAOS-3092 deleted
that compute outright. So ``active`` is the shipped value everywhere, and this
test is what stops a shape being added later without it.

RED CONTROL: on the parent commit every assertion below fails, because none of
these files mentioned the variable at all.
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

SWEEP_KEY = "SYNC_UNRECLAIMABLE_SWEEP"
# The mode itself, not merely "the key is present". A shape that sets the key to
# `shadow` would satisfy a presence check while reproducing the exact production
# state this test exists to prevent.
ACTIVE = "active"


def _yaml(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text())


def _reconciler_environment(path: str) -> dict:
    """The go-reconciler service's resolved environment map.

    Resolved, because two of the three compose shapes build it with a YAML
    merge key (``<<: *go-worker-env-base``). Reading the literal mapping would
    silently miss a value inherited from the anchor -- and, worse, would keep
    passing if a later edit moved this key INTO the anchor, where it would apply
    to every Go worker group rather than the reconciler.
    """
    return _yaml(path)["services"]["go-reconciler"]["environment"]


def test_go_workers_compose_overlay_ships_the_sweep_active() -> None:
    environment = _reconciler_environment("deploy/go-workers/compose-go-workers.yml")
    assert environment[SWEEP_KEY] == f"${{{SWEEP_KEY}:-{ACTIVE}}}"


def test_docker_compose_and_swarm_go_worker_stacks_ship_the_sweep_active() -> None:
    for path in (
        "deploy/docker-compose/compose.go-workers.yml",
        "deploy/docker-swarm/stack.go-workers.yml",
    ):
        environment = _reconciler_environment(path)
        assert environment[SWEEP_KEY] == f"${{{SWEEP_KEY}:-{ACTIVE}}}", path


def test_kubernetes_and_helm_ship_the_sweep_active() -> None:
    kubernetes = _yaml("deploy/kubernetes/configmap.yaml")["data"]
    helm = _yaml("deploy/helm/dev-health/values.yaml")["config"]
    assert kubernetes[SWEEP_KEY] == ACTIVE
    assert helm[SWEEP_KEY] == ACTIVE


def test_env_examples_document_the_shipped_default() -> None:
    """The dotfiles are part of the shape, not documentation of it.

    An operator copying ``.env.example`` gets whatever it says; leaving the
    variable out of these two files is how a hand-built environment silently
    lands back in shadow.
    """
    for path, expected in (
        (".env.example", f'{SWEEP_KEY}="{ACTIVE}"'),
        ("deploy/docker-compose/.env.example", f"{SWEEP_KEY}={ACTIVE}"),
    ):
        lines = (ROOT / path).read_text().splitlines()
        assert expected in lines, path
