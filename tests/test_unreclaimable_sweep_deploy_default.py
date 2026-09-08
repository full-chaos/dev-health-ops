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


def _reconciler(path: str) -> dict:
    """The go-reconciler service, with YAML merge keys resolved.

    Resolved, because two of the three compose shapes build the service from an
    anchor (``<<: *go-worker``, ``<<: *go-worker-env-base``). Reading a literal
    mapping would miss anything inherited -- and, worse, would keep passing if a
    later edit moved this setting INTO the shared anchor, where it would apply
    to every Go worker group rather than the reconciler alone. Only the
    reconciler accepts ``--unreclaimable-sweep``; any other group exits 2 on it.
    """
    return _yaml(path)["services"]["go-reconciler"]


def test_go_workers_compose_overlay_ships_the_sweep_active() -> None:
    """The overlay configures through ``environment:``, like its siblings.

    Unlike the two surfaces below it is not bound by CHAOS-4020's
    credentials-only rule -- it already sets OPERATIONAL_ORDERING_CONTRACT the
    same way, and it is a hand-edited local overlay rather than a rendered
    deployment surface.
    """
    environment = _reconciler("deploy/go-workers/compose-go-workers.yml")["environment"]
    assert environment[SWEEP_KEY] == f"${{{SWEEP_KEY}:-{ACTIVE}}}"


def test_docker_compose_and_swarm_go_worker_stacks_ship_the_sweep_active() -> None:
    """A FLAG on these two, not an environment entry.

    CHAOS-4020's contract (tests/workers/test_go_worker_cli_contract.py) is that
    only credentials render through ``environment:`` here; every other setting
    is visible in ``command:``, where ``docker compose config`` shows the
    deployed configuration. The interpolated default keeps the operator override
    the environment form would have given.
    """
    for path in (
        "deploy/docker-compose/compose.go-workers.yml",
        "deploy/docker-swarm/stack.go-workers.yml",
    ):
        service = _reconciler(path)
        command = [str(argument) for argument in service["command"]]
        assert f"--unreclaimable-sweep=${{{SWEEP_KEY}:-{ACTIVE}}}" in command, (
            path,
            command,
        )
        # And NOT through the environment, or the credentials-only contract
        # breaks the moment someone reads this file for guidance.
        assert SWEEP_KEY not in (service.get("environment") or {}), path


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
