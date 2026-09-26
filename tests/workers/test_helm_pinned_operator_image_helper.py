"""CHAOS-6958: the chart's pinned-operator-image checks, exercised chain-wide.

CHAOS-6951's r1 luna round found three gaps, all pre-existing (reproduced
against the already-merged River hook, not introduced by that PR):

  1. Identity by shape only. A digest-pinned or sha-<12 hex>-tagged image was
     accepted whatever repository it named -- a pinned Python api image
     (ghcr.io/full-chaos/dev-hops-api:sha-<12 hex>) with a matching image.tag
     rendered clean, then failed at runtime with an unrecognized-argument
     error.
  2. Shape only, not a REAL digest. `contains "@sha256:"` treated
     `repo@sha256:not-a-digest` as pinned; a real puller rejects it.
  3. Pull policy leaking from the wrong knob. `imagePullPolicy` fell back to
     `.Values.image.pullPolicy` (the APPLICATION image's policy) for a
     REGISTRY-pinned operator image, so a local `Never` made it unpullable.

Fixed once in `dev-health.pinnedOperatorImageCheck` (_helpers.tpl), used by
river-hooks.yaml's provision-roles hook, its River hook, migrate-job.yaml's
migrate Job and route-activate-hooks.yaml; go-pgbouncer.yaml's own (unrelated,
third-party, non-dho) image gets its own narrower digest-shape fix, pinned
separately below.

The identity check is a DENYLIST of the retired/wrong images this repo
actually publishes under a name a dho verb could reach by mistake
(dev-hops-api, dev-health-query-api, dev-health-go-worker) -- not a positive
allowlist. route-activate-hooks.yaml's own
test_route_activate_operator_image_override_is_honoured (test_helm_route_
activate_hooks.py) already proves a CUSTOM repository name is a supported,
deliberate override; a positive allowlist would have regressed it.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)

_PINNED_DHO = "ghcr.io/full-chaos/dev-health-go-dho:sha-aaaaaaaaaaaa"


def _render_stderr(*sets: str) -> tuple[int, str]:
    argv = ["helm", "template", "review", str(_CHART)]
    for item in sets:
        argv += ["--set", item]
    completed = subprocess.run(argv, capture_output=True, text=True)
    return completed.returncode, completed.stderr


def _render(*sets: str) -> subprocess.CompletedProcess[str]:
    argv = [
        "helm",
        "template",
        "review",
        str(_CHART),
        "--show-only",
        "templates/river-hooks.yaml",
    ]
    for item in sets:
        argv += ["--set", item]
    return subprocess.run(argv, capture_output=True, text=True)


# --- gap 1: identity -- a retired/wrong image must be refused by name -------

_HOOK_SITES = [
    (
        "provision-roles",
        (
            "migrations.hook.provisionRoles.enabled=true",
            "migrations.hook.riverMigrate.enabled=false",
            "migrations.hook.routeActivate.enabled=false",
        ),
        "migrations.hook.provisionRoles.image",
    ),
    (
        "river-migrate",
        (
            "migrations.hook.provisionRoles.enabled=false",
            "migrations.hook.riverMigrate.enabled=true",
            "migrations.hook.routeActivate.enabled=false",
        ),
        "migrations.hook.riverMigrate.image",
    ),
]


@pytest.mark.parametrize(
    "name,base_sets,flag", _HOOK_SITES, ids=[s[0] for s in _HOOK_SITES]
)
def test_hook_refuses_the_python_image_even_with_a_matching_pinned_tag(
    name: str, base_sets: tuple[str, ...], flag: str
) -> None:
    """The r1 round's concrete reproduction: a Python api image, pinned with
    this repo's own sha-<12 hex> convention and lockstep-matching image.tag,
    used to render clean -- exit 0 -- then fail at runtime."""
    code, stderr = _render_stderr(
        *base_sets,
        f"{flag}=ghcr.io/full-chaos/dev-hops-api:sha-aaaaaaaaaaaa",
        "image.tag=sha-aaaaaaaaaaaa",
    )
    assert code != 0, f"{name}: the Python image rendered clean"
    assert "is not a pinned dho image" in stderr, stderr
    assert "Python api image" in stderr, stderr


@pytest.mark.parametrize(
    "retired_repo",
    [
        "ghcr.io/full-chaos/dev-health-query-api",
        "ghcr.io/full-chaos/dev-health-go-worker",
    ],
)
def test_migrate_job_refuses_other_retired_images_by_name(retired_repo: str) -> None:
    code, stderr = _render_stderr(
        f"migrations.hook.image={retired_repo}:sha-aaaaaaaaaaaa"
    )
    assert code != 0, f"{retired_repo} rendered clean"
    assert "is not a pinned dho image" in stderr, stderr


def test_route_activate_custom_operator_name_is_still_honoured() -> None:
    """The identity check is a DENYLIST, not an allowlist: this is the control
    proving a legitimately custom-named, correctly-pinned operator image still
    renders (test_helm_route_activate_hooks.py's own
    test_route_activate_operator_image_override_is_honoured pins the container
    image value; this proves the RENDER itself is not refused)."""
    code, stderr = _render_stderr(
        "migrations.hook.provisionRoles.enabled=true",
        "migrations.hook.riverMigrate.enabled=true",
        "migrations.hook.routeActivate.enabled=true",
        "migrations.hook.routeActivate.image=ghcr.io/example/custom-operator:sha-abc123456789",
    )
    assert code == 0, stderr


# --- gap 2: a malformed digest must be refused, not accepted as "pinned" ----


@pytest.mark.parametrize(
    "name,base_sets,flag", _HOOK_SITES, ids=[s[0] for s in _HOOK_SITES]
)
def test_hook_refuses_a_malformed_digest(
    name: str, base_sets: tuple[str, ...], flag: str
) -> None:
    code, stderr = _render_stderr(
        *base_sets, f"{flag}=ghcr.io/full-chaos/dev-health-go-dho@sha256:not-a-digest"
    )
    assert code != 0, f"{name}: a malformed digest rendered clean"
    assert "is not a pinned dho image" in stderr, stderr


def test_hook_accepts_a_well_formed_digest() -> None:
    """The control: a REAL 64 lowercase-hex-digit digest still renders."""
    code, stderr = _render_stderr(
        "migrations.hook.provisionRoles.enabled=true",
        "migrations.hook.riverMigrate.enabled=false",
        "migrations.hook.routeActivate.enabled=false",
        "migrations.hook.provisionRoles.image=ghcr.io/full-chaos/dev-health-go-dho@sha256:"
        + "a" * 64,
    )
    assert code == 0, stderr


def test_hook_refuses_a_short_digest_that_is_still_valid_hex() -> None:
    """A digest of the wrong LENGTH is still malformed, even when every
    character is valid hex -- this is the case a length-blind regex misses."""
    code, stderr = _render_stderr(
        "migrations.hook.provisionRoles.enabled=true",
        "migrations.hook.riverMigrate.enabled=false",
        "migrations.hook.routeActivate.enabled=false",
        "migrations.hook.provisionRoles.image=ghcr.io/full-chaos/dev-health-go-dho@sha256:"
        + "a" * 10,
    )
    assert code != 0, "a short (10 hex char) digest rendered clean"
    assert "is not a pinned dho image" in stderr, stderr


def test_hook_refuses_a_sideloaded_local_tag_without_the_matching_pull_policy() -> None:
    """`repo:local` is accepted ONLY when the resolved pull policy is Never or
    IfNotPresent -- with Always, a registry pull is attempted and fails
    outright (no registry ever publishes ":local")."""
    code, stderr = _render_stderr(
        "migrations.hook.provisionRoles.enabled=true",
        "migrations.hook.riverMigrate.enabled=false",
        "migrations.hook.routeActivate.enabled=false",
        "migrations.hook.provisionRoles.image=ghcr.io/full-chaos/dev-health-go-dho:local",
        "migrations.hook.provisionRoles.pullPolicy=Always",
    )
    assert code != 0, "repo:local with pullPolicy=Always rendered clean"
    assert "is not a pinned dho image" in stderr, stderr


def test_go_pgbouncer_refuses_a_malformed_digest() -> None:
    code, stderr = _render_stderr(
        "goWorkers.enabled=true",
        "goWorkers.pgbouncer.enabled=true",
        "goWorkers.pgbouncer.image=edoburu/pgbouncer@sha256:not-a-digest",
    )
    assert code != 0, "a malformed pgbouncer digest rendered clean"
    assert "must be pinned by an immutable sha256 digest" in stderr, stderr


def test_go_pgbouncer_accepts_a_well_formed_digest() -> None:
    code, stderr = _render_stderr(
        "goWorkers.enabled=true",
        "goWorkers.pgbouncer.enabled=true",
        "goWorkers.pgbouncer.image=edoburu/pgbouncer@sha256:" + "b" * 64,
    )
    assert code == 0, stderr


# --- gap 3: pull policy must default to IfNotPresent, never leak from the ---
# --- application image's own knob -------------------------------------------


@pytest.mark.parametrize(
    "name,base_sets,flag", _HOOK_SITES, ids=[s[0] for s in _HOOK_SITES]
)
def test_hook_pull_policy_never_leaks_from_the_application_image(
    name: str, base_sets: tuple[str, ...], flag: str
) -> None:
    completed = _render(*base_sets, f"{flag}={_PINNED_DHO}", "image.pullPolicy=Never")
    assert completed.returncode == 0, completed.stderr
    container_name = {
        "provision-roles": "provision-roles",
        "river-migrate": "river-migrate",
    }[name]
    assert f"name: {container_name}\n          image:" in completed.stdout, (
        completed.stdout
    )
    # The container block for this hook must carry IfNotPresent, not the
    # application image's Never.
    block = completed.stdout.split(f"name: {container_name}\n")[1]
    assert "imagePullPolicy: IfNotPresent" in block.split("---")[0], block.split("---")[
        0
    ]


def test_hook_pull_policy_is_still_overridable_per_hook() -> None:
    completed = _render(
        "migrations.hook.provisionRoles.enabled=true",
        "migrations.hook.riverMigrate.enabled=false",
        "migrations.hook.routeActivate.enabled=false",
        f"migrations.hook.provisionRoles.image={_PINNED_DHO}",
        "migrations.hook.provisionRoles.pullPolicy=Always",
        "image.pullPolicy=Never",
    )
    assert completed.returncode == 0, completed.stderr
    block = completed.stdout.split("name: provision-roles\n")[1]
    assert "imagePullPolicy: Always" in block.split("---")[0], block.split("---")[0]
