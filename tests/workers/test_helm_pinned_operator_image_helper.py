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

The identity check (D2684, D2693, then D2696 -- all posted in full to the
CHAOS-6958 Linear thread) is a NAME ALLOWLIST, checked for DIGEST-pinned
references only: the image's last path element, registry/namespace/tag/digest
stripped, must be dev-health-go-operator or dev-health-go-dho,
"mirror-friendly" (any registry/namespace still matches, so a private mirror
under a different digest is unaffected). A sha-<12 hex>-TAGGED or `:local`
reference is NOT identity-checked (D2696: the reproduced P1 was digest-shaped;
this is what keeps test_provisioning_accepts_a_lockstep_match_and_a_sideloaded_
local_image's `dho:local` fixture green untouched, and see
test_hook_accepts_the_python_image_when_only_tag_pinned below for the
disclosed boundary this draws).

A repository cross-check against another hook's own configured image
(migrations.hook.routeActivate.image) was tried and reverted (D2684 then
D2693) -- it broke two EXISTING tests in test_helm_migration_hook_chain.py
that pin a hook's OWN image winning over routeActivate's from a DIFFERENT
repository, by design. A DENYLIST of every non-dho image this project
publishes or has retired runs too, as a second layer with more specific
messages; the allowlist alone already closes the r1 round's reproduction (an
arbitrary third-party image used to render clean).

"Overridable" (test_route_activate_operator_image_override_is_honoured,
test_helm_route_activate_hooks.py) means registry, namespace, tag or digest --
never the image NAME, which the chart never promised (D2696): that test's
fixture is now a same-name mirror path, paired with a new negative test
proving a digest-pinned FOREIGN name is refused
(test_route_activate_refuses_a_foreign_operator_name).
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
def test_hook_refuses_the_python_image_even_when_digest_pinned(
    name: str, base_sets: tuple[str, ...], flag: str
) -> None:
    """The r1 round's concrete reproduction, digest-pinned (D2696 scopes
    identity to digest-pinned references only): a Python api image used to
    render clean -- exit 0 -- then fail at runtime."""
    code, stderr = _render_stderr(
        *base_sets,
        f"{flag}=ghcr.io/full-chaos/dev-hops-api@sha256:" + "a" * 64,
    )
    assert code != 0, f"{name}: the Python image rendered clean"
    assert "is not a pinned dho image" in stderr, stderr
    assert "Python api image" in stderr, stderr


@pytest.mark.parametrize(
    "name,base_sets,flag", _HOOK_SITES, ids=[s[0] for s in _HOOK_SITES]
)
def test_hook_accepts_the_python_image_when_only_tag_pinned(
    name: str, base_sets: tuple[str, ...], flag: str
) -> None:
    """The disclosed scope boundary (D2696): identity is NOT checked for a
    sha-<12 hex>-TAGGED reference, only a digest-pinned one -- the ORIGINAL
    CHAOS-6951 finding used this exact tag form and this repo's own
    lockstep-matching image.tag, and it still renders clean today. Pinned
    here so a future change that silently narrows or widens this boundary is
    visible in the diff, not just in prose."""
    code, stderr = _render_stderr(
        *base_sets,
        f"{flag}=ghcr.io/full-chaos/dev-hops-api:sha-aaaaaaaaaaaa",
        "image.tag=sha-aaaaaaaaaaaa",
    )
    assert code == 0, stderr


@pytest.mark.parametrize(
    "retired_repo",
    [
        "ghcr.io/full-chaos/dev-health-query-api",
        "ghcr.io/full-chaos/dev-health-go-worker",
    ],
)
def test_migrate_job_refuses_other_retired_images_by_name(retired_repo: str) -> None:
    """Asserts the DENYLIST's own message ("has no dho entrypoint"), not just
    a bare refusal: the name allowlist alone would ALSO refuse this image
    (neither name is dev-health-go-operator or dev-health-go-dho), so a
    refusal-only assertion cannot tell the denylist entry apart from the
    allowlist doing the same job."""
    code, stderr = _render_stderr(
        f"migrations.hook.image={retired_repo}@sha256:" + "a" * 64
    )
    assert code != 0, f"{retired_repo} rendered clean"
    assert "has no dho entrypoint" in stderr, stderr


@pytest.mark.parametrize(
    "retired_repo",
    [
        "ghcr.io/full-chaos/dev-health-go-migrate",
        "ghcr.io/full-chaos/dev-health-go-api-tools",
        "ghcr.io/full-chaos/dev-health-go-stream-runner",
        "ghcr.io/full-chaos/dev-health-go-reconciler",
        "ghcr.io/full-chaos/dev-health-go-scheduler",
    ],
)
def test_migrate_job_refuses_every_published_non_dho_image_by_name(
    retired_repo: str,
) -> None:
    """CHAOS-6958 r1 round follow-up: the denylist was incomplete -- a REAL,
    currently-published, non-retired first-party image
    (dev-health-go-migrate, entrypoint dev-health-worker-migrate) satisfied the
    pin-shape rule and rendered clean. This covers every non-dho name this
    project actually builds (.github/workflows/docker-images.yml:1545) or has
    retired (go-workers.yaml's own denylist), dev-health-go-contractcheck
    excepted -- its entrypoint IS dho (docker/go-worker.Dockerfile:121-124)."""
    code, stderr = _render_stderr(
        f"migrations.hook.image={retired_repo}@sha256:" + "b" * 64
    )
    assert code != 0, f"{retired_repo} rendered clean"
    # The denylist's OWN message, not the generic allowlist one -- both would
    # refuse this image (neither name is dev-health-go-operator or
    # dev-health-go-dho either), so this is what actually exercises the
    # denylist entry rather than the allowlist doing the same job.
    assert "has no dho entrypoint" in stderr, stderr


def test_migrate_job_refuses_the_contractcheck_image_under_the_named_allowlist() -> (
    None
):
    """PENDING A RULING, not a silent decision: dev-health-go-contractcheck's
    entrypoint IS dho (docker/go-worker.Dockerfile:121-124, the same target
    shape as dev-health-go-dho and dev-health-go-operator), so it is arguably a
    THIRD name the allowlist should carry -- but D2693 named exactly two
    (dev-health-go-operator, dev-health-go-dho), and this project has never
    pinned migrations.hook.image to dev-health-go-contractcheck in a chart
    value or test, so widening the list unasked is not this test's call. This
    pins the CURRENT, literal two-name behavior; flip it to `code == 0` only
    once the lead's list is confirmed to include the third name."""
    code, stderr = _render_stderr(
        "migrations.hook.image=ghcr.io/full-chaos/dev-health-go-contractcheck@sha256:"
        + "c" * 64
    )
    assert code != 0, stderr
    assert "is not a pinned dho image" in stderr, stderr


def test_allowlist_names_are_anchored_not_substring_matched() -> None:
    """A repository name that merely SHARES A PREFIX with an ALLOWED name (but
    is not it) must still be refused -- an unanchored regex would over-MATCH
    and accept a look-alike, unauthorized image as if it were the real one."""
    code, stderr = _render_stderr(
        "migrations.hook.image=ghcr.io/example/dev-health-go-dhotool@sha256:" + "e" * 64
    )
    assert code != 0, "a look-alike name (dev-health-go-dhotool) rendered clean"
    assert "is not a pinned dho image" in stderr, stderr


def test_third_party_image_is_now_refused_by_the_name_allowlist() -> None:
    """D2693 closed the r1 round's actual reproduction: an arbitrary
    THIRD-PARTY image that satisfies the pin-shape rule used to render clean
    (the denylist alone could never catch it); the name allowlist now refuses
    it directly, no longer a disclosed residual."""
    code, stderr = _render_stderr(
        "migrations.hook.image=postgres:18-alpine@sha256:" + "9" * 64
    )
    assert code != 0, "a third-party image (postgres) rendered clean"
    assert "is not a pinned dho image" in stderr, stderr


def test_route_activate_refuses_a_foreign_operator_name_here_too() -> None:
    """D2696 (final): a custom NAME is never an honoured override -- only
    registry/namespace/tag/digest are. The real fixtures and the negative
    control live in test_helm_route_activate_hooks.py
    (test_route_activate_operator_image_override_is_honoured, now a SAME-NAME
    mirror path, and test_route_activate_refuses_a_foreign_operator_name);
    this is the same assertion from this file's own render helper, so a
    change to either helper shows up in both suites."""
    code, stderr = _render_stderr(
        "migrations.hook.provisionRoles.enabled=true",
        "migrations.hook.riverMigrate.enabled=true",
        "migrations.hook.routeActivate.image=ghcr.io/example/custom-operator@sha256:"
        + "f" * 64,
    )
    assert code != 0, "a digest-pinned foreign operator name rendered clean"
    assert "is not a pinned dho image" in stderr, stderr


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
