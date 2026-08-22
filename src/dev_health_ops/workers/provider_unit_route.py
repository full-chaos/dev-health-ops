"""Fail-closed transport gate for complete provider sync-unit routes.

Routability is derived entirely from the checked-in provider capability matrix
(``contracts/provider-matrix/v1/matrix.json``), which Go generates from its
execution registry. A pair is routable when BOTH hold:

  1. the matrix marks the pair ``route_ready`` -- the route is shipped,
     reviewed, and registered in the execution registry; and
  2. the matrix marks the pair ``plannable`` -- it is the canonical writer
     identity of its family, not an alias that folds into another writer
     (``pr-reviews``/``pr-comments`` fold into ``prs``, ``tests`` into
     ``cicd``, the four ``work-item-*`` aliases into ``work-items``).

There is no third condition. CHAOS-4054 deleted the route enablement
environment plane outright: capability is always on in the binary, the user's
sync config (``IntegrationDataset.is_enabled``) is the only authority on what
should run, and ``-Q`` queue topology in tracked service definitions is the
only authority on where it can run. Nothing about a shipped route's
functionality is hidden behind an environment switch, so a route that is not
running has exactly two visible explanations: the user turned it off, or no
deployed worker consumes its queue.

Landing a new ``route_ready`` row in the matrix is therefore sufficient to move
live traffic, and that is deliberate -- readiness is a reviewed code fact, not
a deployment toggle.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from dev_health_ops.contract_artifacts import contract_directory
from dev_health_ops.sync.family_flags import (
    FAMILY_CANONICAL_DATASET_KEY as _FAMILY_CANONICAL_DATASET_KEY,
)
from dev_health_ops.sync.family_flags import (
    WORK_ITEM_DATASETS as _WORK_ITEM_FAMILY_DATASET_ORDER,
)
from dev_health_ops.workers.provider_family_contract import (
    FamilyExecutionMode,
    provider_family_policy,
    validate_provider_family_claim,
)

# Resolved through contract_artifacts rather than a per-file parents[N] count.
# The count here used to be 3, correct in a checkout and wrong in an installed
# distribution, where the same three hops land on the interpreter's lib
# directory instead of the repository root (CHAOS-3933).
_DEFAULT_MATRIX_CONTRACT_PATH = (
    contract_directory("provider-matrix", "v1") / "matrix.json"
)

# Reassignable only by tests (see tests/workers/test_provider_unit_route.py),
# so a hypothetical second route_ready pair can be exercised end to end
# through a fixture contract without ever editing the checked-in matrix.
# Production code never assigns to this.
_MATRIX_CONTRACT_PATH = _DEFAULT_MATRIX_CONTRACT_PATH

# The planner owns the exact family order and canonical claim identity. Import
# those values rather than copying five dataset names into this admission gate:
# a future planner alias cannot silently acquire a partially-wired route here.
_GITHUB_WORK_ITEM_FAMILY_DATASETS = frozenset(_WORK_ITEM_FAMILY_DATASET_ORDER)


class ProviderUnitRouteError(ValueError):
    """Value-free rejection of an unreadable or incomplete route contract."""


@dataclass(frozen=True, slots=True)
class _MatrixRoutes:
    ready: frozenset[tuple[str, str]]
    plannable: frozenset[tuple[str, str]]
    #: Every route-ready pair mapped onto the canonical writer identity that
    #: serves it. Read by CHAOS-4078, which owns folding an alias-only dataset
    #: selection onto that writer; nothing plans from it yet.
    canonical: Mapping[tuple[str, str], str]


@cache
def _load_matrix_routes(path: Path) -> _MatrixRoutes:
    """Read a capability matrix contract and return its routable pair sets.

    This is the single Python reader of the contract that Go's
    ``BuildProviderMatrix`` / ``TestProviderMatrixMatchesCheckedInContract``
    (``internal/providersync/capability_matrix_test.go``) produces and freezes
    byte-for-byte (CUT-08). A pair absent from the file, or present with
    ``route_ready: false``, can never be routable -- this is the "unknown or
    incomplete kinds fail readiness" half of the PRD requirement. A pair
    present with ``plannable: false`` is an alias identity: real for capability,
    audit, and watermark purposes, never a unit of its own.
    """

    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise ProviderUnitRouteError(
            "provider capability matrix contract is unreadable"
        ) from error
    ready: set[tuple[str, str]] = set()
    plannable: set[tuple[str, str]] = set()
    canonical: dict[tuple[str, str], str] = {}
    for pair in raw.get("pairs", ()):
        key = (
            str(pair["provider"]).strip().lower(),
            str(pair["dataset"]).strip().lower(),
        )
        if pair.get("route_ready") is not True:
            continue
        ready.add(key)
        canonical[key] = str(pair.get("canonical_dataset") or key[1]).strip().lower()
        if pair.get("plannable") is True:
            plannable.add(key)
    return _MatrixRoutes(
        ready=frozenset(ready),
        plannable=frozenset(plannable),
        canonical=canonical,
    )


def _matrix_routes() -> _MatrixRoutes:
    return _load_matrix_routes(_MATRIX_CONTRACT_PATH)


def clear_matrix_cache() -> None:
    """Drop the cached matrix read.

    Test-only: production reads the checked-in contract exactly once per
    process and never reloads it mid-run. Tests call this after repointing
    ``_MATRIX_CONTRACT_PATH`` at a fixture, or after restoring the default,
    so a stale cache entry from an earlier test can't leak in.
    """

    _load_matrix_routes.cache_clear()


def is_route_ready(provider: str, dataset: str) -> bool:
    """Whether the capability matrix marks this pair ``route_ready``."""

    return (
        provider.strip().lower(),
        dataset.strip().lower(),
    ) in _matrix_routes().ready


def is_plannable(provider: str, dataset: str) -> bool:
    """Whether this pair is the canonical, independently plannable identity.

    An alias identity is ``route_ready`` but never plannable: it folds into
    the canonical writer of its family, so planning it separately would mint a
    unit no writer owns.
    """

    return (
        provider.strip().lower(),
        dataset.strip().lower(),
    ) in _matrix_routes().plannable


def canonical_identity(provider: str, dataset: str) -> str | None:
    """The canonical writer identity that serves this pair, or ``None``.

    This is the registry's answer to "which writer owns this alias", published
    so the alias-fold work in CHAOS-4078 has one authority to plan from rather
    than a second hand-maintained alias table. It is deliberately NOT consulted
    by the planners yet: folding an alias-only selection onto its canonical
    writer also requires normalising watermark loading and sync-coverage scope
    matching, and doing it at the planner alone silently breaks both.

    A third surface used to be on that list -- the Celery fallback's processor
    flags -- but CHAOS-4054 step 4 deleted the fallback, so an alias-only
    selection can no longer diverge there. The remaining two are what CHAOS-4078
    still has to normalise.
    """

    key = (provider.strip().lower(), dataset.strip().lower())
    routes = _matrix_routes()
    if key not in routes.ready:
        return None
    canonical = routes.canonical.get(key, key[1])
    if (key[0], canonical) not in routes.plannable:
        return None
    return canonical


def routes_to_river(provider: str, dataset: str) -> bool:
    """Whether the Go runtime may plan and execute this pair.

    Readiness alone is not enough: an alias identity is ready (its capability
    is real, its watermark is real) but is served through its canonical writer,
    never as its own unit.
    """

    return is_route_ready(provider, dataset) and is_plannable(provider, dataset)


def is_github_work_item_direct_alias(provider: str, dataset: str) -> bool:
    """Whether a pair is a malformed persisted alias, not a planner claim."""

    return (
        provider.strip().lower() == "github"
        and dataset.strip().lower() in _GITHUB_WORK_ITEM_FAMILY_DATASETS
        and dataset.strip().lower() != _FAMILY_CANONICAL_DATASET_KEY
    )


def is_atomic_provider_family_direct_alias(provider: str, dataset: str) -> bool:
    """Whether a persisted claim is a non-canonical atomic-family alias."""

    normalized_provider = provider.strip().lower()
    normalized_dataset = dataset.strip().lower()
    policy = provider_family_policy(normalized_provider, normalized_dataset)
    return (
        policy is not None
        and policy.mode is FamilyExecutionMode.ATOMIC_CANONICAL
        and normalized_dataset != policy.canonical_dataset
    )


def is_github_work_item_family_dataset(provider: str, dataset: str) -> bool:
    """Whether a pair belongs to GitHub's planner-collapsed work-item family."""

    return (
        provider.strip().lower() == "github"
        and dataset.strip().lower() in _GITHUB_WORK_ITEM_FAMILY_DATASETS
    )


def is_complete_github_work_item_family_claim(
    provider: str,
    dataset: str,
    processor_flags: Mapping[str, object] | None,
) -> bool:
    """Return whether a Go-admitted GitHub work-item unit is canonical and full.

    Planner output is exactly one canonical ``work-items`` unit carrying all
    five ordered ``family_dataset_*`` flags. The activation route cannot safely
    accept a subset: the Go route writes the complete sixteen-destination
    family, while completion fans watermarks back to every alias. A malformed
    direct alias or a partial canonical row is an ownership/configuration fault
    and is refused before the producer enqueues either runtime.
    """

    return (
        provider.strip().lower() == "github"
        and dataset.strip().lower() == _FAMILY_CANONICAL_DATASET_KEY
        and validate_provider_family_claim(
            provider, dataset, processor_flags, strict_atomic=True
        )
    )
