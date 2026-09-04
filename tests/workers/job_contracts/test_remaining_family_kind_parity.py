"""Cross-language parity between the Python job_contracts mirror's
KIND_REMAINING_* constants and internal/jobs/metrics/remaining/families.json.

CHAOS-5007: families.json is the single source for which remaining-metrics
families exist and what job kind (route_key) each maps to. The Go side
derives its family->kind lookup directly from families.json (see
internal/jobs/metrics/remaining/publisher.go's loadFamilyJobKinds), so it can
no longer drift from it. The Python mirror still needs its own hand-written
KIND_REMAINING_* string constants -- job_contracts/models.py's envelope
dataclasses need a concrete `ClassVar[str]`, not a value loaded from JSON at
import time -- so nothing structurally prevents THIS side from drifting the
way publisher.go's old hardcoded map did. This test is that guard: it
compares the constants' VALUES against families.json's route_key values as
sets, not by name (family names like "membership_backfill" are deliberately
shortened to e.g. KIND_REMAINING_MEMBERSHIP on both the Python and Go sides,
so a name-based lookup would be the wrong axis to assert on).
"""

from __future__ import annotations

import json
from pathlib import Path

from dev_health_ops.workers import job_contracts

_FAMILIES_PATH = (
    Path(__file__).parents[3]
    / "internal"
    / "jobs"
    / "metrics"
    / "remaining"
    / "families.json"
)

_KIND_REMAINING_PREFIX = "KIND_REMAINING_"


def _python_remaining_kinds() -> frozenset[str]:
    """Every `KIND_REMAINING_*` string constant the package actually exports,
    found by INTROSPECTION rather than a hand-typed list -- a hand-typed list
    silently stops proving anything the moment a new KIND_REMAINING_* constant
    is added without also being added here (codex round 1, P3: the orphan/
    stale-constant half of this test's claimed bidirectional check was
    vacuous, since a constant missing from the list is invisible to a set
    built FROM that same list). Introspecting `job_contracts`'s own namespace
    means a new constant is included automatically, so the orphan direction
    is actually checked.
    """
    return frozenset(
        value
        for name, value in vars(job_contracts).items()
        if name.startswith(_KIND_REMAINING_PREFIX) and isinstance(value, str)
    )


def test_python_remaining_kind_constants_match_families_json_route_keys() -> None:
    inventory = json.loads(_FAMILIES_PATH.read_text())
    route_keys = frozenset(family["route_key"] for family in inventory["families"])
    assert route_keys, "families.json produced no families -- test would pass vacuously"

    python_kinds = _python_remaining_kinds()
    assert python_kinds, (
        "found no KIND_REMAINING_* constants on dev_health_ops.workers.job_contracts "
        "-- test would pass vacuously (either the introspection broke, or the "
        "package stopped re-exporting them)"
    )

    missing_in_python = sorted(route_keys - python_kinds)
    orphaned_in_python = sorted(python_kinds - route_keys)
    assert not missing_in_python and not orphaned_in_python, (
        f"families.json route_keys with no matching Python KIND_REMAINING_* "
        f"constant: {missing_in_python} -- Python KIND_REMAINING_* constants "
        f"with no matching families.json route_key (stale/renamed family): "
        f"{orphaned_in_python}"
    )
