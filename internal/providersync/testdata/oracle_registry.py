"""Generic, declarative Python<->Go parity oracle registry (CHAOS-3162).

Every existing oracle in this directory before CHAOS-3162
(python_launchdarkly_normalization_oracle.py and friends) hand-picks which
fields to emit and the Go side hand-picks which fields to assert. That
worked exactly as well as its author's imagination: github/prs's first
oracle was hand-authored and omitted review enrichment while asserting the
omitted fields were zero; its replacement decoded three more fields from the
real function and never asserted them. Two review rounds, same shape of
defect, because nothing forced either oracle to speak for every field it
could see.

This module is the fix for the PLUMBING, not a replacement for any existing
oracle (python_oracle_loader.py's isolation trick -- a fresh stub namespace
so a stock interpreter can execute real production functions, with the
target source staying live -- is correct and stays exactly as it is; this
module is built on top of it, not instead of it).

A pair registers itself by importing its own module under
testdata/oracle_pairs/ and calling `register(...)` there. Nothing in this
file, or in python_generic_row_oracle.py, needs to change to add a pair --
that is the whole point: the loader's old ALLOWED_MODULES dict required
editing loader source for every new heavy-dependency target; this registry
does not.

CODEX FINDING #1 (first adversarial review of this framework, CHAOS-3162):
a declarative comparator does not, by itself, stop a pair's OWN field set
from being hand-picked. `PairSpec.build_row` could return any dict a pair
author chose to construct; nothing forced it to be complete. A future pair
could expose three fields on both the Python and Go sides, omit an entire
phase on both, and the generic comparator would report a perfect match on
that narrowed intersection -- reproducing the exact defect this framework
exists to eliminate, one level up. `reflected_fields` closes that: it is a
REQUIRED, zero-argument callable that returns the complete field set the
underlying PRODUCTION function is capable of emitting, derived by
statically parsing that function's own source (see field_reflection.py),
never by hand-maintaining a second list that can drift from, or simply
start incomplete relative to, the first. `register()` calls it eagerly and
stores the result so python_generic_row_oracle.py can enforce, for every
case: (row keys) | (excluded_fields keys) >= reflected_fields() -- any
reflected field that is neither present in the row nor explicitly,
individually excluded with a written reason is a hard failure, not a
silent gap.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

RowBuilder = Callable[[dict[str, Any]], dict[str, Any]]
FieldReflector = Callable[[], frozenset[str]]


@dataclass(frozen=True)
class PairSpec:
    """One declared Python<->Go comparison boundary.

    id: a stable identifier, e.g. "github/prs/row". Pairs for the same
        provider/dataset MAY register more than one PairSpec when they cross
        genuinely different boundaries (e.g. row construction vs a list
        inclusion decision) -- these are not interchangeable and must not be
        collapsed into one id just to reduce bookkeeping.
    build_row: given one JSON-serializable case dict, calls the REAL,
        live production function chain and returns the result as a plain,
        JSON-serializable dict.
    reflected_fields: zero-argument callable returning the complete,
        non-empty frozenset of field names the underlying PRODUCTION
        function is capable of emitting -- derived by inspecting that
        function's own source (field_reflection.py's dict_literal_keys, for
        a Python builder that assembles its result from named dict
        literals), never hand-maintained separately from build_row. Called
        once, eagerly, at register() time: a pair whose reflection is
        broken fails at import time, not silently the first time someone
        runs its comparison.
    excluded_fields: {field_name: reason}. A field listed here is skipped by
        the comparator on BOTH sides. A reason is required -- this mirrors
        `expected_survivor_reason` in scripts/mutation_harness.py: an
        omission must be declared, in writing, at registration time, never
        discovered by a reader wondering why a field went untested.
    """

    id: str
    build_row: RowBuilder
    reflected_fields: FieldReflector
    excluded_fields: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class _Registered:
    spec: PairSpec
    reflected: frozenset[str]


_REGISTRY: dict[str, _Registered] = {}


def register(spec: PairSpec) -> None:
    if not spec.id or "/" not in spec.id:
        raise ValueError(
            f"pair id must be '<provider>/<dataset>/<boundary>', got {spec.id!r}"
        )
    if spec.id in _REGISTRY:
        raise ValueError(f"pair {spec.id!r} is already registered")
    for name, reason in spec.excluded_fields.items():
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(
                f"pair {spec.id!r}: excluded_fields[{name!r}] needs a non-empty "
                "written reason, not a bare exclusion"
            )
    reflected = spec.reflected_fields()
    if not isinstance(reflected, frozenset) or not reflected:
        raise ValueError(
            f"pair {spec.id!r}: reflected_fields() must return a non-empty "
            f"frozenset, got {reflected!r} -- a pair that cannot state its "
            "own complete field set cannot prove a row is complete either"
        )
    _REGISTRY[spec.id] = _Registered(spec=spec, reflected=reflected)


def get(pair_id: str) -> PairSpec:
    return _get_entry(pair_id).spec


def check_completeness(pair_id: str, case_id: str, row: dict[str, Any]) -> None:
    """Raise if `row` (one case's build_row output) omits any field
    reflected_fields() says the production function can emit, unless that
    field is declared in excluded_fields.

    This is the actual enforcement point for codex finding #1: called by
    python_generic_row_oracle.py once per case (not once per pair), so a
    pair cannot pass a completeness-blind case just because some OTHER case
    happens to exercise the full field set.
    """
    entry = _get_entry(pair_id)
    declared = set(row.keys()) | set(entry.spec.excluded_fields.keys())
    missing = entry.reflected - declared
    if missing:
        raise ValueError(
            f"pair {pair_id!r}, case {case_id!r}: build_row's output is "
            f"missing field(s) {sorted(missing)!r} that the production "
            "function can emit (per reflected_fields()) -- either the row "
            "must include them, or each one needs its own excluded_fields "
            "entry with a written reason. A silently incomplete row is "
            "exactly the defect this framework exists to prevent."
        )


def _get_entry(pair_id: str) -> _Registered:
    if pair_id not in _REGISTRY:
        raise KeyError(
            f"pair {pair_id!r} is not registered. Registering a pair is a side "
            "effect of importing its testdata/oracle_pairs/<module>.py -- the "
            "generic runner imports that module by the naming convention "
            "'oracle_pairs.' + pair_id.split('/')[0] + '_' + pair_id.split('/')[1] "
            "+ '_' + pair_id.split('/')[2] before looking the id up here."
        )
    return _REGISTRY[pair_id]
