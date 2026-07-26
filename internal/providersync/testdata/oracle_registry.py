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
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

RowBuilder = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class PairSpec:
    """One declared Python<->Go comparison boundary.

    id: a stable identifier, e.g. "github/prs/row". Pairs for the same
        provider/dataset MAY register more than one PairSpec when they cross
        genuinely different boundaries (e.g. row construction vs a list
        inclusion decision) -- these are not interchangeable and must not be
        collapsed into one id just to reduce bookkeeping.
    build_row: given one JSON-serializable case dict, calls the REAL,
        live production function chain and returns the COMPLETE result as a
        plain, JSON-serializable dict -- every field the underlying object
        exposes, not a chosen subset. The generic comparator (Go side) diffs
        every key this returns against every key the Go side produces.
    excluded_fields: {field_name: reason}. A field listed here is skipped by
        the comparator on BOTH sides. A reason is required -- this mirrors
        `expected_survivor_reason` in scripts/mutation_harness.py: an
        omission must be declared, in writing, at registration time, never
        discovered by a reader wondering why a field went untested.
    """

    id: str
    build_row: RowBuilder
    excluded_fields: dict[str, str] = field(default_factory=dict)


_REGISTRY: dict[str, PairSpec] = {}


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
    _REGISTRY[spec.id] = spec


def get(pair_id: str) -> PairSpec:
    if pair_id not in _REGISTRY:
        raise KeyError(
            f"pair {pair_id!r} is not registered. Registering a pair is a side "
            "effect of importing its testdata/oracle_pairs/<module>.py -- the "
            "generic runner imports that module by the naming convention "
            "'oracle_pairs.' + pair_id.split('/')[0] + '_' + pair_id.split('/')[1] "
            "+ '_' + pair_id.split('/')[2] before looking the id up here."
        )
    return _REGISTRY[pair_id]
