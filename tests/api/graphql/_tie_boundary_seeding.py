"""The tie-at-LIMIT seeding standard for the go_api dual-run harnesses
(CHAOS-4513).

Ruling (team-lead, 2026-08-29, on CHAOS-4511): "dual-run green" is not
evidence of deterministic ordering. A stage-2 dual-run compares Python rows
to Go rows; if both planes run the same non-deterministic ``ORDER BY ...
LIMIT``, they can still agree on any given run -- and if the seed data never
produces more tied rows than the LIMIT, the truncation boundary is never
exercised at all, so the comparator cannot possibly observe the defect. This
module is the "default affordance" the ticket asks for, so a harness gets
this seeding by calling a helper rather than hand-rolling it (and getting it
wrong the way a single-INSERT-plus-OPTIMIZE proof does -- see the warning on
:func:`seed_multi_part_rows` below).

Two distinct tie classes, both required where they apply to a query:

1. **Value ties at the LIMIT boundary** -- for any ``ORDER BY ... LIMIT``
   query, seed MORE rows tied on the query's ENTIRE declared sort key than
   the LIMIT, so the truncation boundary actually falls inside the tied
   block. A query whose sort key IS the row's own uniqueness key (e.g. a
   GROUP BY key with no other column) cannot have this class of defect --
   note that in the harness instead of skipping seeding silently.

2. **Version-conflict ties on ReplacingMergeTree-backed tables** -- two
   *pre-merge versions of the same logical row* (identical on every ORDER BY
   column, differing only in the version column and payload) are a true tie
   that no tie-break on the row's own key columns can resolve. Any query
   reading a ReplacingMergeTree table (a ``FROM ... FINAL`` or an explicit
   argMax/GROUP BY dedup subquery is the tell) must ALSO seed an unmerged
   version-conflict pair and assert which row survives.

**MUST PROVE ITS KILL** (team-lead, 2026-08-29): a tie-order proof that
passes is not evidence until it has been shown to FAIL against the
tie-break-removed SQL/mutation. Cite both the failing run and the passing
run when writing up TEST-EVIDENCE. If a wrapped aggregation resists the
mutation (the CHAOS-4495 ``breakdown_template`` open question -- an
``argMax(...) GROUP BY`` stage can produce an already-aggregated in-memory
result that sorts stably regardless of tie-break), that is not evidence the
shape is safe; it is *uncharacterised*. Fall back to the SQL-text pin +
documented ``ORDER BY`` semantics and label the live test a
stability-at-scale proof, never a mutation kill, when the mutation cannot be
made to fail.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

# A proof-coverage floor, not a magic number: CHAOS-4513's own investigation
# (the CHAOS-4495 lane, "breakdown_template") found that 5 rows in one
# INSERT collapses to a single part/block, where even a tie-break-free
# ORDER BY returns a stable order incidentally -- and that the hazard only
# reproduced at 30-300 single-row-insert parts. This floor is deliberately
# inside that demonstrated range; callers seeding a genuinely large tied set
# should still use MIN_TIED_ROWS_FOR_MULTI_PART or their own larger count,
# never fewer.
MIN_TIED_ROWS_FOR_MULTI_PART = 40


def seed_multi_part_rows(
    insert_one: Callable[[int], None],
    *,
    count: int = MIN_TIED_ROWS_FOR_MULTI_PART,
) -> None:
    """Calls ``insert_one(i)`` once per row, in ``count`` SEPARATE calls.

    Never call your sink's ``.insert(...)`` once with a list of ``count``
    rows -- that is one INSERT, which ClickHouse is free to (and typically
    does) materialize as a single part, making the scan trivially stable
    with or without a tie-break and proving nothing (CHAOS-4513's own
    "breakdown_template" finding: "the original test was measuring
    nothing... it inserted its rows in ONE INSERT... A single-block scan is
    trivially stable with or without a tie-break"). ``count`` separate
    INSERTs is what forces the multi-part, multi-threaded scan a tie-order
    proof needs to be discriminating. Do NOT run ``OPTIMIZE TABLE ... FINAL``
    after seeding for a value-tie proof -- that merges the parts back into
    one and defeats the point; FINAL is appropriate only when the query
    under test itself reads via FINAL (the version-conflict class below,
    where merge-resolution behavior is exactly what's being proven).

    ``insert_one(i)`` receives the 0-based row index so callers can vary
    whatever per-row identity column (e.g. a UUID pk) needs to stay unique
    while every ORDER BY column stays tied.
    """
    for i in range(count):
        insert_one(i)


def tied_row_count_for_limit(limit: int) -> int:
    """The minimum tied-row count to use for a boundary-tie proof at a given
    LIMIT: strictly more than the limit (so truncation actually lands inside
    the tied block), and never fewer than :data:`MIN_TIED_ROWS_FOR_MULTI_PART`
    (so the seed still forces a multi-part scan even for a tiny limit)."""
    return max(limit + 1, MIN_TIED_ROWS_FOR_MULTI_PART)


def new_tied_identity_pool(count: int) -> list[str]:
    """``count`` distinct identity values (e.g. one per tied row) so rows
    tie on every ORDER BY / sort-key column while remaining distinguishable
    on whatever primary-id column the response actually returns."""
    return [str(uuid.uuid4()) for _ in range(count)]


async def assert_stable_across_repeated_calls(
    fetch_ordered_ids: Callable[[], Any],
    *,
    repeats: int = 8,
) -> Any:
    """Awaits ``fetch_ordered_ids()`` ``repeats`` times (it may return a
    plain value or an awaitable) and asserts every call returned the
    identical ordered id sequence. Returns the (single, shared) sequence for
    the caller's own assertion against an expected value.

    Caller awaits this from an ``async def`` test (pytest-asyncio) -- it
    does not spin its own event loop.

    This proves STABILITY, not a mutation kill by itself -- pair it with an
    explicit tie-break-removed mutation run (see module docstring) before
    citing it as ordering evidence rather than a stability-at-scale proof.
    """
    import inspect

    results = []
    for _ in range(repeats):
        value = fetch_ordered_ids()
        if inspect.isawaitable(value):
            value = await value
        results.append(value)

    first = results[0]
    for i, r in enumerate(results):
        assert r == first, (
            f"run {i}: not stable across {repeats} repeated calls -- "
            f"got {r!r}, run 0 got {first!r} (tie-boundary non-determinism)"
        )
    return first


def version_conflict_pair(
    identity: dict[str, Any],
    *,
    version_column: str,
    older_version: Any,
    newer_version: Any,
    older_payload: dict[str, Any] | None = None,
    newer_payload: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Builds two row dicts sharing ``identity`` (every ORDER BY / sort-key
    column) and differing ONLY in ``version_column`` and payload -- the
    ReplacingMergeTree version-conflict shape CHAOS-4513 requires for any
    query reading such a table. Insert both in SEPARATE calls and do NOT run
    OPTIMIZE ... FINAL before querying through the resolver's own ``FINAL``
    (or argMax/GROUP BY dedup) -- the point is proving the read-time
    resolution picks the newer version correctly while both versions are
    still unmerged parts, matching real production write timing.
    """
    older = {**identity, version_column: older_version, **(older_payload or {})}
    newer = {**identity, version_column: newer_version, **(newer_payload or {})}
    return older, newer


__all__ = [
    "MIN_TIED_ROWS_FOR_MULTI_PART",
    "seed_multi_part_rows",
    "tied_row_count_for_limit",
    "new_tied_identity_pool",
    "assert_stable_across_repeated_calls",
    "version_conflict_pair",
]
