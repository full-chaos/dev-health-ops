"""CHAOS-4506 / CHAOS-4534 NaN-class proof, PYTHON side: does a real
empty-column window reach Python's on-the-wire JSON serialization as a
literal `NaN` token.

STATUS: WRITTEN, NOT YET EXECUTED. Discretionary per orchestrator ruling
2026-08-29 -- the slot's mandatory items are the 3 flow-matrix dual-run
tests and the full `ci/local_validate.sh` gate; this proof runs only if
the slot is still open after those are green, and is fine to leave
written-but-unexecuted if it is not (deploy56 already read prod and found
NO NaN reaching the wire across 538 executions, so its value is
documentation of a latent class, not a blocker).

WHY THIS IS A DIFFERENT SHAPE FROM THE FLOW-MATRIX DUAL-RUN FILE: that
file drives BOTH planes through the real routed HTTP path
(FLOW_MATRIX_QUERY is a registered document). Breakdown has NO registered
document yet -- query_route.go only registers flowMatrix this wave, and
both real production documents that select breakdown
(INVESTMENT_BREAKDOWN_QUERY / INVESTMENT_FULL_QUERY) send
`useInvestment: true` unconditionally and are blocked on CHAOS-4538 --
so there is no live HTTP entry point to dual-run breakdown against. This
file therefore proves only the PYTHON side, by calling `resolve_analytics`
directly (the same seam `test_go_api_dual_run_flow_matrix.py`'s test 3
uses for its Python-side error snapshot). The companion GO-side proof is
`cmd/query-api/internal/analytics/nan_class_live_test.go`
(`//go:build integration`), which exercises `CompileBreakdown` ->
`ExecuteBreakdown` -> the real gqlgen marshal call directly, for the same
reason: no route to drive it through. Together the two files are the
split (populated proves the port / empty documents the divergence) the
standing NaN-class ruling requires; neither file alone is the proof.

MEASURE CHOSEN, AND WHY: `COVERAGE_LINE_PCT`, one of the 9 measures /
6 columns RISK-NOTES enumerates as genuinely AT RISK (`validate.go:184`,
`sql/validate.py`'s Measure.db_expression) -- `Nullable(Float64)` source
column, no NULLIF self-guard. Source table
`testops_coverage_metrics_daily`
(`src/dev_health_ops/migrations/clickhouse/029_testops_tables.sql:155-171`),
plain `MergeTree` (NOT a ReplacingMergeTree/dedup class -- CHAOS-4516 does
not apply here), column `line_coverage_pct Nullable(Float64)`.

THE SHAPE THIS SEEDS IS THE UNCHARACTERISED ONE, NOT THE CLEARED ONE.
BRIEF's "NaN UPDATE" cleared the ZERO-SOURCE-ROW case (a GROUP BY over
zero rows yields zero result rows, so no row exists to carry a NaN --
confirmed live for the FLAG_* measures against `release_impact_daily`,
which has never had a row). The case still marked "unobserved in 64 days,
not impossible" is DIFFERENT: a group that HAS rows (the pipeline ran,
`testops_coverage_metrics_daily` got a row) where the averaged column is
NULL in every one of them. That is exactly what this file seeds --
TWO rows for one repo/day window, `line_coverage_pct = NULL` in both --
so `AVG(line_coverage_pct)` runs against a real non-empty group and the
question is what it returns, not whether it runs at all.

SPLIT, PER THE STANDING RULING: two tests, not one reseeded around the
interesting case. `test_populated_window_returns_real_value_no_nan`
proves the port (a normal float, ordinary JSON, matching the real seeded
average). `test_empty_column_window_reaches_python_as_literal_nan_token`
documents the divergence. Never merge them to get a single green.

WHAT ELSE COULD PRODUCE math.isnan(value) OR A "NaN" SUBSTRING, SO THE
ASSERTION CANNOT PASS FOR THE WRONG REASON:
* A `nan` could come from a python-side arithmetic bug unrelated to the
  ClickHouse column (e.g. 0/0 in a different codepath) -- ruled out by
  pinning the POPULATED case to the exact seeded average value (not just
  asserting "not nan"), so a bug that always produced garbage would fail
  that test.
* A `"NaN"` substring in a JSON body could come from a stray string field
  containing the literal text "NaN" rather than the unquoted numeric
  token json.dumps emits for a real float('nan') -- ruled out by encoding
  the ACTUAL returned value (a Python float, `type(value) is float` and
  `math.isnan(value)` both asserted) through `json.dumps`, not a
  hand-built payload, and checking for the unquoted token specifically
  (`': NaN'` / `, NaN` shape, not merely `"NaN" in body`).
* The org/repo scoping could leak the populated row's value into the
  empty-column group (or vice versa) if both shared an org_id or repo_id
  -- ruled out with distinct org_ids AND distinct repo_ids per test, plus
  an explicit row-count/value assertion pinning which repo's group
  produced which result.

WHY json.dumps(value, allow_nan=True) AND NOT A FULL STRAWBERRY
ROUND-TRIP: BRIEF's NaN CLASS WARNING cites the exact mechanism --
Strawberry's `encode_json` is stdlib `json.dumps`
(`strawberry/http/base.py:54-55`), and `allow_nan=True` is the stdlib
default (unconditionally true unless a caller explicitly passes False,
which Strawberry's call site does not). This test calls that same
primitive with that same default directly on the real resolver output,
rather than driving a full schema execution + HTTP response cycle just to
reach the identical `json.dumps` call -- proportionate for a discretionary
proof; the mechanism cited is the literal stdlib call, not a
reconstruction of it.
"""

from __future__ import annotations

import json
import math
import os
import uuid
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    AnalyticsRequestInput,
    BreakdownRequestInput,
    DateRangeInput,
    DimensionInput,
    MeasureInput,
)
from dev_health_ops.api.graphql.resolvers.analytics import resolve_analytics
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]

# Real column contract per migrations/clickhouse/029_testops_tables.sql:155-171.
_COVERAGE_COLUMNS = [
    "repo_id",
    "day",
    "line_coverage_pct",
    "branch_coverage_pct",
    "lines_total",
    "lines_covered",
    "coverage_delta_pct",
    "uncovered_files_count",
    "coverage_regression_count",
    "team_id",
    "service_id",
    "org_id",
    "computed_at",
]


def _insert_coverage_row(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    repo_id: uuid.UUID,
    day: date,
    line_coverage_pct: float | None,
    computed_at: datetime,
) -> None:
    sink.client.insert(
        "testops_coverage_metrics_daily",
        [
            [
                repo_id,
                day,
                line_coverage_pct,
                None,  # branch_coverage_pct
                1000,  # lines_total
                None,  # lines_covered
                None,  # coverage_delta_pct
                0,  # uncovered_files_count
                0,  # coverage_regression_count
                None,  # team_id
                None,  # service_id
                org_id,
                computed_at,
            ]
        ],
        column_names=_COVERAGE_COLUMNS,
    )


def _breakdown_batch(start: date, end: date) -> AnalyticsRequestInput:
    return AnalyticsRequestInput(
        breakdowns=[
            BreakdownRequestInput(
                dimension=DimensionInput.REPO,
                measure=MeasureInput.COVERAGE_LINE_PCT,
                date_range=DateRangeInput(start_date=start, end_date=end),
                top_n=10,
            )
        ]
    )


def _cleanup(sink: ClickHouseMetricsSink, org_id: str) -> None:
    sink.client.command(
        "ALTER TABLE testops_coverage_metrics_daily DELETE WHERE org_id = {org_id:String} "
        "SETTINGS mutations_sync=2",
        parameters={"org_id": org_id},
    )


@pytest.mark.asyncio
async def test_populated_window_returns_real_value_no_nan():
    """Parity case: a normal, fully-populated window returns the real
    seeded average, not NaN, and its JSON encoding is a normal number.
    Establishes the seam works before using it to observe the divergence
    -- same "prove the harness first" discipline as the flow-matrix
    file's test 1.
    """
    assert CLICKHOUSE_URI is not None
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4506-nan-populated-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 10)
    now = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    try:
        # Two rows, same repo/day-range group, BOTH populated -- average
        # is pinned to a known value (85.0) so the assertion below proves
        # the mechanism reached the right column, not merely "not NaN".
        _insert_coverage_row(
            sink,
            org_id=org_id,
            repo_id=repo_id,
            day=day,
            line_coverage_pct=80.0,
            computed_at=now,
        )
        _insert_coverage_row(
            sink,
            org_id=org_id,
            repo_id=repo_id,
            day=day,
            line_coverage_pct=90.0,
            computed_at=now,
        )

        result = await resolve_analytics(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            _breakdown_batch(date(2026, 8, 1), date(2026, 8, 31)),
        )
    finally:
        _cleanup(sink, org_id)
        sink.close()

    assert len(result.breakdowns) == 1
    items = result.breakdowns[0].items
    assert len(items) == 1, f"expected exactly one REPO group, got {items}"
    value = items[0].value

    assert type(value) is float, f"expected a plain float, got {type(value)}"
    assert not math.isnan(value), f"populated window unexpectedly produced NaN: {value}"
    assert value == pytest.approx(85.0), (
        f"expected the pinned average of the two seeded rows (80.0, 90.0) = 85.0, "
        f"got {value} -- a wrong value here means the seed did not reach the "
        f"measure this test believes it is exercising"
    )

    body = json.dumps({"data": {"value": value}}, allow_nan=True)
    assert "NaN" not in body, f"unexpected NaN token in populated-window JSON: {body}"


@pytest.mark.asyncio
async def test_empty_column_window_reaches_python_as_literal_nan_token():
    """THE divergence this file exists to document: a group that HAS rows
    (the pipeline ran) where `line_coverage_pct` is NULL in every one of
    them. `AVG()` over an all-NULL `Nullable(Float64)` group is the
    UNCHARACTERISED shape BRIEF's NaN UPDATE explicitly left open (as
    opposed to the CLEARED zero-source-row shape). Asserts the raw
    returned value IS NaN (not 0.0, not None -- a wrong fallback here
    would silently prove nothing), and that the exact stdlib primitive
    Strawberry's `encode_json` calls (`json.dumps(..., allow_nan=True)`,
    `strawberry/http/base.py:54-55`) reproduces the unquoted `NaN` token
    RFC 8259 forbids -- an HTTP-200-with-invalid-JSON body, not an
    exception on the Python side.

    Do NOT "fix" this by reseeding around it to make the suite green
    (standing NaN-class ruling) and do NOT read a future green here as a
    regression in this test -- it would mean either the seed stopped
    reproducing the all-NULL group (check row count first) or upstream
    NaN-to-null handling landed (CHAOS-4534, a value-level change owned
    by chris, never to be made unilaterally by a port).
    """
    assert CLICKHOUSE_URI is not None
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4506-nan-empty-column-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 10)
    now = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    try:
        # Two rows, same repo/day-range group -- rows EXIST (distinct from
        # the cleared zero-source-row case) but line_coverage_pct is NULL
        # in both, so the GROUP BY produces one row whose AVG() input set
        # is entirely NULL.
        _insert_coverage_row(
            sink,
            org_id=org_id,
            repo_id=repo_id,
            day=day,
            line_coverage_pct=None,
            computed_at=now,
        )
        _insert_coverage_row(
            sink,
            org_id=org_id,
            repo_id=repo_id,
            day=day,
            line_coverage_pct=None,
            computed_at=now,
        )

        result = await resolve_analytics(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            _breakdown_batch(date(2026, 8, 1), date(2026, 8, 31)),
        )
    finally:
        _cleanup(sink, org_id)
        sink.close()

    assert len(result.breakdowns) == 1
    items = result.breakdowns[0].items
    # A swallowed/degraded path in this resolver would ALSO produce zero
    # items with no exception -- pin the row count so an empty items list
    # here reads as "the mechanism didn't reproduce", never as "pass".
    assert len(items) == 1, (
        f"expected exactly one REPO group (both rows present, both NULL) -- "
        f"got {items}. Zero items would mean the all-NULL group vanished "
        f"from the result set instead of producing a NaN average; that is a "
        f"DIFFERENT, not-yet-characterised behavior, not this test passing "
        f"for the reason it claims to."
    )
    value = items[0].value

    assert type(value) is float, f"expected a plain float, got {type(value)}"
    assert math.isnan(value), (
        f"expected AVG(line_coverage_pct) over an all-NULL group to be NaN "
        f"(ClickHouse's avg() over Nullable(Float64) with every value NULL), "
        f"got {value!r} -- if this is 0.0, a coercion swallowed the NaN "
        f"before it reached Python; if this is None, ClickHouse (or the "
        f"driver) is returning NULL here instead of NaN, which would mean "
        f"this AT-RISK classification needs re-checking against the live "
        f"engine, not that the divergence is safely absent."
    )

    # The exact primitive BRIEF's NaN CLASS WARNING cites -- stdlib
    # json.dumps, allow_nan=True default, same as
    # strawberry/http/base.py:54-55's encode_json.
    body = json.dumps(
        {"data": {"analytics": {"breakdowns": [{"items": [{"value": value}]}]}}},
        allow_nan=True,
    )
    assert '"value": NaN' in body, (
        f"expected the literal, UNQUOTED NaN token RFC 8259 forbids in the "
        f"serialized body (an invalid-JSON HTTP 200, per BRIEF's NaN CLASS "
        f"WARNING) -- got: {body}"
    )
    assert '"NaN"' not in body, (
        'found a QUOTED "NaN" string instead of the unquoted numeric '
        "token -- that is a different, unrelated shape and would not "
        "reproduce the RFC 8259 violation this test documents"
    )
