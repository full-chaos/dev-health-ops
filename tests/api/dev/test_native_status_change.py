from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev import native_status_change as _native_status_change_module
from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
)
from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource
from dev_health_ops.api.dev.status_change_service import (
    MAX_STATUS_ASSESSMENT_ITEMS,
    ChangeCategory,
    ChangeSummaryRequest,
    CompletionState,
    StatusChangeService,
    StatusResultState,
    StatusSnapshotRequest,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


# --- CHAOS-3377 / CHAOS-3386 / CHAOS-3376 (three occurrences of one defect
# class -- generalized here per the "after three fixes of one class, ship a
# closure argument" rule): an aggregate aliased to the SAME bare name as its
# own input column, where that name is ALSO referenced, unqualified,
# somewhere ClickHouse resolves identifiers against SELECT-list aliases
# instead of (or in addition to) the raw source column, nesting an aggregate
# inside another expression that rejects one -- ILLEGAL_AGGREGATION
# (Code 184), unconditionally, on every invocation. Two SEPARATE mechanisms
# have now each produced this symptom, and a single check only ever caught
# one of them:
#   1. WHERE-clause shadowing (CHAOS-3377 ``_PROJECT_DECLARED_FACTS_SQL``,
#      CHAOS-3386): ``any(updated_at) AS updated_at`` alongside
#      ``WHERE updated_at <= ...`` in the SAME query -- WHERE evaluates
#      before the SELECT list exists, so ClickHouse still resolves the bare
#      name against the alias and rejects the aggregate it names.
#   2. Same-SELECT-list sibling shadowing (CHAOS-3376 ``_PULL_REQUESTS_SQL``):
#      ``max(submitted_at) AS submitted_at`` and
#      ``argMax(state, (submitted_at, ...)) AS state`` in the SAME SELECT
#      list. ClickHouse resolves every bare identifier across a WHOLE SELECT
#      list against any alias defined ANYWHERE in that list as one
#      order-independent unit -- reproduced directly: the collision fires
#      with NO ``WITH`` CTE involved at all (single ``SELECT`` list, self-
#      contained), and TEXTUAL ORDER does not matter (the colliding
#      reference sits BEFORE the alias definition in the source and still
#      triggers it). A prior diagnosis of this as "WITH is inlined into its
#      consumer, not materialized" was disproved by the same reproduction: a
#      genuinely CROSS-CTE reference to a same-named column, with no
#      same-list collision anywhere, is completely safe. This is exactly
#      what ``_self_named_aggregate_aliases_filtered_in_where`` (the
#      original, CHAOS-3377-era check below) could not see: it scans ONLY
#      inside ``WHERE`` clauses, and this collision lives in a sibling
#      SELECT-list aggregate argument instead -- confirmed by running the
#      ORIGINAL checker against the pre-fix ``_PULL_REQUESTS_SQL`` text,
#      which reported no offenders even though the live EXPLAIN suite
#      (CHAOS-3376) reproduced Code 184 against it.
# ``_self_named_aggregate_alias_referenced_in_nested_aggregate`` below closes
# mechanism 2 the same way the original check closes mechanism 1: a plain,
# unmarked, string-level structural scan over every ``*_SQL`` constant this
# module defines, so either shape fails here immediately, long before it
# could ever reach a live engine. Deliberately NOT a search for "any bare
# reference anywhere in the same query block": that over-broad version
# false-positived on ``_BLOCKERS_SQL``'s legitimate
# ``max(observed_at) AS observed_at`` ... ``ORDER BY observed_at`` --
# referencing a SELECT-list aggregate alias from ORDER BY is standard, valid
# SQL (ORDER BY runs AFTER aggregation), not this defect. Scoped instead to
# "bare reference used as the argument of ANOTHER recognized aggregate call,
# in the SAME query block as its own definition", which is exactly (and
# only) the shape that nests an aggregate inside an aggregate.
#
# codex review round 2 (HIGH, 2026-08-04): the FIRST version of this guard
# recognized only ``func(single_identifier) AS alias`` against a fixed,
# non-combinator function list -- it returned ``[]`` (silently, with no
# positive control to catch the gap) for ClickHouse's "combinator" suffixed
# aggregates (``argMaxIf``, ``maxIf``, ...: same hazard, an extra condition
# argument) and for aggregate families outside that fixed list entirely
# (``countIf``, ``uniqExact``, ...), and it treated "downstream of the
# alias's own definition, textually" as the nested-aggregate mechanism's
# scope, which -- per the corrected root-cause analysis above -- is not
# actually how ClickHouse resolves this at all (order does not matter; QUERY
# BLOCK does). ``_AGGREGATE_BASE_FUNCS``/``_AGGREGATE_COMBINATOR_SUFFIXES``
# widen recognition to combinator-suffixed, multi-argument producers;
# ``_query_block_spans``/``_query_block_id`` replace "downstream of
# definition" and raw paren-DEPTH matching (which could not tell a sibling
# CTE at the same depth from the SAME query block, and separately let a
# WHERE clause's own end-boundary regex silently swallow text belonging to
# an OUTER, unrelated query once a CTE's own WHERE had no
# GROUP BY/HAVING/ORDER BY/LIMIT of its own to stop at) with the actual
# scope ClickHouse resolves aliases within: same CTE body, or the final/
# outer statement. ``test_alias_shadow_detectors_catch_every_synthetic_bad_sql_control``
# below is the positive control codex asked for: parameterized, minimal,
# KNOWN-BAD synthetic SQL this guard MUST flag, so a future weakening of
# either the function-name recognition or the block-scoping shows up as a
# RED test here, not as a silently-empty ``offenders`` dict.
_AGGREGATE_BASE_FUNCS = (
    "any",
    "anyLast",
    "anyHeavy",
    "max",
    "min",
    "sum",
    "avg",
    "argMax",
    "argMin",
    "greatest",
    "least",
    "count",
    "uniq",
    "uniqExact",
    "uniqCombined",
    "uniqCombined64",
    "uniqHLL12",
    "groupArray",
    "groupUniqArray",
    "median",
    "quantile",
    "quantileExact",
    "stddevPop",
    "stddevSamp",
    "varPop",
    "varSamp",
    "topK",
)
#: ClickHouse "combinator" suffixes -- any base aggregate above may carry
#: ONE of these (``maxIf``, ``argMaxIf``, ``uniqExactIf``, ...). A
#: combinator changes the argument SHAPE (usually adding a trailing
#: condition argument) but the function is still a genuine aggregate,
#: still subject to the same alias-shadowing hazard this guard exists to
#: catch.
_AGGREGATE_COMBINATOR_SUFFIXES = (
    "If",
    "Array",
    "Merge",
    "State",
    "Distinct",
    "OrDefault",
    "OrNull",
    "Resample",
    "ForEach",
    "Simple",
)
_AGGREGATE_CALL_START_RE = re.compile(
    r"\b(?:"
    + "|".join(_AGGREGATE_BASE_FUNCS)
    + r")(?:"
    + "|".join(_AGGREGATE_COMBINATOR_SUFFIXES)
    + r")*\s*\(",
    re.IGNORECASE,
)
_CTE_HEADER_RE = re.compile(r"\b(\w+)\s+AS\s*\(", re.IGNORECASE)
_WHERE_KEYWORD_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)
_STOP_KEYWORD_RE = re.compile(
    r"\bGROUP BY\b|\bHAVING\b|\bORDER BY\b|\bLIMIT\b", re.IGNORECASE
)


def _bare_reference_re(alias: str) -> re.Pattern[str]:
    """A bare (unqualified, non-function-call) occurrence of ``alias``:
    excludes ``x.alias`` (qualified column refs) and ``alias(`` (alias
    happening to collide with a function/table name)."""

    return re.compile(rf"(?<![.\w]){re.escape(alias)}\b(?!\s*\()")


def _matching_close_paren(sql: str, open_idx: int) -> int:
    """The index of the ``)`` that closes the ``(`` at ``open_idx``,
    balancing nested parens (tuples, nested function calls, subqueries)."""

    depth = 0
    for index in range(open_idx, len(sql)):
        if sql[index] == "(":
            depth += 1
        elif sql[index] == ")":
            depth -= 1
            if depth == 0:
                return index
    return open_idx


def _call_spans(sql: str, start_re: re.Pattern[str]) -> list[tuple[int, int, int]]:
    """``(name_start, open_paren_index, matching_close_paren_index)`` for
    every call matching ``start_re``, balancing nested parens (e.g.
    ``argMax(state, (submitted_at, last_synced, review_id))``) so a
    multi-argument or tuple-argument call is still attributed correctly."""

    return [
        (call.start(), call.end() - 1, _matching_close_paren(sql, call.end() - 1))
        for call in start_re.finditer(sql)
    ]


def _query_block_spans(sql: str) -> list[tuple[int, int]]:
    """``(start, end)`` character spans for every CTE body (`` AS ( ... )``
    introduced directly by ``WITH`` or a top-level comma) plus one trailing
    span for whatever follows the LAST CTE's closing paren -- the final/
    outer statement, or the whole string if there are no CTEs at all.

    This is the scope ClickHouse ACTUALLY resolves SELECT-list aliases
    within (verified by direct reproduction: two sibling CTEs sit at the
    exact same raw paren-nesting DEPTH yet do not share alias-resolution
    scope, while two expressions in the SAME CTE body do, regardless of
    which comes first textually) -- raw paren depth alone cannot
    distinguish "sibling block, same depth" from "same block".
    """

    spans: list[tuple[int, int]] = []
    last_close = 0
    for header in _CTE_HEADER_RE.finditer(sql):
        prefix = sql[: header.start()].rstrip()
        if not (prefix.endswith("WITH") or prefix.endswith(",")):
            continue  # a "name AS (" that isn't actually a CTE introducer
        open_idx = header.end() - 1
        close_idx = _matching_close_paren(sql, open_idx)
        spans.append((open_idx, close_idx))
        last_close = close_idx + 1
    spans.append((last_close, len(sql)))
    return spans


def _query_block_id(spans: list[tuple[int, int]], pos: int) -> tuple[int, int] | None:
    for start, end in spans:
        if start <= pos <= end:
            return (start, end)
    return None


def _split_top_level_args(args_text: str) -> list[str]:
    """Split a function call's argument text on commas at PAREN DEPTH 0,
    so a tuple argument (``(submitted_at, last_synced, review_id)``)
    counts as ONE argument, not three."""

    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in args_text:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


class _SelfNamedAlias:
    """One ``<agg>(...args...) AS alias`` match where ``alias`` is the SAME
    bare name as one of the call's own TOP-LEVEL arguments (case-
    insensitively) -- the shared precondition both mechanisms below check a
    same-query-block reference against. ``start``/``end`` bound the WHOLE
    match (function name through the trailing ``AS alias``), used to locate
    which query block the definition itself belongs to and to exclude the
    definition's own occurrence from "is this alias referenced elsewhere"
    scans.
    """

    __slots__ = ("start", "end", "alias")

    def __init__(self, start: int, end: int, alias: str) -> None:
        self.start = start
        self.end = end
        self.alias = alias


def _self_named_aggregate_aliases(sql: str) -> list[_SelfNamedAlias]:
    """Every aggregate call (base or combinator-suffixed, single- or
    multi-argument) in ``sql`` aliased to the SAME bare name as one of its
    OWN top-level arguments."""

    results: list[_SelfNamedAlias] = []
    for name_start, open_idx, close_idx in _call_spans(sql, _AGGREGATE_CALL_START_RE):
        as_match = re.match(r"\s*AS\s+(\w+)", sql[close_idx + 1 :], re.IGNORECASE)
        if not as_match:
            continue
        alias = as_match.group(1)
        args_text = sql[open_idx + 1 : close_idx]
        if any(
            arg.strip().rsplit(".", 1)[-1].casefold() == alias.casefold()
            for arg in _split_top_level_args(args_text)
        ):
            results.append(
                _SelfNamedAlias(name_start, close_idx + 1 + as_match.end(), alias)
            )
    return results


def _self_named_aggregate_aliases_filtered_in_where(sql: str) -> list[str]:
    """Aggregate aliases in ``sql`` that reuse their OWN input column's bare
    name, where that same bare name is ALSO referenced (unqualified) inside
    a WHERE clause belonging to the SAME query block -- the exact
    CHAOS-3377 / CHAOS-3386 defect shape: ``any(updated_at) AS updated_at``
    alongside ``WHERE updated_at <= ...`` in the SAME (not a sibling) query.
    """

    spans = _query_block_spans(sql)
    where_clauses: list[tuple[str, tuple[int, int] | None]] = []
    for keyword in _WHERE_KEYWORD_RE.finditer(sql):
        block = _query_block_id(spans, keyword.start())
        block_end = block[1] if block else len(sql)
        end = block_end
        for stop in _STOP_KEYWORD_RE.finditer(sql, keyword.end(), block_end):
            if _query_block_id(spans, stop.start()) == block:
                end = stop.start()
                break
        where_clauses.append((sql[keyword.end() : end], block))

    findings: list[str] = []
    for entry in _self_named_aggregate_aliases(sql):
        alias_block = _query_block_id(spans, entry.start)
        bare_reference = _bare_reference_re(entry.alias)
        if any(
            bare_reference.search(clause)
            for clause, block in where_clauses
            if block == alias_block
        ):
            findings.append(entry.alias)
    return findings


def _self_named_aggregate_alias_referenced_in_nested_aggregate(
    sql: str,
) -> list[str]:
    """Aggregate aliases in ``sql`` that reuse their OWN input column's bare
    name, where that same bare name is ALSO passed as an argument to ANOTHER
    recognized aggregate call in the SAME query block as the alias's own
    definition -- the CHAOS-3376 ``_PULL_REQUESTS_SQL`` defect shape:
    ``max(submitted_at) AS submitted_at`` and
    ``argMax(state, (submitted_at, ...)) AS state`` in the SAME SELECT list
    (order does not matter). ``Code: 184 (ILLEGAL_AGGREGATION)`` on every
    invocation, exactly like the WHERE-clause mechanism above but
    unreachable by that check (this collision never appears inside a WHERE
    clause at all). Cross-block references (a downstream CTE reading a
    same-named PLAIN column from an upstream CTE) are explicitly NOT
    flagged -- verified safe by direct reproduction.
    """

    spans = _query_block_spans(sql)
    call_spans = [
        (open_idx, close_idx)
        for _, open_idx, close_idx in _call_spans(sql, _AGGREGATE_CALL_START_RE)
    ]
    findings: list[str] = []
    for entry in _self_named_aggregate_aliases(sql):
        alias_block = _query_block_id(spans, entry.start)
        bare_reference = _bare_reference_re(entry.alias)
        for hit in bare_reference.finditer(sql):
            hit_pos = hit.start()
            if entry.start <= hit_pos < entry.end:
                continue  # the alias's own defining occurrence, not a reference
            if _query_block_id(spans, hit_pos) != alias_block:
                continue  # a different query block -- verified safe
            if any(
                open_idx < hit_pos < close_idx for open_idx, close_idx in call_spans
            ):
                findings.append(entry.alias)
                break
    return findings


def test_no_sql_constant_aliases_an_aggregate_to_its_own_filtered_column_name() -> None:
    """Structural, unmarked regression closing the WHERE-clause-shadowing
    AND same-query-block-sibling-shadowing shapes of the CHAOS-3377 /
    CHAOS-3386 / CHAOS-3376 alias-collision defect CLASS -- runs in the
    plain unit suite, never opted out by ``-m "not clickhouse"``. Scans
    every ``*_SQL`` string constant this module defines (not just the
    instances the live probe found broken), so a future SQL constant
    introducing EITHER shape fails here immediately, long before it could
    ever reach a live engine.

    This closes the class as far as static, string-level detection can go:
    it does NOT attempt to prove every conceivable identifier-resolution
    context ClickHouse might shadow an alias in -- that would require a
    real SQL parser, and an over-broad regex-only version of the
    nested-aggregate check (any bare reference anywhere in the same query
    block, not just inside another aggregate call) was tried and rejected
    here because it false-positived on ``_BLOCKERS_SQL``'s legitimate
    ``max(observed_at) AS observed_at`` ... ``ORDER BY observed_at`` (valid:
    ORDER BY runs after aggregation). The live EXPLAIN suite
    (``test_status_change_clickhouse_live.py``, CHAOS-3376 fixture repair)
    remains the backstop for whatever shape still slips past both
    mechanisms here -- but ONLY now that its shared params fixture actually
    supplies every placeholder each SQL constant's text binds, AND (codex
    review round 2) now that it covers every ``*_SQL`` constant the module
    defines, not just 14 of 22; before those two fixes it was
    green-by-vacancy on both axes: which params each attempt supplied, and
    which constants it even attempted.
    """

    scanned = [
        name
        for name in dir(_native_status_change_module)
        if name.endswith("_SQL")
        and isinstance(getattr(_native_status_change_module, name), str)
    ]
    # codex review round 2 (HIGH): "a measurement that did not happen must
    # FAIL, loudly" -- a broken/empty module scan would make the assertion
    # below vacuously true (zero constants scanned, zero offenders found),
    # indistinguishable from "every constant is clean".
    assert scanned, "no *_SQL constants found on the module -- registry scan is broken"

    offenders: dict[str, list[str]] = {}
    for name in scanned:
        value = getattr(_native_status_change_module, name)
        findings = [
            f"{alias} (WHERE)"
            for alias in _self_named_aggregate_aliases_filtered_in_where(value)
        ] + [
            f"{alias} (same-block nested aggregate)"
            for alias in _self_named_aggregate_alias_referenced_in_nested_aggregate(
                value
            )
        ]
        if findings:
            offenders[name] = findings
    assert not offenders, (
        "SQL constant(s) alias an aggregate to the SAME name as a raw "
        f"column referenced in a way ClickHouse resolves against the alias "
        f"instead: {offenders}. ClickHouse rejects the resulting nested/"
        "pre-aggregation aggregate reference with ILLEGAL_AGGREGATION "
        "(CHAOS-3377/CHAOS-3386/CHAOS-3376) -- rename the alias."
    )


#: codex review round 2 (HIGH): parameterized, minimal, KNOWN-BAD synthetic
#: SQL snippets the detectors above MUST flag -- a positive control the
#: original guard never had, so a regex that silently stopped matching
#: (weakened combinator support, a typo, a future refactor narrowing
#: ``_AGGREGATE_BASE_FUNCS``/``_AGGREGATE_COMBINATOR_SUFFIXES``, or breaking
#: the block-scoping in ``_query_block_spans``) would still report an empty
#: ``offenders`` dict above and the guard would pass anyway, indistinguishable
#: from "the codebase is actually clean". Verified directly against the
#: PRE-widening detector: it returned ``[]`` for the ``argMaxIf``/``maxIf``
#: (multi-arg, combinator-suffixed) and ``countIf``/``uniqExact`` (base name
#: outside its fixed list) entries below, even though each is the EXACT
#: self-named-alias shape the live-fixed instances already proved dangerous.
#: (label, synthetic SQL, which mechanism must catch it)
_SYNTHETIC_ALIAS_SHADOW_CONTROLS: tuple[tuple[str, str, str], ...] = (
    (
        "where/max/single-arg (original CHAOS-3377/3386 shape)",
        "SELECT max(updated_at) AS updated_at FROM t WHERE updated_at <= now()",
        "where",
    ),
    (
        "nested/max/single-arg, same block (corrected CHAOS-3376 shape)",
        "SELECT argMax(state, (submitted_at, x)) AS state, "
        "max(submitted_at) AS submitted_at FROM t GROUP BY x",
        "nested",
    ),
    (
        "where/argMaxIf combinator, multi-arg",
        "SELECT argMaxIf(state, updated_at, is_active) AS state FROM t "
        "WHERE state = 'x'",
        "where",
    ),
    (
        "nested/maxIf combinator, multi-arg, same block",
        "SELECT argMax(y, price) AS y2, maxIf(price, is_active) AS price "
        "FROM t GROUP BY x",
        "nested",
    ),
    (
        "where/countIf (base name outside the original fixed list)",
        "SELECT countIf(flag) AS flag FROM t WHERE flag > 0",
        "where",
    ),
    (
        "where/uniqExact (base name outside the original fixed list)",
        "SELECT uniqExact(user_id) AS user_id FROM t WHERE user_id > 0",
        "where",
    ),
    (
        "cross-CTE reference is SAFE and must NOT be flagged (regression "
        "guard for the corrected root cause)",
        "WITH a AS (SELECT argMax(state, x) AS state, max(x) AS watermark "
        "FROM t GROUP BY y), b AS (SELECT argMax(state, watermark) AS z "
        "FROM a GROUP BY y) SELECT * FROM b",
        "nested-safe",
    ),
)


@pytest.mark.parametrize(
    "label,bad_sql,mechanism",
    _SYNTHETIC_ALIAS_SHADOW_CONTROLS,
    ids=[label for label, _, _ in _SYNTHETIC_ALIAS_SHADOW_CONTROLS],
)
def test_alias_shadow_detectors_catch_every_synthetic_bad_sql_control(
    label: str, bad_sql: str, mechanism: str
) -> None:
    """The positive-control counterpart to the structural guard above --
    see its module-level docstring for why each entry exists. If you
    weaken ``_AGGREGATE_BASE_FUNCS``, ``_AGGREGATE_COMBINATOR_SUFFIXES``,
    or the multi-argument / block-scoping logic in
    ``_self_named_aggregate_aliases`` or its two callers, watch THIS test
    go RED before you touch anything else -- that is the point of a
    positive control.
    """

    if mechanism == "where":
        findings = _self_named_aggregate_aliases_filtered_in_where(bad_sql)
        assert findings, f"{label}: detector failed to catch a known-bad SQL control"
    elif mechanism == "nested":
        findings = _self_named_aggregate_alias_referenced_in_nested_aggregate(bad_sql)
        assert findings, f"{label}: detector failed to catch a known-bad SQL control"
    else:
        assert mechanism == "nested-safe"
        findings = _self_named_aggregate_alias_referenced_in_nested_aggregate(bad_sql)
        assert not findings, (
            f"{label}: detector over-flagged a genuinely safe cross-block "
            f"reference: {findings}"
        )


def _scope(
    kind: DirectScope = DirectScope.ISSUE,
    *,
    entity_id: str | None = None,
    repositories: list[str] | None = None,
) -> DevScope:
    entity_type = {
        DirectScope.ISSUE: EntityType.ISSUE,
        DirectScope.PROJECT: EntityType.PROJECT,
        DirectScope.PULL_REQUEST: EntityType.PULL_REQUEST,
        DirectScope.WORK_UNIT: EntityType.WORK_UNIT,
    }[kind]
    entity_id = entity_id or ("issue-1" if kind is DirectScope.ISSUE else "repo-a#pr7")
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=kind,
        repositories=repositories or ["repo-a"],
        entity_refs=[
            DevEntityRef(
                entity_type=entity_type,
                entity_id=entity_id,
                display_label=entity_id,
                repository_id="repo-a",
            )
        ],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


def _deployment_row(
    status: str = "success", *, pr_number: int = 7, repository_id: str = "repo-a"
) -> dict[str, Any]:
    return {
        "repository_id": repository_id,
        "entity_id": "deployment-1",
        "display_label": "Production deployment",
        "status": status,
        "environment": "production",
        "pr_number": pr_number,
        "observed_at": NOW,
        "last_synced": NOW,
    }


@pytest.mark.asyncio
async def test_native_work_unit_status_uses_canonical_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_sql: list[str] = []
    observed_params: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_sql.append(sql)
        observed_params.append(_params)
        if "SELECT max(completed_at) AS last_synced" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_unit_membership AS m FINAL" in sql:
            return [
                {
                    "node_type": "issue",
                    "node_id": "linear:DONE-1",
                    "last_synced": NOW,
                },
                {
                    "node_type": "pr",
                    "node_id": "repo-a#pr7",
                    "last_synced": NOW,
                },
            ]
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_acceptance_checks" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.WORK_UNIT, entity_id="work-unit-1")),
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.READY
    assert result.actual.reason_codes == ()
    assert result.children[0].entity_id == "linear:DONE-1"
    assert result.children[0].required is True
    assert result.pull_requests[0].required is True
    assert any("work_unit_membership" in sql for sql in observed_sql)
    assert any(
        params.get("member_issue_ids") == ["linear:DONE-1"]
        and params.get("member_pr_ids") == ["repo-a#pr7"]
        for params in observed_params
    )


@pytest.mark.asyncio
async def test_native_work_unit_without_complete_membership_run_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert "SELECT max(completed_at) AS last_synced" in sql
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.WORK_UNIT, entity_id="work-unit-1")),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert "required_source_not_fresh" in result.actual.reason_codes
    assert result.source_refs[0].source_system == "work_units"
    assert result.source_refs[0].freshness.value == "unknown"


@pytest.mark.asyncio
async def test_native_empty_complete_work_unit_membership_is_not_source_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "SELECT max(completed_at) AS last_synced" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_unit_membership AS m FINAL" in sql:
            return []
        pytest.fail(f"empty complete membership must not query downstream: {sql}")

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.WORK_UNIT, entity_id="work-unit-1")),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("required_release_evidence_missing",)
    assert result.source_refs[0].source_system == "work_units"
    assert result.source_refs[0].freshness.value == "fresh"


@pytest.mark.asyncio
async def test_native_linked_open_unreviewed_pr_blocks_issue_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "issue-1",
                    "title": "Issue 1",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "open",
                    "review_state": None,
                    "changes_requested": 0,
                    "merged": 0,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_acceptance_checks" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert result.pull_requests[0].required is True
    assert result.actual.reason_codes == (
        "required_pull_request_unmerged",
        "required_review_unresolved",
    )


@pytest.mark.asyncio
async def test_native_linked_merged_unreviewed_pr_still_blocks_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "issue-1",
                    "title": "Issue 1",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": None,
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_scope(), as_of=NOW)
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert result.actual.reason_codes == ("required_review_unresolved",)


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ("jira", "github", "gitlab", "linear"))
async def test_native_issue_reader_applies_same_membership_rule_for_every_provider(
    monkeypatch: pytest.MonkeyPatch,
    provider: str,
) -> None:
    observed_params: list[dict[str, Any]] = []
    parent_id = f"{provider}:parent"
    child_id = f"{provider}:child"

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_params.append(params)
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": parent_id,
                    "title": "Parent",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "work_item_id": child_id,
                    "title": "Child",
                    "status": "in_progress",
                    "parent_id": parent_id,
                    "updated_at": NOW,
                    "last_synced": NOW,
                },
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id=parent_id), as_of=NOW),
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.NOT_READY
    assert "required_child_incomplete" in result.actual.reason_codes
    assert "child_requirement_unknown" not in result.actual.reason_codes
    assert result.children[0].required is True
    assert any(
        ref.source_system == "work_graph" and ref.freshness.value == "fresh"
        for ref in result.source_refs
    )
    assert observed_params
    assert all(params["org_id"] == "org-a" for params in observed_params)


@pytest.mark.asyncio
async def test_native_open_incoming_blocker_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return [
                {
                    "entity_id": "jira:BLOCK-1",
                    "display_label": "Open blocker",
                    "status": "in_progress",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "jira:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                _pull_request_row(
                    number=7, state="merged", review_state="APPROVED", merged=1
                )
            ]
        if "FROM deployments" in sql:
            # Same (repository_id, pr_number) pair as the PR row above --
            # the round-3 pair-admission fix requires this.
            return [_deployment_row(repository_id="repo-x", pr_number=7)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="jira:DONE-1"), as_of=NOW),
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert result.actual.reason_codes == ("open_blocker",)
    assert result.blockers[0].entity_id == "jira:BLOCK-1"


@pytest.mark.asyncio
async def test_native_done_issue_is_ready_after_fresh_zero_blocker_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                _pull_request_row(
                    number=7, state="merged", review_state="APPROVED", merged=1
                )
            ]
        if "FROM deployments" in sql:
            # Same (repository_id, pr_number) pair as the PR row above --
            # the round-3 pair-admission fix requires this.
            return [_deployment_row(repository_id="repo-x", pr_number=7)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="linear:DONE-1"), as_of=NOW),
    )

    assert result.state is StatusResultState.COMPLETE
    assert result.actual.state is CompletionState.READY
    assert result.actual.reason_codes == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_source", ("pull_requests", "deployments"))
async def test_native_source_failure_cannot_masquerade_as_empty_optional_data(
    monkeypatch: pytest.MonkeyPatch,
    failing_source: str,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if failing_source == "pull_requests" and "FROM git_pull_requests" in sql:
            raise RuntimeError("pull request source unavailable")
        if failing_source == "deployments" and "FROM deployments" in sql:
            raise RuntimeError("deployment source unavailable")
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="linear:DONE-1"), as_of=NOW),
    )

    assert result.state is StatusResultState.DEGRADED
    assert result.actual.state is CompletionState.INDETERMINATE
    assert "required_source_not_fresh" in result.actual.reason_codes
    assert ("required_release_evidence_missing" in result.actual.reason_codes) is (
        failing_source == "deployments"
    )
    assert any(
        ref.source_system == failing_source and ref.freshness.value == "unavailable"
        for ref in result.source_refs
    )


@pytest.mark.asyncio
async def test_native_missing_release_evidence_is_never_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_graph_edges AS edge" in sql:
            return []
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "linear:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(entity_id="linear:DONE-1"), as_of=NOW),
    )

    assert result.state is StatusResultState.INSUFFICIENT_EVIDENCE
    assert result.actual.state is CompletionState.INDETERMINATE
    assert result.actual.reason_codes == ("required_release_evidence_missing",)


@pytest.mark.asyncio
async def test_multi_repo_blocker_watermark_requires_complete_scope_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker_sql: list[str] = []

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_graph_projection_runs" in sql:
            marker_sql.append(sql)
            return []  # one missing repo means the aggregate HAVING returns no row
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "jira:DONE-1",
                    "title": "Done issue",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(
            _scope(entity_id="jira:DONE-1", repositories=["repo-a", "repo-b"]),
            as_of=NOW,
        ),
    )

    assert result.actual.state is CompletionState.INDETERMINATE
    assert "required_source_not_fresh" in result.actual.reason_codes
    assert marker_sql
    assert "countDistinctIf" in marker_sql[0]
    assert "length({repository_ids:Array(String)})" in marker_sql[0]


@pytest.mark.asyncio
async def test_native_pr_ci_never_invents_required_check_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM git_pull_requests" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PULL_REQUEST)),
    )

    assert result.ci[0].required is None
    assert result.ci[0].skipped_required_work is None
    assert result.actual.state is CompletionState.INDETERMINATE
    assert "ci_requirement_unknown" in result.actual.reason_codes
    assert any(
        "cannot prove required work ran" in warning for warning in result.warnings
    )


@pytest.mark.asyncio
async def test_native_pr_green_pipeline_with_skipped_required_check_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM git_pull_requests" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1",
                    "display_label": "CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_acceptance_checks" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-1",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci1#acceptance",
                    "display_label": "acceptance",
                    "requirement": "required",
                    "conclusion": "skipped",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PULL_REQUEST)),
    )

    assert result.actual.state is CompletionState.NOT_READY
    assert "required_ci_work_skipped" in result.actual.reason_codes
    assert result.ci[0].required is True
    assert result.ci[0].skipped_required_work is True


@pytest.mark.parametrize(
    ("older_result", "newer_result", "expected_state"),
    (
        ("failed", "passed", CompletionState.READY),
        ("passed", "failed", CompletionState.NOT_READY),
    ),
)
@pytest.mark.asyncio
async def test_native_pr_assesses_only_the_latest_ci_run_as_a_unit(
    monkeypatch: pytest.MonkeyPatch,
    older_result: str,
    newer_result: str,
    expected_state: CompletionState,
) -> None:
    older = NOW - timedelta(hours=1)
    observed_ci_params: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM git_pull_requests" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": 7,
                    "entity_id": "repo-a#pr7",
                    "display_label": "PR 7",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM ci_pipeline_runs" in sql:
            observed_ci_params.append(params)
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-new",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-new",
                    "display_label": "New CI",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "run_id": "run-old",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-old",
                    "display_label": "Old CI",
                    "conclusion": "failure",
                    "observed_at": older,
                    "last_synced": older,
                },
            ]
        if "FROM ci_acceptance_checks" in sql:
            observed_ci_params.append(params)
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-new",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-new#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": newer_result,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "run_id": "run-new",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-new#optional",
                    "display_label": "optional",
                    "requirement": "optional",
                    "conclusion": "failed",
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": "repo-a",
                    "run_id": "run-old",
                    "pr_number": 7,
                    "entity_id": "repo-a#ci-old#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": older_result,
                    "observed_at": older,
                    "last_synced": older,
                },
            ]
        if "FROM deployments" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "entity_id": "deployment-1",
                    "status": "success",
                    "environment": "production",
                    "pr_number": 7,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM operational_incidents" in sql:
            return [
                {
                    "entity_id": "incident-1",
                    "display_label": "Resolved incident",
                    "status": "resolved",
                    "active": 0,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(DirectScope.PULL_REQUEST), as_of=NOW),
    )

    assert result.actual.state is expected_state
    assert {fact.entity_id for fact in result.ci} == {
        "repo-a#ci-new#required",
        "repo-a#ci-new#optional",
    }
    assert ("required_ci_not_passing" in result.actual.reason_codes) is (
        newer_result == "failed"
    )
    assert observed_ci_params
    assert all(params["org_id"] == "org-a" for params in observed_ci_params)
    assert all(params["as_of"] == NOW for params in observed_ci_params)


@pytest.mark.asyncio
async def test_native_reader_preserves_the_completion_assessment_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_limits: list[int] = []

    async def fake_query(
        _client: object, _sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_limits.append(int(params["limit"]))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    assert observed_limits
    # CHAOS-3297 s2 round 2 (codex HIGH): _WORK_ITEMS_SQL requests one
    # sentinel row beyond the bound (1,001) so truncation can be detected
    # even though the declared parent consumes one row of the shared
    # budget -- every other query still binds exactly the bound.
    assert set(observed_limits) == {1_000, 1_001}


@pytest.mark.asyncio
async def test_native_parent_inclusive_source_cap_never_fabricates_a_ready_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 2 (codex HIGH) regression: _WORK_ITEMS_SQL fetches
    the declared parent AND its children from one query sharing a single
    LIMIT. Simulate a Done parent with 999 newer completed children plus one
    OLDER incomplete child that falls off the fetch window (1 parent + 1000
    children = 1001 rows total, one more than the 1,000-item bound). Before
    this fix, ``len(children)`` would be exactly 999 (< 1,000), so
    ``assessment_source_limit_reached`` never fired and the omitted
    incomplete child let the service report a fabricated 999/999
    READY/COMPLETE. The fix must detect the truncation via the sentinel row
    and never present that as a trustworthy ratio.
    """

    def _work_item_row(
        work_item_id: str, status: str, *, updated_at: datetime, parent_id: str | None
    ) -> dict[str, Any]:
        return {
            "repository_id": "repo-a",
            "work_item_id": work_item_id,
            "title": work_item_id,
            "status": status,
            "parent_id": parent_id,
            "project_id": None,
            "project_key": None,
            "updated_at": updated_at,
            "last_synced": NOW,
        }

    parent = _work_item_row("issue-1", "done", updated_at=NOW, parent_id=None)
    newest_children = [
        _work_item_row(
            f"child-{index:04d}",
            "done",
            updated_at=NOW - timedelta(minutes=index),
            parent_id="issue-1",
        )
        for index in range(999)
    ]
    # The oldest row -- by ``ORDER BY ... updated_at DESC``, this is the
    # very last row and the first one a LIMIT cuts off.
    oldest_incomplete_child = _work_item_row(
        "child-oldest",
        "in_progress",
        updated_at=NOW - timedelta(days=365),
        parent_id="issue-1",
    )
    all_work_item_rows = [parent, *newest_children, oldest_incomplete_child]

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if (
            "SELECT toString(repo_id) AS repository_id, work_item_id, title, status,"
            in sql
        ):
            return all_work_item_rows[: int(params["limit"])]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    # Regression: the pre-fix code reported 999/999 READY/COMPLETE here.
    assert result.actual.state is not CompletionState.READY
    assert result.state is not StatusResultState.COMPLETE
    assert "assessment_source_limit_reached" in result.actual.reason_codes
    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None


@pytest.mark.asyncio
async def test_native_mixed_issue_pr_membership_truncation_never_fabricates_a_ready_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 3 (codex HIGH) regression: _WORK_UNIT_MEMBERS_SQL
    mixes issue and PR members in ONE query sharing a single LIMIT, then
    splits them post-fetch by node_type. Simulate 500 issue members + 502
    PR members (1,002 total rows against the 1,000-item bound) -- neither
    the resulting ``member_issue_ids`` (500) nor ``member_pr_ids`` (500,
    after the shared LIMIT truncates to 1,000 total) ever reaches 1,000
    alone, so before this fix neither downstream arm's own length check
    could ever detect the drop of the last (oldest, by node_id) PR member.
    The fix must detect the truncation via the membership sentinel and
    never present a fabricated denominator or a false READY/COMPLETE.
    """
    issue_members = [
        {
            "node_type": "issue",
            "node_id": f"linear:ISSUE-{index:04d}",
            "last_synced": NOW,
        }
        for index in range(500)
    ]
    pr_members = [
        {
            "node_type": "pr",
            "node_id": f"repo-a#pr{index:04d}",
            "last_synced": NOW,
        }
        for index in range(502)
    ]
    # _WORK_UNIT_MEMBERS_SQL orders by (node_type, node_id) ASC -- issues
    # ('issue') sort before PRs ('pr'), so a 1,000-row LIMIT keeps every
    # issue and the first 500 PRs, dropping the last 2 PRs entirely.
    all_membership_rows = issue_members + pr_members

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "SELECT max(completed_at) AS last_synced" in sql:
            return [{"last_synced": NOW}]
        if "FROM work_unit_membership AS m FINAL" in sql:
            return all_membership_rows[: int(params["limit"])]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(
            _scope(DirectScope.WORK_UNIT, entity_id="work-unit-1"), max_items=100
        ),
    )

    assert result.actual.state is not CompletionState.READY
    assert result.state is not StatusResultState.COMPLETE
    assert "assessment_source_limit_reached" in result.actual.reason_codes
    assert result.actual.required_child_total is None
    assert result.actual.required_child_complete is None


@pytest.mark.asyncio
async def test_native_high_churn_ci_never_hides_another_prs_failing_latest_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 5 (codex HIGH) exact repro: _CI_SQL orders by
    ``observed_at DESC`` (a global, per-EVENT bound) and only collapses to
    latest-run-per-PR AFTER the fetch. PR A has 1,000 newer CI runs (all
    passing); PR B has a single, much OLDER, FAILING latest run. The global
    bound admits every PR A run and none of PR B's -- PR B's run is never
    fetched at all, so the latest-run collapse has nothing to recover it
    from, and neither PR A's nor PR B's post-collapse row count ever
    reaches the 1,000-item bound (there's exactly one collapsed row per
    PR). Before this fix the service reported a clean READY/COMPLETE
    covering only PR A; the fix must detect the truncation via the
    sentinel and never present that as trustworthy.
    """
    ci_runs_for_pr_a = [
        {
            "repository_id": "repo-a",
            "run_id": f"run-a-{index:04d}",
            "pr_number": 1,
            "entity_id": f"repo-a#ci#run-a-{index:04d}",
            "display_label": "CI",
            "conclusion": "success",
            "observed_at": NOW - timedelta(minutes=index),
            "last_synced": NOW,
        }
        for index in range(1_000)
    ]
    # PR B's only run: far older than every PR A run, and failing.
    ci_run_for_pr_b = {
        "repository_id": "repo-a",
        "run_id": "run-b-old",
        "pr_number": 2,
        "entity_id": "repo-a#ci#run-b-old",
        "display_label": "CI",
        "conclusion": "failure",
        "observed_at": NOW - timedelta(days=365),
        "last_synced": NOW,
    }
    all_ci_rows = ci_runs_for_pr_a + [ci_run_for_pr_b]

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "work_item_id": "issue-1",
                    "title": "Issue 1",
                    "status": "done",
                    "parent_id": "",
                    "updated_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM git_pull_requests AS pr" in sql:
            return [
                {
                    "repository_id": "repo-a",
                    "number": number,
                    "entity_id": f"repo-a#pr{number}",
                    "display_label": f"PR {number}",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
                for number in (1, 2)
            ]
        if "FROM ci_pipeline_runs" in sql:
            return all_ci_rows[: int(params["limit"])]
        if "FROM ci_acceptance_checks" in sql:
            # Only PR A's latest run has a matching required-check
            # classification -- PR B's run is never fetched in the first
            # place, so it can never have one either; that's the point.
            return [
                {
                    "repository_id": "repo-a",
                    "run_id": "run-a-0000",
                    "pr_number": 1,
                    "entity_id": "repo-a#ci#run-a-0000#required",
                    "display_label": "required",
                    "requirement": "required",
                    "conclusion": "success",
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments" in sql:
            # pr_number=1 so the (repository_id, pr_number) pair filter
            # admits it as PR A's release evidence -- isolates the
            # assertions below to the CI truncation mechanism, not an
            # unrelated missing-release-evidence confound.
            return [_deployment_row(pr_number=1)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    result = await service.status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_scope(), max_items=100),
    )

    # Regression: the pre-fix code reported READY/COMPLETE covering only
    # PR A's (passing) latest run, PR B's failing run never having been
    # fetched at all.
    assert result.actual.state is not CompletionState.READY
    assert result.state is not StatusResultState.COMPLETE
    assert "assessment_source_limit_reached" in result.actual.reason_codes


@pytest.mark.asyncio
async def test_bounded_read_sentinel_boundary_is_a_strict_greater_than(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 6 (codex MEDIUM): the round-5 parametrized
    service-level test (test_status_change_service.py) built
    RawStatusSnapshot objects with the truncation flag set BY HAND,
    which tests service propagation only -- it never actually calls
    _bounded_read, so codex mutated its `>` to `>=` and every one of
    those tests stayed green while exactly-1,000 rows false-positived
    again. _bounded_read is the SHARED mechanism behind
    pull_requests/ci/ci_acceptance/deployments/incidents -- pin its
    comparison directly here, independent of any caller's scope/wiring,
    so that class of regression can never hide behind downstream test
    setup again.
    """
    source = ClickHouseStatusChangeSource(object(), now=NOW)
    scope = _scope()

    async def fake_query_source_has_exactly_the_requested_count(
        _client: object, _sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        # The source has exactly 1,000 real rows -- even though we asked
        # for the sentinel (1,001), that's all that comes back.
        return [{"row": i} for i in range(min(int(params["limit"]), 1_000))]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts",
        fake_query_source_has_exactly_the_requested_count,
    )
    rows, _ref, _warning, truncated = await source._bounded_read(
        "pull_requests", "SELECT 1", {"limit": 1}, scope, requested=1_000
    )
    assert len(rows) == 1_000
    assert truncated is False

    async def fake_query_source_has_more_than_requested(
        _client: object, _sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        # The source genuinely has MORE than requested; it honors the
        # sentinel limit and returns exactly that many.
        return [{"row": i} for i in range(int(params["limit"]))]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts",
        fake_query_source_has_more_than_requested,
    )
    rows, _ref, _warning, truncated = await source._bounded_read(
        "pull_requests", "SELECT 1", {"limit": 1}, scope, requested=1_000
    )
    assert len(rows) == 1_000  # trimmed back down to `requested`
    assert truncated is True


def _pr_row(number: int) -> dict[str, Any]:
    return {
        "repository_id": "repo-a",
        "number": number,
        "entity_id": f"repo-a#pr{number}",
        "display_label": f"PR {number}",
        "state": "merged",
        "review_state": "APPROVED",
        "changes_requested": 0,
        "merged": 1,
        "observed_at": NOW,
        "last_synced": NOW,
    }


def _declared_issue_row() -> dict[str, Any]:
    return {
        "repository_id": "repo-a",
        "work_item_id": "issue-1",
        "title": "Issue 1",
        "status": "done",
        "parent_id": "",
        "updated_at": NOW,
        "last_synced": NOW,
    }


@pytest.mark.asyncio
async def test_native_pull_requests_exact_cap_vs_sentinel_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 6 (codex MEDIUM): the unit test above pins the
    shared comparison in isolation; this proves it's correctly WIRED for
    a real _bounded_read caller end to end -- exactly 1,000 real pull
    requests must never truncate (no reason code), and 1,001 must, both
    at the RawStatusSnapshot boundary (row count + flag) and in the
    final StatusChangeService assessment.
    """
    pr_rows_at_cap = [_pr_row(index) for index in range(1_000)]
    pr_rows_over_cap = pr_rows_at_cap + [_pr_row(1_000)]

    def _fake_query_for(pr_rows: list[dict[str, Any]]):
        async def fake_query(
            _client: object, sql: str, params: dict[str, Any]
        ) -> list[dict[str, Any]]:
            if "FROM work_items FINAL" in sql and "parent_id" in sql:
                return [_declared_issue_row()]
            if "FROM git_pull_requests AS pr" in sql:
                return pr_rows[: int(params["limit"])]
            return []

        return fake_query

    source = ClickHouseStatusChangeSource(object(), now=NOW)
    scope = _scope()

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts",
        _fake_query_for(pr_rows_at_cap),
    )
    raw_at_cap = await source.status_snapshot(
        org_id="org-a", scope=scope, as_of=NOW, limit=MAX_STATUS_ASSESSMENT_ITEMS
    )
    assert len(raw_at_cap.pull_requests) == 1_000
    assert raw_at_cap.pull_requests_source_truncated is False

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts",
        _fake_query_for(pr_rows_over_cap),
    )
    raw_over_cap = await source.status_snapshot(
        org_id="org-a", scope=scope, as_of=NOW, limit=MAX_STATUS_ASSESSMENT_ITEMS
    )
    assert len(raw_over_cap.pull_requests) == 1_000
    assert raw_over_cap.pull_requests_source_truncated is True

    # Final assessment: the still-monkeypatched over-cap source drives
    # the real StatusChangeService end to end.
    result = await StatusChangeService(source).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(scope, max_items=100)
    )
    assert "assessment_source_limit_reached" in result.actual.reason_codes


@pytest.mark.asyncio
async def test_native_blockers_manual_sentinel_path_exact_cap_vs_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3297 s2 round 6 (codex MEDIUM): blockers has its OWN
    hand-rolled sentinel check (its try/except around a raw query_dicts
    call, needed to distinguish a genuine source failure from an empty
    result, means it can't share _bounded_read) -- a bug in the shared
    helper's comparison would never touch this path, and a bug here
    would never be caught by the _bounded_read unit test above, so it
    needs an independent pin.
    """

    def _blocker_row(index: int) -> dict[str, Any]:
        return {
            "entity_id": f"blocker-{index:04d}",
            "display_label": f"Blocker {index}",
            "status": "resolved",
            "observed_at": NOW,
            "last_synced": NOW,
        }

    blocker_rows_at_cap = [_blocker_row(index) for index in range(1_000)]
    blocker_rows_over_cap = blocker_rows_at_cap + [_blocker_row(1_000)]

    def _fake_query_for(blocker_rows: list[dict[str, Any]]):
        async def fake_query(
            _client: object, sql: str, params: dict[str, Any]
        ) -> list[dict[str, Any]]:
            if "FROM work_items FINAL" in sql and "parent_id" in sql:
                return [_declared_issue_row()]
            if "FROM work_graph_projection_runs" in sql:
                return [{"last_synced": NOW}]
            if "INNER JOIN work_items AS blocker FINAL" in sql:
                return blocker_rows[: int(params["limit"])]
            return []

        return fake_query

    source = ClickHouseStatusChangeSource(object(), now=NOW)
    scope = _scope()

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts",
        _fake_query_for(blocker_rows_at_cap),
    )
    raw_at_cap = await source.status_snapshot(
        org_id="org-a", scope=scope, as_of=NOW, limit=MAX_STATUS_ASSESSMENT_ITEMS
    )
    assert len(raw_at_cap.blockers) == 1_000
    assert raw_at_cap.blockers_source_truncated is False

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts",
        _fake_query_for(blocker_rows_over_cap),
    )
    raw_over_cap = await source.status_snapshot(
        org_id="org-a", scope=scope, as_of=NOW, limit=MAX_STATUS_ASSESSMENT_ITEMS
    )
    assert len(raw_over_cap.blockers) == 1_000
    assert raw_over_cap.blockers_source_truncated is True

    result = await StatusChangeService(source).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(scope, max_items=100)
    )
    assert "assessment_source_limit_reached" in result.actual.reason_codes


@pytest.mark.asyncio
async def test_native_change_reader_returns_only_canonical_observed_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_item_transitions" in sql:
            return [
                {
                    "entity_id": "issue-1",
                    "display_label": "Issue 1",
                    "from_status": "in_progress",
                    "to_status": "done",
                    "observed_at": NOW - timedelta(hours=1),
                    "last_synced": NOW,
                }
            ]
        if "FROM work_graph_edges" in sql:
            return [
                {
                    "change_id": "edge-1",
                    "source_type": "issue",
                    "source_id": "issue-1",
                    "edge_type": "implements",
                    "target_type": "pr",
                    "target_id": "repo-a#pr7",
                    "provenance": "native",
                    "confidence": 1.0,
                    "observed_at": NOW - timedelta(minutes=30),
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))
    request = ChangeSummaryRequest(
        scope=_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
    )

    result = await service.change_summary("org-a", "permission-v1", request)

    assert [change.change_id for change in result.changes] == [
        result.changes[0].change_id,
        "edge-1",
    ]
    assert result.changes[1].relationship_chain == (
        "issue-1",
        "implements",
        "repo-a#pr7",
    )
    assert all(change.claim_kind.value == "observed" for change in result.changes)


def _org_scope(*, team_ids: list[str] | None = None) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.ORGANIZATION,
        repositories=[],
        entity_refs=[],
        team_ids=team_ids or [],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


@pytest.mark.asyncio
async def test_organization_scope_status_snapshot_enumerates_repos_natively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3255: organization scope must not read as an empty repo set.

    ``DevScope.repositories``/``entity_refs`` are empty for organization
    scope (the wire contract forbids attaching entities to it), so the
    native source must re-derive the authorized repository set itself from
    ``org_id`` rather than reading the (empty) bounded scope fields.
    """
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM repos FINAL" in sql:
            assert params == {"org_id": "org-a"}
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        if "FROM git_pull_requests AS pr" in sql:
            return [_pull_request_row()]
        if "FROM deployments" in sql:
            return [_deployment_row()]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_org_scope()),
    )

    deployment_calls = [item for item in observed if "FROM deployments" in item["sql"]]
    pull_request_calls = [
        item for item in observed if "FROM git_pull_requests AS pr" in item["sql"]
    ]
    assert pull_request_calls, "expected the pull_requests read to execute"
    assert deployment_calls, "expected the deployments read to execute"
    assert pull_request_calls[0]["params"]["repository_ids"] == ["repo-x", "repo-y"]
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." not in result.warnings
    )
    # Assert the SQL text itself carries the organization branch, not just
    # that a mocked row came back: a mock that returns rows unconditionally
    # (ignoring the real WHERE clause) would still pass even if the
    # 'organization' branch were deleted from _PULL_REQUESTS_SQL/
    # _DEPLOYMENTS_SQL (CHAOS-3255 follow-up — this is the regression the
    # prior HIGH finding actually shipped, and a table-name-only mock
    # cannot detect it re-appearing).
    assert "IN ('organization', 'repository')" in pull_request_calls[0]["sql"]
    assert "IN ('organization', 'repository')" in deployment_calls[0]["sql"]
    assert result.pull_requests, "organization scope must surface PR facts"
    assert result.deployments, "organization scope must surface deployment facts"


@pytest.mark.asyncio
async def test_organization_scope_change_summary_enumerates_repos_natively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM repos FINAL" in sql:
            assert params == {"org_id": "org-a"}
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        if "FROM work_item_transitions" in sql:
            return [
                {
                    "entity_id": "issue-1",
                    "display_label": "Issue 1",
                    "from_status": "in_progress",
                    "to_status": "done",
                    "observed_at": NOW - timedelta(hours=1),
                    "last_synced": NOW,
                }
            ]
        if "FROM work_graph_edges" in sql:
            return [
                {
                    "change_id": "edge-1",
                    "source_type": "issue",
                    "source_id": "issue-1",
                    "edge_type": "implements",
                    "target_type": "pr",
                    "target_id": "repo-x#pr7",
                    "provenance": "native",
                    "confidence": 1.0,
                    "observed_at": NOW - timedelta(minutes=30),
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    request = ChangeSummaryRequest(
        scope=_org_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
    )

    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", request)

    transitions_calls = [
        item for item in observed if "FROM work_item_transitions" in item["sql"]
    ]
    relationship_calls = [
        item for item in observed if "FROM work_graph_edges" in item["sql"]
    ]
    assert transitions_calls
    assert relationship_calls
    assert transitions_calls[0]["params"]["repository_ids"] == ["repo-x", "repo-y"]
    assert "Observed-change scope was not widened." not in result.warnings
    # CHAOS-3255 follow-up: _TRANSITIONS_SQL/_RELATIONSHIPS_SQL previously had
    # no 'organization' branch, so change_summary.v1 silently dropped status
    # transitions and work-graph relationships even with a full repo set.
    # Assert the SQL text itself, not just the mocked row: a table-name-only
    # mock returns rows regardless of the real WHERE clause and would not
    # catch the 'organization' branch being deleted again.
    assert "IN ('organization', 'repository')" in transitions_calls[0]["sql"]
    assert "IN ('organization', 'repository')" in relationship_calls[0]["sql"]
    change_ids = {change.change_id for change in result.changes}
    assert any(
        change.entity_id == "issue-1" and change.before == "in_progress"
        for change in result.changes
    ), "organization scope must surface status transition changes"
    assert "edge-1" in change_ids, (
        "organization scope must surface relationship changes"
    )


@pytest.mark.asyncio
async def test_organization_scope_with_no_authorized_repos_is_explicit_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM repos FINAL" in sql:
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_org_scope()),
    )

    # Zero authorized repositories must surface as explicit degraded/unavailable
    # evidence, never as a silently-complete or partial answer.
    assert result.state is StatusResultState.DEGRADED
    assert result.declared is None
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." in result.warnings
    )


@pytest.mark.asyncio
async def test_team_filtered_organization_scope_is_never_widened_to_the_full_org(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A team filter narrows organization scope; no native query here applies
    it, so organization-native enumeration must not kick in and silently
    return every repository in the org instead of respecting the filter."""

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM repos FINAL" in sql:
            # If this executes, the org-native repository derivation
            # incorrectly ran despite the team filter.
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_org_scope(team_ids=["team-a"])),
    )

    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." in result.warnings
    )


# ---------------------------------------------------------------------------
# CHAOS-3303 — a committed team subject re-derives owned repositories from
# team_repo_ownership at query time and executes real team-scoped reads.
#
# N0 (test_chaos_3301_controls.py) already proves the fail-closed floor
# using a bare ``object()`` client, which raises before any query can run --
# that control stays valid unchanged (the new team-repo lookup also raises
# against that fake and is caught the same way). These tests are the
# positive/negative pair the CHAOS-3303 planning brief calls "must flip
# GREEN": a team WITH real ownership rows gets real facts, and a team whose
# ownership query genuinely returns zero rows (as opposed to a client
# failure) still fails closed, never silently empty.
# ---------------------------------------------------------------------------


def _team_scope(*, team_id: str = "team-platform") -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.TEAM,
        repositories=[],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM,
                entity_id=team_id,
                display_label="Platform",
            )
        ],
        team_ids=[team_id],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


@pytest.mark.asyncio
async def test_team_scope_status_snapshot_re_derives_owned_repos_and_executes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM team_repo_ownership" in sql:
            assert params == {
                "org_id": "org-a",
                "team_id": "team-platform",
                "as_of": NOW,
            }
            return [{"repository_id": "repo-x"}, {"repository_id": "repo-y"}]
        # _PULL_REQUESTS_SQL now embeds the canonical-primary-attribution
        # subquery INSIDE its own single query string (no separate
        # query_dicts round trip for it) -- matching on
        # "work_item_team_attributions" here would incorrectly intercept
        # that composite query itself, since the substring appears within
        # it. Match the outer query's own unique marker instead.
        if "FROM git_pull_requests AS pr" in sql:
            return [_pull_request_row()]
        if "FROM deployments" in sql:
            # Same (repository_id, pr_number) pair as the PR row above --
            # the round-3 pair-admission fix requires deployments to
            # genuinely trace through an admitted PR, not merely share a
            # bare PR number.
            return [_deployment_row(repository_id="repo-x", pr_number=3)]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope()),
    )

    pull_request_calls = [
        item for item in observed if "FROM git_pull_requests AS pr" in item["sql"]
    ]
    deployment_calls = [item for item in observed if "FROM deployments" in item["sql"]]
    assert pull_request_calls, "expected the pull_requests read to execute"
    assert deployment_calls, "expected the deployments read to execute"
    assert pull_request_calls[0]["params"]["repository_ids"] == ["repo-x", "repo-y"]
    # Assert the SQL text itself carries the canonical-primary-attribution
    # team branch, not just that a mocked row came back -- a table-name-only
    # mock would still pass even if 'team' were dropped from the
    # disjunction, or if it fell back to the coarser repository-membership
    # arm (CHAOS-3303 round 2's own regression).
    assert "IN ('issue', 'project', 'team')" in pull_request_calls[0]["sql"]
    assert "work_item_team_attributions" in pull_request_calls[0]["sql"]
    assert "is_primary = 1" in pull_request_calls[0]["sql"]
    assert result.pull_requests, "team scope must surface pull-request facts"
    assert result.deployments, "team scope must surface deployment facts"
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." not in result.warnings
    )
    # A team has no single declared/children completion tree; this is
    # structural (see TEAM_NOT_APPLICABLE_SOURCES), never a data gap.
    assert result.declared is None
    assert result.children == ()
    assert result.blockers == ()
    # declared_optional now includes TEAM (status_change_service.py), so real
    # fresh evidence with no declared status is COMPLETE, not
    # INSUFFICIENT_EVIDENCE -- the end-to-end proof this issue's health
    # services depend on.
    assert result.state is StatusResultState.COMPLETE


@pytest.mark.asyncio
async def test_team_scope_never_queries_declared_work_items_or_blockers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Structural, not a data gap: _WORK_ITEMS_SQL/_BLOCKERS_SQL must never
    even be attempted for a team subject, exactly like organization/
    repository scope already never attempts them.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-x"}]
        if "FROM work_items FINAL" in sql:
            raise AssertionError("_WORK_ITEMS_SQL must not run for a team subject")
        if "FROM work_graph_edges" in sql and "blocker" in sql:
            raise AssertionError("_BLOCKERS_SQL must not run for a team subject")
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope()),
    )
    assert result.declared is None


@pytest.mark.asyncio
async def test_team_scope_change_summary_re_derives_owned_repos_and_executes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": dict(params)})
        if "FROM team_repo_ownership" in sql:
            assert params == {
                "org_id": "org-a",
                "team_id": "team-platform",
                "as_of": NOW,
            }
            return [{"repository_id": "repo-x"}]
        if "FROM work_item_transitions" in sql:
            return [
                {
                    "entity_id": "issue-1",
                    "display_label": "Issue 1",
                    "from_status": "in_progress",
                    "to_status": "done",
                    "observed_at": NOW - timedelta(hours=1),
                    "last_synced": NOW,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    request = ChangeSummaryRequest(
        scope=_team_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
    )

    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", request)

    transitions_calls = [
        item for item in observed if "FROM work_item_transitions" in item["sql"]
    ]
    assert transitions_calls
    assert transitions_calls[0]["params"]["repository_ids"] == ["repo-x"]
    assert "work_item_team_attributions" in transitions_calls[0]["sql"]
    assert "is_primary = 1" in transitions_calls[0]["sql"]
    assert any(
        change.entity_id == "issue-1" and change.before == "in_progress"
        for change in result.changes
    ), "team scope must surface status transition changes"
    assert "Observed-change scope was not widened." not in result.warnings


@pytest.mark.asyncio
async def test_team_scope_with_zero_owned_repositories_is_explicit_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuinely empty team_repo_ownership result (not a client failure --
    see N0 for that case) must still fail closed, never silently empty.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return []
        raise AssertionError(
            "no fact read may run once the team has zero owned repositories"
        )

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope()),
    )

    assert result.declared is None
    assert (
        "Status reads require the complete authorized repository set; "
        "scope was not widened." in result.warnings
    )


# ---------------------------------------------------------------------------
# CHAOS-3303 round 2 (Codex HIGH, 2026-08-02): repository co-location is not
# team ownership. A PARENT team with team_repo_ownership access to a shared
# repository must NEVER receive facts whose canonical PRIMARY work-item
# attribution belongs to a different (here, CHILD) team -- every one of the
# nine team arms must exclude them. Parametrized across all nine arms (not
# sampled) per the round-2 directive; the CHILD-team run is the required
# positive control proving the mock (and the underlying plumbing) genuinely
# discriminates by team_id rather than always returning empty.
#
# The mock cannot execute the embedded work_item_team_attributions subquery
# (it interprets SQL by table-name substring, not by running it) -- so, like
# every other structural regression check already in this file (see the
# CHAOS-3255 'organization' branch assertions above), each case ALSO asserts
# the query's own SQL text still contains the canonical-attribution join
# text, so a future edit that silently reverts to bare repository-membership
# admission is caught even though the mock's behavioral check alone could
# not detect it.
# ---------------------------------------------------------------------------

_PARENT_TEAM_ID = "team-parent"
_CHILD_TEAM_ID = "team-child"
_SHARED_REPO_ID = "repo-shared"
_CHILD_WORK_ITEM_ID = "work-item-child-owned"


def _team_scope_for(team_id: str) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.TEAM,
        repositories=[],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.TEAM, entity_id=team_id, display_label=team_id
            )
        ],
        team_ids=[team_id],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=NOW - timedelta(days=14),
            end=NOW - timedelta(days=7),
            timezone="UTC",
        ),
    )


def _generic_delivery_change_row() -> dict[str, Any]:
    """The row shape every _*_CHANGES_SQL query feeds into the SAME generic
    ``ClickHouseStatusChangeSource._delivery_changes`` mapper -- one shape
    covers all five delivery-projection cases regardless of which table the
    real SQL selects from.
    """

    return {
        "change_id": f"change-{_CHILD_WORK_ITEM_ID}",
        "entity_id": f"{_SHARED_REPO_ID}#pr7",
        "display_label": "Child-owned delivery event",
        "before_value": None,
        "after_value": "changed",
        "observed_at": NOW - timedelta(hours=1),
        "last_synced": NOW,
    }


def _make_team_repo_only_fake_query(*, target_marker: str, target_row: dict[str, Any]):
    """A fake_query that admits ``target_row`` for the query matching
    ``target_marker`` ONLY when ``params["team_id"] == _CHILD_TEAM_ID`` --
    the parent (mere repository co-location) always sees an empty result
    for that same arm, proving exclusion rather than a client-side crash or
    an accidentally-always-empty mock.
    """

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": _SHARED_REPO_ID}]
        if target_marker in sql:
            if params.get("team_id") == _CHILD_TEAM_ID:
                return [target_row]
            return []
        return []

    return fake_query


_STATUS_SNAPSHOT_CASE = (
    "pull_requests",
    "reviews.review_state AS review_state",
    lambda: _pull_request_row(),
    lambda result: result.pull_requests,
)

_CHANGE_SUMMARY_CASES = (
    (
        "transitions",
        "FROM work_item_transitions AS transition FINAL",
        lambda: {
            "entity_id": _CHILD_WORK_ITEM_ID,
            "display_label": "Child work item",
            "from_status": "in_progress",
            "to_status": "done",
            "observed_at": NOW - timedelta(hours=1),
            "last_synced": NOW,
        },
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.STATUS
        ],
    ),
    (
        "relationships",
        "edge_id AS change_id",
        lambda: {
            "change_id": "edge-child-1",
            "source_type": "issue",
            "source_id": _CHILD_WORK_ITEM_ID,
            "edge_type": "implements",
            "target_type": "pull_request",
            "target_id": f"{_SHARED_REPO_ID}#pr7",
            "provenance": "native",
            "confidence": 1.0,
            "observed_at": NOW - timedelta(hours=1),
            "last_synced": NOW,
        },
        lambda result: [
            c
            for c in result.changes
            if c.category
            in {
                ChangeCategory.RELATIONSHIP,
                ChangeCategory.BLOCKER,
                ChangeCategory.DEPENDENCY,
            }
        ],
    ),
    (
        "pull_request_changes",
        "'#state#'",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.PULL_REQUEST
        ],
    ),
    (
        "review_changes",
        "FROM git_pull_request_reviews AS review FINAL",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.REVIEW
        ],
    ),
    (
        "ci_changes",
        "FROM ci_pipeline_runs AS run FINAL",
        _generic_delivery_change_row,
        lambda result: [c for c in result.changes if c.category is ChangeCategory.CI],
    ),
    (
        "deployment_changes",
        "FROM deployments AS deployment FINAL",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.DEPLOYMENT
        ],
    ),
    (
        "incident_changes",
        "INNER JOIN deployments AS deployment FINAL",
        _generic_delivery_change_row,
        lambda result: [
            c for c in result.changes if c.category is ChangeCategory.INCIDENT
        ],
    ),
)


@pytest.mark.asyncio
async def test_team_arm_excludes_child_owned_facts_pull_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _label, marker, row_factory, extract = _STATUS_SNAPSHOT_CASE
    row = row_factory()

    parent_query = _make_team_repo_only_fake_query(target_marker=marker, target_row=row)
    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", parent_query
    )
    parent_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope_for(_PARENT_TEAM_ID)),
    )
    assert extract(parent_result) == (), (
        "a team with mere repository co-location must not receive facts "
        "canonically owned by a different team"
    )

    observed_sql: list[str] = []

    async def child_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_sql.append(sql)
        return await parent_query(_client, sql, params)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", child_query
    )
    child_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for(_CHILD_TEAM_ID))
    )
    assert extract(child_result), (
        "positive control: the canonically-owning team must receive the fact "
        "-- proves the mock discriminates rather than always excluding"
    )
    target_sql = next(sql for sql in observed_sql if marker in sql)
    assert "work_item_team_attributions" in target_sql
    assert "is_primary = 1" in target_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "label,marker,row_factory,extract",
    _CHANGE_SUMMARY_CASES,
    ids=[case[0] for case in _CHANGE_SUMMARY_CASES],
)
async def test_team_arm_excludes_child_owned_facts_change_summary(
    monkeypatch: pytest.MonkeyPatch,
    label: str,
    marker: str,
    row_factory: Any,
    extract: Any,
) -> None:
    del label
    row = row_factory()

    def _request(team_id: str) -> ChangeSummaryRequest:
        return ChangeSummaryRequest(
            scope=_team_scope_for(team_id),
            current_start=NOW - timedelta(days=7),
            current_end=NOW,
            comparison_start=NOW - timedelta(days=14),
            comparison_end=NOW - timedelta(days=7),
        )

    parent_query = _make_team_repo_only_fake_query(target_marker=marker, target_row=row)
    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", parent_query
    )
    parent_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", _request(_PARENT_TEAM_ID))
    assert extract(parent_result) == [], (
        "a team with mere repository co-location must not receive facts "
        "canonically owned by a different team"
    )

    observed_sql: list[str] = []

    async def child_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_sql.append(sql)
        return await parent_query(_client, sql, params)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", child_query
    )
    child_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", _request(_CHILD_TEAM_ID))
    assert extract(child_result), (
        "positive control: the canonically-owning team must receive the fact "
        "-- proves the mock discriminates rather than always excluding"
    )
    target_sql = next(sql for sql in observed_sql if marker in sql)
    assert "work_item_team_attributions" in target_sql
    assert "is_primary = 1" in target_sql


@pytest.mark.asyncio
async def test_team_arm_excludes_child_owned_facts_deployments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ninth arm: _DEPLOYMENTS_SQL has no canonical-attribution join of
    its own -- team-scoped deployments are admitted only through an already
    team-owned PR (pr_numbers, derived from the now-correctly-filtered
    _PULL_REQUESTS_SQL rows). Exclusion is therefore proven end to end: the
    parent's canonically-excluded PR never contributes a PR number, so its
    deployment is never admitted either, despite sharing the same repo.
    """

    child_pr_number = 77

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": _SHARED_REPO_ID}]
        if "reviews.review_state AS review_state" in sql:
            if params.get("team_id") == _CHILD_TEAM_ID:
                return [_pull_request_row(number=child_pr_number)]
            return []
        if "FROM deployments FINAL" in sql:
            if child_pr_number in (params.get("pr_numbers") or []):
                # _pull_request_row's repository_id is hardcoded "repo-x"
                # (not parametrized) -- match it here so the round-3
                # pair-admission fix sees a genuinely matching pair.
                return [
                    _deployment_row(repository_id="repo-x", pr_number=child_pr_number)
                ]
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    parent_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a",
        "permission-v1",
        StatusSnapshotRequest(_team_scope_for(_PARENT_TEAM_ID)),
    )
    assert parent_result.deployments == (), (
        "a team with mere repository co-location must not receive deployment "
        "facts reachable only through a PR canonically owned by a different team"
    )

    child_result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for(_CHILD_TEAM_ID))
    )
    assert child_result.deployments, (
        "positive control: the canonically-owning team must receive the "
        "deployment via its own admitted PR"
    )


def _pull_request_row(
    *,
    number: int = 3,
    state: str = "open",
    review_state: str | None = None,
    changes_requested: int = 0,
    merged: int = 0,
) -> dict[str, Any]:
    return {
        "repository_id": "repo-x",
        "number": number,
        "entity_id": f"repo-x#pr{number}",
        "display_label": f"PR {number}",
        "state": state,
        "review_state": review_state,
        "changes_requested": changes_requested,
        "merged": merged,
        "observed_at": NOW,
        "last_synced": NOW,
    }


@pytest.mark.asyncio
async def test_deployment_admission_uses_repo_pr_pairs_not_flattened_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 3 (HIGH): _DEPLOYMENTS_SQL's ``ifNull(pull_request_number,
    0) IN {pr_numbers}`` arm matches a bare, cross-repository-flattened PR
    NUMBER -- with two repos in the team's accessible set, repo A's
    team-owned PR #77 must not admit repo B's UNRELATED, differently-owned
    PR #77 deployment, nor any incident reachable only through it.
    """

    repo_a, repo_b = "repo-a-owned", "repo-b-other"
    collision_number = 77

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": repo_a}, {"repository_id": repo_b}]
        if "SELECT 1 AS found" in sql:
            return []
        if "reviews.review_state AS review_state" in sql:
            # Only repo A's PR is canonically team-owned; repo B's
            # same-numbered PR is a genuinely different, unrelated pull
            # request this team never owned (round 2's canonical-attribution
            # join already excludes it -- exercised by the parent/child
            # tests above).
            return [
                {
                    "repository_id": repo_a,
                    "number": collision_number,
                    "entity_id": f"{repo_a}#pr{collision_number}",
                    "display_label": f"PR {collision_number}",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments FINAL" in sql:
            # Simulates the SQL's own (repository-agnostic) admission: with
            # pr_numbers=[77], ifNull(pull_request_number, 0) IN {77}
            # matches BOTH repos' deployments -- the collision this fix
            # must resolve on the Python side, by (repository_id,
            # pr_number) PAIR rather than bare number.
            return [
                {
                    "repository_id": repo_a,
                    "entity_id": "deploy-a",
                    "display_label": "Deploy A",
                    "status": "success",
                    "environment": "production",
                    "pr_number": collision_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": repo_b,
                    "entity_id": "deploy-b",
                    "display_label": "Deploy B (unrelated PR, same number)",
                    "status": "success",
                    "environment": "production",
                    "pr_number": collision_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
            ]
        if "FROM operational_incidents" in sql:
            deployment_pairs = params.get("deployment_pairs") or []
            rows: list[dict[str, Any]] = []
            if (repo_a, "deploy-a") in deployment_pairs:
                rows.append(
                    {
                        "entity_id": "incident-a",
                        "display_label": "Incident A",
                        "status": "resolved",
                        "active": False,
                        "observed_at": NOW,
                        "last_synced": NOW,
                    }
                )
            if (repo_b, "deploy-b") in deployment_pairs:
                rows.append(
                    {
                        "entity_id": "incident-b",
                        "display_label": (
                            "Incident reachable only via the wrongly-"
                            "admitted deployment"
                        ),
                        "status": "resolved",
                        "active": False,
                        "observed_at": NOW,
                        "last_synced": NOW,
                    }
                )
            return rows
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-1"))
    )

    deployment_ids_seen = {d.entity_id for d in result.deployments}
    assert deployment_ids_seen == {"deploy-a"}, (
        "repo B's unrelated same-numbered-PR deployment must be excluded "
        "by the (repository_id, pr_number) pair check"
    )
    incident_ids_seen = {i.entity_id for i in result.incidents}
    assert incident_ids_seen == {"incident-a"}, (
        "an incident reachable ONLY through the wrongly-admitted deployment "
        "must never propagate to the wrong team"
    )


@pytest.mark.asyncio
async def test_team_attribution_subquery_bounds_reassignment_by_as_of(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 3 (HIGH): the canonical-attribution subquery's
    max(computed_at) must be bounded by as_of -- a work item reassigned
    from team A to team B at t2 must not rewrite a t1 query's result in
    either direction: team A's t1 snapshot must keep the in-window facts,
    and team B's t1 snapshot must not retroactively gain them.
    """

    t1 = NOW - timedelta(days=3)
    t2 = NOW

    def _admit(team_id: str | None, as_of: datetime | None) -> bool:
        if as_of is None or team_id is None:
            return False
        if as_of < t2:
            return team_id == "team-a"
        return team_id == "team-b"

    observed_sql: list[str] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-1"}]
        if "FROM work_item_transitions AS transition FINAL" in sql:
            observed_sql.append(sql)
            if _admit(params.get("team_id"), params.get("as_of")):
                return [
                    {
                        "entity_id": "wi-reassigned",
                        "display_label": "Reassigned item",
                        "from_status": "in_progress",
                        "to_status": "done",
                        "observed_at": NOW - timedelta(hours=1),
                        "last_synced": NOW,
                    }
                ]
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )

    async def _status_changes(team_id: str, *, current_end: datetime) -> list[Any]:
        request = ChangeSummaryRequest(
            scope=_team_scope_for(team_id),
            current_start=current_end - timedelta(days=1),
            current_end=current_end,
            comparison_start=current_end - timedelta(days=2),
            comparison_end=current_end - timedelta(days=1),
        )
        result = await StatusChangeService(
            ClickHouseStatusChangeSource(object(), now=NOW)
        ).change_summary("org-a", "permission-v1", request)
        return [c for c in result.changes if c.category is ChangeCategory.STATUS]

    team_a_at_t1 = await _status_changes("team-a", current_end=t1)
    team_b_at_t1 = await _status_changes("team-b", current_end=t1)
    team_a_at_t2 = await _status_changes("team-a", current_end=t2)
    team_b_at_t2 = await _status_changes("team-b", current_end=t2)

    assert team_a_at_t1, "team A's t1 snapshot must keep the item's in-window facts"
    assert not team_b_at_t1, (
        "team B must not retroactively gain facts it did not own at t1"
    )
    assert not team_a_at_t2, (
        "team A must not keep the item's facts after a real reassignment at t2"
    )
    assert team_b_at_t2, (
        "team B must see the item once genuinely reassigned to it at t2"
    )

    assert observed_sql, "the transitions arm must have actually been queried"
    assert all("computed_at <=" in sql for sql in observed_sql), (
        "the canonical-attribution subquery must bound max(computed_at) by "
        "as_of, not take a global maximum"
    )


@pytest.mark.asyncio
async def test_team_with_only_unlinked_repo_activity_discloses_coverage_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 3 (MEDIUM): a team whose accessible repos contain ONLY
    unlinked delivery facts (standalone PR/deployment activity with no
    canonical work-item chain) must not resolve a clean READY/COMPLETE with
    zero attributed facts and no disclosure -- the exclusion itself (repo
    access != ownership) is correct, but the silent completeness is not.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-unlinked-only"}]
        if "SELECT 1 AS found" in sql:
            return [{"found": 1}]
        return []  # every canonically-scoped read finds nothing

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-solo"))
    )

    assert result.pull_requests == ()
    assert result.deployments == ()
    assert result.actual.state is CompletionState.INDETERMINATE, (
        "a coverage gap must never resolve as READY"
    )
    assert result.state is StatusResultState.DEGRADED, (
        "a coverage gap must never resolve as a clean COMPLETE"
    )
    assert any(
        "attribution coverage" in warning or "could not be canonically" in warning
        for warning in result.warnings
    ), "the coverage gap must be disclosed in warnings, not silently absorbed"


@pytest.mark.asyncio
async def test_team_with_genuinely_no_repo_activity_stays_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control for the coverage-gap disclosure above: a team whose
    accessible repos have NO activity at all (not even unlinked) must not
    be forced into a false coverage-gap disclosure -- the existence check
    itself must discriminate, not fire unconditionally for every empty team.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-genuinely-empty"}]
        return []  # including "SELECT 1 AS found": nothing exists at all

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-quiet"))
    )

    assert not any(
        "attribution coverage" in warning or "could not be canonically" in warning
        for warning in result.warnings
    ), "a genuinely empty team must not be wrongly flagged with a coverage gap"


@pytest.mark.asyncio
async def test_incident_propagation_is_scoped_by_repository_not_bare_deployment_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 4 (HIGH): deployment IDs are only unique PER REPO in the
    schema. Round 3's pair filter correctly excludes repo B's deployment
    from ``deployments``, but _INCIDENTS_SQL matched incident edges on the
    bare ``edge.deployment_id`` -- an incident edge on the EXCLUDED
    (repo-b, deployment_id) pair still leaked into the team snapshot
    because the same deployment_id string collides across repos and
    repo B remains in the team's authorized repository_ids.
    """

    repo_a, repo_b = "repo-a-owned", "repo-b-other"
    shared_deployment_id = "42"  # deliberately identical across both repos
    pr_number = 5

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": repo_a}, {"repository_id": repo_b}]
        if "SELECT 1 AS found" in sql:
            return []
        if "reviews.review_state AS review_state" in sql:
            return [
                {
                    "repository_id": repo_a,
                    "number": pr_number,
                    "entity_id": f"{repo_a}#pr{pr_number}",
                    "display_label": f"PR {pr_number}",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "FROM deployments FINAL" in sql:
            return [
                {
                    "repository_id": repo_a,
                    "entity_id": shared_deployment_id,
                    "display_label": "Deploy repo A",
                    "status": "success",
                    "environment": "production",
                    "pr_number": pr_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
                {
                    "repository_id": repo_b,
                    "entity_id": shared_deployment_id,
                    "display_label": "Deploy repo B (unrelated, same ID)",
                    "status": "success",
                    "environment": "production",
                    "pr_number": pr_number,
                    "observed_at": NOW,
                    "last_synced": NOW,
                },
            ]
        if "FROM operational_incidents" in sql:
            # Discriminate by which admission shape the code actually
            # sends: the fixed code passes repo-scoped (repo_id,
            # deployment_id) PAIRS; the pre-fix code passed a bare
            # deployment_id list that cannot tell repo A's admitted "42"
            # apart from repo B's excluded "42".
            deployment_pairs = params.get("deployment_pairs")
            if deployment_pairs is not None:
                if (repo_b, shared_deployment_id) in deployment_pairs:
                    return [_incident_row("incident-leak")]
                return []
            deployment_ids = params.get("deployment_ids") or []
            if shared_deployment_id in deployment_ids:
                return [_incident_row("incident-leak")]
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-1"))
    )

    assert {d.entity_id for d in result.deployments} == {shared_deployment_id}
    assert result.incidents == (), (
        "an incident edge on the excluded (repo-b, deployment-id) pair "
        "must not leak into the team snapshot merely because the bare "
        "deployment_id collides with an admitted deployment in another repo"
    )


def _incident_row(entity_id: str) -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "display_label": "Leaked incident",
        "status": "resolved",
        "active": False,
        "observed_at": NOW,
        "last_synced": NOW,
    }


@pytest.mark.asyncio
async def test_coverage_probe_failure_fails_closed_not_silently_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 4 (HIGH): a TimeoutError/query error on the coverage
    probe collapsed to an empty result -- the SAME shape as "genuinely
    nothing found" -- silently restoring the exact false-confidence
    READY/COMPLETE state the round-3 probe exists to prevent. A probe
    FAILURE must disclose exactly like a probe that finds activity.
    """

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-flaky"}]
        if "SELECT 1 AS found" in sql:
            raise TimeoutError("coverage probe timed out")
        return []  # PRs, deployments genuinely empty

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-flaky"))
    )

    assert result.actual.state is CompletionState.INDETERMINATE, (
        "a failed coverage probe must never resolve as READY"
    )
    assert result.state is StatusResultState.DEGRADED, (
        "a failed coverage probe must never resolve as a clean COMPLETE"
    )
    assert any(
        "coverage probe" in warning or "cannot rule out" in warning
        for warning in result.warnings
    ), "the probe failure itself must be disclosed, not silently absorbed"


@pytest.mark.asyncio
async def test_unlinked_activity_probe_is_bounded_by_as_of(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round 4 (MEDIUM): _TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL had no
    as_of predicate -- a pull request or deployment created strictly AFTER
    the snapshot's as_of would still trip the probe, falsely degrading a
    historical (as_of=t1) snapshot with activity that had not happened yet
    at t1. The probe's SELECT list has no timestamp column to filter
    client-side (it only returns ``1 AS found``), so this can only be
    enforced inside the SQL text itself -- verified structurally, mirroring
    the as_of-bound structural check used for the round-3 canonical-
    attribution subquery.
    """

    observed_sql: list[str] = []

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_repo_ownership" in sql:
            return [{"repository_id": "repo-solo"}]
        if "SELECT 1 AS found" in sql:
            observed_sql.append(sql)
            return []
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-solo"))
    )

    assert observed_sql, "the unlinked-activity probe must have actually been queried"
    for sql in observed_sql:
        pr_clause, _, deployment_clause = sql.partition("UNION ALL")
        assert "created_at <= {as_of" in pr_clause, (
            "the pull-request arm of the probe must bound by as_of, "
            "mirroring the root _PULL_REQUESTS_SQL bound"
        )
        assert "<= {as_of" in deployment_clause, (
            "the deployment arm of the probe must bound by as_of, "
            "mirroring the root _DEPLOYMENTS_SQL bound"
        )


@pytest.mark.asyncio
async def test_partial_unlinked_activity_alongside_linked_facts_stays_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ratified policy decision (round 4, team-lead, 2026-08-02): the
    coverage-gap probe fires ONLY when a team's attributed facts
    (pull_requests AND deployment_rows) are COMPLETELY empty -- see
    _TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL's docstring. A team with at least
    one genuinely attributed pull request in ANY repo stays a clean,
    undisclosed result even if a DIFFERENT repo in its accessible set has
    purely unlinked activity of its own. Partial-gap detection (an
    unlinked-specific count query distinguishing "some repos have
    unattributed activity" from "all of them do") is explicitly deferred,
    not a defect -- this test pins the shipped behavior so any future
    change to it is deliberate.
    """

    repo_attributed, repo_unlinked_only = "repo-attributed", "repo-unlinked-only"
    probe_calls = 0

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        nonlocal probe_calls
        if "FROM team_repo_ownership" in sql:
            return [
                {"repository_id": repo_attributed},
                {"repository_id": repo_unlinked_only},
            ]
        if "reviews.review_state AS review_state" in sql:
            return [
                {
                    "repository_id": repo_attributed,
                    "number": 1,
                    "entity_id": f"{repo_attributed}#pr1",
                    "display_label": "PR 1",
                    "state": "merged",
                    "review_state": "APPROVED",
                    "changes_requested": 0,
                    "merged": 1,
                    "observed_at": NOW,
                    "last_synced": NOW,
                }
            ]
        if "SELECT 1 AS found" in sql:
            probe_calls += 1
        return []  # deployments empty; probe must never be invoked (guard is False)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).status_snapshot(
        "org-a", "permission-v1", StatusSnapshotRequest(_team_scope_for("team-mixed"))
    )

    # Codex round 5 (MEDIUM): a fake probe that unconditionally returns []
    # cannot distinguish "the probe was never invoked" (the policy this
    # test claims to pin) from "the probe was invoked and happened to find
    # nothing" -- only an invocation count can. This is the actual pin;
    # the warnings assertion below is necessary but not sufficient on its
    # own.
    assert probe_calls == 0, (
        "the coverage-gap probe must not be invoked at all when the team "
        "already has attributed facts -- the all-empty-only guard must "
        "short-circuit before the probe query, not merely produce no "
        "warning from it"
    )
    assert not any(
        "attribution coverage" in warning or "could not be canonically" in warning
        for warning in result.warnings
    ), (
        "partial coverage (linked facts present, unattributed activity "
        "elsewhere in the team's repos) is the documented, deferred "
        "policy -- must NOT be flagged in this round"
    )


# --- CHAOS-3377 residual defect (live acceptance probe, 2026-08-04): the
# declared-state/target-date read must reach RawStatusSnapshot end to end for
# a real PROJECT-scope call -- a fake-client unit test, unlike the live
# ClickHouse EXPLAIN-PLAN test in test_status_change_clickhouse_live.py,
# cannot see a SQL alias collision, but it DOES catch the complementary
# failure mode: the Python side reading the wrong column key out of the row
# (e.g. a rename on one side of the SQL/Python boundary without the other).
# Together the two tests cover both halves of this fix. ---


@pytest.mark.asyncio
async def test_native_project_scope_surfaces_declared_state_and_target_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Deliberately distinct from ``as_of`` (below) -- ``_datetime``'s
    # fallback-to-``as_of`` behavior for a missing/None value would
    # otherwise mask a Python-side column-key mismatch: if the code read
    # the wrong dict key, ``row.get(...)`` returns ``None`` and
    # ``declared_project_observed_at`` would silently become ``as_of``
    # rather than surfacing as wrong.
    declared_updated_at = NOW.replace(hour=9)

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        # CHAOS-3563: the declared-facts query now reads
        # ``project_declared_state_history``, a table no other query in
        # this module touches -- a unique, unambiguous match.
        if "FROM project_declared_state_history" in sql:
            return [
                {
                    "state": "started",
                    "target_date": None,
                    "declared_updated_at": declared_updated_at,
                    "last_synced": NOW,
                    "is_active": 1,
                    "provider_count": 1,
                    "bounded_count": 1,
                    "total_count": 1,
                    "earliest_known_updated_at": declared_updated_at,
                }
            ]
        if "INNER JOIN project ON 1 = 1" in sql:
            # PROJECT_REPOSITORIES_SQL: derives the project's own
            # repository set from its (sentinel, repo-less) work items.
            return [{"repository_id": "00000000-0000-0000-0000-000000000000"}]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    source = ClickHouseStatusChangeSource(object(), now=NOW)
    scope = _scope(
        DirectScope.PROJECT,
        entity_id="project-1",
        repositories=[],
    )
    raw = await source.status_snapshot(
        org_id="org-a", scope=scope, as_of=NOW, limit=100
    )

    assert raw.declared_project_state == "started"
    assert raw.declared_project_target_date is None
    assert raw.declared_project_observed_at == declared_updated_at.replace(tzinfo=UTC)
    assert not any("projects" in warning for warning in raw.warnings)


# --- CHAOS-3563 review condition: "absent" must never conflate "no declared
# state was ever recorded" with "as_of predates the backfill floor -- history
# exists, but not far enough back to answer this instant". The two cases
# share the same `project_rows` truthiness (a row IS always returned by the
# real query -- see _PROJECT_DECLARED_FACTS_SQL's own docstring on why the
# `bounded, unbounded` cross join is never empty) but must diverge in
# caller-visible behavior: declared_project_state stays None in BOTH (never
# fabricate), but only the floor-breach case adds an explicit warning. ---


@pytest.mark.asyncio
async def test_native_project_scope_declared_state_is_explicit_floor_breach_not_silent_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    earliest = NOW.replace(hour=9) - timedelta(days=30)

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM project_declared_state_history" in sql:
            # bounded_count == 0: every retained row postdates as_of.
            # total_count > 0: history DOES exist for this project -- just
            # not far enough back. Must never be reported the same way as
            # "this project has no declared-state history at all".
            return [
                {
                    "state": None,
                    "target_date": None,
                    "declared_updated_at": None,
                    "last_synced": None,
                    "is_active": None,
                    "provider_count": 0,
                    "bounded_count": 0,
                    "total_count": 1,
                    "earliest_known_updated_at": earliest,
                }
            ]
        if "INNER JOIN project ON 1 = 1" in sql:
            return [{"repository_id": "00000000-0000-0000-0000-000000000000"}]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    source = ClickHouseStatusChangeSource(object(), now=NOW)
    scope = _scope(DirectScope.PROJECT, entity_id="project-1", repositories=[])
    raw = await source.status_snapshot(
        org_id="org-a", scope=scope, as_of=NOW, limit=100
    )

    # Never fabricate: no state is served for an instant we cannot verify.
    assert raw.declared_project_state is None
    assert raw.declared_project_target_date is None
    assert raw.declared_project_observed_at is None
    # But the absence must be EXPLICIT, not silent -- distinguishable from
    # the genuinely-no-history case pinned immediately below.
    assert any("predates the retained floor" in warning for warning in raw.warnings), (
        "as_of predating the backfill floor must surface as its own "
        "explicit signal, not collapse into plain absence"
    )


@pytest.mark.asyncio
async def test_native_project_scope_declared_state_genuinely_absent_has_no_floor_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the same distinction: a project with NO retained
    declared-state history at all (total_count == 0) is genuinely absent,
    not a floor breach -- must NOT carry the floor-breach warning text.
    """

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM project_declared_state_history" in sql:
            return [
                {
                    "state": None,
                    "target_date": None,
                    "declared_updated_at": None,
                    "last_synced": None,
                    "is_active": None,
                    "provider_count": 0,
                    "bounded_count": 0,
                    "total_count": 0,
                    "earliest_known_updated_at": None,
                }
            ]
        if "INNER JOIN project ON 1 = 1" in sql:
            return [{"repository_id": "00000000-0000-0000-0000-000000000000"}]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    source = ClickHouseStatusChangeSource(object(), now=NOW)
    scope = _scope(DirectScope.PROJECT, entity_id="project-1", repositories=[])
    raw = await source.status_snapshot(
        org_id="org-a", scope=scope, as_of=NOW, limit=100
    )

    assert raw.declared_project_state is None
    assert not any("predates the retained floor" in warning for warning in raw.warnings)
