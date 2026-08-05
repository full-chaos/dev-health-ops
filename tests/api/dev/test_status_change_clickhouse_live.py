"""Live schema analysis for every Ask Dev status/change native query family."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev import native_status_change as native_status_change_module
from dev_health_ops.api.dev.native_status_change import (
    _BLOCKER_PROJECTION_RULE_VERSION,
    _BLOCKER_WATERMARK_SQL,
    _BLOCKERS_SQL,
    _CI_ACCEPTANCE_SQL,
    _CI_CHANGES_SQL,
    _CI_SQL,
    _DEPLOYMENT_CHANGES_SQL,
    _DEPLOYMENTS_SQL,
    _INCIDENT_CHANGES_SQL,
    _INCIDENTS_SQL,
    _ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL,
    _PROJECT_DECLARED_FACTS_SQL,
    _PULL_REQUEST_CHANGES_SQL,
    _PULL_REQUESTS_SQL,
    _RELATIONSHIPS_SQL,
    _REVIEW_CHANGES_SQL,
    _TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL,
    _TEAM_REPOSITORIES_SQL,
    _TRANSITIONS_SQL,
    _WORK_ITEMS_SQL,
    _WORK_UNIT_MEMBERS_SQL,
    _WORK_UNIT_MEMBERSHIP_WATERMARK_SQL,
    PROJECT_REPOSITORIES_SQL,
)

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
ORG_ID = "019fa8b7-2a25-7680-80dd-39fc20d3296d"
REPOSITORY_ID = "11111111-1111-4111-8111-111111111111"
NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(not CLICKHOUSE_URI, reason="Requires migrated CLICKHOUSE_URI"),
]


@pytest.fixture(scope="module")
def ch_client() -> Any:
    import clickhouse_connect

    assert CLICKHOUSE_URI is not None
    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield client
    finally:
        client.close()


def _fixtures() -> tuple[tuple[str, str, dict[str, object]], ...]:
    common: dict[str, object] = {
        "org_id": ORG_ID,
        "repository_ids": [REPOSITORY_ID],
        "scope_type": "issue",
        "entity_id": "issue-target",
        "pr_number": 1,
        "as_of": NOW,
        "start": NOW - timedelta(days=7),
        "end": NOW,
        "limit": 100,
        # CHAOS-3376: every arm below is ONE query TEXT shared across every
        # scope_type -- ClickHouse binds every named placeholder that
        # appears in the text before it ever evaluates which disjunction
        # arm is true, so the work_unit-only `{member_issue_ids:...}` /
        # `{member_pr_ids:...}` and the team-only `{team_id:...}` arms
        # still require a substitution even under `scope_type = 'issue'`
        # fixtures, exactly as the real runtime's `common` dict in
        # `ClickHouseStatusChangeSource._raw_status_snapshot` always
        # supplies all three regardless of the resolved scope (empty
        # lists / "" for the arms that don't apply). Omitting them let
        # this guard go green-by-vacancy: pytest never reached a real
        # EXPLAIN, it failed constructing the fixture-less call before
        # any SQL text was substituted -- so nothing here ever exercised
        # the migrated schema at all.
        "member_issue_ids": [],
        "member_pr_ids": [],
        "team_id": "",
    }
    return (
        ("work_items", _WORK_ITEMS_SQL, common),
        ("pull_requests", _PULL_REQUESTS_SQL, common),
        ("ci_runs", _CI_SQL, {**common, "pr_numbers": [1]}),
        ("deployments", _DEPLOYMENTS_SQL, {**common, "pr_numbers": [1]}),
        (
            "incidents",
            _INCIDENTS_SQL,
            # _INCIDENTS_SQL binds `{deployment_pairs:Array(Tuple(String,
            # String))}` (repository_id, entity_id) pairs -- mirroring the
            # real runtime's `{**common, "deployment_pairs": deployment_pairs}`
            # call in `_raw_status_snapshot`, NOT a bare `deployment_ids`
            # list, which is not a placeholder this query's text contains.
            {**common, "deployment_pairs": [("repo", "deployment-1")]},
        ),
        (
            "work_item_transitions",
            _TRANSITIONS_SQL,
            {**common, "start": NOW - timedelta(days=7), "end": NOW},
        ),
        (
            "work_graph_relationships",
            _RELATIONSHIPS_SQL,
            common,
        ),
        ("pull_request_changes", _PULL_REQUEST_CHANGES_SQL, common),
        ("review_changes", _REVIEW_CHANGES_SQL, common),
        ("ci_changes", _CI_CHANGES_SQL, common),
        ("deployment_changes", _DEPLOYMENT_CHANGES_SQL, common),
        ("incident_changes", _INCIDENT_CHANGES_SQL, common),
        # CHAOS-3376 codex review round 2 (HIGH): this suite covered only 14
        # of the module's 22 `*_SQL` constants -- these three (and the five
        # standalone tests below) were among the 8 that had NO live-EXPLAIN
        # coverage at all before this round. All three carry both the
        # `{org_id:String}` tenant predicate and the `{limit:UInt32}` page
        # bound this parametrized inventory asserts, so they join it
        # directly rather than needing a standalone test.
        ("work_unit_members", _WORK_UNIT_MEMBERS_SQL, common),
        ("ci_acceptance_checks", _CI_ACCEPTANCE_SQL, {**common, "pr_numbers": [1]}),
        ("blockers", _BLOCKERS_SQL, common),
    )


@pytest.mark.parametrize(
    "name,sql,params", _fixtures(), ids=[name for name, _, _ in _fixtures()]
)
def test_status_change_query_parses_against_production_schema(
    name: str, sql: str, params: dict[str, object], ch_client: Any
) -> None:
    assert "{org_id:String}" in sql, f"{name} lacks an explicit tenant predicate"
    assert "{limit:UInt32}" in sql, f"{name} lacks a server-owned result bound"
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(f"{name} EXPLAIN failed: {exc}\nparams={params!r}\n{sql}")
    assert plan.result_rows, f"{name} returned an empty query plan"


def test_project_repository_derivation_parses_against_production_schema(
    ch_client: Any,
) -> None:
    """Real-engine validation for ``PROJECT_REPOSITORIES_SQL``.

    Codex adversarial review (MEDIUM, 2026-08-03): the unit fake for this
    query is a predicate evaluator, not a SQL engine, so nothing else proves
    the text is valid ClickHouse -- in particular the ``repos`` sub-select and
    the repository-less sentinel comparison. Kept out of the parametrized
    inventory above only because that inventory asserts a ``{limit:UInt32}``
    bound: this query resolves an *authorization* set, where a truncated
    result is a silently narrowed read rather than a smaller page, so it
    deliberately has none.
    """

    sql = PROJECT_REPOSITORIES_SQL
    assert "{org_id:String}" in sql, "derivation lacks an explicit tenant predicate"
    # CHAOS-3374 round 2: the identity CTE bounds itself to at most one row via
    # ``HAVING count() = 1`` (a bare ``LIMIT 1`` with no ``ORDER BY`` would
    # pick nondeterministically between two same-``(org_id, id)``,
    # different-provider rows -- the catalog's own ReplacingMergeTree key is
    # ``(org_id, provider, id)``), so no ``LIMIT`` of any kind belongs here.
    assert "LIMIT" not in sql.upper(), "an authorization set must not be truncated"
    params: dict[str, object] = {
        "org_id": ORG_ID,
        "entity_id": "project-target",
        "as_of": NOW,
    }
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(
            f"project_repositories EXPLAIN failed: {exc}\nparams={params!r}\n{sql}"
        )
    assert plan.result_rows, "project_repositories returned an empty query plan"


def test_project_declared_facts_query_parses_against_production_schema(
    ch_client: Any,
) -> None:
    """Real-engine validation for ``_PROJECT_DECLARED_FACTS_SQL``
    (CHAOS-3377 residual defect, live acceptance probe 2026-08-04).

    The live acceptance probe never rendered a "Declared state: ..." clause
    for a project whose catalog row unambiguously carried one. Root cause:
    this query's ``SELECT`` aliased its aggregate to the SAME name as the
    raw column the ``WHERE`` clause filters on (``any(updated_at) AS
    updated_at``) -- ClickHouse resolves the ``WHERE`` reference against
    that alias rather than the raw column, and rejects an aggregate
    function inside ``WHERE`` with ``Code: 184
    (ILLEGAL_AGGREGATION)``, on EVERY invocation, unconditionally (never a
    timestamp-skew or evidence-cap artifact -- the unit fake in
    ``test_chaos_3377_status_answer_render.py`` is a predicate evaluator,
    not a SQL engine, so it could not have caught this; only the real
    engine can). ``native_status_change.py``'s ``_read`` helper catches the
    exception and reports the ``projects`` source as merely "unavailable" --
    indistinguishable, from the caller's perspective, from a genuinely
    absent declared state, which is exactly why this went unnoticed.
    """

    sql = _PROJECT_DECLARED_FACTS_SQL
    assert "{org_id:String}" in sql, "declared-facts read lacks a tenant predicate"
    params: dict[str, object] = {
        "org_id": ORG_ID,
        "entity_id": "project-target",
        "as_of": NOW,
    }
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(
            f"project_declared_facts EXPLAIN failed: {exc}\nparams={params!r}\n{sql}"
        )
    assert plan.result_rows, "project_declared_facts returned an empty query plan"


def test_work_unit_membership_watermark_query_parses_against_production_schema(
    ch_client: Any,
) -> None:
    """Real-engine validation for ``_WORK_UNIT_MEMBERSHIP_WATERMARK_SQL``.

    CHAOS-3376 codex review round 2 (HIGH): one of 8 constants this suite
    had zero live coverage for. A scalar watermark read
    (``max(completed_at) AS last_synced``), not a page -- kept out of the
    parametrized inventory above, which asserts a ``{limit:UInt32}`` bound
    on every entry, exactly like ``PROJECT_REPOSITORIES_SQL`` and
    ``_PROJECT_DECLARED_FACTS_SQL`` above.
    """

    sql = _WORK_UNIT_MEMBERSHIP_WATERMARK_SQL
    assert "{org_id:String}" in sql, "watermark read lacks a tenant predicate"
    params: dict[str, object] = {"org_id": ORG_ID, "as_of": NOW}
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(
            f"work_unit_membership_watermark EXPLAIN failed: {exc}\n"
            f"params={params!r}\n{sql}"
        )
    assert plan.result_rows, (
        "work_unit_membership_watermark returned an empty query plan"
    )


def test_blocker_watermark_query_parses_against_production_schema(
    ch_client: Any,
) -> None:
    """Real-engine validation for ``_BLOCKER_WATERMARK_SQL`` -- another
    scalar watermark read, not a page (see the docstring immediately
    above). CHAOS-3376 codex review round 2 (HIGH).
    """

    sql = _BLOCKER_WATERMARK_SQL
    assert "{org_id:String}" in sql, "watermark read lacks a tenant predicate"
    params: dict[str, object] = {
        "org_id": ORG_ID,
        "blocker_rule_version": _BLOCKER_PROJECTION_RULE_VERSION,
        "repository_ids": [REPOSITORY_ID],
        "as_of": NOW,
    }
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(
            f"blocker_watermark EXPLAIN failed: {exc}\nparams={params!r}\n{sql}"
        )
    assert plan.result_rows, "blocker_watermark returned an empty query plan"


def test_organization_authorized_repositories_query_parses_against_production_schema(
    ch_client: Any,
) -> None:
    """Real-engine validation for
    ``_ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL``. CHAOS-3376 codex review
    round 2 (HIGH). An authorization set, like ``PROJECT_REPOSITORIES_SQL``
    above: a truncated result would be a silently narrowed read, not a
    smaller page, so it deliberately carries no ``LIMIT`` of any kind.
    """

    sql = _ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL
    assert "{org_id:String}" in sql, "derivation lacks an explicit tenant predicate"
    assert "LIMIT" not in sql.upper(), "an authorization set must not be truncated"
    params: dict[str, object] = {"org_id": ORG_ID}
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(
            f"organization_authorized_repositories EXPLAIN failed: {exc}\n"
            f"params={params!r}\n{sql}"
        )
    assert plan.result_rows, (
        "organization_authorized_repositories returned an empty query plan"
    )


def test_team_repo_has_unlinked_activity_query_parses_against_production_schema(
    ch_client: Any,
) -> None:
    """Real-engine validation for ``_TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL``.
    CHAOS-3376 codex review round 2 (HIGH). An EXISTS-shaped probe with two
    literal ``LIMIT 1`` arms (not a server-owned ``{limit:UInt32}`` page
    bound), so it is tested standalone rather than joining the
    parametrized inventory above.
    """

    sql = _TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL
    assert "{org_id:String}" in sql, "activity probe lacks a tenant predicate"
    params: dict[str, object] = {
        "org_id": ORG_ID,
        "repository_ids": [REPOSITORY_ID],
        "as_of": NOW,
    }
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(
            f"team_repo_has_unlinked_activity EXPLAIN failed: {exc}\n"
            f"params={params!r}\n{sql}"
        )
    assert plan.result_rows, (
        "team_repo_has_unlinked_activity returned an empty query plan"
    )


def test_team_repositories_query_parses_against_production_schema(
    ch_client: Any,
) -> None:
    """Real-engine validation for ``_TEAM_REPOSITORIES_SQL``. CHAOS-3376
    codex review round 2 (HIGH). Also covered by a dedicated mutation-tested
    live file (``test_team_scope_clickhouse_live.py``) -- included here too
    so the registry-equality guard below never has to special-case "tested
    elsewhere" as an exemption from THIS file's inventory; every constant
    this module defines gets a real EXPLAIN in this one canonical place.
    """

    sql = _TEAM_REPOSITORIES_SQL
    assert "{org_id:String}" in sql, "derivation lacks an explicit tenant predicate"
    params: dict[str, object] = {
        "org_id": ORG_ID,
        "team_id": "team-target",
        "as_of": NOW,
    }
    try:
        plan = ch_client.query("EXPLAIN PLAN " + sql, parameters=params)
    except Exception as exc:  # noqa: BLE001 - enrich the failing SQL fixture
        pytest.fail(
            f"team_repositories EXPLAIN failed: {exc}\nparams={params!r}\n{sql}"
        )
    assert plan.result_rows, "team_repositories returned an empty query plan"


#: Every ``*_SQL`` constant ``native_status_change`` defines that this file
#: covers OUTSIDE the parametrized inventory above -- each has its own
#: standalone test (a different call shape: no ``{limit:UInt32}`` bound, or
#: extra required params) for the same reason ``PROJECT_REPOSITORIES_SQL``
#: and ``_PROJECT_DECLARED_FACTS_SQL`` already did. Kept as an explicit,
#: hand-maintained set (rather than "whatever isn't in `_fixtures()`") so
#: the registry-equality assertion below can distinguish "a standalone test
#: really exists for this name" from "this name simply doesn't appear in
#: `_fixtures()` yet, for no good reason" -- the exact distinction CHAOS-3376
#: codex review round 2 found missing.
_STANDALONE_TESTED_SQL_NAMES = frozenset(
    {
        "PROJECT_REPOSITORIES_SQL",
        "_PROJECT_DECLARED_FACTS_SQL",
        "_WORK_UNIT_MEMBERSHIP_WATERMARK_SQL",
        "_BLOCKER_WATERMARK_SQL",
        "_ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL",
        "_TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL",
        "_TEAM_REPOSITORIES_SQL",
    }
)


def _all_runtime_sql_constant_names() -> frozenset[str]:
    """Every ``*_SQL`` string constant ``native_status_change`` currently
    defines -- the authoritative registry, derived by introspection (the
    same mechanism the alias-shadowing structural guard in
    ``test_native_status_change.py`` uses for its own module scan) rather
    than a hand-maintained list, so a newly added constant is picked up
    automatically instead of requiring someone to remember to register it
    here too.
    """

    return frozenset(
        name
        for name in dir(native_status_change_module)
        if name.endswith("_SQL")
        and isinstance(getattr(native_status_change_module, name), str)
    )


def _parametrized_inventory_sql_names() -> frozenset[str]:
    """The constant NAME behind each ``_fixtures()`` entry's SQL text,
    resolved by object identity against the module's own attributes --
    ``_fixtures()`` passes the imported constant object directly (not a
    copy), so ``is`` correctly attributes each entry back to the exact
    name it came from without a second, driftable hand-maintained mapping.
    """

    sql_text_to_name = {
        id(value): name
        for name, value in vars(native_status_change_module).items()
        if name.endswith("_SQL") and isinstance(value, str)
    }
    return frozenset(sql_text_to_name[id(sql)] for _, sql, _ in _fixtures())


def test_every_runtime_sql_constant_is_covered_by_this_live_suite() -> None:
    """CHAOS-3376 codex review round 2 (HIGH): this suite covered only 14 of
    the module's 22 ``*_SQL`` constants -- 8 (``_WORK_UNIT_MEMBERSHIP_
    WATERMARK_SQL``, ``_WORK_UNIT_MEMBERS_SQL``, ``_BLOCKER_WATERMARK_SQL``,
    ``_BLOCKERS_SQL``, ``_CI_ACCEPTANCE_SQL``,
    ``_ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL``,
    ``_TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL``, ``_TEAM_REPOSITORIES_SQL``)
    had NO live-EXPLAIN coverage at all, silently -- the exact same
    "green-by-vacancy" shape as the original CHAOS-3376 defect, just at the
    level of "which constants this file even attempts" instead of "which
    params each attempt supplies". This asserts the module's live registry
    (every ``*_SQL`` constant that exists right now) equals exactly the
    union of the parametrized inventory and the explicitly-tracked
    standalone tests -- so a newly added ``*_SQL`` constant that nobody
    wires into either place fails THIS assertion immediately, rather than
    silently joining the 8 that went uncovered until this review round.
    """

    registry = _all_runtime_sql_constant_names()
    assert registry, "the module defines no *_SQL constants -- registry scan is broken"
    covered = _parametrized_inventory_sql_names() | _STANDALONE_TESTED_SQL_NAMES
    assert covered == registry, (
        f"uncovered by this live suite: {sorted(registry - covered)}; "
        f"tracked here but no longer exist on the module: "
        f"{sorted(covered - registry)}"
    )


def test_direct_work_item_precedes_children_before_limit() -> None:
    assert "ORDER BY (work_item_id = {entity_id:String}) DESC" in _WORK_ITEMS_SQL
