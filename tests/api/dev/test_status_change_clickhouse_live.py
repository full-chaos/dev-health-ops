"""Live schema analysis for every Ask Dev status/change native query family."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev.native_status_change import (
    _CI_CHANGES_SQL,
    _CI_SQL,
    _DEPLOYMENT_CHANGES_SQL,
    _DEPLOYMENTS_SQL,
    _INCIDENT_CHANGES_SQL,
    _INCIDENTS_SQL,
    _PROJECT_DECLARED_FACTS_SQL,
    _PULL_REQUEST_CHANGES_SQL,
    _PULL_REQUESTS_SQL,
    _RELATIONSHIPS_SQL,
    _REVIEW_CHANGES_SQL,
    _TRANSITIONS_SQL,
    _WORK_ITEMS_SQL,
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
    }
    return (
        ("work_items", _WORK_ITEMS_SQL, common),
        ("pull_requests", _PULL_REQUESTS_SQL, common),
        ("ci_runs", _CI_SQL, {**common, "pr_numbers": [1]}),
        ("deployments", _DEPLOYMENTS_SQL, {**common, "pr_numbers": [1]}),
        (
            "incidents",
            _INCIDENTS_SQL,
            {**common, "deployment_ids": ["deployment-1"]},
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


def test_direct_work_item_precedes_children_before_limit() -> None:
    assert "ORDER BY (work_item_id = {entity_id:String}) DESC" in _WORK_ITEMS_SQL
