"""EXPLAIN-based SQL validation for every GraphQL resolver query (CHAOS-1752).

This test is the project's defense against the failure mode that surfaced in
CHAOS-1751: resolver code that compiles but emits SQL referencing columns or
tables that do not exist in the ClickHouse schema. The unit tests for those
resolvers were all green because they mocked ``query_dicts``; the bugs only
fired in production when the SQL hit a real database. EXPLAIN closes that
gap by running every parameterized query through ClickHouse's parser and
analyzer — without executing the data path — against the exact schema the
production migrations create.

Bugs this test catches by construction
--------------------------------------
* CHAOS-1751 #2 — ``max(computed_at) AS computed_at`` shadowing the source
  column in ``_fetch_latest_rows``. ClickHouse rejects with
  ``Code: 184. ILLEGAL_AGGREGATION`` during analysis.
* CHAOS-1751 #3 — ``SELECT toString(repo_id) AS repo_id, full_name FROM repos``
  against the real ``repos`` table (whose columns are ``id`` and ``repo``).
  ClickHouse rejects with ``Code: 47. UNKNOWN_IDENTIFIER`` during identifier
  resolution.

What this test does **not** catch
---------------------------------
CHAOS-1751 #1 was a Python ``AttributeError`` (the resolver called a method
that didn't exist on the sink wrapper). That class of bug is structurally
outside SQL validation; it's caught by type-checked context and unit tests
that pass a real sink wrapper.

Operation
---------
Each registered fixture in :mod:`sql_explain_fixtures` is replayed against a
:class:`CapturingSink` that records ``(sql, params)`` pairs. The recorded
queries are then sent to ClickHouse as ``EXPLAIN SYNTAX`` statements. This
exercises:

* SQL parsing
* Table and column existence
* Function signature resolution
* Aggregation legality
* Parameter binding shape

It deliberately does **not** execute the queries — EXPLAIN is cheap (<5ms
per query in practice) so the whole suite runs in <30s end-to-end against a
fresh ClickHouse container.

When CLICKHOUSE_URI is unset the test skips. CI always sets it (see the
test workflow in ``.github/workflows/test.yml``).
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import pytest
from _sql_explain_helpers import CapturingSink  # type: ignore[import-not-found]  # noqa: E402
from sql_explain_fixtures import (  # type: ignore[import-not-found]  # noqa: E402
    ALL_RESOLVER_SQL_FIXTURES,
    ResolverSQLFixture,
)

logger = logging.getLogger(__name__)

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI") or os.environ.get(
    "EXPLAIN_CLICKHOUSE_URI"
)

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason=(
            "Requires CLICKHOUSE_URI pointing at a ClickHouse container with "
            "production DDL applied (e.g. clickhouse://ch:ch@localhost:8123/default)"
        ),
    ),
]


@pytest.fixture(scope="module")
def ch_client() -> Any:
    """Return a clickhouse-connect client with production schema applied.

    Migrations are tracked in CH's own ``schema_migrations`` table; re-running
    ``ensure_schema`` is idempotent and cheap.
    """
    import clickhouse_connect

    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # narrowed by pytestmark.skipif
    sink = ClickHouseMetricsSink(dsn=CLICKHOUSE_URI)
    try:
        sink.ensure_schema()
    finally:
        sink.close()

    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield client
    finally:
        client.close()


def _capture(fixture: ResolverSQLFixture) -> list[tuple[str, dict[str, Any]]]:
    sink = CapturingSink()
    asyncio.run(fixture(sink))
    return list(sink.calls)


@pytest.mark.parametrize(
    "fixture_name,fixture",
    ALL_RESOLVER_SQL_FIXTURES,
    ids=[name for name, _ in ALL_RESOLVER_SQL_FIXTURES],
)
def test_resolver_sql_parses_against_real_schema(
    fixture_name: str,
    fixture: ResolverSQLFixture,
    ch_client: Any,
) -> None:
    """Every SQL the resolver emits must parse against the production schema.

    The fixture exercises each SQL-emitting helper in the resolver with
    representative arguments. Failures here mean one of:

    * a column or table referenced by the resolver does not exist;
    * the SQL is syntactically invalid;
    * an aggregation/projection conflict exists in the query;
    * a ClickHouse-specific function signature does not match.

    All of these would have fired at runtime as a 500 in production.
    """
    captured = _capture(fixture)
    assert captured, (
        f"Resolver fixture {fixture_name!r} produced no SQL; the fixture is "
        "either empty or no longer exercises any query helper. Add coverage."
    )

    for index, (sql, params) in enumerate(captured):
        try:
            ch_client.command(f"EXPLAIN PLAN {sql}", parameters=params)
        except Exception as exc:  # noqa: BLE001 — re-raised with context below
            pytest.fail(
                f"[{fixture_name}] EXPLAIN PLAN rejected query #{index}:\n"
                f"  Error: {exc}\n"
                f"  Params: {params!r}\n"
                f"  SQL:\n{sql}"
            )
