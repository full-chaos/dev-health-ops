"""Helpers for harvesting SQL from resolver code paths (CHAOS-1752).

The :func:`CapturingSink` is the mechanism that lets us walk every resolver
helper, recording the ``(sql, params)`` calls it makes to ``query_dicts``
without executing them. Each resolver registers a small async fixture in
``sql_explain_fixtures.py`` that exercises its SQL-emitting helpers with
representative arguments; the test then replays each captured query through
``EXPLAIN SYNTAX`` against a real ClickHouse with production DDL applied.

This file is intentionally test-side only: the resolver code is not modified
to expose SQL.  Helpers must continue to look like ``await query_dicts(sink,
sql, params)``; this module supplies the sink.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class CapturingSink:
    """Minimal sink stand-in that records ``(sql, params)`` instead of executing.

    ``dev_health_ops.api.queries.client.query_dicts`` selects a backend in this
    order:

      1. ``isinstance(sink.dsn, str) and sink.dsn`` → live ClickHouse via thread.
      2. ``hasattr(sink, "query_dicts")`` → ``sink.query_dicts(sql, params)``.
      3. fallback to ``sink.query(sql, parameters=params)``.

    By exposing :meth:`query_dicts` (and *not* a ``dsn`` attribute) we land in
    branch 2 — the SQL and parameter dict are recorded verbatim and never
    leave the test process.
    """

    backend_type = "clickhouse"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append((query, dict(params or {})))
        return []


@dataclass
class FakeGraphQLContext:
    """Stand-in for ``GraphQLContext`` accepted by helpers that take a context.

    Carries the capturing client and a stable sample org_id. Other context
    attributes are unset by design — helpers that look at them in production
    code paths fall outside the SQL surface this test guards. If a helper
    starts depending on additional attributes, add them here.
    """

    client: Any
    org_id: str = "00000000-0000-0000-0000-000000000001"
    db_url: str = "clickhouse://test/test"
    request_id: str = "test-explain-request"
    persisted_query_id: str | None = None
    loaders: Any = None
    team_loader: Any = None
    team_by_name_loader: Any = None
    repo_loader: Any = None
    repo_by_name_loader: Any = None
    cache: Any = None
    user: Any = None
    db_session: Any = None
    session: Any = None
    extra: dict[str, Any] = field(default_factory=dict)
