"""
AI Attribution — PostgreSQL semantic layer decision.

AI attribution is **pure analytics data**: it tracks which PRs, commits, issues,
and workflow runs had AI involvement, along with confidence scores and evidence.

Per the dual-database contract (docs/architecture/database-architecture.md):
  - PostgreSQL = semantic layer (users, orgs, teams, credentials, settings)
  - ClickHouse = analytics layer (metrics, time-series, attribution signals)

AI attribution records have no semantic-layer identity.  They are never joined
to Postgres tables, not user-visible in the semantic API, and carry no billing
or access-control implications.  All reads and writes go through:

    dev_health_ops.metrics.sinks.clickhouse.ai_attribution.AIAttributionMixin

Therefore, **no SQLAlchemy mixin is defined for ai_attribution**.

If a future requirement surfaces (e.g., attribution overrides surfaced in the
Postgres-backed settings API), introduce an Alembic migration and a mixin at
that point.  For now, this file is intentionally empty.
"""
