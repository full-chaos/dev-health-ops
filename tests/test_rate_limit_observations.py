"""Tests for the provider-rate-limit-observation store's schema (CHAOS-2758).

The store's only writer was ``run_sync_unit``'s ``except RateLimitException``
branch, deleted along with the rest of that dead Celery task body -- there is
no live producer of this table anywhere (Python or Go) any more. The
``ProviderRateLimitObservation`` model and migration 0031 stay checked in
(a future cooldown-gating consumer, CHAOS-2760, still reads this shape), so
this file now only pins that the migration itself is a guarded, retry-safe
(idempotent) upgrade/downgrade.
"""

from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine


def _load_migration_0031():
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0031_add_provider_rate_limit_observations"
    )


def test_migration_0031_idempotent_upgrade():
    migration = _load_migration_0031()
    assert migration.revision == "0031"
    assert migration.down_revision == "0030"

    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            # Operations.context() installs the module-global `alembic.op`
            # proxy (the same `from alembic import op` the migration file
            # imports) for the duration of the block -- the documented way to
            # unit-test an individual migration's upgrade()/downgrade().
            with Operations.context(ctx):
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "provider_rate_limit_observations" in inspector.get_table_names()
                column_names = {
                    col["name"]
                    for col in inspector.get_columns("provider_rate_limit_observations")
                }
                assert "route_family_attribution" in column_names
                index_names = {
                    ix["name"]
                    for ix in inspector.get_indexes("provider_rate_limit_observations")
                }
                assert "ix_provider_rate_limit_observations_cooldown" in index_names
                assert "ix_provider_rate_limit_observations_org_id" in index_names

                # Re-running upgrade() must be a no-op, not an error (guarded
                # create-if-missing, per the 0020/0025 convention).
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert (
                    inspector.get_table_names().count(
                        "provider_rate_limit_observations"
                    )
                    == 1
                )

                migration.downgrade()
                inspector = sa.inspect(conn)
                assert (
                    "provider_rate_limit_observations"
                    not in inspector.get_table_names()
                )

                # downgrade() on an already-absent table is also a no-op.
                migration.downgrade()

                # And upgrade() works again from a clean slate.
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "provider_rate_limit_observations" in inspector.get_table_names()
    finally:
        engine.dispose()
