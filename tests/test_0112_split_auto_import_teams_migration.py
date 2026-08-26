"""Proof for alembic 0112 (CHAOS-4323): auto_import_teams -> three flags.

Exercises the migration's actual upgrade()/downgrade() against an in-memory
SQLite ``sync_configurations`` table (same harness shape as
tests/test_worker_operator_audit_migration.py) rather than the ORM model, so
this pins the migration's OWN read/decode/write logic independent of the live
schema.
"""

from __future__ import annotations

import importlib
import json

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

_MODULE = (
    "dev_health_ops.alembic.versions.0112_split_auto_import_teams_into_three_categories"
)


def _migration():
    return importlib.import_module(_MODULE)


def _table(connection: sa.engine.Connection) -> sa.Table:
    metadata = sa.MetaData()
    table = sa.Table(
        "sync_configurations",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("provider", sa.String(64), nullable=False, server_default="gitlab"),
        sa.Column("sync_options", sa.JSON, nullable=False),
    )
    metadata.create_all(connection)
    return table


def test_module_chains_after_0111():
    migration = _migration()
    assert migration.revision == "0112"
    assert migration.down_revision == "0111"


def test_upgrade_true_flag_sets_all_three_true():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {
                    "id": "row-1",
                    "sync_options": {"auto_import_teams": True, "owner": "acme"},
                }
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        row = connection.execute(
            sa.select(table.c.sync_options).where(table.c.id == "row-1")
        ).scalar_one()
        options = json.loads(row) if isinstance(row, str) else row
        assert options["auto_import_teams"] is True
        assert options["auto_import_projects"] is True
        assert options["auto_import_members"] is True
        # Untouched keys survive.
        assert options["owner"] == "acme"


def test_upgrade_false_or_absent_flag_sets_all_three_false():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {"id": "row-false", "sync_options": {"auto_import_teams": False}},
                {"id": "row-absent", "sync_options": {"owner": "acme"}},
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        for row_id in ("row-false", "row-absent"):
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == row_id)
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert options["auto_import_teams"] is False
            assert options["auto_import_projects"] is False
            assert options["auto_import_members"] is False


def test_upgrade_is_idempotent():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [{"id": "row-1", "sync_options": {"auto_import_teams": True}}],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()
            first = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
            migration.upgrade()
            second = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
        assert first == second


def test_upgrade_skips_unreadable_row_without_raising():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                # A JSON array (not an object) cannot carry the flags -- must
                # be left alone, not guessed at, mirroring 0108's UNREADABLE
                # handling.
                {"id": "row-bad", "sync_options": ["not", "a", "dict"]},
                {"id": "row-good", "sync_options": {"auto_import_teams": True}},
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        bad = connection.execute(
            sa.select(table.c.sync_options).where(table.c.id == "row-bad")
        ).scalar_one()
        bad_options = json.loads(bad) if isinstance(bad, str) else bad
        assert bad_options == ["not", "a", "dict"]

        good = connection.execute(
            sa.select(table.c.sync_options).where(table.c.id == "row-good")
        ).scalar_one()
        good_options = json.loads(good) if isinstance(good, str) else good
        assert good_options["auto_import_projects"] is True


def test_upgrade_github_provider_clamps_projects_false():
    """CHAOS-4323 final round (codex adversarial-review, HIGH): GitHub has no
    "Projects" import (providers/team_capabilities.py). A migration that
    derived all three flags solely from the legacy auto_import_teams value
    would give every enabled GitHub config auto_import_projects=True -- an
    unsupported flag it never explicitly chose, which the API's
    capability-validation boundary would then reject on the config's very
    next, unrelated PATCH. teams and members must still turn on."""
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {
                    "id": "row-github",
                    "provider": "github",
                    "sync_options": {"auto_import_teams": True},
                },
                {
                    "id": "row-github-caps-and-spaces",
                    "provider": " GitHub ",
                    "sync_options": {"auto_import_teams": True},
                },
                {
                    "id": "row-gitlab",
                    "provider": "gitlab",
                    "sync_options": {"auto_import_teams": True},
                },
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        for row_id in ("row-github", "row-github-caps-and-spaces"):
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == row_id)
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert options["auto_import_teams"] is True, row_id
            assert options["auto_import_projects"] is False, row_id
            assert options["auto_import_members"] is True, row_id

        gitlab_row = connection.execute(
            sa.select(table.c.sync_options).where(table.c.id == "row-gitlab")
        ).scalar_one()
        gitlab_options = (
            json.loads(gitlab_row) if isinstance(gitlab_row, str) else gitlab_row
        )
        assert gitlab_options["auto_import_projects"] is True


def test_upgrade_provider_with_no_auto_import_support_clamps_all_three_false():
    """CHAOS-4323 narrow follow-up round (codex adversarial-review, HIGH):
    the first provider-aware fix only special-cased GitHub/projects, but
    launchdarkly and pagerduty are valid sync_configurations.provider values
    (PROVIDER_SYNC_TARGETS in api/admin/routers/sync.py) with NO
    auto-import capability at all -- absent from
    team_capabilities._AUTO_IMPORT_CAPABILITIES, so
    auto_import_capabilities() treats them exactly like an
    unrecognized/future provider (all three categories unsupported). An
    enabled legacy row for either would otherwise get all three flags
    True, and the API's capability-validation boundary would then reject
    that config's next, unrelated PATCH for EVERY category, not just
    projects. jira/linear (unlike gitlab, already covered above) are
    exercised here too as a second full-support control."""
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {
                    "id": "row-launchdarkly",
                    "provider": "launchdarkly",
                    "sync_options": {"auto_import_teams": True},
                },
                {
                    "id": "row-pagerduty",
                    "provider": "pagerduty",
                    "sync_options": {"auto_import_teams": True},
                },
                {
                    "id": "row-unknown-provider",
                    "provider": "some-future-provider",
                    "sync_options": {"auto_import_teams": True},
                },
                {
                    "id": "row-jira",
                    "provider": "jira",
                    "sync_options": {"auto_import_teams": True},
                },
                {
                    "id": "row-linear",
                    "provider": "linear",
                    "sync_options": {"auto_import_teams": True},
                },
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        for row_id in (
            "row-launchdarkly",
            "row-pagerduty",
            "row-unknown-provider",
        ):
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == row_id)
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert options["auto_import_teams"] is False, row_id
            assert options["auto_import_projects"] is False, row_id
            assert options["auto_import_members"] is False, row_id

        for row_id in ("row-jira", "row-linear"):
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == row_id)
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert options["auto_import_teams"] is True, row_id
            assert options["auto_import_projects"] is True, row_id
            assert options["auto_import_members"] is True, row_id


def test_downgrade_drops_new_keys_and_keeps_auto_import_teams():
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {
                    "id": "row-1",
                    "sync_options": {"auto_import_teams": True, "owner": "acme"},
                }
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert "auto_import_projects" in options

            migration.downgrade()

            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == "row-1")
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
        assert "auto_import_projects" not in options
        assert "auto_import_members" not in options
        assert options["auto_import_teams"] is True
        assert options["owner"] == "acme"


def test_upgrade_quarantines_malformed_legacy_flag_values_to_false():
    """CHAOS-4323 round 2 (codex adversarial-review, MEDIUM): sync_options
    was never schema-validated before this PR, so a legacy row could carry a
    non-bool value for auto_import_teams. Python's bool() truthiness would
    treat the STRING "false" as enabled (bool("false") is True) -- the
    migration must use a strict identity check instead and quarantine
    anything that isn't the real JSON boolean true to false."""
    engine = sa.create_engine("sqlite:///:memory:")
    migration = _migration()
    with engine.connect() as connection:
        table = _table(connection)
        connection.execute(
            table.insert(),
            [
                {
                    "id": "row-string-false",
                    "sync_options": {"auto_import_teams": "false"},
                },
                {
                    "id": "row-string-true",
                    "sync_options": {"auto_import_teams": "true"},
                },
                {"id": "row-int-one", "sync_options": {"auto_import_teams": 1}},
                {"id": "row-null", "sync_options": {"auto_import_teams": None}},
            ],
        )
        connection.commit()

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()

        for row_id in (
            "row-string-false",
            "row-string-true",
            "row-int-one",
            "row-null",
        ):
            row = connection.execute(
                sa.select(table.c.sync_options).where(table.c.id == row_id)
            ).scalar_one()
            options = json.loads(row) if isinstance(row, str) else row
            assert options["auto_import_teams"] is False, row_id
            assert options["auto_import_projects"] is False, row_id
            assert options["auto_import_members"] is False, row_id
