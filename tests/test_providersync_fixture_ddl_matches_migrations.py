from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE_PATH = (
    _REPO_ROOT / "internal" / "providersync" / "repository_postgres_integration_test.go"
)


def _migration_source(filename: str) -> str:
    return (
        _REPO_ROOT / "src" / "dev_health_ops" / "alembic" / "versions" / filename
    ).read_text(encoding="utf-8")


def _fixture_source() -> str:
    return _FIXTURE_PATH.read_text(encoding="utf-8")


def _fixture_function(fixture_source: str) -> str:
    """The body of createProviderSyncFixture, not the whole test file.

    Slicing to just this function (rather than grepping the whole file) keeps
    a false match in an unrelated test's ad-hoc SQL from passing this check
    for the wrong reason.
    """
    start = fixture_source.index("func createProviderSyncFixture(")
    # The function ends at the closing brace of the `for _, statement := range`
    # loop's enclosing function -- the next top-level `func ` marks that.
    end = fixture_source.index("\nfunc ", start + 1)
    return fixture_source[start:end]


def test_providersync_fixture_matches_integration_data_model_migration() -> None:
    """internal/providersync's Go fixture vs. alembic 0015 (CHAOS-4050).

    createProviderSyncFixture previously hand-invented its own schema for
    integrations, integration_sources, integration_datasets, sync_runs, and
    sync_run_units instead of deriving it from alembic -- the exact class of
    gap that let CHAOS-4041 (a nonexistent org_id column) ship green against a
    schema production does not have. This compares the two directly so drift
    in either direction is caught the day it happens, the same mechanism
    test_effect_snapshot_migration.py already uses for the 0092/0093 snapshot
    table.
    """
    migration_source = _migration_source("0015_add_integration_data_model.py")
    fixture_source = _fixture_source()
    fixture_ddl = _fixture_function(fixture_source)

    # Real FKs alembic 0015 declares among these five tables, keyed only on
    # columns this fixture actually carries (columns alembic requires but no
    # providersync query reads -- integrations.name/provider/is_active/...,
    # sync_runs.triggered_by/mode/..., etc. -- are intentionally not
    # reproduced; see the createProviderSyncFixture doc comment).
    for table, fk_column, referenced in (
        ("integration_sources", "integration_id", "integrations"),
        ("integration_datasets", "integration_id", "integrations"),
        ("sync_run_units", "source_id", "integration_sources"),
        ("sync_run_units", "sync_run_id", "sync_runs"),
    ):
        assert (
            f'ForeignKeyConstraint(["{fk_column}"], ["{referenced}.id"])'
            in migration_source
        ), (
            f"expected alembic 0015 to FK {table}.{fk_column} -> {referenced}.id "
            "-- if this changed, the fixture's FK needs to change with it"
        )
        assert f"REFERENCES public.{referenced}(id)" in fixture_ddl, (
            f"fixture is missing the real 0015 FK {table}.{fk_column} -> "
            f"{referenced}.id"
        )

    assert "uq_integration_datasets_org_integration_dataset" in migration_source
    assert "uq_integration_datasets_org_integration_dataset" in fixture_ddl
    assert "UNIQUE (org_id, integration_id, dataset_key)" in fixture_ddl


def test_providersync_fixture_matches_dataset_unavailable_marker_migration() -> None:
    """alembic 0106 (CHAOS-4048) columns must stay column-for-column."""
    migration_source = _migration_source(
        "0106_add_integration_dataset_unavailable_marker.py"
    )
    fixture_ddl = _fixture_function(_fixture_source())
    assert "sa.String(length=64)" in migration_source
    assert "unavailable_reason varchar(64)" in fixture_ddl
    for column in ("unavailable_since", "unavailable_last_seen_at"):
        assert column in migration_source
        assert column in fixture_ddl


def test_providersync_fixture_matches_chunked_persistence_migration() -> None:
    """alembic 0102's digest columns are varchar(64), not unbounded text.

    A prior version of this fixture declared aggregate_digest and
    manifest_digest as plain `text`, the same hand-authored-text-masks-a-
    varchar-conflict shape CHAOS-4043 hit on daily_metrics_runs.status: the
    suite would accept a digest wider than production's real column ever
    could, and any future comparison against these columns would be
    parse-time-fragile in a way this fixture could never reveal.
    """
    migration_source = _migration_source(
        "0102_add_sync_unit_chunked_provider_persistence.py"
    )
    fixture_ddl = _fixture_function(_fixture_source())

    assert '"aggregate_digest", sa.String(length=64)' in migration_source
    assert "aggregate_digest varchar(64)" in fixture_ddl
    assert "aggregate_digest text" not in fixture_ddl

    assert '"manifest_digest", sa.String(length=64)' in migration_source
    assert "manifest_digest varchar(64)" in fixture_ddl
    assert "manifest_digest text" not in fixture_ddl

    for constraint in (
        "ck_sync_chunk_checkpoint_next_ordinal",
        "ck_sync_chunk_checkpoint_prepared_chunks",
        "ck_sync_chunk_checkpoint_total_chunks",
        "ck_sync_chunk_checkpoint_final_ordinal",
        "ck_sync_chunk_checkpoint_cursor",
        "ck_sync_chunk_checkpoint_committed_rows",
        "ck_sync_chunk_checkpoint_complete_fence",
        "ck_sync_chunk_ordinal",
        "ck_sync_chunk_total",
        "ck_sync_chunk_cursors",
        "ck_sync_chunk_payload_bytes",
        "ck_sync_chunk_payload_object",
        "ck_sync_chunk_ledger_object",
        "ck_sync_chunk_status",
    ):
        assert constraint in migration_source, constraint
        assert constraint in fixture_ddl, (
            f"{constraint} is in migration 0102 but missing from the Go "
            "integration fixture -- the fixture is more permissive than "
            "production"
        )


def test_providersync_fixture_keeps_both_watermark_unique_constraints() -> None:
    """sync_watermarks carries TWO real unique keys, from two migrations.

    0001 (the table's origin) declares uq_sync_watermark_org_repo_target on
    (org_id, repo_id, target). 0015 adds the newer source_id/dataset_key
    columns and its own uq_sync_watermark_org_source_dataset -- WITHOUT ever
    dropping the legacy constraint. A migrated production database therefore
    enforces both at once.

    An earlier draft of this fixture treated the legacy (org_id, repo_id,
    target) constraint as fabricated and dropped it, which a codex
    adversarial review caught: that made the fixture MORE permissive than
    production on exactly the class of schema-drift bug this ticket is
    about. Two sync_watermarks rows sharing an (org_id, repo_id, target) but
    differing in source_id/dataset_key would pass this fixture and then hit
    a real unique violation in production.
    """
    initial_schema = _migration_source("0001_initial_schema.py")
    integration_data_model = _migration_source("0015_add_integration_data_model.py")
    fixture_ddl = _fixture_function(_fixture_source())

    assert "uq_sync_watermark_org_repo_target" in initial_schema
    assert "uq_sync_watermark_org_repo_target" in fixture_ddl
    assert "UNIQUE (org_id, repo_id, target)" in fixture_ddl

    assert "uq_sync_watermark_org_source_dataset" in integration_data_model
    assert "uq_sync_watermark_org_source_dataset" in fixture_ddl
    assert "UNIQUE (org_id, source_id, dataset_key)" in fixture_ddl

    # 0015 must not be the migration that drops the legacy constraint -- if a
    # future migration does, this fixture needs to drop it too, deliberately,
    # not by accident.
    assert "uq_sync_watermark_org_repo_target" not in integration_data_model


def test_providersync_fixture_matches_dispatch_transport_fence_migration() -> None:
    """alembic 0049's route-fence trigger and CHECK constraints, verbatim.

    Production refuses to persist a claimed sync_dispatch_outbox row whose
    kind has no active (unpaused) Celery route. A prior version of this
    fixture had no sync_dispatch_transport_routes table, no route-fence
    trigger, and neither of the two CHECK constraints below at all, so this
    suite could not observe that invariant existing -- an incoherent claim
    (a claim_token with no claim_transport) was silently accepted forever.
    """
    migration_source = _migration_source("0049_add_sync_dispatch_transport_fence.py")
    fixture_ddl = _fixture_function(_fixture_source())

    assert "sync_dispatch_transport_routes" in fixture_ddl
    for kind in (
        "dispatch_sync_run",
        "finalize_sync_run",
        "post_sync",
        "reference_discovery",
    ):
        assert kind in migration_source
        assert kind in fixture_ddl

    for constraint in (
        "ck_sync_dispatch_transport_routes_kind",
        "ck_sync_dispatch_transport_routes_transport",
        "ck_sync_dispatch_transport_routes_rollback",
        "ck_sync_dispatch_transport_routes_generation",
        "ck_sync_dispatch_transport_routes_pause_timestamp",
        "ck_sync_dispatch_outbox_claim_route_coherence",
        "ck_sync_dispatch_outbox_dispatched_route_coherence",
    ):
        assert constraint in migration_source, constraint
        assert constraint in fixture_ddl, (
            f"{constraint} is in migration 0049 but missing from the Go "
            "integration fixture"
        )

    assert "enforce_sync_dispatch_outbox_route_fence" in migration_source
    assert "enforce_sync_dispatch_outbox_route_fence" in fixture_ddl
    assert "sync dispatch kind has no active celery route" in migration_source
    assert "sync dispatch kind has no active celery route" in fixture_ddl

    # sync_dispatch_outbox itself (and its run/kind unique key) is 0020, not
    # 0049 -- 0049 only adds the transport columns and the fence on top of it.
    outbox_origin = _migration_source("0020_add_sync_dispatch_outbox.py")
    assert "uq_sync_dispatch_outbox_run_kind" in outbox_origin
    assert "uq_sync_dispatch_outbox_run_kind" in fixture_ddl
    assert 'ForeignKeyConstraint(["sync_run_id"], ["sync_runs.id"])' in outbox_origin
    assert "REFERENCES public.sync_runs(id)" in fixture_ddl
