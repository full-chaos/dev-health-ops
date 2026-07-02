"""Tests for ``dev-hops maintenance scrub-error-text`` (CHAOS-2780).

Covers: dry-run vs apply per-column counts, DB byte-identity during dry-run,
redact vs truncate_only classification, per-column length caps (including the
outbox's tighter 2000-char cap), CAS-race handling (text + JSON columns),
``--org`` scoping (direct ``org_id`` column and the ``job_runs`` ->
``scheduled_jobs`` join), and JSON sibling-key preservation for
``sync_configurations.last_sync_stats``.

Every fixture secret is assembled via ``_fake_secret(...)`` with a neutral
name, matching the Gitleaks-safety convention established in
``tests/test_error_sanitize.py`` (see that file's module docstring for why).
"""

from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timezone
from typing import Any

import pytest
from sqlalchemy import Text, cast, create_engine, select, update
from sqlalchemy.orm import Session

from dev_health_ops.maintenance.scrub_error_text import (
    KIND_JSON_ERROR_KEY,
    REGISTRY,
    ColumnCounters,
    _process_json_error_key_column,
    _process_text_column,
    collect_counters,
    run_scrub_error_text,
)
from dev_health_ops.models import (
    BackfillJob,
    Base,
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    SyncConfiguration,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
)
from dev_health_ops.sync.dispatch_outbox import (
    _MAX_ERROR_LENGTH as _OUTBOX_MAX_ERROR_LENGTH,
)
from dev_health_ops.sync.error_sanitize import (
    DEFAULT_MAX_ERROR_TEXT_LENGTH,
    REDACTION_MARKER,
)


def _fake_secret(*parts: str) -> str:
    return "".join(parts)


_LEAK_1 = _fake_secret("ghp_", "FAKEqwerty1234567890AB")
_LEAK_2 = _fake_secret("ghp_", "FAKEzyxwvu0987654321CD")
_LEAK_3 = _fake_secret("ghp_", "FAKElmnopq5566778899EF")
_LEAK_4 = _fake_secret("QwertyUserXqzln", "PassphraseValueXqz456")


def _org() -> str:
    return f"org-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Row factories -- minimal required fields only. SQLite (used for these
# tests) does not enforce FK constraints, so FK-typed columns are populated
# with plausible standalone UUIDs except where the test itself needs a real
# parent row (job_runs -> scheduled_jobs org-join tests).
# ---------------------------------------------------------------------------


def _seed_sync_run_unit(
    session: Session, *, org_id: str, error: str | None
) -> SyncRunUnit:
    row = SyncRunUnit(
        org_id=org_id,
        sync_run_id=uuid.uuid4(),
        integration_id=uuid.uuid4(),
        source_id=uuid.uuid4(),
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode="incremental",
        status="failed",
        error=error,
    )
    session.add(row)
    session.flush()
    return row


def _seed_sync_run(session: Session, *, org_id: str, error: str | None) -> SyncRun:
    row = SyncRun(
        org_id=org_id,
        integration_id=uuid.uuid4(),
        triggered_by="manual",
        mode="incremental",
        status="failed",
        total_units=0,
        completed_units=0,
        failed_units=0,
        error=error,
    )
    session.add(row)
    session.flush()
    return row


def _seed_reference_discovery(
    session: Session, *, org_id: str, error: str | None
) -> SyncRunReferenceDiscovery:
    row = SyncRunReferenceDiscovery(
        sync_run_id=uuid.uuid4(),
        org_id=org_id,
        status="failed",
        attempts=1,
        available_at=datetime.now(timezone.utc),
        error=error,
    )
    session.add(row)
    session.flush()
    return row


def _seed_dispatch_outbox(
    session: Session, *, org_id: str, last_error: str | None
) -> SyncDispatchOutbox:
    row = SyncDispatchOutbox(
        org_id=org_id,
        sync_run_id=uuid.uuid4(),
        kind="dispatch_sync_run",
        status="pending",
        available_at=datetime.now(timezone.utc),
        attempts=1,
        last_error=last_error,
    )
    session.add(row)
    session.flush()
    return row


def _seed_scheduled_job(session: Session, *, org_id: str) -> ScheduledJob:
    row = ScheduledJob(
        name=f"job-{uuid.uuid4().hex[:8]}",
        job_type="sync",
        schedule_cron="0 * * * *",
        org_id=org_id,
    )
    session.add(row)
    session.flush()
    return row


def _seed_job_run(
    session: Session,
    *,
    job_id: uuid.UUID,
    error: str | None,
    error_traceback: str | None = None,
) -> JobRun:
    row = JobRun(job_id=job_id)
    row.error = error
    row.error_traceback = error_traceback
    session.add(row)
    session.flush()
    return row


def _seed_backfill_job(
    session: Session, *, org_id: str, error_message: str | None
) -> BackfillJob:
    row = BackfillJob(
        org_id=org_id,
        sync_config_id=uuid.uuid4(),
        since_date=date(2026, 1, 1),
        before_date=date(2026, 1, 2),
        error_message=error_message,
    )
    session.add(row)
    session.flush()
    return row


def _seed_sync_configuration(
    session: Session,
    *,
    org_id: str,
    last_sync_error: str | None = None,
    last_sync_stats: dict | None = None,
) -> SyncConfiguration:
    row = SyncConfiguration(
        name=f"cfg-{uuid.uuid4().hex[:8]}", provider="github", org_id=org_id
    )
    row.last_sync_error = last_sync_error
    row.last_sync_stats = last_sync_stats
    session.add(row)
    session.flush()
    return row


def _seed_integration_credential(
    session: Session, *, org_id: str, last_test_error: str | None
) -> IntegrationCredential:
    row = IntegrationCredential(
        provider="github", name=f"cred-{uuid.uuid4().hex[:8]}", org_id=org_id
    )
    row.last_test_error = last_test_error
    session.add(row)
    session.flush()
    return row


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _new_db(tmp_path) -> str:
    db_path = tmp_path / f"scrub-{uuid.uuid4().hex}.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    engine.dispose()
    return f"sqlite:///{db_path}"


def _ns(
    db_uri: str,
    *,
    apply: bool,
    org: str | None = None,
    batch_size: int = 1000,
    org_explicit: bool | None = None,
):
    # Mirrors cli._resolve_org's contract: a Namespace built directly
    # (bypassing cli.main's sentinel resolution) has no way to know whether
    # --org was actually typed vs. env-defaulted, so callers that pass
    # `org=...` are assumed to mean "explicitly scoped" -- matching every
    # pre-existing call site in this file -- unless overridden.
    if org_explicit is None:
        org_explicit = org is not None
    return argparse.Namespace(
        db=db_uri,
        apply=apply,
        org=org,
        batch_size=batch_size,
        org_explicit=org_explicit,
    )


def _fetch(session: Session, model: Any, row_id: uuid.UUID) -> Any:
    """``session.get`` typed loosely on purpose: every assertion below reads
    a nullable mapped attribute (and, for the JSON entry, indexes into it),
    which mypy can't narrow through a ``Model | None`` return without a
    per-call assertion -- this makes that assertion once, here."""
    row = session.get(model, row_id)
    assert row is not None
    return row


def _table_spec(model: type):
    return next(t for t in REGISTRY if t.model is model)


# ---------------------------------------------------------------------------
# Registry sanity
# ---------------------------------------------------------------------------


def test_registry_covers_all_ten_columns_with_correct_labels_and_caps():
    by_key = {(t.label, c.name): c for t in REGISTRY for c in t.columns}
    assert len(by_key) == 10
    assert ("sync_run_units", "error") in by_key
    assert ("sync_runs", "error") in by_key
    # Table #3's label is derived from the model's __tablename__, never a
    # hand-typed string -- this assertion would fail immediately if anyone
    # reintroduced the ticket's singular/plural typo.
    assert ("sync_run_reference_discoveries", "error") in by_key
    assert ("sync_dispatch_outbox", "last_error") in by_key
    assert by_key[("sync_dispatch_outbox", "last_error")].max_length == 2000
    assert (
        by_key[("sync_dispatch_outbox", "last_error")].max_length
        == _OUTBOX_MAX_ERROR_LENGTH
    )
    assert ("job_runs", "error") in by_key
    assert ("job_runs", "error_traceback") in by_key
    assert ("backfill_jobs", "error_message") in by_key
    assert ("sync_configurations", "last_sync_error") in by_key
    stats_col = by_key[("sync_configurations", "last_sync_stats")]
    assert stats_col.kind == KIND_JSON_ERROR_KEY
    assert ("integration_credentials", "last_test_error") in by_key
    for (_table, _col), spec in by_key.items():
        if (_table, _col) != ("sync_dispatch_outbox", "last_error"):
            assert spec.max_length == DEFAULT_MAX_ERROR_TEXT_LENGTH


# ---------------------------------------------------------------------------
# Full lifecycle: dry-run -> apply -> second apply, across every column
# ---------------------------------------------------------------------------


def test_scrub_dry_run_apply_and_reapply_across_all_columns(tmp_path):
    db_uri = _new_db(tmp_path)
    org_id = _org()
    dirty = f"403 rate limited -- Authorization: Bearer {_LEAK_1}"
    dirty_json_error = f"upstream 401: authorization=Bearer {_LEAK_2}"

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        unit = _seed_sync_run_unit(session, org_id=org_id, error=dirty)
        run = _seed_sync_run(session, org_id=org_id, error=dirty)
        discovery = _seed_reference_discovery(session, org_id=org_id, error=dirty)
        outbox = _seed_dispatch_outbox(session, org_id=org_id, last_error=dirty)
        job = _seed_scheduled_job(session, org_id=org_id)
        job_run = _seed_job_run(
            session, job_id=job.id, error=dirty, error_traceback=dirty
        )
        backfill = _seed_backfill_job(session, org_id=org_id, error_message=dirty)
        sync_config = _seed_sync_configuration(
            session,
            org_id=org_id,
            last_sync_error=dirty,
            last_sync_stats={
                "error": dirty_json_error,
                "phase": "dispatch_enqueue",
                "other": 1,
            },
        )
        cred = _seed_integration_credential(
            session, org_id=org_id, last_test_error=dirty
        )
        session.commit()
    engine.dispose()

    # --- dry run: correct counts, DB untouched ---
    counters, had_failure = collect_counters(_ns(db_uri, apply=False))
    assert had_failure is False
    assert counters is not None
    for key in (
        ("sync_run_units", "error"),
        ("sync_runs", "error"),
        ("sync_run_reference_discoveries", "error"),
        ("sync_dispatch_outbox", "last_error"),
        ("job_runs", "error"),
        ("job_runs", "error_traceback"),
        ("backfill_jobs", "error_message"),
        ("sync_configurations", "last_sync_error"),
        ("sync_configurations", "last_sync_stats"),
        ("integration_credentials", "last_test_error"),
    ):
        c = counters[key]
        assert c.scanned == 1, key
        assert c.redact == 1, key
        assert c.truncate_only == 0, key
        assert c.skipped_concurrent == 0, key
    dry_run_counters = counters

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        assert _fetch(session, SyncRunUnit, unit.id).error == dirty
        assert _fetch(session, SyncRun, run.id).error == dirty
        assert _fetch(session, SyncRunReferenceDiscovery, discovery.id).error == dirty
        assert _fetch(session, SyncDispatchOutbox, outbox.id).last_error == dirty
        fetched_job_run = _fetch(session, JobRun, job_run.id)
        assert fetched_job_run.error == dirty
        assert fetched_job_run.error_traceback == dirty
        assert _fetch(session, BackfillJob, backfill.id).error_message == dirty
        fetched_config = _fetch(session, SyncConfiguration, sync_config.id)
        assert fetched_config.last_sync_error == dirty
        assert fetched_config.last_sync_stats["error"] == dirty_json_error
        assert _fetch(session, IntegrationCredential, cred.id).last_test_error == dirty
    engine.dispose()

    # --- apply: counts match the dry run exactly (quiesced DB, no races) ---
    counters, had_failure = collect_counters(_ns(db_uri, apply=True))
    assert had_failure is False
    assert counters is not None
    for key, dry_c in dry_run_counters.items():
        applied_c = counters[key]
        assert applied_c.scanned == dry_c.scanned, key
        assert applied_c.redact == dry_c.redact, key
        assert applied_c.truncate_only == dry_c.truncate_only, key
        assert applied_c.skipped_concurrent == 0, key

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        for value in (
            _fetch(session, SyncRunUnit, unit.id).error,
            _fetch(session, SyncRun, run.id).error,
            _fetch(session, SyncRunReferenceDiscovery, discovery.id).error,
            _fetch(session, SyncDispatchOutbox, outbox.id).last_error,
            _fetch(session, JobRun, job_run.id).error,
            _fetch(session, JobRun, job_run.id).error_traceback,
            _fetch(session, BackfillJob, backfill.id).error_message,
            _fetch(session, SyncConfiguration, sync_config.id).last_sync_error,
            _fetch(session, IntegrationCredential, cred.id).last_test_error,
        ):
            assert value is not None
            assert REDACTION_MARKER in value
            assert _LEAK_1 not in value

        fetched_config = _fetch(session, SyncConfiguration, sync_config.id)
        assert REDACTION_MARKER in fetched_config.last_sync_stats["error"]
        assert _LEAK_2 not in fetched_config.last_sync_stats["error"]
        assert fetched_config.last_sync_stats["phase"] == "dispatch_enqueue"
        assert fetched_config.last_sync_stats["other"] == 1
    engine.dispose()

    # --- second apply: scrub-level idempotency, zero changes ---
    counters, had_failure = collect_counters(_ns(db_uri, apply=True))
    assert had_failure is False
    assert counters is not None
    for key, c in counters.items():
        assert c.redact == 0, key
        assert c.truncate_only == 0, key
        assert c.skipped_concurrent == 0, key


def test_run_scrub_error_text_cli_entrypoint_exit_code_and_report(tmp_path, capsys):
    db_uri = _new_db(tmp_path)
    org_id = _org()
    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        _seed_sync_run(session, org_id=org_id, error=f"leak {_LEAK_3}")
        session.commit()
    engine.dispose()

    rc = run_scrub_error_text(_ns(db_uri, apply=False))
    assert rc == 0
    out = capsys.readouterr().out
    assert "sync_runs.error" in out
    assert "would_redact" in out
    assert "Dry-run: pass --apply" in out


# ---------------------------------------------------------------------------
# Clean-row classification + per-column caps
# ---------------------------------------------------------------------------


def test_scrub_classifies_truncate_only_and_respects_per_column_caps(tmp_path):
    db_uri = _new_db(tmp_path)
    org_id = _org()

    clean_short = "connection reset by peer while fetching page 3 of 10"
    clean_at_default_cap = "z" * DEFAULT_MAX_ERROR_TEXT_LENGTH
    clean_over_default_cap = "z" * (DEFAULT_MAX_ERROR_TEXT_LENGTH + 200)
    clean_at_outbox_cap = "y" * _OUTBOX_MAX_ERROR_LENGTH
    clean_over_outbox_cap = "y" * (_OUTBOX_MAX_ERROR_LENGTH + 100)

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        short_row = _seed_sync_run(session, org_id=org_id, error=clean_short)
        at_cap_row = _seed_sync_run(session, org_id=org_id, error=clean_at_default_cap)
        over_cap_row = _seed_sync_run(
            session, org_id=org_id, error=clean_over_default_cap
        )
        outbox_at_cap = _seed_dispatch_outbox(
            session, org_id=org_id, last_error=clean_at_outbox_cap
        )
        # Over the outbox's 2000 cap but well under the 4000 default -- this
        # only truncates if the outbox's tighter cap is actually applied.
        outbox_over_cap = _seed_dispatch_outbox(
            session, org_id=org_id, last_error=clean_over_outbox_cap
        )
        session.commit()
    engine.dispose()

    counters, had_failure = collect_counters(_ns(db_uri, apply=True))
    assert had_failure is False
    assert counters is not None
    sync_runs_c = counters[("sync_runs", "error")]
    assert sync_runs_c.scanned == 3
    assert sync_runs_c.redact == 0
    assert sync_runs_c.truncate_only == 1

    outbox_c = counters[("sync_dispatch_outbox", "last_error")]
    assert outbox_c.scanned == 2
    assert outbox_c.redact == 0
    assert outbox_c.truncate_only == 1

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        assert _fetch(session, SyncRun, short_row.id).error == clean_short
        assert _fetch(session, SyncRun, at_cap_row.id).error == clean_at_default_cap
        over_cap_new = _fetch(session, SyncRun, over_cap_row.id).error
        assert over_cap_new != clean_over_default_cap
        assert len(over_cap_new) == DEFAULT_MAX_ERROR_TEXT_LENGTH
        assert over_cap_new.endswith("...[truncated]")

        assert (
            _fetch(session, SyncDispatchOutbox, outbox_at_cap.id).last_error
            == clean_at_outbox_cap
        )
        outbox_new = _fetch(session, SyncDispatchOutbox, outbox_over_cap.id).last_error
        assert outbox_new != clean_over_outbox_cap
        assert len(outbox_new) == _OUTBOX_MAX_ERROR_LENGTH
        assert outbox_new.endswith("...[truncated]")
    engine.dispose()


# ---------------------------------------------------------------------------
# CAS races
# ---------------------------------------------------------------------------


def test_scrub_text_column_cas_race_counts_skipped_concurrent(tmp_path):
    db_uri = _new_db(tmp_path)
    org_id = _org()
    stale_dirty = f"push rejected using {_LEAK_3}"
    concurrent_value = "RateLimitException: 403 rate limited -- [REDACTED]"

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        run = _seed_sync_run(session, org_id=org_id, error=stale_dirty)
        session.commit()
        row_id = run.id

    # A concurrent (already-sanitized) write lands between our conceptual
    # scan and our CAS update.
    with Session(engine, expire_on_commit=False) as session:
        session.execute(
            update(SyncRun).where(SyncRun.id == row_id).values(error=concurrent_value)
        )
        session.commit()

    table_spec = _table_spec(SyncRun)
    col_spec = table_spec.columns[0]
    counters = ColumnCounters()
    with Session(engine, expire_on_commit=False) as session:
        _process_text_column(
            session,
            table_spec,
            col_spec,
            row_id,
            stale_dirty,
            apply=True,
            counters=counters,
        )
        session.commit()

    assert counters.skipped_concurrent == 1
    assert counters.redact == 0
    assert counters.truncate_only == 0

    with Session(engine, expire_on_commit=False) as session:
        assert _fetch(session, SyncRun, row_id).error == concurrent_value
    engine.dispose()


def test_scrub_json_column_cas_race_counts_skipped_concurrent(tmp_path):
    db_uri = _new_db(tmp_path)
    org_id = _org()
    stale_stats = {
        "error": f"authorization=Bearer {_LEAK_4}",
        "phase": "dispatch_enqueue",
    }
    concurrent_stats = {
        "error": "already sanitized by a concurrent writer",
        "phase": "dispatch_enqueue",
    }

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        config = _seed_sync_configuration(
            session, org_id=org_id, last_sync_stats=stale_stats
        )
        session.commit()
        row_id = config.id
        # Capture the raw-text witness the real scan step would have read,
        # before the concurrent write below invalidates it.
        stale_raw_text = session.execute(
            select(cast(SyncConfiguration.last_sync_stats, Text)).where(
                SyncConfiguration.id == row_id
            )
        ).scalar_one()

    with Session(engine, expire_on_commit=False) as session:
        session.execute(
            update(SyncConfiguration)
            .where(SyncConfiguration.id == row_id)
            .values(last_sync_stats=concurrent_stats)
        )
        session.commit()

    table_spec = _table_spec(SyncConfiguration)
    col_spec = next(c for c in table_spec.columns if c.kind == KIND_JSON_ERROR_KEY)
    counters = ColumnCounters()
    with Session(engine, expire_on_commit=False) as session:
        _process_json_error_key_column(
            session,
            table_spec,
            col_spec,
            row_id,
            dict(stale_stats),
            stale_raw_text,
            apply=True,
            counters=counters,
        )
        session.commit()

    assert counters.skipped_concurrent == 1
    assert counters.redact == 0

    with Session(engine, expire_on_commit=False) as session:
        assert (
            _fetch(session, SyncConfiguration, row_id).last_sync_stats
            == concurrent_stats
        )
    engine.dispose()


# ---------------------------------------------------------------------------
# --org scoping
# ---------------------------------------------------------------------------


def test_scrub_org_scoping_direct_column(tmp_path):
    db_uri = _new_db(tmp_path)
    org_a = _org()
    org_b = _org()
    dirty = f"push rejected using {_LEAK_1}"

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        row_a = _seed_sync_run(session, org_id=org_a, error=dirty)
        row_b = _seed_sync_run(session, org_id=org_b, error=dirty)
        session.commit()
    engine.dispose()

    counters, had_failure = collect_counters(_ns(db_uri, apply=True, org=org_a))
    assert had_failure is False
    assert counters is not None
    c = counters[("sync_runs", "error")]
    assert c.scanned == 1
    assert c.redact == 1

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        assert _fetch(session, SyncRun, row_a.id).error != dirty
        assert REDACTION_MARKER in _fetch(session, SyncRun, row_a.id).error
        assert _fetch(session, SyncRun, row_b.id).error == dirty
    engine.dispose()


def test_scrub_org_scoping_job_runs_via_scheduled_jobs_join(tmp_path):
    db_uri = _new_db(tmp_path)
    org_a = _org()
    org_b = _org()
    dirty = f"push rejected using {_LEAK_1}"

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        job_a = _seed_scheduled_job(session, org_id=org_a)
        job_b = _seed_scheduled_job(session, org_id=org_b)
        run_a = _seed_job_run(session, job_id=job_a.id, error=dirty)
        run_b = _seed_job_run(session, job_id=job_b.id, error=dirty)
        session.commit()
    engine.dispose()

    counters, had_failure = collect_counters(_ns(db_uri, apply=True, org=org_a))
    assert had_failure is False
    assert counters is not None
    c = counters[("job_runs", "error")]
    assert c.scanned == 1
    assert c.redact == 1

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        assert _fetch(session, JobRun, run_a.id).error != dirty
        assert REDACTION_MARKER in _fetch(session, JobRun, run_a.id).error
        assert _fetch(session, JobRun, run_b.id).error == dirty
    engine.dispose()


def test_scrub_org_id_env_var_does_not_silently_scope_without_explicit_flag(
    tmp_path, monkeypatch
):
    """CHAOS-2780 codex HIGH: the root ``--org`` argument defaults to the
    ``ORG_ID`` env var, so an operator who simply has ORG_ID exported in
    their shell (ordinary env usage, not an explicit scope opt-in) must
    still get an ALL-orgs scrub -- not a silent single-tenant one whose
    "0 would-change" reads as a false whole-DB completion signal. Exercises
    the REAL parsing path (``cli.build_parser`` + ``parser.parse_args`` +
    ``cli._resolve_org``), not the test-only ``_ns()`` shortcut, since the
    bug lives in that parsing layer."""
    import dev_health_ops.cli as cli_module

    db_uri = _new_db(tmp_path)
    org_a = _org()
    org_b = _org()
    dirty = f"push rejected using {_LEAK_1}"

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        row_a = _seed_sync_run(session, org_id=org_a, error=dirty)
        row_b = _seed_sync_run(session, org_id=org_b, error=dirty)
        session.commit()
    engine.dispose()

    # The root --org default is captured at build_parser() call time, so
    # ORG_ID must be set BEFORE building the parser -- exactly like a real
    # shell export would be in place before the process starts.
    monkeypatch.setenv("ORG_ID", org_a)
    parser = cli_module.build_parser()

    argv_no_flag = [
        "maintenance",
        "scrub-error-text",
        "--apply",
        "--db",
        db_uri,
    ]
    ns = parser.parse_args(argv_no_flag)
    cli_module._resolve_org(ns)
    assert ns.org_explicit is False
    assert ns.org == org_a  # env fallback still resolves normally...

    counters, had_failure = collect_counters(ns)
    assert had_failure is False
    assert counters is not None
    c = counters[("sync_runs", "error")]
    # ...but BOTH orgs' rows were scanned/scrubbed -- the env value never
    # scoped the run.
    assert c.scanned == 2
    assert c.redact == 2

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        assert REDACTION_MARKER in _fetch(session, SyncRun, row_a.id).error
        assert REDACTION_MARKER in _fetch(session, SyncRun, row_b.id).error
    engine.dispose()


@pytest.mark.parametrize(
    ("label", "make_argv"),
    [
        (
            "long form after subcommand",
            lambda db_uri, org: [
                "maintenance",
                "scrub-error-text",
                "--apply",
                "--db",
                db_uri,
                "--org",
                org,
            ],
        ),
        (
            "long form before subcommand",
            lambda db_uri, org: [
                "--org",
                org,
                "maintenance",
                "scrub-error-text",
                "--apply",
                "--db",
                db_uri,
            ],
        ),
        (
            "equals-joined form",
            lambda db_uri, org: [
                "maintenance",
                "scrub-error-text",
                "--apply",
                "--db",
                db_uri,
                f"--org={org}",
            ],
        ),
        (
            # CHAOS-2780 codex HIGH round 2: argparse's allow_abbrev=True
            # default means an unambiguous PREFIX of --org also counts as
            # explicit. A prior fix that scanned raw argv for the literal
            # "--org" token missed this -- misclassifying an operator's
            # intentionally-scoped --apply as "no flag" and silently
            # widening it to ALL orgs, the inverse (and more dangerous)
            # failure vs. the original ORG_ID-env-leak bug.
            "abbreviated form after subcommand",
            lambda db_uri, org: [
                "maintenance",
                "scrub-error-text",
                "--apply",
                "--db",
                db_uri,
                "--or",
                org,
            ],
        ),
        (
            "abbreviated form before subcommand",
            lambda db_uri, org: [
                "--or",
                org,
                "maintenance",
                "scrub-error-text",
                "--apply",
                "--db",
                db_uri,
            ],
        ),
        (
            "abbreviated equals-joined form",
            lambda db_uri, org: [
                "maintenance",
                "scrub-error-text",
                "--apply",
                "--db",
                db_uri,
                f"--or={org}",
            ],
        ),
    ],
)
def test_scrub_every_org_flag_form_scopes_explicitly_even_with_org_id_env_set(
    tmp_path, monkeypatch, label, make_argv
):
    """Companion to the test above, parametrized over every form argparse
    recognizes as --org: an ACTUALLY-typed --org (long, `=`-joined, or an
    unambiguous abbreviation, before or after the subcommand) must scope the
    run to that org, regardless of what ORG_ID happens to be set to."""
    import dev_health_ops.cli as cli_module

    db_uri = _new_db(tmp_path)
    org_a = _org()
    org_b = _org()
    dirty = f"push rejected using {_LEAK_1}"

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        row_a = _seed_sync_run(session, org_id=org_a, error=dirty)
        row_b = _seed_sync_run(session, org_id=org_b, error=dirty)
        session.commit()
    engine.dispose()

    monkeypatch.setenv("ORG_ID", org_b)
    parser = cli_module.build_parser()
    argv_explicit = make_argv(db_uri, org_a)

    ns = parser.parse_args(argv_explicit)
    cli_module._resolve_org(ns)
    assert ns.org_explicit is True, label
    assert ns.org == org_a, label

    counters, had_failure = collect_counters(ns)
    assert had_failure is False, label
    assert counters is not None
    c = counters[("sync_runs", "error")]
    assert c.scanned == 1, label
    assert c.redact == 1, label

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        assert REDACTION_MARKER in _fetch(session, SyncRun, row_a.id).error, label
        assert _fetch(session, SyncRun, row_b.id).error == dirty, label
    engine.dispose()

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        assert REDACTION_MARKER in _fetch(session, SyncRun, row_a.id).error
        assert _fetch(session, SyncRun, row_b.id).error == dirty
    engine.dispose()


# ---------------------------------------------------------------------------
# JSON sibling-key preservation / non-string 'error' handling
# ---------------------------------------------------------------------------


def test_scrub_json_error_key_preserves_siblings_and_skips_non_string_or_missing(
    tmp_path,
):
    db_uri = _new_db(tmp_path)
    org_id = _org()
    dirty_json_error = f"authorization=Bearer {_LEAK_4}"

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        with_error = _seed_sync_configuration(
            session,
            org_id=org_id,
            last_sync_stats={
                "error": dirty_json_error,
                "phase": "dispatch_enqueue",
                "other": 1,
            },
        )
        non_string_error = _seed_sync_configuration(
            session,
            org_id=org_id,
            last_sync_stats={"error": 12345, "phase": "structured"},
        )
        no_error_key = _seed_sync_configuration(
            session,
            org_id=org_id,
            last_sync_stats={"phase": "ok", "count": 3},
        )
        session.commit()
    engine.dispose()

    counters, had_failure = collect_counters(_ns(db_uri, apply=True))
    assert had_failure is False
    assert counters is not None
    c = counters[("sync_configurations", "last_sync_stats")]
    assert c.scanned == 3
    assert c.redact == 1
    assert c.truncate_only == 0

    engine = create_engine(db_uri)
    with Session(engine, expire_on_commit=False) as session:
        redacted = _fetch(session, SyncConfiguration, with_error.id).last_sync_stats
        assert REDACTION_MARKER in redacted["error"]
        assert _LEAK_4 not in redacted["error"]
        assert redacted["phase"] == "dispatch_enqueue"
        assert redacted["other"] == 1

        assert _fetch(
            session, SyncConfiguration, non_string_error.id
        ).last_sync_stats == {
            "error": 12345,
            "phase": "structured",
        }
        assert _fetch(session, SyncConfiguration, no_error_key.id).last_sync_stats == {
            "phase": "ok",
            "count": 3,
        }
    engine.dispose()


# ---------------------------------------------------------------------------
# No database configured
# ---------------------------------------------------------------------------


def test_run_scrub_error_text_fails_fast_without_db_uri(monkeypatch):
    monkeypatch.delenv("POSTGRES_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    rc = run_scrub_error_text(_ns("", apply=False))
    assert rc == 1
