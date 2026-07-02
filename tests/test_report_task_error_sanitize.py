"""CHAOS-2784: report_runs.error / error_traceback must not persist raw
exception text.

``execute_saved_report``'s failure handler used to write ``str(exc)`` and
``traceback.format_exc()`` straight into ``report_runs.error`` /
``report_runs.error_traceback`` -- if the report execution path raises an
exception whose message embeds a credential (e.g. an upstream API client
surfacing an ``Authorization`` header, the same shape CHAOS-2758 found in
``sync_run_units.error``), it would persist verbatim. This proves the sink now
routes through ``sanitize_error_text`` (CHAOS-2766) before either DB write and
before the task's return value.

See ``tests/test_error_sanitize.py`` for the module docstring explaining why
every fixture secret is assembled via ``_fake_secret(...)`` at runtime with a
neutral name instead of a literal -- required to defeat CI's Gitleaks scan.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dev_health_ops.models.git import Base
from dev_health_ops.models.reports import ReportRun, ReportRunStatus, SavedReport
from dev_health_ops.sync.error_sanitize import REDACTION_MARKER
from tests._helpers import tables_of


def _fake_secret(*parts: str) -> str:
    """Assemble a synthetic, redaction-target-shaped fixture at runtime (see
    tests/test_error_sanitize.py's module docstring for why this isn't a
    plain string literal -- Gitleaks matches file bytes, not runtime
    values)."""
    return "".join(parts)


_FIXTURE_1 = _fake_secret("ghp_", "FAKEqrstuvwxyz1234567890AB")


@pytest.fixture
def sync_session_maker(tmp_path):
    db_path = tmp_path / "report-task-sanitize.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine, tables=tables_of(SavedReport, ReportRun))
    return sessionmaker(bind=engine, expire_on_commit=False)


def test_execute_saved_report_sanitizes_secret_bearing_failure(
    monkeypatch, sync_session_maker
):
    with sync_session_maker() as seed:
        report = SavedReport(
            org_id="org-1",
            name="Weekly health",
            report_plan={},
            parameters={},
        )
        seed.add(report)
        seed.flush()
        report_uuid = report.id

        run = ReportRun(
            report_id=report_uuid,
            status=ReportRunStatus.PENDING.value,
            triggered_by="manual",
        )
        seed.add(run)
        seed.flush()
        run_uuid = run.id

        seed.commit()

    @contextmanager
    def fake_session_scope():
        session = sync_session_maker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    async def _raise_with_secret(plan, chart_specs, clickhouse_dsn):
        raise RuntimeError(
            f"upstream call failed -- Authorization: Bearer {_FIXTURE_1}"
        )

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync", fake_session_scope
    )
    monkeypatch.setattr(
        "dev_health_ops.db.require_clickhouse_uri", lambda: "clickhouse://fake/db"
    )
    monkeypatch.setattr("dev_health_ops.db.reset_async_engines", lambda: None)
    monkeypatch.setattr(
        "dev_health_ops.reports.engine.execute_report", _raise_with_secret
    )
    # SQLite (unlike the production Postgres backend) doesn't round-trip
    # tzinfo on DateTime(timezone=True) columns, so the previously-persisted
    # `started_at` comes back naive while `completed_at` is computed fresh as
    # tz-aware -- an unrelated test-harness artifact, not something this
    # sanitize fix touches. Short-circuit the (irrelevant to this test)
    # duration_seconds computation rather than fighting sqlite's tz handling.
    monkeypatch.setattr(
        "dev_health_ops.workers.report_task._datetime_or_none", lambda value: None
    )

    from dev_health_ops.workers.report_task import execute_saved_report

    result = execute_saved_report(str(report_uuid), str(run_uuid))

    assert result["status"] == "failed"
    assert _FIXTURE_1 not in result["error"]
    assert REDACTION_MARKER in result["error"]

    with sync_session_maker() as verify:
        persisted = verify.get(ReportRun, run_uuid)
        assert persisted is not None

        assert persisted.error is not None
        assert _FIXTURE_1 not in persisted.error
        assert REDACTION_MARKER in persisted.error
        assert "upstream call failed" in persisted.error

        assert persisted.error_traceback is not None
        assert _FIXTURE_1 not in persisted.error_traceback
        assert REDACTION_MARKER in persisted.error_traceback
