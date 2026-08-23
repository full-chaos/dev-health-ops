"""The Python write paths for the executed-proof ledger (CHAOS-4114).

Two of them exist, they run in different processes, and they fail in opposite
directions:

* ``plan_sync_run`` stamps ATTEMPTED. It writes ``sync_run_units`` through the
  ORM, so sweeping "the write paths" by grepping for INSERT statements misses
  it entirely -- and missing it is the FAIL-OPEN direction: the gate reads an
  unrecorded pair as never-attempted and bootstraps it through forever, which
  is the CHAOS-4048/4049 shape the gate exists to catch.
* the sync-unit worker's terminal stamp records PROVEN. Missing it is
  fail-closed and self-healing, but still wrong.

Both are asserted here against a real session, plus the monotonicity the whole
ledger rests on: nothing may un-prove a route.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncExecutedProofLedger,
    SyncRunMode,
)
from dev_health_ops.sync.executed_proof_ledger import (
    record_executed_proof_attempts,
    record_executed_proof_terminal,
    result_proves_execution,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run

ORG_ID = "org-chaos-4114"
_NOW = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _ledger(session: Session) -> dict[str, tuple[datetime | None, datetime | None]]:
    """``provider/dataset -> (attempted_at, proven_at)``, read from the table.

    Deliberately a Core select over the table rather than an ORM query. The
    writers here are bulk upserts that the session's identity map never sees,
    so an ORM read hands back the stale instance it already has cached and the
    assertions below would be checking Python memory instead of the database.
    """

    session.expire_all()
    table = SyncExecutedProofLedger.__table__
    return {
        f"{row.provider}/{row.dataset_key}": (
            _naive_utc(row.attempted_at),
            _naive_utc(row.proven_at),
        )
        for row in session.execute(select(table)).all()
    }


def _naive_utc(value: datetime | None) -> datetime | None:
    """SQLite has no timezone type and returns these stamps naive.

    Normalizing both sides keeps the assertions about WHICH instant was
    recorded -- the thing that carries the monotonicity guarantee -- rather
    than about the storage backend's tzinfo handling.
    """

    if value is None:
        return None
    if value.tzinfo is None:
        # Already what SQLite handed back: the UTC instant that was written,
        # minus the label. Calling astimezone() here would reinterpret it as
        # LOCAL time and shift it by the host's offset.
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def test_planning_records_every_pair_as_attempted(db_session) -> None:
    integration = Integration(
        org_id=ORG_ID, provider="github", name="acme", config={}, is_active=True
    )
    db_session.add(integration)
    db_session.flush()
    for external_id in ("acme/api", "acme/web"):
        db_session.add(
            IntegrationSource(
                org_id=ORG_ID,
                integration_id=integration.id,
                provider="github",
                source_type="repo",
                external_id=external_id,
                name=external_id.rsplit("/", 1)[-1],
                full_name=external_id,
                metadata_={},
                is_enabled=True,
                discovered_at=_NOW,
                last_seen_at=_NOW,
            )
        )
    for dataset_key in ("commits", "prs"):
        db_session.add(
            IntegrationDataset(
                org_id=ORG_ID,
                integration_id=integration.id,
                dataset_key=dataset_key,
                is_enabled=True,
                options={},
            )
        )
    db_session.flush()

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    assert plan.total_units > 0, "the fixture planned nothing, so it proves nothing"

    ledger = _ledger(db_session)
    # One row per PAIR, not per unit: two sources times two datasets is four
    # units and two pairs.
    assert set(ledger) == {"github/commits", "github/prs"}
    for entry in ledger.values():
        attempted_at, proven_at = entry
        assert attempted_at is not None
        assert proven_at is None, (
            "planning a unit proved its pair. Only a terminal success with a "
            "positive persisted-row count may do that -- otherwise the gate "
            "passes every route that has merely been tried."
        )


def test_a_terminal_success_proves_and_nothing_ever_unproves(db_session) -> None:
    record_executed_proof_attempts(db_session, [("GitHub", " Commits ")], now=_NOW)
    ledger = _ledger(db_session)
    assert set(ledger) == {"github/commits"}, (
        "case and whitespace variants must fold onto one normalized row; two "
        "rows would split one route's proof in half, and the gate reads only "
        "the normalized form"
    )
    assert ledger["github/commits"][1] is None

    proven_at = _NOW + timedelta(minutes=5)
    record_executed_proof_terminal(
        db_session,
        provider="github",
        dataset_key="commits",
        status="success",
        result={"go_provider_route": {"records": 12}},
        now=proven_at,
    )
    assert _ledger(db_session)["github/commits"][1] == _naive_utc(proven_at)

    # Every way a later write could un-prove or re-date the route.
    for label, call in (
        (
            "a later empty-but-successful window",
            lambda: record_executed_proof_terminal(
                db_session,
                provider="github",
                dataset_key="commits",
                status="success",
                result={"go_provider_route": {"records": 0}},
                now=_NOW + timedelta(hours=1),
            ),
        ),
        (
            "a later failure",
            lambda: record_executed_proof_terminal(
                db_session,
                provider="github",
                dataset_key="commits",
                status="failed",
                result={"error": "boom"},
                now=_NOW + timedelta(hours=2),
            ),
        ),
        (
            "a later successful window that persisted rows again",
            lambda: record_executed_proof_terminal(
                db_session,
                provider="github",
                dataset_key="commits",
                status="success",
                result={"persisted": 4},
                now=_NOW + timedelta(hours=3),
            ),
        ),
        (
            "a re-plan",
            lambda: record_executed_proof_attempts(
                db_session, [("github", "commits")], now=_NOW + timedelta(hours=4)
            ),
        ),
    ):
        call()
        attempted_at, entry_proven_at = _ledger(db_session)["github/commits"]
        assert entry_proven_at == _naive_utc(proven_at), (
            f"{label} moved the proving instant"
        )
        assert attempted_at == _naive_utc(_NOW), (
            f"{label} moved the first-attempt instant"
        )


def test_an_unnormalizable_identity_is_dropped_rather_than_written(
    db_session,
) -> None:
    """A pair with an empty half could only make a row no lookup can match.

    Writing it would violate the table's normalization CHECK on PostgreSQL and
    abort whatever transaction it rode in -- for a unit that was already
    unplannable.
    """

    record_executed_proof_attempts(
        db_session, [("", "commits"), ("github", "   "), (None, None)], now=_NOW
    )
    record_executed_proof_terminal(
        db_session,
        provider="  ",
        dataset_key="prs",
        status="success",
        result={"persisted": 9},
        now=_NOW,
    )
    assert _ledger(db_session) == {}


@pytest.mark.parametrize(
    ("payload", "proven"),
    [
        ({"go_provider_route": {"records": 1}}, True),
        ({"go_provider_route": {"records": 0}}, False),
        # effects_written counts committed BATCHES, and a batch can carry zero
        # rows. Proving on it would readmit the exact CHAOS-4049 shape.
        ({"go_provider_route": {"records": 0, "effects_written": 3}}, False),
        ({"persisted": 2}, True),
        ({"persisted": 0}, False),
        ({"persisted": "NaN"}, False),
        ({"persisted": -1}, False),
        # 19 digits overflows bigint; 18 is always in range.
        ({"persisted": 9999999999999999999}, False),
        ({"persisted": 999999999999999999}, True),
        ({"go_provider_route": "not-an-object"}, False),
        ({}, False),
        (None, False),
    ],
)
def test_result_proves_execution_accepts_only_real_row_counts(payload, proven) -> None:
    assert result_proves_execution(payload) is proven
