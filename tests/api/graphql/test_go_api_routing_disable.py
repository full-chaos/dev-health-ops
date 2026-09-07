"""`disable` -- the off-ramp. CI-visible coverage, no database required.

`enable` shipped without a way to turn anything off. For several hours on
2026-09-07, fifteen operations were live on the Go plane, four of them
measurably divergent, and the only way to move them back was hand-written
SQL -- run twice, by a human, under an explicit grant, because the tool
could not do it. Plan section 5 says "rollback is a registry change, not an
image rollback"; that registry change is what this verb makes possible.

Everything here runs with `DEV_HEALTH_POSTGRES_TEST_URI` unset. That is
deliberate: this PR's predecessor twice parked the only proof of a fix in a
database-gated file, where CI skipped it. The round-trip against real
Postgres lives in `test_go_api_routing_disable_db.py` and is additional
coverage, never the sole coverage.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.graphql import go_api_cli
from dev_health_ops.api.graphql.go_api_operation_catalog import catalog_entries
from dev_health_ops.api.graphql.go_api_routing_admin import (
    DISABLE_MODES,
    ModeChange,
    apply_disable,
)


def _ns(**overrides: Any) -> argparse.Namespace:
    ns = argparse.Namespace(
        operations="all-registered",
        mode="python",
        candidate_build=None,
        review_evidence=None,
        apply=False,
    )
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


def _disable(**overrides: Any) -> int:
    return asyncio.run(go_api_cli._cmd_routing_disable(_ns(**overrides)))


def test_disable_offers_only_unreachable_modes() -> None:
    """`canary`/`primary` are enable's job and have preflights this verb lacks.

    Offering them here would let someone turn traffic ON through a command
    that deliberately skips the digest-agreement and proof checks.
    """
    assert DISABLE_MODES == ("python", "disabled", "shadow")
    assert "canary" not in DISABLE_MODES
    assert "primary" not in DISABLE_MODES


def test_apply_without_review_evidence_is_refused(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A mode change is a decision; a decision with no durable reason is
    unreadable weeks later -- the same complaint that produced UNPROVEN."""
    assert _disable(apply=True, review_evidence=None) == 2
    assert "--apply requires --review-evidence" in capsys.readouterr().err


def test_blank_review_evidence_does_not_satisfy_the_requirement(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _disable(apply=True, review_evidence="   ") == 2
    assert "--apply requires --review-evidence" in capsys.readouterr().err


def test_an_unknown_operation_is_refused(capsys: pytest.CaptureFixture[str]) -> None:
    assert _disable(operations="featureFlags,notAnOperation") == 2
    err = capsys.readouterr().err
    assert "unknown operation(s) notAnOperation" in err


def test_all_registered_resolves_from_the_catalog() -> None:
    catalog = dict(catalog_entries())
    resolved, error = go_api_cli._resolve_requested_operations(
        "all-registered", catalog
    )
    assert error is None
    assert resolved == sorted(catalog)


class _RecordingSession:
    """Captures the SQL `apply_disable` emits, with no database.

    codex r1 (P3) was right that asserting on `inspect.getsource` proves
    nothing about behaviour: the previous version of these tests checked
    that the source CONTAINED `continue`, which a refactor could satisfy
    while breaking the guarantee. These execute the function.
    """

    def __init__(self, rowcount: int = 1) -> None:
        self.statements: list[str] = []
        self._rowcount = rowcount

    async def execute(self, statement: Any) -> Any:
        self.statements.append(
            str(statement.compile(compile_kwargs={"literal_binds": True}))
        )

        class _Result:
            rowcount = self._rowcount

        return _Result()


def test_apply_never_inserts_a_row_that_does_not_exist() -> None:
    """Executed, not inspected: a plan entry with no row emits no SQL."""
    session = _RecordingSession()
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode=None,
        new_mode="python",
        candidate_build=None,
    )
    applied = asyncio.run(
        apply_disable(
            cast(AsyncSession, session), schema_digest="sha", changes=[change]
        )
    )
    assert applied == []
    assert session.statements == [], "apply_disable emitted SQL for a missing row"


def test_apply_emits_update_never_insert() -> None:
    """The statement must be an UPDATE. An upsert here could create a row."""
    session = _RecordingSession()
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode="canary",
        new_mode="python",
        candidate_build="b",
    )
    asyncio.run(
        apply_disable(
            cast(AsyncSession, session), schema_digest="sha", changes=[change]
        )
    )
    assert len(session.statements) == 1
    sql = session.statements[0].upper()
    assert sql.startswith("UPDATE")
    assert "INSERT" not in sql


@pytest.mark.parametrize("reachable_mode", ["canary", "primary"])
def test_apply_refuses_to_set_a_reachable_mode(reachable_mode: str) -> None:
    """codex r1 (P1): a hand-built ModeChange turned routing ON.

    `plan_disable` validated the mode and `apply_disable` did not, so any
    caller bypassing the planner could use the OFF ramp to turn an
    operation on. An invariant checked only by the caller is an invariant
    the next caller breaks -- so it is enforced at the write.
    """
    session = _RecordingSession()
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode="python",
        new_mode=reachable_mode,
        candidate_build="b",
    )
    with pytest.raises(ValueError, match="may only set"):
        asyncio.run(
            apply_disable(
                cast(AsyncSession, session), schema_digest="sha", changes=[change]
            )
        )
    assert session.statements == [], "refused, but SQL was emitted anyway"


def test_the_candidate_build_guard_is_part_of_the_update() -> None:
    """codex r1 (P1): the guard was time-of-check/time-of-use.

    `plan_disable` read the build, then `apply_disable` updated by the
    3-column key only -- so a row repointed in between was still written.
    The guard now rides in the WHERE, making check and write one statement.
    """
    session = _RecordingSession()
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode="canary",
        new_mode="python",
        candidate_build="build-A",
    )
    asyncio.run(
        apply_disable(
            cast(AsyncSession, session),
            schema_digest="sha",
            changes=[change],
            expected_candidate_build="build-A",
        )
    )
    where = session.statements[0].split("WHERE", 1)[1]
    assert "current_candidate_build" in where
    assert "build-A" in where


def test_without_a_guard_the_update_does_not_constrain_the_build() -> None:
    """The converse: an unguarded disable must still work on any build.

    Constraining it unconditionally would make the off-ramp fail exactly
    when an operator does not know which build is live -- which is when
    they most need it.
    """
    session = _RecordingSession()
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode="canary",
        new_mode="python",
        candidate_build="build-A",
    )
    asyncio.run(
        apply_disable(
            cast(AsyncSession, session), schema_digest="sha", changes=[change]
        )
    )
    where = session.statements[0].split("WHERE", 1)[1]
    assert "current_candidate_build" not in where


def test_a_repointed_row_is_reported_by_absence_not_silence() -> None:
    """rowcount 0 means the guarded UPDATE matched nothing."""
    session = _RecordingSession(rowcount=0)
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode="canary",
        new_mode="python",
        candidate_build="build-A",
    )
    applied = asyncio.run(
        apply_disable(
            cast(AsyncSession, session),
            schema_digest="sha",
            changes=[change],
            expected_candidate_build="build-A",
        )
    )
    assert applied == [], "a row that did not match was reported as applied"


def test_a_row_already_in_the_target_mode_is_a_noop() -> None:
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode="python",
        new_mode="python",
        candidate_build="b",
    )
    assert change.is_noop is True


def test_a_missing_row_is_a_noop_not_an_error() -> None:
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode=None,
        new_mode="python",
        candidate_build=None,
    )
    assert change.is_noop is True


def test_a_real_mode_change_is_not_a_noop() -> None:
    change = ModeChange(
        operation="featureFlags",
        document_digest="d",
        current_mode="canary",
        new_mode="shadow",
        candidate_build="b",
    )
    assert change.is_noop is False


def test_recorded_by_is_resolved_not_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    """ "Who" must come from the tool, never from the operator's prose.

    `review_evidence` answers "why" and is free text; conflating the two
    makes both unreliable, which is why they are separate columns.
    """
    for var in ("DEV_HOPS_OPERATOR", "SUDO_USER", "USER", "LOGNAME"):
        monkeypatch.delenv(var, raising=False)
    assert go_api_cli._recorded_by() == "unknown"

    monkeypatch.setenv("DEV_HOPS_OPERATOR", "alice")
    assert go_api_cli._recorded_by() == "alice"


def test_disable_takes_no_query_api_url() -> None:
    """The off-ramp must work when query-api is DOWN.

    It also removes the never-print-the-URL obligation entirely rather than
    re-solving it -- five review rounds went into that problem on `enable`.
    """
    import inspect

    source = inspect.getsource(go_api_cli._cmd_routing_disable)
    assert "query_api_url" not in source
    assert "_fetch_go_plane_registry" not in source


def test_enable_records_the_acknowledgement_durably() -> None:
    """An acknowledged-unproven row carries its reason on the ROW.

    Before this, the only record was a WARNING at the moment it happened:
    on 2026-09-07 fifteen operations were enabled on an explicit ruling and
    that ruling lived in a chat message.
    """
    ns = argparse.Namespace(review_evidence="chris ruled it, 09:14Z")
    unproven = go_api_cli._enable_review_evidence(ns, unproven=True)
    assert unproven == "ACKNOWLEDGED-UNPROVEN: chris ruled it, 09:14Z"

    proven = go_api_cli._enable_review_evidence(ns, unproven=False)
    assert proven == "chris ruled it, 09:14Z"

    blank = go_api_cli._enable_review_evidence(
        argparse.Namespace(review_evidence=None), unproven=True
    )
    assert blank == "ACKNOWLEDGED-UNPROVEN: no reason given"
