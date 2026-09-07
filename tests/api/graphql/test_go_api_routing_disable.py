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
from typing import Any

import pytest

from dev_health_ops.api.graphql import go_api_cli
from dev_health_ops.api.graphql.go_api_operation_catalog import catalog_entries
from dev_health_ops.api.graphql.go_api_routing_admin import (
    DISABLE_MODES,
    ModeChange,
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


def test_apply_never_inserts_a_row_that_does_not_exist() -> None:
    """Turning something OFF must not be able to turn something ON.

    A `ModeChange` with no current row is skipped, never written: inserting
    a `python` row for an operation nobody ever enabled would manufacture
    history that reads as a deliberate decision.
    """
    import inspect

    from dev_health_ops.api.graphql import go_api_routing_admin as admin

    source = inspect.getsource(admin.apply_disable)
    assert "if change.current_mode is None:" in source
    assert "continue" in source


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
