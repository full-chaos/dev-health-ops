from __future__ import annotations

import logging
from typing import Any

import pytest

from dev_health_ops.workers import team_autoimport


def test_run_team_autoimport_skips_non_capable_provider(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = team_autoimport.run_team_autoimport(
        provider="launchdarkly",
        org_id="org-1",
        credentials={"token": "secret"},
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "provider_not_import_capable"
    assert result["projects_imported"] == 0
    assert "provider is not import-capable" in caplog.text


def test_run_team_autoimport_skips_missing_populator(caplog) -> None:
    caplog.set_level(logging.INFO)

    result = team_autoimport.run_team_autoimport(
        provider="ms-teams",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={"sync_options": {"auto_import_teams": True}},
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "populator_not_available"
    assert result["members_imported"] == 0
    assert "no populator module is available" in caplog.text


def test_run_team_autoimport_calls_resolved_populator(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def populate(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"members_imported": 2}

    monkeypatch.setattr(
        team_autoimport, "_resolve_populator", lambda provider: populate
    )

    result = team_autoimport.run_team_autoimport(
        provider="jira",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={"project_keys": ["OPS"]},
        analytics_db_url="clickhouse://config-dsn",
    )

    assert result == {
        "status": "success",
        "provider": "jira",
        "org_id": "org-1",
        "members_imported": 2,
    }
    assert calls == [
        {
            "org_id": "org-1",
            "credentials": {"token": "secret"},
            "scope": {
                "project_keys": ["OPS"],
                # CHAOS-4323: no "sync_options" in the caller's scope means
                # unrestricted -- every category defaults True (see
                # team_autoimport_categories.import_categories_from_sync_options).
                "import_categories": {
                    "teams": True,
                    "projects": True,
                    "members": True,
                },
                "analytics_db": "clickhouse://config-dsn",
            },
        }
    ]


def test_run_team_autoimport_strict_noops_non_capable_provider(caplog) -> None:
    """A provider with no reference tier (e.g. launchdarkly) must NOT fail the
    strict pre-sync discovery stage — it is a successful no-op so the run can
    proceed to dispatch (CHAOS-2740)."""
    caplog.set_level(logging.INFO)

    result = team_autoimport.run_team_autoimport_strict(
        provider="launchdarkly",
        org_id="org-1",
        credentials={"token": "secret"},
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "provider_not_import_capable"
    assert result["projects_imported"] == 0
    assert "not import-capable" in caplog.text
    # No reference keys claimed -> _verify_reference_readback no-ops and the
    # discovery ledger is stamped success.
    assert "reference_team_keys" not in result
    assert "reference_sprint_ids" not in result


def test_run_team_autoimport_strict_raises_capable_provider_missing_populator(
    monkeypatch,
) -> None:
    """Genuine failures are preserved: a capable provider whose populator module
    is unavailable still fails strict discovery visibly."""
    monkeypatch.setattr(team_autoimport, "_provider_capability", lambda provider: True)
    monkeypatch.setattr(team_autoimport, "_resolve_populator", lambda provider: None)

    with pytest.raises(ValueError, match="populator is unavailable"):
        team_autoimport.run_team_autoimport_strict(
            provider="github",
            org_id="org-1",
            credentials={"token": "secret"},
        )


def test_run_team_autoimport_strict_ignores_org_disabled_selection(
    monkeypatch,
) -> None:
    """CHAOS-4430 red test: reference discovery's strict populate path must
    honour the org's CHAOS-4323 per-category selection, the same way the
    best-effort ``run_team_autoimport`` already does.

    Executed proof (local, org 70d529e0-3c06-4597-8480-794fd02328b6, real
    data): ``sync_configurations`` for provider=github and provider=gitlab
    both carry ``auto_import_teams/projects/members`` = False. Yet
    ``sync_run_reference_discoveries`` shows hourly success rows for both
    providers (e.g. github sync_run 702a2dd5-6833-5191-98c9-738d60905036,
    updated_at 2026-08-28 22:03:22 UTC), and ClickHouse ``team_memberships``
    carries 454 fresh github rows / ``team_project_ownership`` carries 208
    fresh gitlab rows (source=provider_access, last write 2026-08-28
    22:03:1x UTC) -- exactly matching those reference-discovery timestamps.

    Root cause: ``run_team_autoimport_strict`` (unlike ``run_team_autoimport``)
    never reads ``scope["sync_options"]`` / never sets
    ``import_categories`` on the populator scope, so every populator call
    defaults every category to True regardless of what the org selected --
    the toggle a user unchecks in the sync-config UI has no effect on this
    path at all.
    """
    calls: list[dict[str, Any]] = []

    def populate(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"members_imported": 0}

    monkeypatch.setattr(
        team_autoimport, "_resolve_populator", lambda provider: populate
    )

    team_autoimport.run_team_autoimport_strict(
        provider="github",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={
            "sync_options": {
                "auto_import_teams": False,
                "auto_import_projects": False,
                "auto_import_members": False,
            }
        },
    )

    assert len(calls) == 1
    # DESIRED: an org with every CHAOS-4323 category off must not have any of
    # them imported by reference discovery either. FAILS on origin/main:
    # calls[0]["scope"] carries no "import_categories" key at all today, so
    # the populator falls back to "unrestricted" (every category True) --
    # see team_autoimport_categories.import_categories_from_sync_options's
    # None-sync_options branch, which is exactly what happens because this
    # call site drops sync_options on the floor instead of threading it.
    assert calls[0]["scope"].get("import_categories") == {
        "teams": False,
        "projects": False,
        "members": False,
    }


@pytest.mark.parametrize(
    "teams,projects,members",
    [
        (True, True, True),
        (False, False, False),
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, False),
        (True, False, True),
        (False, True, True),
    ],
)
def test_run_team_autoimport_strict_threads_every_selection_combination(
    monkeypatch, teams: bool, projects: bool, members: bool
) -> None:
    """CHAOS-4437: every one of the 8 CHAOS-4323 selection combinations must
    reach the populator unchanged, not just the all-off case the red test
    pinned."""
    calls: list[dict[str, Any]] = []

    def populate(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"members_imported": 0}

    monkeypatch.setattr(
        team_autoimport, "_resolve_populator", lambda provider: populate
    )

    team_autoimport.run_team_autoimport_strict(
        provider="github",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={
            "sync_options": {
                "auto_import_teams": teams,
                "auto_import_projects": projects,
                "auto_import_members": members,
            }
        },
    )

    assert calls[0]["scope"]["import_categories"] == {
        "teams": teams,
        "projects": projects,
        "members": members,
    }


def test_run_team_autoimport_strict_records_per_category_telemetry(
    monkeypatch,
) -> None:
    """CHAOS-4437: the strict path must emit a written|skipped_selection
    outcome per provider per category, independent of what the populator
    itself does with the selection."""
    recorded: list[dict[str, Any]] = []

    def fake_recorder(*, provider: str, category: str, outcome: str) -> None:
        recorded.append(
            {"provider": provider, "category": category, "outcome": outcome}
        )

    monkeypatch.setattr(
        team_autoimport,
        "record_team_autoimport_reference_category_outcome",
        fake_recorder,
    )
    monkeypatch.setattr(
        team_autoimport, "_resolve_populator", lambda provider: lambda **_: {}
    )

    team_autoimport.run_team_autoimport_strict(
        provider="gitlab",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={
            "sync_options": {
                "auto_import_teams": True,
                "auto_import_projects": False,
                "auto_import_members": False,
            }
        },
    )

    by_category = {row["category"]: row for row in recorded}
    assert by_category["teams"] == {
        "provider": "gitlab",
        "category": "teams",
        "outcome": "written",
    }
    assert by_category["projects"]["outcome"] == "skipped_selection"
    assert by_category["members"]["outcome"] == "skipped_selection"
    assert all(row["provider"] == "gitlab" for row in recorded)


def test_run_team_autoimport_strict_treats_non_canonical_sync_options_as_unrestricted(
    monkeypatch,
) -> None:
    """CHAOS-4437 (codex review, P1): when a caller marks its sync_options as
    NOT canonical (reference_discovery._load_discovery_context's fallback to
    Integration.config when no SyncConfiguration row exists), the strict path
    must NOT treat that dict as an authoritative selection -- doing so would
    default every missing category to False and silently flip "unrestricted"
    into "everything off" for orgs with no canonical sync config yet. This is
    the regression codex caught: a real (non-None) sync_options dict passed
    import_categories_from_sync_options's "authoritative" branch regardless
    of where it came from."""
    calls: list[dict[str, Any]] = []

    def populate(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {}

    monkeypatch.setattr(
        team_autoimport, "_resolve_populator", lambda provider: populate
    )

    team_autoimport.run_team_autoimport_strict(
        provider="github",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={
            # Integration.config-shaped: has "owner" but none of the three
            # auto_import_* keys -- exactly what a pre-CHAOS-4323 or
            # no-canonical-config integration looks like.
            "sync_options": {"owner": "full-chaos"},
            "sync_options_is_canonical": False,
        },
    )

    assert calls[0]["scope"]["import_categories"] == {
        "teams": True,
        "projects": True,
        "members": True,
    }


def test_run_team_autoimport_strict_trusts_sync_options_when_canonical_flag_absent(
    monkeypatch,
) -> None:
    """Legacy callers (backfill's run_backfill_for_config) that predate the
    sync_options_is_canonical flag always pass the real canonical
    SyncConfiguration.sync_options under scope["sync_options"] -- omitting
    the flag entirely must default to trusting it (canonical=True), not to
    "unrestricted", or backfill's real selections would stop being honoured."""
    calls: list[dict[str, Any]] = []

    def populate(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {}

    monkeypatch.setattr(
        team_autoimport, "_resolve_populator", lambda provider: populate
    )

    team_autoimport.run_team_autoimport_strict(
        provider="github",
        org_id="org-1",
        credentials={"token": "secret"},
        scope={
            "sync_options": {
                "auto_import_teams": False,
                "auto_import_projects": False,
                "auto_import_members": True,
            }
            # sync_options_is_canonical intentionally absent.
        },
    )

    assert calls[0]["scope"]["import_categories"] == {
        "teams": False,
        "projects": False,
        "members": True,
    }
