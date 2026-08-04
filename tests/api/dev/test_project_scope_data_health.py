"""Project data-health must measure the project, not the organization.

``NativeDataHealthReader._watermark`` filters with
``AND (empty({repository_ids}) OR toString(repo_id) IN {repository_ids})``, and
``DataHealthService`` then falls back to ``observation.relevant_repository_ids``
when the scope contributed none. A committed PROJECT subject carries no
repository dimension at all, so both behaviors combined measured every
repository in the organization and reported the result as the project's
coverage -- and because source health is a *mandatory* source of the project
status plan, unrelated healthy repositories could make a project's evidence
coverage read complete.

Codex adversarial review (HIGH, 2026-08-03). The reader now resolves the same
repository set the status/change reader derives, from the same shared
``PROJECT_REPOSITORIES_SQL``, or fails closed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev import data_health_service
from dev_health_ops.api.dev.data_health_service import NativeDataHealthReader
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ResolvedTimeRange,
    ScopeResolution,
    ScopeResolutionOutcome,
)

NOW = datetime(2026, 8, 4, tzinfo=UTC)
ORG_ID = "org-under-test"
PROJECT_ID = "13e65c04-40ec-4a95-8216-f7c2ce233244"
PROJECT_REPOSITORY_ID = "aaaaaaaa-0000-0000-0000-000000000001"
UNRELATED_REPOSITORY_ID = "bbbbbbbb-0000-0000-0000-000000000002"


def _time_range() -> ResolvedTimeRange:
    return ResolvedTimeRange(
        "UTC",
        NOW - timedelta(days=30),
        NOW,
        (NOW - timedelta(days=30)).isoformat(),
        NOW.isoformat(),
        NOW - timedelta(days=60),
        NOW - timedelta(days=30),
        (NOW - timedelta(days=60)).isoformat(),
        (NOW - timedelta(days=30)).isoformat(),
    )


def _project_resolution(*, team_filter: bool = False) -> ScopeResolution:
    """A committed project subject, in the shape the resolver produces.

    ``repository_id=None`` is not an arbitrary choice: the catalog's project
    query selects ``NULL AS repository_id``, so this is the only shape a
    project commit can have.
    """

    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(AuthorizedEntity(EntityKind.PROJECT, PROJECT_ID, "Ask Dev", None),),
        team_filters=(
            (AuthorizedEntity(EntityKind.TEAM, "team-a", "Team A", None),)
            if team_filter
            else ()
        ),
        candidates=(),
        time_range=_time_range(),
    )


def _repository_resolution() -> ScopeResolution:
    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(
            AuthorizedEntity(
                EntityKind.REPOSITORY,
                PROJECT_REPOSITORY_ID,
                "org/repo",
                PROJECT_REPOSITORY_ID,
            ),
        ),
        team_filters=(),
        candidates=(),
        time_range=_time_range(),
    )


class _FakeClient:
    """Answers the three query shapes ``NativeDataHealthReader`` issues."""

    def __init__(self, *, derived: list[str] | None, fail: bool = False) -> None:
        self.derived = derived
        self.fail = fail
        self.watermark_params: list[dict[str, Any]] = []
        self.org_wide_enumerated = False

    async def __call__(
        self, _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM work_items FINAL" in sql and "SELECT DISTINCT" in sql:
            if self.fail:
                raise RuntimeError("clickhouse unavailable")
            return [{"repository_id": value} for value in (self.derived or [])]
        if "groupUniqArray(toString(id))" in sql:
            # The org-wide fallback: reaching it for a project scope is the
            # bug, so record rather than merely serve it.
            self.org_wide_enumerated = True
            return [
                {
                    "repository_ids": [
                        PROJECT_REPOSITORY_ID,
                        UNRELATED_REPOSITORY_ID,
                    ]
                }
            ]
        self.watermark_params.append(dict(params))
        return [
            {
                "watermark": NOW - timedelta(hours=1),
                "covered_repository_ids": list(params.get("repository_ids") or []),
            }
        ]


def _install(monkeypatch: pytest.MonkeyPatch, client: _FakeClient) -> None:
    monkeypatch.setattr(data_health_service, "query_dicts", client)


@pytest.mark.asyncio
async def test_project_data_health_is_bound_to_the_projects_repositories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(derived=[PROJECT_REPOSITORY_ID])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(),
        source_systems=["work_items"],
    )

    assert [params["repository_ids"] for params in client.watermark_params] == [
        [PROJECT_REPOSITORY_ID]
    ]
    assert not client.org_wide_enumerated
    assert observations[0].relevant_repository_ids == (PROJECT_REPOSITORY_ID,)
    assert UNRELATED_REPOSITORY_ID not in observations[0].relevant_repository_ids


@pytest.mark.asyncio
async def test_unresolvable_project_repositories_fail_closed_not_org_wide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty derivation is a refusal, never "measure everything instead"."""

    client = _FakeClient(derived=[])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(),
        source_systems=["work_items"],
    )

    assert client.watermark_params == []
    assert not client.org_wide_enumerated
    # ``configured is None`` is what DataHealthService maps to
    # DataHealthState.UNAVAILABLE -- an explicit "not measured for this
    # subject", never a silent organization-wide substitute.
    assert observations[0].configured is None
    assert observations[0].warning == "project_repository_scope_unavailable"


@pytest.mark.asyncio
async def test_failed_project_derivation_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(derived=None, fail=True)
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(),
        source_systems=["work_items"],
    )

    assert not client.org_wide_enumerated
    assert observations[0].configured is None
    assert observations[0].warning == "project_repository_scope_unavailable"


@pytest.mark.asyncio
async def test_team_filtered_project_data_health_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No data-health query applies a team filter either."""

    client = _FakeClient(derived=[PROJECT_REPOSITORY_ID])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(team_filter=True),
        source_systems=["work_items"],
    )

    assert client.watermark_params == []
    assert observations[0].configured is None
    assert observations[0].warning == "project_repository_scope_unavailable"


@pytest.mark.asyncio
async def test_repository_scope_behaviour_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: only the no-repository PROJECT case changed."""

    client = _FakeClient(derived=[])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_repository_resolution(),
        source_systems=["work_items"],
    )

    assert [params["repository_ids"] for params in client.watermark_params] == [
        [PROJECT_REPOSITORY_ID]
    ]
    assert observations[0].warning != "project_repository_scope_unavailable"


@pytest.mark.asyncio
async def test_acr_stays_optional_for_an_unresolvable_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fail-closed arm must not swallow acr's own optional semantics."""

    client = _FakeClient(derived=[])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(),
        source_systems=["acr", "work_items"],
    )

    by_source = {item.source_system: item for item in observations}
    assert by_source["acr"].warning == "acr_optional"
    assert by_source["acr"].required is False
    assert by_source["work_items"].warning == "project_repository_scope_unavailable"
