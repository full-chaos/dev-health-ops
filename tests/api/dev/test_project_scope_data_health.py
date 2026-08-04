"""PROJECT/TEAM data-health must measure the subject, not the organization.

``NativeDataHealthReader._watermark`` filters with
``AND (empty({repository_ids}) OR toString(repo_id) IN {repository_ids})``, and
``DataHealthService`` then falls back to ``observation.relevant_repository_ids``
when the scope contributed none. A committed PROJECT or TEAM subject carries
no repository dimension at all, so both behaviors combined measured every
repository in the organization and reported the result as the subject's
coverage -- and because source health is a *mandatory* source of the
project/team status plan, unrelated healthy repositories could make a
subject's evidence coverage read complete.

Codex adversarial review (HIGH, 2026-08-03) fixed this for PROJECT (#1453):
the reader resolves the same repository set the status/change reader
derives, from the same shared ``PROJECT_REPOSITORIES_SQL``, or fails closed.

CHAOS-3375 found two residual gaps in that fix, both covered below:

1. A committed TEAM direct scope reaches the *exact same* widening bug
   (``production_runtime._scope_request``'s ``DirectScope.TEAM`` branch
   commits a TEAM entity with ``repository_id=None``, just like PROJECT) --
   #1453 only special-cased ``EntityKind.PROJECT``, so a team subject fell
   straight through to the org-wide ``empty(repository_ids) OR ...`` arm,
   completely unguarded. Fixed by re-deriving via the same
   ``_TEAM_REPOSITORIES_SQL`` ``native_status_change.
   ClickHouseStatusChangeSource.team_repository_ids`` uses.
2. A failed derivation (query error) and a *successful* derivation that
   legitimately returns zero rows (the subject genuinely owns no
   repositories) were collapsed into the same ``[]`` return and reported
   identically as ``..._unavailable`` / ``DataHealthState.UNAVAILABLE``.
   The two are now distinct: a measured-empty subject reports
   ``..._empty`` / ``DataHealthState.NO_DATA`` ("queried, genuinely
   nothing in scope"), never conflated with "could not be measured at
   all".
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
TEAM_ID = "team-9f3d0f9e-04ee-4e0a-8f1f-2c5f6a2b7c11"
TEAM_REPOSITORY_ID = "cccccccc-0000-0000-0000-000000000003"
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


def _team_resolution() -> ScopeResolution:
    """A committed team subject, in the shape ``_scope_request`` produces.

    ``repository_id=None`` is not an arbitrary choice: teams are resolved
    organization-scoped with no repository dimension
    (``ScopeResolutionService._committed_scope_for``'s own docstring), so
    this is the only shape a team commit can have.
    """

    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(AuthorizedEntity(EntityKind.TEAM, TEAM_ID, "Team Nine", None),),
        team_filters=(),
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
    """Answers the four query shapes ``NativeDataHealthReader`` issues."""

    def __init__(
        self,
        *,
        derived: list[str] | None,
        fail: bool = False,
        identity_resolved: bool = True,
        identity_check_fails: bool = False,
    ) -> None:
        self.derived = derived
        self.fail = fail
        # Only consulted when ``derived`` is empty and the identity-check
        # query fires (``_project_identity_resolved``): whether the
        # provider-identity CTE resolves the committed project id to exactly
        # one active catalog row.
        self.identity_resolved = identity_resolved
        self.identity_check_fails = identity_check_fails
        self.watermark_params: list[dict[str, Any]] = []
        self.identity_check_calls: list[dict[str, Any]] = []
        self.org_wide_enumerated = False

    async def __call__(
        self, _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "SELECT count() AS resolved FROM project" in sql:
            # _PROJECT_IDENTITY_RESOLVED_SQL
            self.identity_check_calls.append(dict(params))
            if self.identity_check_fails:
                raise RuntimeError("clickhouse unavailable")
            return [{"resolved": 1 if self.identity_resolved else 0}]
        if "FROM work_items FINAL" in sql and "SELECT DISTINCT" in sql:
            # PROJECT_REPOSITORIES_SQL
            if self.fail:
                raise RuntimeError("clickhouse unavailable")
            return [{"repository_id": value} for value in (self.derived or [])]
        if "team_repo_ownership" in sql:
            # _TEAM_REPOSITORIES_SQL
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
async def test_project_with_zero_repositories_is_measured_empty_not_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A *successful* derivation returning zero rows is a real, measured

    answer ("this project genuinely owns no repositories") -- never widened
    to the organization, and never conflated with a *failed* derivation
    (below). It must still never enumerate the org-wide repository set, and
    must never reach ``_watermark`` with an empty ``repository_ids`` list
    either (that would silently re-trigger the exact widening this fix
    exists to close).
    """

    client = _FakeClient(derived=[], identity_resolved=True)
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(),
        source_systems=["work_items"],
    )

    assert client.watermark_params == []
    assert not client.org_wide_enumerated
    # The identity-resolution disambiguation query ran and confirmed the
    # project id still resolves -- that is what licenses "measured empty"
    # rather than "unavailable" below.
    assert client.identity_check_calls == [{"org_id": ORG_ID, "entity_id": PROJECT_ID}]
    assert observations[0].warning == "project_repository_scope_empty"
    assert observations[0].watermark is None
    assert observations[0].relevant_repository_ids == ()


@pytest.mark.asyncio
async def test_project_with_unresolvable_identity_is_unavailable_not_measured_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3375 (post-CHAOS-3374 rebase): ``PROJECT_REPOSITORIES_SQL``

    now ``INNER JOIN``s the provider-identity CTE (``HAVING count() = 1``,
    ``is_active = 1``), so a retired project or a same-id cross-provider
    collision empties the join and returns zero rows -- indistinguishable
    from a genuinely repo-less project by row count alone. The standing
    fail-closed ruling is that an unresolvable identity must never read as
    a clean, measured-empty answer: this must be
    ``project_repository_scope_unavailable``, not ``..._empty``.
    """

    client = _FakeClient(derived=[], identity_resolved=False)
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(),
        source_systems=["work_items"],
    )

    assert client.watermark_params == []
    assert not client.org_wide_enumerated
    assert client.identity_check_calls == [{"org_id": ORG_ID, "entity_id": PROJECT_ID}]
    assert observations[0].configured is None
    assert observations[0].warning == "project_repository_scope_unavailable"


@pytest.mark.asyncio
async def test_project_identity_check_failure_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed disambiguation query is itself treated as unresolved --

    there is no honest "assume resolved" default when the check that would
    prove it cannot run.
    """

    client = _FakeClient(derived=[], identity_check_fails=True)
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_project_resolution(),
        source_systems=["work_items"],
    )

    assert client.watermark_params == []
    assert not client.org_wide_enumerated
    assert observations[0].configured is None
    assert observations[0].warning == "project_repository_scope_unavailable"


@pytest.mark.asyncio
async def test_failed_project_derivation_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A *failed* derivation (query error) is unmeasured, distinct from the

    measured-empty case above -- ``configured is None`` is what
    ``DataHealthService`` maps to ``DataHealthState.UNAVAILABLE``, an
    explicit "not measured for this subject", never a silent
    organization-wide substitute and never the "no data" a measured-empty
    project reports.
    """

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
async def test_team_data_health_is_bound_to_the_teams_repositories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3375: a committed TEAM subject must be bound to its own

    ``team_repo_ownership`` repositories, the same way a PROJECT subject is
    bound to ``PROJECT_REPOSITORIES_SQL`` above -- #1453 never covered this
    scope kind at all.
    """

    client = _FakeClient(derived=[TEAM_REPOSITORY_ID])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_team_resolution(),
        source_systems=["work_items"],
    )

    assert [params["repository_ids"] for params in client.watermark_params] == [
        [TEAM_REPOSITORY_ID]
    ]
    assert not client.org_wide_enumerated
    assert observations[0].relevant_repository_ids == (TEAM_REPOSITORY_ID,)
    assert UNRELATED_REPOSITORY_ID not in observations[0].relevant_repository_ids


@pytest.mark.asyncio
async def test_team_with_zero_owned_repositories_is_measured_empty_not_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A team that genuinely owns zero repositories (a real, measured

    ``team_repo_ownership`` result) must never be widened to the
    organization, and must be distinct from a failed team derivation below.
    """

    client = _FakeClient(derived=[])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_team_resolution(),
        source_systems=["work_items"],
    )

    assert client.watermark_params == []
    assert not client.org_wide_enumerated
    assert observations[0].warning == "team_repository_scope_empty"
    assert observations[0].watermark is None
    assert observations[0].relevant_repository_ids == ()


@pytest.mark.asyncio
async def test_failed_team_derivation_fails_closed_not_org_wide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3375: before this fix, a team subject reached this exact

    ``empty(repository_ids) OR ...`` widening completely unguarded -- the
    fake client's ``org_wide_enumerated`` flag would have flipped ``True``
    and every mandatory source's watermark query would have run unfiltered
    across the whole organization.
    """

    client = _FakeClient(derived=None, fail=True)
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_team_resolution(),
        source_systems=["work_items"],
    )

    assert client.watermark_params == []
    assert not client.org_wide_enumerated
    assert observations[0].configured is None
    assert observations[0].warning == "team_repository_scope_unavailable"


@pytest.mark.asyncio
async def test_acr_stays_optional_for_a_measured_empty_team(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fail-closed/measured-empty arms must not swallow acr's own

    optional semantics.
    """

    client = _FakeClient(derived=[])
    _install(monkeypatch, client)

    observations = await NativeDataHealthReader(object(), None).read(
        org_id=ORG_ID,
        scope=_team_resolution(),
        source_systems=["acr", "work_items"],
    )

    by_source = {item.source_system: item for item in observations}
    assert by_source["acr"].warning == "acr_optional"
    assert by_source["acr"].required is False
    assert by_source["work_items"].warning == "team_repository_scope_empty"


@pytest.mark.asyncio
async def test_repository_scope_behaviour_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: only the no-repository PROJECT/TEAM cases changed."""

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
async def test_acr_stays_optional_for_a_measured_empty_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fail-closed/measured-empty arms must not swallow acr's own

    optional semantics.
    """

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
    assert by_source["work_items"].warning == "project_repository_scope_empty"
