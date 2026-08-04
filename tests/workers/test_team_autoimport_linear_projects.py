"""CHAOS-3365: Linear PROJECTS become resolvable Ask Dev subjects.

``PROJECTS_QUERY``/``LinearClient.iter_projects`` already existed and had no
caller, so ``projects`` only ever held team-derived attribution rows keyed
``{org}:linear:{team_key}``. A user naming a real Linear project ("Ask Dev")
got ``NO_AUTHORIZED_MATCH``.

The GraphQL page these tests feed the worker is GENERATED FROM THE PRODUCER:
the node shape is parsed out of ``PROJECTS_QUERY`` itself, and
:func:`_project_node` fails loudly if the query requests a field the fixture
does not model. A hand-authored page would keep passing after the real query
changed shape.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.providers.linear.client import PROJECTS_QUERY, LinearClient
from dev_health_ops.workers import team_autoimport_linear

ASK_DEV_PROJECT_ID = "13e65c04-40ec-4a95-8216-f7c2ce233244"
ASK_DEV_PROJECT_NAME = "Ask Dev"
OTHER_PROJECT_ID = "32f7879e-00ed-486f-884a-bfb4e93e2f48"


# --------------------------------------------------------------------------
# Fixture generation FROM the producer (PROJECTS_QUERY), not by hand.
# --------------------------------------------------------------------------


def _parse_selection(text: str, start: int) -> tuple[dict[str, Any], int]:
    """Parse a GraphQL selection set beginning just after its opening brace."""

    fields: dict[str, Any] = {}
    index = start
    token = ""
    last: str | None = None
    while index < len(text):
        char = text[index]
        if char == "{":
            assert last is not None, "selection set with no preceding field name"
            sub, index = _parse_selection(text, index + 1)
            fields[last] = sub
            last = None
            continue
        if char == "}":
            if token:
                fields[token] = None
            return fields, index + 1
        if char.isspace():
            if token:
                fields[token] = None
                last = token
                token = ""
        else:
            token += char
        index += 1
    raise AssertionError("unbalanced selection set in PROJECTS_QUERY")


def _projects_node_selection() -> dict[str, Any]:
    connection_at = PROJECTS_QUERY.index("projects(")
    brace_at = PROJECTS_QUERY.index("{", connection_at)
    connection, _ = _parse_selection(PROJECTS_QUERY, brace_at + 1)
    nodes = connection.get("nodes")
    assert isinstance(nodes, dict), "PROJECTS_QUERY no longer selects projects.nodes"
    return nodes


def _project_node(**overrides: Any) -> dict[str, Any]:
    """Build one ``projects.nodes`` entry covering exactly what the query asks for."""

    scalars: dict[str, Any] = {
        "id": ASK_DEV_PROJECT_ID,
        "name": ASK_DEV_PROJECT_NAME,
        "description": "Ask Dev subject resolution",
        "trashed": None,
        "progress": 0.4,
        "startDate": "2026-06-01",
        "targetDate": "2026-09-01",
        "createdAt": "2026-06-01T09:00:00.000Z",
        "updatedAt": "2026-07-30T12:00:00.000Z",
        "archivedAt": None,
        "url": f"https://linear.app/fullchaos/project/{ASK_DEV_PROJECT_ID}",
    }
    objects: dict[str, Any] = {
        "status": {"id": "st-1", "name": "In Progress", "type": "started"},
        "lead": {"id": "usr-1", "name": "Dev User", "email": "dev@example.com"},
        "teams": {"nodes": [{"id": "team-1", "key": "CHAOS"}]},
    }

    selection = _projects_node_selection()
    node: dict[str, Any] = {}
    unmodelled: list[str] = []
    for field, sub in selection.items():
        source = objects if sub is not None else scalars
        if field not in source:
            unmodelled.append(field)
            continue
        node[field] = source[field]
    assert not unmodelled, (
        "PROJECTS_QUERY requests fields this fixture does not model: "
        f"{sorted(unmodelled)}. Model them here so the generated page keeps "
        "matching the real producer."
    )

    unknown = set(overrides) - set(node)
    assert not unknown, (
        f"override targets a field PROJECTS_QUERY does not select: {unknown}"
    )
    node.update(overrides)
    return node


def test_generated_page_carries_the_fields_the_worker_reads() -> None:
    """A measurement that did not happen must fail: prove the producer still
    selects every field :func:`_linear_project_records` depends on."""

    selection = _projects_node_selection()
    for required in ("id", "name", "updatedAt", "archivedAt", "trashed", "status"):
        assert required in selection, (
            f"PROJECTS_QUERY stopped selecting {required!r}; the Linear project "
            "catalog rows silently lose their id/name/version/activity source."
        )


# --------------------------------------------------------------------------
# Sink + provider doubles
# --------------------------------------------------------------------------


@dataclass
class FakeDimensionSink:
    projects: dict[tuple[str, str, str], ProjectRecord]
    members: dict[tuple[str, str], MemberRecord]
    memberships: dict[tuple[str, str, str, str, str], TeamMembershipRecord]
    ownership: dict[tuple[str, str, str, str, str], TeamProjectOwnershipRecord]
    teams: dict[tuple[str, str], dict[str, Any]]
    closed: bool = False

    def write_projects(self, rows: list[ProjectRecord]) -> None:
        for row in rows:
            self.projects[(row.org_id, row.provider, row.id)] = row

    def write_members(self, rows: list[MemberRecord]) -> None:
        for row in rows:
            self.members[(row.org_id, row.member_id)] = row

    def write_team_memberships(self, rows: list[TeamMembershipRecord]) -> None:
        for row in rows:
            self.memberships[
                (row.org_id, row.provider, row.team_id, row.member_id, row.source)
            ] = row

    def write_team_project_ownership(
        self, rows: list[TeamProjectOwnershipRecord]
    ) -> None:
        for row in rows:
            self.ownership[
                (row.org_id, row.provider, row.project_id, row.team_id, row.source)
            ] = row

    async def insert_teams(self, teams: list[dict[str, Any]]) -> None:
        for team in teams:
            self.teams[(str(team["org_id"]), str(team["id"]))] = team

    def close(self) -> None:
        self.closed = True


def _fake_sink() -> FakeDimensionSink:
    return FakeDimensionSink(
        projects={}, members={}, memberships={}, ownership={}, teams={}
    )


def _install_team_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    async def discover_linear(self: object, api_key: str) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="linear",
                provider_team_id="CHAOS",
                name="Fullchaos",
                associations={"project_keys": ["CHAOS"]},
            )
        ]

    async def discover_members_linear(
        self: object, api_key: str, team_key: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="linear",
                provider_identity="dev@example.com",
                display_name="Dev User",
                email="dev@example.com",
            )
        ]

    monkeypatch.setattr(
        team_autoimport_linear.TeamDiscoveryService, "discover_linear", discover_linear
    )
    monkeypatch.setattr(
        team_autoimport_linear.TeamMembershipService,
        "discover_members_linear",
        discover_members_linear,
    )


def _page(nodes: list[dict[str, Any]], *, cursor: str | None = None) -> dict[str, Any]:
    """One ``projects`` connection page; ``cursor`` set means "more follow"."""

    return {
        "projects": {
            "nodes": nodes,
            "pageInfo": {
                "hasNextPage": cursor is not None,
                "endCursor": cursor,
            },
        }
    }


def _install_projects_pages(
    monkeypatch: pytest.MonkeyPatch,
    pages: list[Any],
    *,
    projects_error: Exception | None = None,
) -> list[dict[str, Any]]:
    """Serve ``PROJECTS_QUERY`` from the real client's transport seam.

    ``iter_projects``, its pagination loop and ``PROJECTS_QUERY`` stay REAL —
    only the HTTP round trip is replaced — so the paging behaviour under test is
    the production one. Dispatch is on ``query is PROJECTS_QUERY`` (identity),
    so this cannot silently answer some other query.

    A page entry may be an ``Exception``, which is raised when that page is
    requested — that is how a mid-pagination failure is exercised. Returns the
    list of variables each call received, so a test can assert what was asked
    for (e.g. ``includeArchived``).
    """

    calls: list[dict[str, Any]] = []
    remaining = list(pages)

    def _execute(
        self: LinearClient, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if query is PROJECTS_QUERY:
            calls.append(dict(variables or {}))
            if projects_error is not None:
                raise projects_error
            assert remaining, "iter_projects requested more pages than were staged"
            page = remaining.pop(0)
            if isinstance(page, Exception):
                raise page
            return page
        # Team/cycle lookups are out of scope here; an empty payload makes them
        # a clean no-op rather than an exception the worker would swallow.
        return {}

    monkeypatch.setattr(LinearClient, "_execute", _execute)
    return calls


def _populate(
    monkeypatch: pytest.MonkeyPatch,
    nodes: list[dict[str, Any]],
    *,
    sink: FakeDimensionSink | None = None,
    scope: dict[str, Any] | None = None,
    projects_error: Exception | None = None,
) -> tuple[dict[str, Any], FakeDimensionSink]:
    _install_team_discovery(monkeypatch)
    _install_projects_pages(monkeypatch, [_page(nodes)], projects_error=projects_error)
    target = sink if sink is not None else _fake_sink()
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope=scope if scope is not None else {"mode": "sync_config"},
        sink=target,
    )
    return summary, target


# --------------------------------------------------------------------------
# The gap this ticket closes
# --------------------------------------------------------------------------


def test_linear_project_is_written_as_a_catalog_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED before CHAOS-3365: no row existed for the Linear project at all."""

    summary, sink = _populate(monkeypatch, [_project_node()])

    key = ("org-1", "linear", ASK_DEV_PROJECT_ID)
    assert key in sink.projects, (
        "Linear project was not imported; a user naming it still gets "
        "NO_AUTHORIZED_MATCH"
    )
    row = sink.projects[key]
    assert row.name == ASK_DEV_PROJECT_NAME
    assert row.project_key is None
    assert row.is_active == 1
    assert row.provider == "linear"
    # Versioned by observation time, not Linear's mtime — see
    # test_every_native_row_is_versioned_by_observation_time.
    assert row.updated_at == row.last_synced
    assert summary["native_projects_imported"] == 1


def test_project_row_id_is_the_linear_uuid_not_the_name_or_a_composite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The id-space choice IS the feature.

    ``work_items.project_id`` carries the raw Linear project UUID, so a scope
    committed against this row selects work items with no query change. A name
    or an ``{org}:linear:{key}`` composite would resolve and then match nothing.
    """

    _, sink = _populate(monkeypatch, [_project_node()])

    ids = {row.id for row in sink.projects.values() if row.project_key is None}
    assert ids == {ASK_DEV_PROJECT_ID}
    assert ASK_DEV_PROJECT_NAME not in ids
    assert f"org-1:linear:{ASK_DEV_PROJECT_NAME}" not in ids


def test_project_rows_coexist_with_team_derived_attribution_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dedupe must not let one id space evict the other.

    ``_dedupe_projects`` keys on ``(org_id, provider, id)``. Both row kinds are
    provider ``linear`` in the same org, so a dedupe keyed on anything coarser
    (or an id scheme that collided) would drop the team attribution rows the
    ownership join depends on.
    """

    summary, sink = _populate(monkeypatch, [_project_node()])

    assert ("org-1", "linear", "org-1:linear:CHAOS") in sink.projects, (
        "team-derived attribution row was evicted by the new project rows"
    )
    assert ("org-1", "linear", ASK_DEV_PROJECT_ID) in sink.projects
    assert summary["projects_imported"] == 2
    assert summary["native_projects_imported"] == 1
    # The ownership row still points at the team-derived project id.
    assert (
        "org-1",
        "linear",
        "org-1:linear:CHAOS",
        "CHAOS",
        "native",
    ) in sink.ownership


def test_rerun_is_idempotent_across_both_id_spaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = _fake_sink()
    for _ in range(2):
        _populate(monkeypatch, [_project_node()], sink=sink)

    assert len(sink.projects) == 2
    assert len(sink.ownership) == 1


def test_duplicate_nodes_across_pages_collapse_to_one_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink = _populate(monkeypatch, [_project_node(), _project_node()])

    assert len([row for row in sink.projects.values() if row.project_key is None]) == 1


# --------------------------------------------------------------------------
# The is_active predicate
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "state", ["planned", "started", "paused", "completed", "canceled"]
)
def test_every_lifecycle_state_stays_resolvable(
    monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    """Ratified: lifecycle state is not activity.

    ``scope_catalog`` filters the catalog on ``is_active = 1``; keying activity
    off ``state`` would make a project unresolvable the moment it completed —
    exactly when people start asking what it delivered.
    """

    _, sink = _populate(
        monkeypatch, [_project_node(status={"id": "s", "name": state, "type": state})]
    )

    assert sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)].is_active == 1


def test_archived_project_is_marked_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    _, sink = _populate(
        monkeypatch,
        [_project_node(archivedAt="2026-07-31T00:00:00.000Z")],
    )

    assert sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)].is_active == 0


def test_the_worker_actually_asks_linear_for_archived_projects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without this the archived test above is theatre.

    Linear's ``projects`` connection defaults ``includeArchived: false``, so an
    archived project simply STOPS appearing. A worker that writes only the rows
    it is handed can never write the ``is_active = 0`` that retires a subject —
    it would go on resolving a project that no longer exists. Asserting on a
    hand-fed archived node proves nothing unless production actually requests
    archived nodes, which is what this asserts.
    """

    _install_team_discovery(monkeypatch)
    calls = _install_projects_pages(monkeypatch, [_page([_project_node()])])
    team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=_fake_sink(),
    )

    assert calls, "PROJECTS_QUERY was never executed"
    assert calls[0].get("includeArchived") is True, (
        "archived projects were not requested, so archiving a project can never "
        "retire its catalog row"
    )


def test_archiving_a_project_between_runs_retires_the_catalog_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lifecycle path end to end: active on run one, archived on run two.

    ``updated_at`` is the ReplacingMergeTree version column and Linear bumps
    ``updatedAt`` when a project is archived, so the retiring row must also
    carry the LATER version or the merge would keep the active one.
    """

    sink = _fake_sink()
    _populate(monkeypatch, [_project_node()], sink=sink)
    live = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]
    assert live.is_active == 1

    _populate(
        monkeypatch,
        [
            _project_node(
                status={"id": "s", "name": "Done", "type": "completed"},
                archivedAt="2026-08-01T09:00:00.000Z",
                updatedAt="2026-08-01T09:00:00.000Z",
            )
        ],
        sink=sink,
    )
    retired = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]

    assert retired.is_active == 0
    assert retired.updated_at > live.updated_at, (
        "the retiring row must win the ReplacingMergeTree merge"
    )


# --------------------------------------------------------------------------
# Pagination — the loop this ticket puts into production for the first time
# --------------------------------------------------------------------------


def test_projects_are_collected_across_real_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_team_discovery(monkeypatch)
    second = _project_node(id=OTHER_PROJECT_ID, name="Go Worker Runtime Migration")
    calls = _install_projects_pages(
        monkeypatch,
        [_page([_project_node()], cursor="cursor-1"), _page([second])],
    )
    target = _fake_sink()
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=target,
    )

    assert summary["native_projects_imported"] == 2
    assert summary["native_projects_complete"] is True
    assert ("org-1", "linear", OTHER_PROJECT_ID) in target.projects
    assert [call.get("after") for call in calls] == [None, "cursor-1"]


def test_a_cursor_that_does_not_advance_stops_instead_of_looping_forever(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A degraded ``pageInfo`` must not hang the sync run.

    ``hasNextPage: true`` with a repeated or missing ``endCursor`` re-requests
    the same page. Per-request HTTP timeouts do not bound that loop, so it stalls
    the whole run rather than failing it. The staged transport asserts if more
    pages are requested than were provided, so an unbounded loop shows up as a
    failure here rather than as a hung test.
    """

    _install_team_discovery(monkeypatch)
    _install_projects_pages(
        monkeypatch,
        [
            _page([_project_node()], cursor="stuck"),
            _page([_project_node(id=OTHER_PROJECT_ID, name="Second")], cursor="stuck"),
        ],
    )
    target = _fake_sink()
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=target,
    )

    # Both pages were consumed and the loop then stopped rather than re-asking.
    assert summary["native_projects_imported"] == 2
    # ...and the run is reported INCOMPLETE. Keeping the prefix silently would
    # make a truncated catalog indistinguishable from a complete one.
    assert summary["native_projects_complete"] is False


def test_a_missing_cursor_stops_instead_of_looping_forever(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_team_discovery(monkeypatch)
    _install_projects_pages(
        monkeypatch,
        [{"projects": {"nodes": [_project_node()], "pageInfo": {"hasNextPage": True}}}],
    )
    target = _fake_sink()
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=target,
    )

    assert summary["native_projects_imported"] == 1
    assert summary["native_projects_complete"] is False


def test_a_failure_on_a_later_page_does_not_discard_the_whole_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial page set is self-healing next run; discarding everything is not.

    The rows are accumulated one node at a time rather than through a list
    comprehension over the generator, so the prefix already fetched survives the
    exception. The run is still reported INCOMPLETE — keeping the prefix is only
    safe because the caller can tell it is a prefix.
    """

    _install_team_discovery(monkeypatch)
    _install_projects_pages(
        monkeypatch,
        [_page([_project_node()], cursor="cursor-1"), RuntimeError("linear 502")],
    )
    target = _fake_sink()
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=target,
    )

    assert summary["native_projects_imported"] == 1, (
        "the page fetched before the failure must survive"
    )
    assert summary["native_projects_complete"] is False
    assert ("org-1", "linear", ASK_DEV_PROJECT_ID) in target.projects
    assert ("org-1", "linear", "org-1:linear:CHAOS") in target.projects


# --------------------------------------------------------------------------
# Stored lifecycle (CHAOS-3365 enrichment; surfacing it is CHAOS-3368)
# --------------------------------------------------------------------------


def test_linear_lifecycle_fields_are_stored_verbatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink = _populate(
        monkeypatch,
        [_project_node(status={"id": "s", "name": "On Hold", "type": "paused"})],
    )

    row = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]
    assert row.state == "paused", (
        "the stored lifecycle must be status.type, the non-deprecated category"
    )
    assert row.target_date == date(2026, 9, 1)
    assert row.url.endswith(ASK_DEV_PROJECT_ID)
    # Storing lifecycle must not leak back into resolvability.
    assert row.is_active == 1


@pytest.mark.parametrize("bad_date", [None, "", "not-a-date", "2026-13-45"])
def test_an_unusable_target_date_does_not_drop_the_project(
    monkeypatch: pytest.MonkeyPatch, bad_date: str | None
) -> None:
    _, sink = _populate(monkeypatch, [_project_node(targetDate=bad_date)])

    row = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]
    assert row.target_date is None
    assert row.is_active == 1


def test_team_derived_rows_keep_empty_lifecycle_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The attribution rows describe a TEAM and have no project lifecycle.

    They must read back as empty rather than inheriting a neighbouring project's
    state, which is what would happen if the fields were populated positionally.
    """

    _, sink = _populate(
        monkeypatch,
        [_project_node(status={"id": "s", "name": "Done", "type": "completed"})],
    )

    team_row = sink.projects[("org-1", "linear", "org-1:linear:CHAOS")]
    assert team_row.state == ""
    assert team_row.target_date is None
    assert team_row.url == ""


# --------------------------------------------------------------------------
# Failure isolation and node hygiene
# --------------------------------------------------------------------------


def test_projects_failure_does_not_discard_team_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    summary, sink = _populate(
        monkeypatch, [], projects_error=RuntimeError("linear 500")
    )

    assert summary["native_projects_imported"] == 0
    assert ("org-1", "linear", "org-1:linear:CHAOS") in sink.projects
    assert summary["team_project_ownership_imported"] == 1


def test_projects_failure_is_fatal_under_strict_reference_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match="linear 500"):
        _populate(
            monkeypatch,
            [],
            scope={"mode": "sync_config", "strict_reference_discovery": True},
            projects_error=RuntimeError("linear 500"),
        )


def test_nodes_without_an_id_are_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    summary, sink = _populate(
        monkeypatch, [_project_node(id=""), _project_node(id="   ")]
    )

    assert summary["native_projects_imported"] == 0
    assert not [row for row in sink.projects.values() if row.project_key is None]


def test_unnamed_project_falls_back_to_its_id_as_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink = _populate(monkeypatch, [_project_node(name="")])

    assert (
        sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)].name
        == ASK_DEV_PROJECT_ID
    )


def test_every_native_row_is_versioned_by_observation_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One clock for the ReplacingMergeTree version column.

    Retirement tombstones are stamped when an absence is OBSERVED. If active
    rows were versioned by Linear's own mtime instead, a project that came back
    after a false absence would carry an unchanged, older mtime and lose to its
    own tombstone forever — a transient mistake made permanent. Versioning every
    native row by observation time makes the most recent look at the world win.
    """

    before = datetime.now(timezone.utc)
    _, sink = _populate(
        monkeypatch, [_project_node(updatedAt="2020-01-01T00:00:00.000Z")]
    )
    after = datetime.now(timezone.utc)

    row = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]
    assert before <= row.updated_at <= after, (
        "the row was versioned by Linear's mtime, not by observation time"
    )
    assert row.updated_at == row.last_synced


@pytest.mark.parametrize("bad_value", ["not-a-timestamp", None, ""])
def test_an_unusable_updated_at_does_not_drop_the_project(
    monkeypatch: pytest.MonkeyPatch, bad_value: str | None
) -> None:
    """Linear's mtime no longer drives the version, so a bad one is harmless."""

    _, sink = _populate(monkeypatch, [_project_node(updatedAt=bad_value)])

    row = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]
    assert row.is_active == 1
    assert row.updated_at.tzinfo is not None


def test_a_trashed_project_is_retired_even_though_it_is_not_archived(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Linear trashes a deleted project before purging it.

    ``trashed`` is a different flag from ``archivedAt``, so a predicate reading
    only ``archivedAt`` leaves a deleted project resolvable — the user names
    something that no longer exists and gets an answer about it.
    """

    _, sink = _populate(monkeypatch, [_project_node(trashed=True)])

    assert sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)].is_active == 0


def test_deleting_a_project_between_runs_retires_the_catalog_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = _fake_sink()
    _populate(monkeypatch, [_project_node()], sink=sink)
    live = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]
    assert live.is_active == 1

    _populate(monkeypatch, [_project_node(trashed=True)], sink=sink)
    retired = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]

    assert retired.is_active == 0
    assert retired.updated_at > live.updated_at, (
        "the retiring row must carry the later ReplacingMergeTree version"
    )


def test_the_query_does_not_depend_on_linears_deprecated_state_field() -> None:
    """``Project.state`` is ``@deprecated(reason: Use project.status instead)``.

    Selecting a deprecated field is not a cosmetic problem: when Linear removes
    it the WHOLE GraphQL operation is rejected, so native project refresh dies
    entirely rather than merely losing lifecycle enrichment. A mocked transport
    cannot notice that, so the guard is on the query text itself.
    """

    selection = _projects_node_selection()
    assert "state" not in selection, (
        "PROJECTS_QUERY selects the deprecated Project.state; use status.type"
    )
    status = selection.get("status")
    assert isinstance(status, dict) and "type" in status


# --------------------------------------------------------------------------
# Strict reference discovery must not accept a partial enumeration
# --------------------------------------------------------------------------


def test_strict_reference_discovery_fails_on_an_incomplete_enumeration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under strict discovery a truncated project set is a failed run.

    Non-strict keeps the prefix and reports it as incomplete; strict callers
    asked for a guaranteed-complete reference set, so a stalled cursor has to
    raise rather than quietly hand back part of the workspace.
    """

    from dev_health_ops.providers.linear.client import LinearPaginationIncomplete

    _install_team_discovery(monkeypatch)
    _install_projects_pages(
        monkeypatch,
        [
            _page([_project_node()], cursor="stuck"),
            _page([_project_node(id=OTHER_PROJECT_ID, name="Second")], cursor="stuck"),
        ],
    )

    with pytest.raises(LinearPaginationIncomplete):
        team_autoimport_linear.populate(
            org_id="org-1",
            credentials={"api_key": "lin-key"},
            scope={"mode": "sync_config", "strict_reference_discovery": True},
            sink=_fake_sink(),
        )


# --------------------------------------------------------------------------
# Completeness must never be claimed for a run that fetched nothing
# --------------------------------------------------------------------------


def test_a_client_setup_failure_is_not_reported_as_a_complete_enumeration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ "0 projects, complete" reads as "this workspace has no projects".

    The flag used to default to true and be cleared only by the enumeration
    handler, so any failure BEFORE that -- constructing the client, for
    instance -- fell through the outer handler untouched. Absence-based
    retirement is gated on this flag, so a lying flag would retire the entire
    catalog.
    """

    _install_team_discovery(monkeypatch)
    monkeypatch.setattr(
        team_autoimport_linear,
        "LinearClient",
        _raising_client("cannot construct client"),
    )

    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=_fake_sink(),
    )

    assert summary["native_projects_imported"] == 0
    assert summary["native_projects_complete"] is False


def _raising_client(message: str) -> Any:
    def _factory(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(message)

    return _factory


def test_an_empty_workspace_with_an_empty_catalog_is_not_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    summary, sink = _populate(monkeypatch, [], sink=_fake_sink())

    assert summary["native_projects_imported"] == 0
    assert summary["native_projects_complete"] is True


# --------------------------------------------------------------------------
# A degraded response must never be accepted as an authoritative snapshot
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "page"),
    [
        ("no projects connection", {}),
        ("projects is not an object", {"projects": None}),
        ("no nodes list", {"projects": {"pageInfo": {"hasNextPage": False}}}),
        ("nodes is not a list", {"projects": {"nodes": {}, "pageInfo": {}}}),
        ("no pageInfo", {"projects": {"nodes": []}}),
        ("pageInfo omits hasNextPage", {"projects": {"nodes": [], "pageInfo": {}}}),
    ],
)
def test_a_malformed_page_is_not_clean_exhaustion(
    monkeypatch: pytest.MonkeyPatch, label: str, page: dict[str, Any]
) -> None:
    """An HTTP 200 carrying nonsense must not read as "the workspace is empty".

    Every one of these shapes previously defaulted to zero nodes with no
    hasNextPage, which is indistinguishable from a clean final page. That
    snapshot drives absence-based retirement, so "empty because broken" being
    read as "empty because true" is how a transport glitch retires a catalog.
    """

    _install_team_discovery(monkeypatch)
    _install_projects_pages(monkeypatch, [page])
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=_fake_sink(),
    )

    assert summary["native_projects_complete"] is False, label


def test_a_truncated_terminal_page_is_reported_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dangerous shape: a good first page, then a malformed second one.

    The empty-response guard does NOT cover this — the run returned real
    projects, so it looks healthy. Only refusing to call a malformed page
    "exhaustion" stops the omitted projects being retired.
    """

    sink = _fake_sink()
    _install_team_discovery(monkeypatch)
    _install_projects_pages(monkeypatch, [_page([_project_node()])])
    team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=sink,
    )
    assert sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)].is_active == 1

    # Next run: page 1 returns a DIFFERENT project and claims more follow;
    # page 2 comes back malformed.
    _install_projects_pages(
        monkeypatch,
        [
            _page([_project_node(id=OTHER_PROJECT_ID, name="Other")], cursor="c1"),
            {"projects": {"nodes": None, "pageInfo": None}},
        ],
    )
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=sink,
    )

    assert summary["native_projects_complete"] is False
    assert sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)].is_active == 1, (
        "a project omitted by a TRUNCATED enumeration was retired"
    )


@pytest.mark.parametrize(
    ("label", "page"),
    [
        ("no projects connection", {}),
        ("projects is not an object", {"projects": None}),
        ("projects is a list", {"projects": [1, 2]}),
        ("no nodes list", {"projects": {"pageInfo": {"hasNextPage": False}}}),
        ("nodes is not a list", {"projects": {"nodes": {}, "pageInfo": {}}}),
        ("pageInfo is None", {"projects": {"nodes": [], "pageInfo": None}}),
        ("no pageInfo", {"projects": {"nodes": []}}),
        ("pageInfo omits hasNextPage", {"projects": {"nodes": [], "pageInfo": {}}}),
    ],
)
def test_iter_projects_rejects_a_malformed_page_deliberately(
    monkeypatch: pytest.MonkeyPatch, label: str, page: dict[str, Any]
) -> None:
    """Assert the EXCEPTION TYPE, not merely that something went wrong.

    The worker-level tests only observe the outcome (incomplete, nothing
    retired), which is reached by any exception at all — including a TypeError
    from indexing None. That cannot tell deliberate validation apart from an
    accidental crash, so those tests survive removing individual validation
    clauses. This pins the producer's actual contract.
    """

    from dev_health_ops.providers.linear.client import (
        LinearAuth,
        LinearPaginationIncomplete,
    )

    monkeypatch.setattr(
        LinearClient,
        "_execute",
        lambda self, query, variables=None: page,
    )
    client = LinearClient(auth=LinearAuth(api_key="lin-key"), org_id="org-1")
    try:
        with pytest.raises(LinearPaginationIncomplete):
            list(client.iter_projects(include_archived=True))
    finally:
        client.close()


@pytest.mark.parametrize(
    ("label", "has_next"),
    [
        ("null", None),
        ("zero", 0),
        ("empty string", ""),
        ("string false", "false"),
        ("string true", "true"),
        ("one", 1),
        ("empty list", []),
    ],
)
def test_a_non_boolean_hasNextPage_is_not_clean_exhaustion(
    monkeypatch: pytest.MonkeyPatch, label: str, has_next: Any
) -> None:
    """Presence is not a signal; the VALUE has to be a real boolean.

    Testing a wrong-typed value by truthiness accepts null/0/"" as a clean
    terminal page. A malformed response carrying a genuine prefix would then
    mark enumeration complete and authorise retiring every project it omitted —
    the same defect the structural validation was added to close, one level
    down. The earlier malformed-shape cases all OMITTED the key, so none of them
    could catch this.
    """

    from dev_health_ops.providers.linear.client import (
        LinearAuth,
        LinearPaginationIncomplete,
    )

    page = {
        "projects": {
            "nodes": [_project_node()],
            "pageInfo": {"hasNextPage": has_next, "endCursor": None},
        }
    }
    monkeypatch.setattr(
        LinearClient, "_execute", lambda self, query, variables=None: page
    )
    client = LinearClient(auth=LinearAuth(api_key="lin-key"), org_id="org-1")
    try:
        with pytest.raises(LinearPaginationIncomplete):
            list(client.iter_projects(include_archived=True))
    finally:
        client.close()


def test_a_non_boolean_hasNextPage_marks_the_run_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker-level consequence: a truthy prefix plus a bogus flag."""

    sink = _fake_sink()
    _populate(monkeypatch, [_project_node()], sink=sink)

    _install_team_discovery(monkeypatch)
    _install_projects_pages(
        monkeypatch,
        [
            {
                "projects": {
                    "nodes": [_project_node(id=OTHER_PROJECT_ID, name="Other")],
                    "pageInfo": {"hasNextPage": None, "endCursor": None},
                }
            }
        ],
    )
    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=sink,
    )

    assert summary["native_projects_complete"] is False
    assert sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)].is_active == 1


def test_rows_are_versioned_when_observed_not_when_the_worker_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two overlapping runs must be ordered by what they SAW, not when they began.

    Versioning by worker-start time lets a run that started earlier but observed
    a project later write a stale version, losing to a concurrent run's
    tombstone and keeping a live project retired. Simulated by making
    enumeration take observable time and asserting the row's version postdates
    the point where the run had already started.
    """

    _install_team_discovery(monkeypatch)

    started = datetime.now(timezone.utc)
    page = _page([_project_node()])

    def _slow_execute(
        self: LinearClient, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if query is PROJECTS_QUERY:
            # Stand in for a slow workspace / stalled run.
            time.sleep(0.05)
            return page
        return {}

    monkeypatch.setattr(LinearClient, "_execute", _slow_execute)
    sink = _fake_sink()
    team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=sink,
    )

    row = sink.projects[("org-1", "linear", ASK_DEV_PROJECT_ID)]
    assert row.updated_at > started + timedelta(milliseconds=40), (
        "the row was versioned at worker start, so a concurrent run that "
        "observed the world later could lose to it"
    )
