"""CHAOS-3219: synthetic ``projects`` catalog rows for the ask-dev-world.

The ``projects`` ClickHouse table (``migrations/clickhouse/051_team_attribution_
dimensions.sql``) backs ``EntityKind.PROJECT`` in the Ask Dev scope catalog
(``api/dev/scope_catalog.py``) and is one of the two ``ALIAS_AWARE_ENTITY_KINDS``
CHAOS-3388's acronym/parenthetical-alias resolution reads from. No existing
fixture generator writes to it -- ``dev-hops fixtures generate`` has never
needed a PROJECT-kind subject distinct from a REPOSITORY one. The ask-dev-world
needs it for two registry-required realizations: the acronym-alias subject
class, and the deleted subject class (a project row whose latest
ReplacingMergeTree version carries ``is_active = 0``).

Kept intentionally small and standalone (no dependency on
:class:`~dev_health_ops.fixtures.generator.SyntheticDataGenerator`) so it is
usable independently of a full repo generation pass.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

#: Same fixture namespace every other generator in this package derives
#: deterministic ids from (``generator.py``, ``demo_identity.py``'s callers,
#: ``runner.py``'s ``_default_org``). A project's own catalog id is pinned to
#: its ``repo_full_name`` (provider ``synthetic``) so it satisfies arm 1 of
#: ``_project_identity.project_identity_match`` -- ``work_items.project_id ==
#: projects.id`` -- against the exact ``project_id`` value
#: ``fixtures/generators/work_items.py`` already stamps onto every generated
#: work item for that repo (``project_id=proj`` where ``proj`` is the repo's
#: full name). No new work-item-side wiring is required.
FIXTURE_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

#: The provider label every ask-dev-world project row and every synthetic
#: work item share -- ``_project_identity_match``'s provider guard requires
#: the two sides to agree, and ``SyntheticDataGenerator``'s default
#: ``provider`` is already ``"synthetic"``.
SYNTHETIC_PROVIDER = "synthetic"


@dataclass(frozen=True, slots=True)
class ProjectRecord:
    """One row of the ``projects`` table (id, org_id, provider, project_key,
    name, is_active, updated_at, last_synced)."""

    id: str
    org_id: str
    name: str
    provider: str = SYNTHETIC_PROVIDER
    project_key: str | None = None
    is_active: bool = True
    updated_at: datetime | None = None
    last_synced: datetime | None = None


def project_id_for_repo(repo_full_name: str) -> str:
    """The ``projects.id`` value for a repo-backed project (== repo full name).

    Deliberately the raw repo full name, not a derived UUID: arm 1 of
    ``_project_identity.project_identity_match`` compares
    ``work_items.project_id`` directly against ``projects.id`` with no
    UUID parsing involved, and ``fixtures/generators/work_items.py`` already
    writes the repo's full name onto ``project_id`` verbatim.
    """

    return repo_full_name


def build_project_record(
    *,
    org_id: str,
    repo_full_name: str,
    display_name: str | None = None,
    is_active: bool = True,
    as_of: datetime,
) -> ProjectRecord:
    """One repo-backed PROJECT catalog row, active as of ``as_of``.

    ``as_of`` must be derived from the world's pinned ``now`` by the caller
    (``fixtures/world.py``) -- never ``datetime.now()`` here, per the
    CHAOS-3392 no-wall-clock rule for new fixture-world code.
    """

    return ProjectRecord(
        id=project_id_for_repo(repo_full_name),
        org_id=org_id,
        name=display_name or repo_full_name,
        provider=SYNTHETIC_PROVIDER,
        project_key=None,
        is_active=is_active,
        updated_at=as_of,
        last_synced=as_of,
    )


def build_retired_project_version(
    active_record: ProjectRecord, *, retired_as_of: datetime
) -> ProjectRecord:
    """A superseding, ``is_active=0`` version of ``active_record``.

    ``projects`` is a ``ReplacingMergeTree(updated_at)`` keyed on
    ``(org_id, provider, id)`` -- inserting a second row with the same key
    and a LATER ``updated_at`` is how a project is "deleted"/archived without
    ever issuing a DELETE. Realizes ``subjects.json``'s ``deleted`` class.
    """

    if retired_as_of <= (active_record.updated_at or retired_as_of):
        raise ValueError(
            "build_retired_project_version: retired_as_of must be strictly "
            "after the active record's updated_at, or ReplacingMergeTree "
            "will not deterministically prefer the retired version"
        )
    return ProjectRecord(
        id=active_record.id,
        org_id=active_record.org_id,
        name=active_record.name,
        provider=active_record.provider,
        project_key=active_record.project_key,
        is_active=False,
        updated_at=retired_as_of,
        last_synced=retired_as_of,
    )


_PROJECT_COLUMNS = (
    "id",
    "org_id",
    "provider",
    "project_key",
    "name",
    "is_active",
    "updated_at",
    "last_synced",
)


def _row_for_insert(record: ProjectRecord) -> list[Any]:
    updated_at = record.updated_at or datetime.now(timezone.utc)
    last_synced = record.last_synced or updated_at
    return [
        record.id,
        record.org_id,
        record.provider,
        record.project_key,
        record.name,
        1 if record.is_active else 0,
        updated_at,
        last_synced,
    ]


async def insert_projects(client: Any, records: list[ProjectRecord]) -> None:
    """Insert ``projects`` rows via the raw clickhouse-connect client.

    Deliberately bypasses ``ClickHouseStore`` (out of Lane 1a's file
    territory -- ``src/dev_health_ops/storage/**`` belongs to other lanes) and
    talks to ``client.insert`` directly, the same underlying call
    ``ClickHouseStore._insert_rows`` makes.

    PR #1602 review F2 (CONFIRMED): also mirrors every row into the additive
    ``project_declared_state_history`` table (migration 074) -- CHAOS-3563's
    ``_PROJECT_DECLARED_FACTS_SQL`` reads that table exclusively now, so a
    ``projects``-only fixture write left a fixture project's ``is_active``
    toggle (e.g. ``build_retired_project_version``'s ``deleted`` subject
    class -- an ``is_active = 0`` version superseding an active one)
    invisible to it, even though the fixture corpus depends on that toggle
    being readable. ``projects`` write first (PR #1602 review F5's
    ordering, applied here too): a missing history table (schema not yet
    migrated) must not abort the more fundamental catalog write.
    """

    if not records:
        return
    matrix = [_row_for_insert(record) for record in records]
    await asyncio.to_thread(
        client.insert,
        "projects",
        matrix,
        column_names=list(_PROJECT_COLUMNS),
    )
    await asyncio.to_thread(
        client.insert,
        "project_declared_state_history",
        matrix,
        column_names=list(_PROJECT_COLUMNS),
    )
