from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class Team(Base):
    __tablename__ = "teams"

    id: Mapped[str] = mapped_column(
        Text, primary_key=True, comment="Unique team identifier (slug)"
    )
    org_id: Mapped[str] = mapped_column(
        Text, nullable=False, index=True, server_default=""
    )
    team_uuid: Mapped[uuid.UUID | None] = mapped_column(
        GUID, unique=True, default=uuid.uuid4, comment="Internal unique identifier"
    )
    name: Mapped[str] = mapped_column(Text, nullable=False, comment="Team display name")
    description: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Team description"
    )
    members: Mapped[list[str] | None] = mapped_column(
        JSON, default=list, comment="List of member identities"
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __init__(
        self,
        id: str,
        name: str,
        description: str | None = None,
        members: list[str] | None = None,
        updated_at: datetime | None = None,
        team_uuid: uuid.UUID | None = None,
        org_id: str = "",
        repo_patterns: list[str] | None = None,
        manual_members: list[str] | None = None,
    ):
        self.id = id
        self.team_uuid = team_uuid or uuid.uuid4()
        self.name = name
        self.description = description
        self.members = members or []
        self.updated_at = updated_at or datetime.now(timezone.utc)
        self.org_id = org_id
        # NOT a mapped_column: this Postgres-mapped class has no repo_patterns
        # DB column (repo_patterns is a ClickHouse-only concept -- see
        # migration 025_teams_project_repo.sql -- and this model predates and
        # is unrelated to that table's schema). Kept as a plain instance
        # attribute so ClickHouseStore.insert_teams's getattr(item,
        # "repo_patterns", []) picks it up (storage/clickhouse.py) without a
        # Postgres migration; SQLAlchemy ignores unmapped attributes on
        # flush, so this is a no-op for any Postgres-backed store.
        self.repo_patterns = repo_patterns or []
        # NOT a mapped_column either, same reasoning as repo_patterns above --
        # manual_members is the ClickHouse-only admin-override provenance
        # column added by migration 079 (CHAOS-4321). UNLIKE repo_patterns,
        # this attribute is deliberately left UNSET (not defaulted to [])
        # when the caller doesn't pass it -- codex review, CHAOS-4338 round
        # 1, P1: `providers/teams.py`'s real sync path constructs this SAME
        # Team class without manual_members, and `ClickHouseStore.
        # insert_teams` treats `hasattr(item, "manual_members") is False`
        # as "this caller never learned about the column" -> it batches a
        # CURRENT-value lookup and preserves whatever an admin already set,
        # instead of silently overwriting it with []. Defaulting to []
        # unconditionally here would have made every real provider sync
        # clobber an existing admin override on its next write. Fixture
        # callers that want the attribute considered "set" (even to [])
        # pass manual_members=[] explicitly, or (as
        # generate_team_ownership_edges does for the one admin-override
        # team) mutate the attribute directly on an already-constructed
        # Team -- both produce a real hasattr()-visible attribute.
        if manual_members is not None:
            self.manual_members = manual_members


class JiraProjectOpsTeamLink(Base):
    __tablename__ = "jira_project_ops_team_links"

    project_key: Mapped[str] = mapped_column(
        Text, primary_key=True, comment="Jira project key"
    )
    ops_team_id: Mapped[str] = mapped_column(
        Text, primary_key=True, comment="Atlassian Ops team ID"
    )
    project_name: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Jira project name"
    )
    ops_team_name: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Atlassian Ops team name"
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __init__(
        self,
        project_key: str,
        ops_team_id: str,
        project_name: str,
        ops_team_name: str,
        updated_at: datetime | None = None,
    ):
        self.project_key = project_key
        self.ops_team_id = ops_team_id
        self.project_name = project_name
        self.ops_team_name = ops_team_name
        self.updated_at = updated_at or datetime.now(timezone.utc)
