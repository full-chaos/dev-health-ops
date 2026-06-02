"""Shared utilities and instance-attribute declarations for generator mixins.

This module hosts :class:`BaseGeneratorMixin`, which declares the instance
attributes set by :class:`~dev_health_ops.fixtures.generator.SyntheticDataGenerator`'s
``__init__`` and provides the helper methods reused across domain mixins.
"""

from __future__ import annotations

import hashlib
import random
import uuid
from datetime import date, datetime, timezone
from typing import Any

from dev_health_ops.fixtures.demo_identity import demo_team_identity
from dev_health_ops.models.teams import Team


class BaseGeneratorMixin:
    """Shared instance-attribute declarations and helper methods.

    The instance attributes below are populated by
    :meth:`SyntheticDataGenerator.__init__`. Declaring them here lets type
    checkers resolve cross-mixin attribute access without runtime overhead.
    """

    # Instance attributes set by SyntheticDataGenerator.__init__
    repo_name: str
    repo_id: uuid.UUID
    provider: str
    assigned_teams: list[Team] | None
    authors: list[tuple[str, str]]
    unassigned_authors: list[tuple[str, str]]
    repo_authors: list[tuple[str, str]]
    files: list[str]

    def _pick_assigned_team_id(self, key: str | None = None) -> str | None:
        if not self.assigned_teams:
            return None
        if key is None:
            return str(random.choice(self.assigned_teams).id)
        team_index = int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16) % len(
            self.assigned_teams
        )
        return str(self.assigned_teams[team_index].id)

    def _get_service_id(self) -> str:
        service_ids = [
            "api-gateway",
            "auth-service",
            "data-pipeline",
            "web-frontend",
            "worker-queue",
        ]
        service_index = int(
            hashlib.sha256(self.repo_name.encode("utf-8")).hexdigest(), 16
        ) % len(service_ids)
        return service_ids[service_index]

    def _resolve_repo_authors(self) -> list[tuple[str, str]]:
        if self.assigned_teams is None:
            return list(self.authors)
        if self.assigned_teams:
            member_identities = {
                str(member).strip().lower()
                for team in self.assigned_teams
                for member in (team.members or [])
            }
            filtered = [
                (name, email)
                for name, email in self.authors
                if str(email).strip().lower() in member_identities
                or str(name).strip().lower() in member_identities
            ]
            if filtered:
                return filtered
            return list(self.authors)
        return list(self.unassigned_authors)

    def get_team_assignment(self, count: int = 2) -> dict[str, Any]:
        """
        Returns a consistent assignment of authors to teams.
        Output includes 'teams' (List[Team]) and 'member_map' (email -> (id, name)).
        """
        teams = []
        member_map = {}

        # Ensure at least 1 author per team if possible, loop if more teams than authors
        # For simplicity, just chunk authors.
        chunk_size = max(1, len(self.authors) // count)

        for i in range(count):
            start = i * chunk_size
            # Last team gets the rest
            end = (i + 1) * chunk_size if i < count - 1 else len(self.authors)
            team_members = self.authors[start:end]

            # Stable IDs
            # Curated, believable team identities; falls back to the legacy
            # team-{n}/Team {n} scheme once the curated list is exhausted.
            curated_team = demo_team_identity(i)
            if curated_team is not None:
                team_id, team_name = curated_team
            else:
                team_id = f"team-{i + 1}"
                team_name = f"Team {i + 1}"

            teams.append(
                Team(
                    id=team_id,
                    name=team_name,
                    members=[email for _, email in team_members],
                )
            )

            for name, email in team_members:
                member_map[str(email).strip().lower()] = (team_id, team_name)
                member_map[str(name).strip().lower()] = (team_id, team_name)

        return {"teams": teams, "member_map": member_map}

    def _resolve_team(
        self,
        member_map: dict[str, Any] | None,
        author_name: str,
        author_email: str,
    ) -> tuple[str | None, str | None]:
        if not member_map:
            return None, None
        for key in (author_email, author_name):
            if not key:
                continue
            entry = member_map.get(str(key).strip().lower())
            if entry:
                return entry[0], entry[1]
        return None, None

    def _get_member_map(self) -> dict[str, tuple[str, str]]:
        """Return the member→(team_id, team_name) map from team assignments."""
        if self.assigned_teams:
            member_map: dict[str, tuple[str, str]] = {}
            for team in self.assigned_teams:
                for member in team.members or []:
                    member_map[str(member).strip().lower()] = (team.id, team.name)
            return member_map
        return self.get_team_assignment().get("member_map", {})

    def _stable_hash_int(self, *parts: object) -> int:
        payload = "::".join(str(part) for part in parts)
        return int(hashlib.sha256(payload.encode("utf-8")).hexdigest(), 16)

    def _allocate_fallback_team_counts(
        self,
        work_item_count: int,
        weights: list[int],
    ) -> list[int]:
        if work_item_count <= 0 or not weights:
            return []

        team_count = min(work_item_count, len(weights))
        counts = [1] * team_count
        remaining = work_item_count - team_count
        if remaining <= 0:
            return counts

        selected_weights = weights[:team_count]
        total_weight = sum(selected_weights)
        remainders: list[tuple[float, int]] = []
        for idx, weight in enumerate(selected_weights):
            exact = remaining * weight / total_weight
            extra = int(exact)
            counts[idx] += extra
            remainders.append((exact - extra, idx))

        assigned = sum(counts)
        for _, idx in sorted(remainders, key=lambda item: (-item[0], item[1]))[
            : work_item_count - assigned
        ]:
            counts[idx] += 1

        return counts

    def _build_fallback_team_sequence(
        self,
        completed_day: date,
        work_item_count: int,
        teams_to_use: list[tuple[str, str]],
    ) -> list[tuple[str, str]]:
        if work_item_count <= 0 or not teams_to_use:
            return []
        if len(teams_to_use) == 1:
            return [teams_to_use[0]] * work_item_count

        if work_item_count >= 6 and len(teams_to_use) >= 4:
            selected_team_count = 4
            weights = [5, 3, 2, 1]
        elif work_item_count >= 4 and len(teams_to_use) >= 3:
            selected_team_count = 3
            weights = [6, 3, 1]
        else:
            selected_team_count = 2
            weights = [7, 3]

        start = self._stable_hash_int(
            self.repo_name,
            completed_day.isoformat(),
            "team-fallback",
        ) % len(teams_to_use)
        rotated = [
            teams_to_use[(start + i) % len(teams_to_use)]
            for i in range(len(teams_to_use))
        ]
        selected_teams = rotated[:selected_team_count]
        counts = self._allocate_fallback_team_counts(work_item_count, weights)

        sequence: list[tuple[str, str]] = []
        for team, count in zip(selected_teams, counts, strict=False):
            sequence.extend([team] * count)
        return sequence

    def _build_fallback_team_plan(
        self,
        items_to_process: list[
            tuple[str, str, str, str | None, datetime, datetime | None, datetime | None]
        ],
        member_map: dict[str, tuple[str, str]],
        teams_to_use: list[tuple[str, str]],
    ) -> dict[int, tuple[str, str]]:
        plan: dict[int, tuple[str, str]] = {}
        unresolved_by_cell: dict[tuple[str, date], list[int]] = {}

        for idx, item in enumerate(items_to_process):
            _, _, _, assignee, _, started_at, completed_at = item
            if started_at is None or completed_at is None:
                continue
            if (completed_at - started_at).total_seconds() <= 0:
                continue
            if assignee and member_map.get(str(assignee).strip().lower()):
                continue
            unresolved_by_cell.setdefault(
                (self.repo_name, completed_at.date()), []
            ).append(idx)

        for (_, completed_day), indices in unresolved_by_cell.items():
            sequence = self._build_fallback_team_sequence(
                completed_day=completed_day,
                work_item_count=len(indices),
                teams_to_use=teams_to_use,
            )
            ordered_indices = sorted(
                indices,
                key=lambda item_idx: (
                    items_to_process[item_idx][6]
                    or datetime.min.replace(tzinfo=timezone.utc),
                    items_to_process[item_idx][0],
                ),
            )
            for item_idx, team in zip(ordered_indices, sequence, strict=False):
                plan[item_idx] = team

        return plan

    def _default_release_refs(self, days: int) -> list[str]:
        return [f"v1.{i}.0" for i in range(max(1, days // 7))]
