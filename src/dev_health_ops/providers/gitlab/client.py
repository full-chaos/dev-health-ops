from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)
from dev_health_ops.providers._ratelimit import gate_call
from dev_health_ops.providers.utils import EnvSpec, read_env_spec

logger = logging.getLogger(__name__)


class _ListManager(Protocol):
    def list(self, *args: object, **kwargs: object) -> Iterable[object]: ...


class _GitLabIssueLike(Protocol):
    notes: _ListManager
    resource_label_events: _ListManager
    resource_state_events: _ListManager
    links: _ListManager


class _GitLabMergeRequestLike(Protocol):
    notes: _ListManager
    resource_state_events: _ListManager


class _GitLabEpicLike(Protocol):
    notes: _ListManager
    issues: _ListManager
    resource_state_events: _ListManager


@dataclass(frozen=True)
class GitLabAuth:
    token: str
    base_url: str = "https://gitlab.com"


class GitLabWorkClient:
    """
    Work-tracking oriented GitLab client using python-gitlab.
    """

    def __init__(
        self,
        *,
        auth: GitLabAuth,
        per_page: int = 100,
        gate: RateLimitGate | None = None,
    ) -> None:
        import gitlab  # python-gitlab

        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        self.gate = gate or RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

        self.gl = gitlab.Gitlab(
            auth.base_url,
            private_token=auth.token,
            per_page=self.per_page,
        )

    @classmethod
    def from_env(cls) -> GitLabWorkClient:
        env = read_env_spec(
            EnvSpec(
                required={"token": "GITLAB_TOKEN"},
                optional={"base_url": ("GITLAB_URL", "https://gitlab.com")},
                missing_error="GitLab token required (set GITLAB_TOKEN)",
            )
        )
        return cls(
            auth=GitLabAuth(
                token=str(env["token"]),
                base_url=str(env["base_url"]),
            )
        )

    def get_project(self, project_id_or_path: str) -> Any:
        with gate_call(self.gate):
            return self.gl.projects.get(project_id_or_path)

    def iter_project_issues(
        self,
        *,
        project_id_or_path: str,
        state: str = "all",
        updated_after: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[Any]:
        project = self.get_project(project_id_or_path)
        params: dict[str, Any] = {"state": state}
        if updated_after is not None:
            params["updated_after"] = updated_after.isoformat()
        issues = project.issues.list(iterator=True, **params)
        count = 0
        for issue in issues:
            yield issue
            count += 1
            if limit is not None and count >= int(limit):
                return

    def iter_project_merge_requests(
        self,
        *,
        project_id_or_path: str,
        state: str = "all",
        updated_after: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[Any]:
        """Iterate merge requests for a project."""
        project = self.get_project(project_id_or_path)
        params: dict[str, Any] = {"state": state}
        if updated_after is not None:
            params["updated_after"] = updated_after.isoformat()
        mrs = project.mergerequests.list(iterator=True, **params)
        count = 0
        for mr in mrs:
            yield mr
            count += 1
            if limit is not None and count >= int(limit):
                return

    def get_issue_notes(
        self,
        issue: _GitLabIssueLike,
        *,
        limit: int = 500,
    ) -> list[Any]:
        """Get notes/comments for an issue."""
        try:
            with gate_call(self.gate):
                notes = list(issue.notes.list(per_page=100, iterator=True))[:limit]
            return notes
        except Exception as exc:
            logger.debug("Failed to fetch issue notes: %s", exc)
            return []

    def get_mr_notes(
        self,
        mr: _GitLabMergeRequestLike,
        *,
        limit: int = 500,
    ) -> list[Any]:
        """Get notes/comments for a merge request."""
        try:
            with gate_call(self.gate):
                notes = list(mr.notes.list(per_page=100, iterator=True))[:limit]
            return notes
        except Exception as exc:
            logger.debug("Failed to fetch MR notes: %s", exc)
            return []

    def get_issue_resource_label_events(
        self,
        issue: _GitLabIssueLike,
        *,
        limit: int = 300,
    ) -> list[Any]:
        """Get resource label events for an issue."""
        try:
            with gate_call(self.gate):
                events = list(
                    issue.resource_label_events.list(per_page=100, iterator=True)
                )[:limit]
            return events
        except Exception as exc:
            logger.debug("Failed to fetch issue label events: %s", exc)
            return []

    def get_issue_resource_state_events(
        self,
        issue: _GitLabIssueLike,
        *,
        limit: int = 100,
    ) -> list[Any]:
        """Get resource state events for an issue (open/close/reopen)."""
        try:
            with gate_call(self.gate):
                events = list(
                    issue.resource_state_events.list(per_page=100, iterator=True)
                )[:limit]
            return events
        except Exception as exc:
            logger.debug("Failed to fetch issue state events: %s", exc)
            return []

    def get_mr_resource_state_events(
        self,
        mr: _GitLabMergeRequestLike,
        *,
        limit: int = 100,
    ) -> list[Any]:
        """Get resource state events for a merge request."""
        try:
            with gate_call(self.gate):
                events = list(
                    mr.resource_state_events.list(per_page=100, iterator=True)
                )[:limit]
            return events
        except Exception as exc:
            logger.debug("Failed to fetch MR state events: %s", exc)
            return []

    def get_issue_links(
        self,
        issue: _GitLabIssueLike,
    ) -> list[Any]:
        """Get linked issues for an issue."""
        try:
            with gate_call(self.gate):
                links = list(issue.links.list(per_page=100, iterator=True))
            return links
        except Exception as exc:
            logger.debug("Failed to fetch issue links: %s", exc)
            return []

    def iter_project_milestones(
        self,
        *,
        project_id_or_path: str,
        state: str = "all",
    ) -> Iterable[Any]:
        """Iterate milestones for a project."""
        project = self.get_project(project_id_or_path)
        milestones = project.milestones.list(state=state, iterator=True)
        yield from milestones

    def iter_group_milestones(
        self,
        *,
        group_id_or_path: str,
        state: str = "all",
    ) -> Iterable[Any]:
        """Iterate milestones for a group."""
        try:
            with gate_call(self.gate):
                group = self.gl.groups.get(group_id_or_path)
            milestones = group.milestones.list(state=state, iterator=True)
            yield from milestones
        except Exception as exc:
            logger.debug("Failed to fetch group milestones: %s", exc)
            return

    # ─────────────────────────────────────────────────────────────────────────
    # Epic methods (group-level)
    # ─────────────────────────────────────────────────────────────────────────

    def get_group(self, group_id_or_path: str) -> Any:
        """Get a GitLab group by ID or path."""
        with gate_call(self.gate):
            return self.gl.groups.get(group_id_or_path)

    def iter_group_epics(
        self,
        *,
        group_id_or_path: str,
        state: str = "all",
        updated_after: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[Any]:
        """
        Iterate epics for a group.

        GitLab Epics are group-level resources (not project-level).
        Requires GitLab Premium or Ultimate.

        Args:
            group_id_or_path: Group ID or URL-encoded path
            state: Filter by state ("opened", "closed", "all")
            updated_after: Only return epics updated after this datetime
            limit: Maximum number of epics to return

        Yields:
            GitLab Epic objects
        """
        try:
            group = self.get_group(group_id_or_path)
            params: dict[str, Any] = {"state": state}
            if updated_after is not None:
                params["updated_after"] = updated_after.isoformat()

            with gate_call(self.gate):
                epics = group.epics.list(iterator=True, **params)

            count = 0
            for epic in epics:
                yield epic
                count += 1
                if limit is not None and count >= int(limit):
                    return
        except Exception as exc:
            # Epics require GitLab Premium/Ultimate - gracefully handle
            if "403" in str(exc) or "404" in str(exc):
                logger.debug(
                    "Epics not available for group %s (requires Premium/Ultimate): %s",
                    group_id_or_path,
                    exc,
                )
            else:
                logger.warning("Failed to fetch group epics: %s", exc)
            return

    def get_epic_notes(
        self,
        epic: _GitLabEpicLike,
        *,
        limit: int = 500,
    ) -> list[Any]:
        """Get notes/comments for an epic."""
        try:
            with gate_call(self.gate):
                notes = list(epic.notes.list(per_page=100, iterator=True))[:limit]
            return notes
        except Exception as exc:
            logger.debug("Failed to fetch epic notes: %s", exc)
            return []

    def get_epic_issues(
        self,
        epic: _GitLabEpicLike,
    ) -> list[Any]:
        """
        Get issues linked to an epic.

        Returns list of issue objects that are children of this epic.
        """
        try:
            with gate_call(self.gate):
                issues = list(epic.issues.list(per_page=100, iterator=True))
            return issues
        except Exception as exc:
            logger.debug("Failed to fetch epic issues: %s", exc)
            return []

    def get_epic_resource_state_events(
        self,
        epic: _GitLabEpicLike,
        *,
        limit: int = 100,
    ) -> list[Any]:
        """Get resource state events for an epic (open/close/reopen)."""
        try:
            with gate_call(self.gate):
                events = list(
                    epic.resource_state_events.list(per_page=100, iterator=True)
                )[:limit]
            return events
        except Exception as exc:
            logger.debug("Failed to fetch epic state events: %s", exc)
            return []
