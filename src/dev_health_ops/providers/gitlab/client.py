from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)

logger = logging.getLogger(__name__)


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
        gate: Optional[RateLimitGate] = None,
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
    def from_env(cls) -> "GitLabWorkClient":
        token = os.getenv("GITLAB_TOKEN") or ""
        url = os.getenv("GITLAB_URL") or "https://gitlab.com"
        if not token:
            raise ValueError("GitLab token required (set GITLAB_TOKEN)")
        return cls(auth=GitLabAuth(token=token, base_url=url))

    def get_project(self, project_id_or_path: str) -> Any:
        self.gate.wait_sync()
        try:
            project = self.gl.projects.get(project_id_or_path)
            self.gate.reset()
            return project
        except Exception:
            self.gate.penalize(None)
            raise

    def iter_project_issues(
        self,
        *,
        project_id_or_path: str,
        state: str = "all",
        updated_after: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Iterable[Any]:
        project = self.get_project(project_id_or_path)
        params: Dict[str, Any] = {"state": state}
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
        updated_after: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Iterable[Any]:
        """Iterate merge requests for a project."""
        project = self.get_project(project_id_or_path)
        params: Dict[str, Any] = {"state": state}
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
        issue: Any,
        *,
        limit: int = 500,
    ) -> List[Any]:
        """Get notes/comments for an issue."""
        try:
            self.gate.wait_sync()
            notes = list(issue.notes.list(per_page=100, iterator=True))[:limit]
            self.gate.reset()
            return notes
        except Exception as exc:
            logger.debug("Failed to fetch issue notes: %s", exc)
            self.gate.penalize(None)
            return []

    def get_mr_notes(
        self,
        mr: Any,
        *,
        limit: int = 500,
    ) -> List[Any]:
        """Get notes/comments for a merge request."""
        try:
            self.gate.wait_sync()
            notes = list(mr.notes.list(per_page=100, iterator=True))[:limit]
            self.gate.reset()
            return notes
        except Exception as exc:
            logger.debug("Failed to fetch MR notes: %s", exc)
            self.gate.penalize(None)
            return []

    def get_issue_resource_label_events(
        self,
        issue: Any,
        *,
        limit: int = 300,
    ) -> List[Any]:
        """Get resource label events for an issue."""
        try:
            self.gate.wait_sync()
            events = list(
                issue.resource_label_events.list(per_page=100, iterator=True)
            )[:limit]
            self.gate.reset()
            return events
        except Exception as exc:
            logger.debug("Failed to fetch issue label events: %s", exc)
            self.gate.penalize(None)
            return []

    def get_issue_resource_state_events(
        self,
        issue: Any,
        *,
        limit: int = 100,
    ) -> List[Any]:
        """Get resource state events for an issue (open/close/reopen)."""
        try:
            self.gate.wait_sync()
            events = list(
                issue.resource_state_events.list(per_page=100, iterator=True)
            )[:limit]
            self.gate.reset()
            return events
        except Exception as exc:
            logger.debug("Failed to fetch issue state events: %s", exc)
            self.gate.penalize(None)
            return []

    def get_mr_resource_state_events(
        self,
        mr: Any,
        *,
        limit: int = 100,
    ) -> List[Any]:
        """Get resource state events for a merge request."""
        try:
            self.gate.wait_sync()
            events = list(mr.resource_state_events.list(per_page=100, iterator=True))[
                :limit
            ]
            self.gate.reset()
            return events
        except Exception as exc:
            logger.debug("Failed to fetch MR state events: %s", exc)
            self.gate.penalize(None)
            return []

    def get_issue_links(
        self,
        issue: Any,
    ) -> List[Any]:
        """Get linked issues for an issue."""
        try:
            self.gate.wait_sync()
            links = list(issue.links.list(per_page=100, iterator=True))
            self.gate.reset()
            return links
        except Exception as exc:
            logger.debug("Failed to fetch issue links: %s", exc)
            self.gate.penalize(None)
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
        for ms in milestones:
            yield ms

    def iter_group_milestones(
        self,
        *,
        group_id_or_path: str,
        state: str = "all",
    ) -> Iterable[Any]:
        """Iterate milestones for a group."""
        try:
            self.gate.wait_sync()
            group = self.gl.groups.get(group_id_or_path)
            self.gate.reset()
            milestones = group.milestones.list(state=state, iterator=True)
            for ms in milestones:
                yield ms
        except Exception as exc:
            logger.debug("Failed to fetch group milestones: %s", exc)
            self.gate.penalize(None)
            return

    # ─────────────────────────────────────────────────────────────────────────
    # Epic methods (group-level)
    # ─────────────────────────────────────────────────────────────────────────

    def get_group(self, group_id_or_path: str) -> Any:
        """Get a GitLab group by ID or path."""
        self.gate.wait_sync()
        try:
            group = self.gl.groups.get(group_id_or_path)
            self.gate.reset()
            return group
        except Exception:
            self.gate.penalize(None)
            raise

    def iter_group_epics(
        self,
        *,
        group_id_or_path: str,
        state: str = "all",
        updated_after: Optional[datetime] = None,
        limit: Optional[int] = None,
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
            params: Dict[str, Any] = {"state": state}
            if updated_after is not None:
                params["updated_after"] = updated_after.isoformat()

            self.gate.wait_sync()
            epics = group.epics.list(iterator=True, **params)
            self.gate.reset()

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
            self.gate.penalize(None)
            return

    def get_epic_notes(
        self,
        epic: Any,
        *,
        limit: int = 500,
    ) -> List[Any]:
        """Get notes/comments for an epic."""
        try:
            self.gate.wait_sync()
            notes = list(epic.notes.list(per_page=100, iterator=True))[:limit]
            self.gate.reset()
            return notes
        except Exception as exc:
            logger.debug("Failed to fetch epic notes: %s", exc)
            self.gate.penalize(None)
            return []

    def get_epic_issues(
        self,
        epic: Any,
    ) -> List[Any]:
        """
        Get issues linked to an epic.

        Returns list of issue objects that are children of this epic.
        """
        try:
            self.gate.wait_sync()
            issues = list(epic.issues.list(per_page=100, iterator=True))
            self.gate.reset()
            return issues
        except Exception as exc:
            logger.debug("Failed to fetch epic issues: %s", exc)
            self.gate.penalize(None)
            return []

    def get_epic_resource_state_events(
        self,
        epic: Any,
        *,
        limit: int = 100,
    ) -> List[Any]:
        """Get resource state events for an epic (open/close/reopen)."""
        try:
            self.gate.wait_sync()
            events = list(epic.resource_state_events.list(per_page=100, iterator=True))[
                :limit
            ]
            self.gate.reset()
            return events
        except Exception as exc:
            logger.debug("Failed to fetch epic state events: %s", exc)
            self.gate.penalize(None)
            return []
