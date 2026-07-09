from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urlparse

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
    create_rate_limit_gate,
)
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers._ratelimit import gate_call
from dev_health_ops.providers.gitlab.ratelimit import maybe_raise_gitlab_rate_limit
from dev_health_ops.providers.usage import UsageRecorder
from dev_health_ops.providers.utils import EnvSpec, read_env_spec

logger = logging.getLogger(__name__)

_DIAGNOSTIC_HEADER_NAMES = (
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "retry-after",
    "x-request-id",
    "x-runtime",
)


def _diagnostic_headers(headers: object) -> dict[str, str]:
    get_items = getattr(headers, "items", None)
    if get_items is None:
        return {}
    lowered = {str(k).lower(): str(v) for k, v in get_items()}
    return {name: lowered[name] for name in _DIAGNOSTIC_HEADER_NAMES if name in lowered}


def _maybe_raise_gitlab_rate_limit(exc: BaseException) -> None:
    """Raise RateLimitException if exc is a GitLab rate-limit error (HTTP 429,
    or 403 carrying rate-limit headers); otherwise return None so callers
    continue their existing handling.

    Delegates the actual 429/header-qualified-403 predicate + delay
    computation to ``providers/gitlab/ratelimit.py`` (CHAOS-2773 CS1), itself
    built on the shared ``providers/_ratelimit.py`` helpers
    (``gitlab_403_is_rate_limited`` / ``gitlab_resolve_retry_after_seconds``,
    #1142) -- so exactly one predicate/delay implementation exists for
    GitLab, not a second copy inline here.
    """
    import gitlab  # python-gitlab; keep lazy to match existing import semantics

    if isinstance(exc, RateLimitException):
        raise exc
    if not isinstance(exc, gitlab.exceptions.GitlabError):
        return None
    status = getattr(exc, "response_code", None)
    headers = getattr(exc, "response_headers", None) or {}

    def _hdr(name: str) -> str | None:
        try:
            return headers.get(name)
        except AttributeError:
            return None

    try:
        maybe_raise_gitlab_rate_limit(
            status=status, headers=headers, request_id=_hdr("X-Request-Id")
        )
    except RateLimitException as rate_limit_exc:
        raise rate_limit_exc from exc
    return None


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
        org_id: str | None = None,
    ) -> None:
        import gitlab  # python-gitlab

        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        host = urlparse(auth.base_url).hostname or "gitlab.com"
        self.gate = gate or create_rate_limit_gate(
            "gitlab",
            org_id=org_id,
            host=host,
            config=RateLimitConfig(initial_backoff_seconds=1.0),
        )
        from dev_health_ops.providers.gitlab.budget import GITLAB_USAGE_RESOLVER

        self._usage = UsageRecorder(resolver=GITLAB_USAGE_RESOLVER)

        self.gl = gitlab.Gitlab(
            auth.base_url,
            private_token=auth.token,
            per_page=self.per_page,
        )

    @classmethod
    def from_env(cls, *, org_id: str | None = None) -> GitLabWorkClient:
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
            ),
            org_id=org_id,
        )

    def get_project(self, project_id_or_path: str) -> Any:
        try:
            with gate_call(self.gate):
                project = self.gl.projects.get(project_id_or_path)
            self._record_last_response_usage("GET /projects/:id")
            return project
        except Exception as exc:
            self._record_exception_usage("GET /projects/:id", exc)
            _maybe_raise_gitlab_rate_limit(exc)
            raise

    def _record_usage_observation(
        self,
        *,
        transport: str,
        operation: str,
        headers: dict[str, str],
        rate_limit: dict[str, Any],
        status: int | None = None,
    ) -> None:
        # Aggregation/keying by route_family lives in the shared recorder
        # (CHAOS-2754); this client only owns the header extraction below.
        self._usage.record(
            transport=transport,
            operation=operation,
            headers=headers,
            rate_limit=rate_limit,
            status=status,
        )

    def _record_rest_usage(
        self,
        operation: str,
        *,
        headers: object | None = None,
        status: int | None = None,
    ) -> None:
        safe_headers = _diagnostic_headers(headers or {})
        rate_limit: dict[str, Any] = {}
        for source, target in {
            "ratelimit-remaining": "remaining",
            "ratelimit-reset": "reset",
            "ratelimit-limit": "limit",
            "retry-after": "retry_after",
        }.items():
            value = safe_headers.get(source)
            if value is not None:
                rate_limit[target] = value
        self._record_usage_observation(
            transport="rest",
            operation=operation,
            headers=safe_headers,
            rate_limit=rate_limit,
            status=status,
        )

    def _record_last_response_usage(self, operation: str) -> None:
        headers = getattr(self.gl, "last_response_headers", None)
        status = getattr(self.gl, "last_response_code", None)
        self._record_rest_usage(
            operation,
            headers=headers,
            status=status if isinstance(status, int) else None,
        )

    def _record_exception_usage(self, operation: str, exc: BaseException) -> None:
        status = getattr(exc, "response_code", None)
        self._record_rest_usage(
            operation,
            headers=getattr(exc, "response_headers", None),
            status=status if isinstance(status, int) else None,
        )

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._usage.drain()

    def _gated_iter(self, iterable: Any) -> Iterator[Any]:
        """Consume a python-gitlab lazy iterator with each page fetch routed
        through the shared rate-limit gate. ``list(iterator=True)`` fetches
        pages lazily as the iterator is consumed, so gating only the list()
        creation leaves later page requests uncoordinated; gating each
        ``next()`` keeps multi-page fetches under the distributed gate.
        """
        iterator = iter(iterable)
        while True:
            try:
                with gate_call(self.gate):
                    try:
                        item = next(iterator)
                    except StopIteration:
                        return
                self._record_last_response_usage("GET iterator page")
            except StopIteration:
                return
            except Exception as exc:
                self._record_exception_usage("GET iterator page", exc)
                _maybe_raise_gitlab_rate_limit(exc)
                raise
            yield item

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
        with gate_call(self.gate):
            issues = project.issues.list(iterator=True, **params)
        count = 0
        for issue in self._gated_iter(issues):
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
        with gate_call(self.gate):
            mrs = project.mergerequests.list(iterator=True, **params)
        count = 0
        for mr in self._gated_iter(mrs):
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
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
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
        with gate_call(self.gate):
            milestones = project.milestones.list(state=state, iterator=True)
        yield from self._gated_iter(milestones)

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
            with gate_call(self.gate):
                milestones = group.milestones.list(state=state, iterator=True)
            yield from self._gated_iter(milestones)
        except Exception as exc:
            _maybe_raise_gitlab_rate_limit(exc)
            logger.debug("Failed to fetch group milestones: %s", exc)
            return

    # ─────────────────────────────────────────────────────────────────────────
    # Epic methods (group-level)
    # ─────────────────────────────────────────────────────────────────────────

    def get_group(self, group_id_or_path: str) -> Any:
        """Get a GitLab group by ID or path."""
        try:
            with gate_call(self.gate):
                return self.gl.groups.get(group_id_or_path)
        except Exception as exc:
            _maybe_raise_gitlab_rate_limit(exc)
            raise

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
            for epic in self._gated_iter(epics):
                yield epic
                count += 1
                if limit is not None and count >= int(limit):
                    return
        except Exception as exc:
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
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
            _maybe_raise_gitlab_rate_limit(exc)
            logger.debug("Failed to fetch epic state events: %s", exc)
            return []
