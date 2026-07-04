"""Canonical GitLab "security" code-dataset client (CHAOS-2773 CS10).

This is the GitLab-wave PATHFINDER: the first canonical code client built
directly on the shared ``providers/_http.py::InstrumentedRESTCore`` (CHAOS-2773
CS1). It is the ``providers/gitlab/`` migration target for the "security"
code dataset (vulnerability findings + dependency-scan alerts) that
historically lived on the FROZEN ``connectors/gitlab.py``
(``GitLabConnector.get_security_alerts``), which resolved the project via
python-gitlab and then rode the un-instrumented
``connectors/utils/rest.py::GitLabRESTClient`` (``get_vulnerability_findings``
/ ``get_dependencies``). AGENTS.md bans new code under ``connectors/``, and
actuals instrumentation (CHAOS-2754) requires a client that owns a
``UsageRecorder`` -- so this module ports the fetch/error-handling/field-
mapping logic and adds the recorder the frozen connector could never carry.
Later GitLab-wave changesets (CS11 pipelines+deployments, CS12 tests, CS13
commits+stats, ...) copy this shape.

Behavior parity with ``GitLabConnector.get_security_alerts``
(``connectors/gitlab.py`` ~1268-1379, riding ``connectors/utils/rest.py``'s
``get_vulnerability_findings`` / ``get_dependencies`` ~594-648):

* Resolves the project the same way the connector does -- one ``GET
  /projects/{id_or_path}`` first, using the RETURNED numeric ``id`` for the
  two endpoint calls below (the connector does this via
  ``self.gitlab.projects.get(project_identifier).id``). A failure here
  (401/403/404/429/5xx) always propagates -- never suppressed, matching the
  connector's own project-resolution failure semantics.
* ``GET /projects/{id}/vulnerability_findings`` and ``GET /projects/{id}/
  dependencies`` -- SINGLE PAGE each. This is a pre-existing connector quirk
  (neither ``rest.py`` method loops on ``page``/``X-Next-Page``), preserved
  here for strict parity (CS10 is scoped alongside the parallel CS3 GitHub
  pathfinder; default is parity, not a behavior change).
* A PLAIN 403 or a 404 on either optional endpoint degrades to an empty
  result for that endpoint (best-effort, matching the connector's
  ``"Forbidden:"`` / ``"Not found:"`` string-matched suppression) -- but a
  HEADER-QUALIFIED 403 (some self-managed instances front a throttle with
  403 instead of 429) or a 429 raises the canonical ``RateLimitException``
  instead of being swallowed, via the shared
  ``providers/gitlab/ratelimit.py`` classifier (the SAME one
  ``GitLabWorkClient`` / ``GitLabFeatureFlagsClient`` use -- no second
  403-qualification implementation for GitLab).
* Field mapping into ``SecurityAlertData`` (``connectors/models.py``,
  UNCHANGED) is reproduced field-for-field, including the dependency alert's
  ``created_at=datetime.now(timezone.utc)`` placeholder -- the dependency
  scan API carries no per-vulnerability timestamp; that is the connector's
  own pre-existing choice, not something this migration invents.

Every operation this client issues carries an explicit family prefix
(CHAOS-2773 CS1's ``OperationResolver`` prefix short-circuit,
``providers/usage.py``) so usage resolution is deterministic by
construction rather than by marker-substring tuning -- see
``providers/gitlab/budget.py``'s ``security``, ``pipelines``,
``deployments``, ``tests``, and ``project`` route families.
"""

from __future__ import annotations

import logging
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatch
from typing import Any

import httpx

from dev_health_ops.connectors.models import Repository, SecurityAlertData
from dev_health_ops.exceptions import (
    APIException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.providers._http import (
    GITLAB_DIAGNOSTIC_HEADER_NAMES,
    InstrumentedRESTCore,
    gitlab_rest_base_url,
)
from dev_health_ops.providers.gitlab.ratelimit import (
    build_gitlab_rate_limit_exception,
    classify_gitlab_status,
)

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://gitlab.com"
_DEFAULT_TIMEOUT_SECONDS = 15.0
_DEFAULT_MAX_RETRIES = 5

# GitLab's REST rate-limit reset header (RateLimit-Reset, not the
# X-RateLimit-Reset default InstrumentedRESTCore assumes) -- mirrors
# providers/gitlab/feature_flags.py / providers/gitlab/client.py.
_RESET_HEADER_NAME = "RateLimit-Reset"

# Explicit family prefix every operation this client issues is labeled with
# (see the module docstring's "Every operation" paragraph).
_SECURITY_FAMILY_PREFIX = "security"
_PROJECT_FAMILY_PREFIX = "project"
_PIPELINES_FAMILY_PREFIX = "pipelines"
_DEPLOYMENTS_FAMILY_PREFIX = "deployments"
_TESTS_FAMILY_PREFIX = "tests"
_MERGE_REQUESTS_FAMILY_PREFIX = "merge_requests"
_NOTES_FAMILY_PREFIX = "notes"


@dataclass(frozen=True)
class GitLabPipelineData:
    pipeline_id: str
    status: str | None
    created_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class GitLabDeploymentData:
    deployment_id: str
    deployment_iid: Any
    status: str | None
    environment: str | None
    created_at: datetime | None
    finished_at: datetime | None
    sha: str | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class GitLabCommitData:
    commit_id: str
    message: str | None
    author_name: str | None
    authored_date: datetime | None
    committer_name: str | None
    committed_date: datetime | None
    parent_ids: tuple[str, ...]


@dataclass(frozen=True)
class GitLabCommitStatsData:
    commit_id: str
    additions: int
    deletions: int


@dataclass(frozen=True, slots=True)
class GitLabBlameRange:
    starting_line: int
    ending_line: int
    commit_sha: str
    author: str
    author_email: str
    age_seconds: int
    lines: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GitLabFileBlame:
    file_path: str
    ranges: tuple[GitLabBlameRange, ...] = ()


@dataclass(frozen=True)
class GitLabProjectData:
    id: int
    name: str
    path_with_namespace: str
    web_url: str | None
    default_branch: str
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    stars: int = 0
    forks: int = 0


class _GitLabSecurityForbidden(APIException):
    """Marker for a PLAIN (non-rate-limited) GitLab 403 on an optional
    security endpoint.

    The shared core's DEFAULT 403 classification raises the same
    ``AuthenticationException`` a genuine 401 raises, which would make a
    plain-403 and a real auth failure indistinguishable to a caller trying
    to suppress-and-degrade on ONLY the former (matching the connector's
    best-effort behavior). This subclass of the canonical ``APIException``
    exists purely so :meth:`GitLabCodeClient._get_vulnerability_findings` /
    :meth:`GitLabCodeClient._get_dependency_alerts` can catch it specifically
    -- it is never raised for 401, and never escapes project resolution
    without also being catchable the same way callers already catch
    ``APIException``.
    """


def _is_retryable_status(response: httpx.Response) -> bool:
    """429/5xx retry unconditionally; a 403 retries ONLY when header-
    qualified (mirrors ``providers/gitlab/feature_flags.py``'s retry
    predicate -- some self-managed instances front a throttle with 403
    instead of 429)."""
    if response.status_code in {429, 500, 502, 503, 504}:
        return True
    if response.status_code == 403:
        return classify_gitlab_status(
            status=403, headers=response.headers
        ).is_rate_limit
    return False


def _resolve_retry_after(response: httpx.Response) -> float | None:
    """Adapts the shared core's response-based ``resolve_retry_after`` hook
    to the header-based GitLab classifier every other GitLab client call
    site uses directly."""
    from dev_health_ops.providers._ratelimit import gitlab_resolve_retry_after_seconds

    return gitlab_resolve_retry_after_seconds(response.headers)


def _classify_error(response: httpx.Response, operation: str) -> None:
    """GitLab 403 triage for the shared core's ``classify_error`` hook.

    A header-qualified 403 raises the canonical ``RateLimitException`` via
    ``providers/gitlab/ratelimit.py`` (the SAME classifier
    ``GitLabWorkClient`` / ``GitLabFeatureFlagsClient`` use). A PLAIN 403
    raises :class:`_GitLabSecurityForbidden` instead of the core's default
    ``AuthenticationException`` -- 401 and a plain 403 must stay
    distinguishable so the optional-endpoint fetchers below can suppress
    ONLY the latter, never a genuine auth failure. Every other status
    (401/404/429/5xx) falls through unchanged to the core's default
    classification.
    """
    if response.status_code != 403:
        return
    classification = classify_gitlab_status(status=403, headers=response.headers)
    if classification.is_rate_limit:
        raise build_gitlab_rate_limit_exception(
            status=403,
            headers=response.headers,
            classification=classification,
        )
    raise _GitLabSecurityForbidden(f"gitlab forbidden on {operation}: {response.text}")


def _map_vulnerability_finding(item: dict[str, Any]) -> SecurityAlertData:
    """Mirrors ``GitLabConnector.get_security_alerts``'s vulnerability-finding
    mapping (``connectors/gitlab.py`` ~1287-1322) field-for-field."""
    created_at: datetime | None = None
    created_at_value = item.get("created_at")
    if created_at_value:
        try:
            created_at = datetime.fromisoformat(created_at_value.replace("Z", "+00:00"))
        except Exception:
            logger.debug(
                "Failed to parse vulnerability finding created_at: %s",
                created_at_value,
            )

    cve_id: str | None = None
    for identifier in item.get("identifiers", []) or []:
        if identifier.get("type") == "cve":
            cve_id = identifier.get("name")
            break

    return SecurityAlertData(
        alert_id=f"gitlab_vuln:{item['id']}",
        source="gitlab_vulnerability",
        severity=item.get("severity"),
        state=item.get("state"),
        package_name=None,
        cve_id=cve_id,
        url=(item.get("links", {}) or {}).get("url"),
        title=item.get("name"),
        description=None,
        created_at=created_at,
        fixed_at=None,
        dismissed_at=None,
    )


def _map_dependency_alert(
    dep: dict[str, Any], vuln: dict[str, Any]
) -> SecurityAlertData:
    """Mirrors ``GitLabConnector.get_security_alerts``'s dependency-alert
    mapping (``connectors/gitlab.py`` ~1342-1358) field-for-field, including
    the ``created_at=datetime.now(timezone.utc)`` placeholder (the
    dependency scan API carries no per-vulnerability timestamp)."""
    return SecurityAlertData(
        alert_id=f"gitlab_dep:{vuln['id']}",
        source="gitlab_dependency",
        severity=vuln.get("severity"),
        state=None,
        package_name=dep.get("name"),
        cve_id=None,
        url=vuln.get("url"),
        title=vuln.get("name"),
        description=None,
        created_at=datetime.now(timezone.utc),
        fixed_at=None,
        dismissed_at=None,
    )


def _parse_gitlab_blame_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.debug("Failed to parse GitLab blame authored date: %s", value)
        return None


def _map_file_blame(file_path: str, items: list[Any]) -> GitLabFileBlame:
    now = datetime.now(timezone.utc)
    line_no = 1
    ranges: list[GitLabBlameRange] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_lines = item.get("lines") or []
        lines = [line for line in raw_lines if isinstance(line, str)]
        if not lines:
            continue
        commit = item.get("commit") or {}
        if not isinstance(commit, dict):
            commit = {}
        authored_at = _parse_gitlab_blame_datetime(commit.get("authored_date"))
        age_seconds = int((now - authored_at).total_seconds()) if authored_at else 0
        ending_line = line_no + len(lines) - 1
        ranges.append(
            GitLabBlameRange(
                starting_line=line_no,
                ending_line=ending_line,
                commit_sha=str(commit.get("id") or ""),
                author=str(commit.get("author_name") or "Unknown"),
                author_email=str(commit.get("author_email") or ""),
                age_seconds=age_seconds,
                lines=tuple(lines),
            )
        )
        line_no = ending_line + 1
    return GitLabFileBlame(file_path=file_path, ranges=tuple(ranges))


def _parse_gitlab_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.debug("Failed to parse GitLab datetime: %s", value)
        return None


def _map_pipeline(item: dict[str, Any]) -> GitLabPipelineData:
    created_at = _parse_gitlab_datetime(item.get("created_at"))
    return GitLabPipelineData(
        pipeline_id=str(item.get("id", "")),
        status=item.get("status"),
        created_at=created_at,
        started_at=_parse_gitlab_datetime(item.get("started_at")) or created_at,
        finished_at=_parse_gitlab_datetime(item.get("finished_at")),
    )


def _map_deployment(item: dict[str, Any]) -> GitLabDeploymentData:
    environment = item.get("environment")
    environment_name = (
        environment.get("name") if isinstance(environment, dict) else None
    )
    sha = item.get("sha")
    return GitLabDeploymentData(
        deployment_id=str(item.get("id", "")),
        deployment_iid=item.get("iid"),
        status=item.get("status"),
        environment=environment_name,
        created_at=_parse_gitlab_datetime(item.get("created_at")),
        finished_at=_parse_gitlab_datetime(item.get("finished_at")),
        sha=str(sha) if sha is not None else None,
        raw_payload=dict(item),
    )


def _map_commit(item: dict[str, Any]) -> GitLabCommitData:
    parent_ids = item.get("parent_ids")
    return GitLabCommitData(
        commit_id=str(item.get("id") or item.get("short_id") or ""),
        message=item.get("message"),
        author_name=item.get("author_name"),
        authored_date=_parse_gitlab_datetime(item.get("authored_date")),
        committer_name=item.get("committer_name"),
        committed_date=_parse_gitlab_datetime(item.get("committed_date")),
        parent_ids=tuple(str(parent) for parent in parent_ids or []),
    )


def _coerce_int(value: object) -> int:
    if not isinstance(value, str | int | float):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _map_commit_stats(commit_id: str, item: dict[str, Any]) -> GitLabCommitStatsData:
    stats = item.get("stats")
    if not isinstance(stats, dict):
        stats = {}
    return GitLabCommitStatsData(
        commit_id=commit_id,
        additions=_coerce_int(stats.get("additions")),
        deletions=_coerce_int(stats.get("deletions")),
    )


def _map_project(item: dict[str, Any]) -> GitLabProjectData:
    project_id = _coerce_int(item.get("id"))
    name = str(item.get("name") or project_id)
    full_name = str(item.get("path_with_namespace") or item.get("path") or name)
    return GitLabProjectData(
        id=project_id,
        name=name,
        path_with_namespace=full_name,
        web_url=item.get("web_url"),
        default_branch=str(item.get("default_branch") or "main"),
        description=item.get("description"),
        created_at=_parse_gitlab_datetime(item.get("created_at")),
        updated_at=_parse_gitlab_datetime(item.get("last_activity_at")),
        stars=_coerce_int(item.get("star_count")),
        forks=_coerce_int(item.get("forks_count")),
    )


def _project_to_repository(project: GitLabProjectData) -> Repository:
    return Repository(
        id=project.id,
        name=project.name,
        full_name=project.path_with_namespace,
        default_branch=project.default_branch,
        description=project.description,
        url=project.web_url,
        created_at=project.created_at,
        updated_at=project.updated_at,
        language=None,
        stars=project.stars,
        forks=project.forks,
    )


def _format_gitlab_window_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _encode_project_id(project_id: int | str) -> str:
    """Percent-encode a project id/path for use as ONE URL path segment.

    Mirrors ``_resolve_project_id``'s own encoding below (and
    ``feature_flags.py``'s ``encoded_project``): digits are unreserved per
    RFC 3986 so a plain numeric id round-trips unchanged, while a
    namespaced path (``group/project``) is escaped exactly as GitLab's own
    docs require for the ``/projects/{id_or_path}`` family (GitLab decodes
    the WHOLE path segment server-side). ``safe=""`` also forces '/' itself
    to be escaped -- the caller-supplied ``project_id`` is not trusted to be
    a bare numeric id, so a value smuggling extra path segments (``../``) or
    query metacharacters (``?``) collapses to inert data in a single
    segment instead of being interpreted as additional URL structure.
    """
    return urllib.parse.quote(str(project_id), safe="")


class GitLabCodeClient:
    """Canonical GitLab code-dataset client (CHAOS-2773 CS10 pathfinder).

    Covers migrated GitLab code datasets (security, pipelines/deployments,
    tests, commits + aggregate commit stats). Built on the shared ``InstrumentedRESTCore`` so
    every physical HTTP hop is recorded through ONE owned ``UsageRecorder``,
    mirroring ``GitLabWorkClient`` / ``GitLabFeatureFlagsClient``'s
    rate-limit conventions.

    :param private_token: GitLab personal/project access token.
    :param base_url: GitLab instance URL (self-hosted support).
    :param timeout: Request timeout in seconds.
    :param max_retries: Maximum retry attempts on 429 / header-qualified 403
        / 5xx errors.
    :param transport: Optional httpx transport override (tests only).
    """

    def __init__(
        self,
        *,
        private_token: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: float = _DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        # Deferred import mirrors providers/gitlab/feature_flags.py: budget.py
        # is the source of truth for the route-family vocabulary, imported
        # lazily to avoid a module-load-order cycle.
        from dev_health_ops.providers.gitlab.budget import GITLAB_USAGE_RESOLVER

        self._core = InstrumentedRESTCore(
            base_url=gitlab_rest_base_url(base_url),
            provider="gitlab",
            resolver=GITLAB_USAGE_RESOLVER,
            headers={"PRIVATE-TOKEN": private_token},
            timeout=timeout,
            max_retries=max_retries,
            reset_header_name=_RESET_HEADER_NAME,
            diagnostic_header_names=GITLAB_DIAGNOSTIC_HEADER_NAMES,
            is_retryable_status=_is_retryable_status,
            resolve_retry_after=_resolve_retry_after,
            classify_error=_classify_error,
            transport=transport,
        )
        self._graphql_url = f"{base_url.rstrip('/')}/api/graphql"

    async def get_project(self, project_id: int | str) -> GitLabProjectData:
        encoded = _encode_project_id(project_id)
        response = await self._core.request(
            "GET",
            f"/projects/{encoded}",
            operation=f"{_PROJECT_FAMILY_PREFIX}:GET /projects/{{id}}",
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise APIException(f"Unexpected GitLab project response: {type(payload)!r}")
        return _map_project(payload)

    async def list_projects(
        self,
        *,
        group_name: str | int | None = None,
        search: str | None = None,
        pattern: str | None = None,
        membership: bool = False,
        max_projects: int | None = None,
        per_page: int = 100,
    ) -> list[Repository]:
        params: dict[str, Any] = {}
        if search:
            params["search"] = search
        if membership:
            params["membership"] = True
        if group_name is None:
            path = "/projects"
            operation = f"{_PROJECT_FAMILY_PREFIX}:GET /projects"
        else:
            encoded_group = _encode_project_id(group_name)
            path = f"/groups/{encoded_group}/projects"
            operation = f"{_PROJECT_FAMILY_PREFIX}:GET /groups/{{id}}/projects"

        max_items = (
            1_000_000
            if pattern and max_projects is not None
            else max_projects or 1_000_000
        )
        raw_projects = await self._get_gitlab_list(
            path,
            operation=operation,
            params=params,
            per_page=per_page,
            paginate=True,
            max_items=max_items,
        )
        repositories: list[Repository] = []
        lowered_pattern = pattern.lower() if pattern else None
        for raw_project in raw_projects:
            project = _map_project(raw_project)
            if lowered_pattern and not fnmatch(
                project.path_with_namespace.lower(), lowered_pattern
            ):
                continue
            repositories.append(_project_to_repository(project))
            if max_projects is not None and len(repositories) >= max_projects:
                break
        return repositories

    async def list_repository_tree(
        self,
        project_id: int | str,
        *,
        ref: str,
        per_page: int = 100,
        max_items: int = 1_000_000,
    ) -> list[dict[str, Any]]:
        encoded = _encode_project_id(project_id)
        return await self._get_gitlab_list(
            f"/projects/{encoded}/repository/tree",
            operation=f"{_PROJECT_FAMILY_PREFIX}:GET /projects/{{id}}/repository/tree",
            params={"ref": ref, "recursive": True},
            per_page=per_page,
            paginate=True,
            max_items=max_items,
        )

    async def _resolve_project_id(self, project_id_or_path: int | str) -> int:
        """Resolve a project id/path to its numeric id, mirroring the
        connector's ``self.gitlab.projects.get(project_identifier).id``. Any
        error here (401/403/404/429/5xx) propagates -- never suppressed."""
        encoded = urllib.parse.quote(str(project_id_or_path), safe="")
        response = await self._core.request(
            "GET",
            f"/projects/{encoded}",
            operation=f"{_SECURITY_FAMILY_PREFIX}:GET /projects/{{id}}",
        )
        payload = response.json()
        return int(payload["id"])

    async def get_security_alerts(
        self,
        project_id: int | str,
        *,
        max_alerts: int | None = None,
        per_page: int = 100,
    ) -> list[SecurityAlertData]:
        """Fetch vulnerability findings + dependency-scan alerts for a
        project.

        Mirrors ``GitLabConnector.get_security_alerts`` field-for-field. Both
        underlying endpoints are best-effort: a plain 403/404 degrades to no
        rows for that endpoint (logged at debug) instead of failing the
        whole fetch; a rate limit (429, or a header-qualified 403)
        PROPAGATES as the canonical ``RateLimitException`` (never
        swallowed).
        """
        actual_project_id = await self._resolve_project_id(project_id)
        alerts: list[SecurityAlertData] = list(
            await self._get_vulnerability_findings(actual_project_id, per_page=per_page)
        )
        alerts.extend(
            await self._get_dependency_alerts(actual_project_id, per_page=per_page)
        )
        if max_alerts is not None:
            return alerts[:max_alerts]
        return alerts

    async def _get_vulnerability_findings(
        self, project_id: int, *, per_page: int
    ) -> list[SecurityAlertData]:
        try:
            response = await self._core.request(
                "GET",
                f"/projects/{project_id}/vulnerability_findings",
                operation=(
                    f"{_SECURITY_FAMILY_PREFIX}:GET "
                    "/projects/{id}/vulnerability_findings"
                ),
                params={"per_page": per_page},
            )
        except (NotFoundException, _GitLabSecurityForbidden) as exc:
            logger.warning(
                "GitLab security endpoint unavailable provider=gitlab project_id=%s "
                "endpoint=vulnerability_findings error=%s",
                project_id,
                exc,
            )
            return []
        payload = response.json()
        if not isinstance(payload, list):
            raise APIException(
                f"Unexpected GitLab vulnerability findings response: {type(payload)!r}"
            )
        return [_map_vulnerability_finding(item) for item in payload]

    async def _get_dependency_alerts(
        self, project_id: int, *, per_page: int
    ) -> list[SecurityAlertData]:
        try:
            response = await self._core.request(
                "GET",
                f"/projects/{project_id}/dependencies",
                operation=f"{_SECURITY_FAMILY_PREFIX}:GET /projects/{{id}}/dependencies",
                params={"per_page": per_page},
            )
        except (NotFoundException, _GitLabSecurityForbidden) as exc:
            logger.warning(
                "GitLab security endpoint unavailable provider=gitlab project_id=%s "
                "endpoint=dependencies error=%s",
                project_id,
                exc,
            )
            return []
        payload = response.json()
        if not isinstance(payload, list):
            raise APIException(
                f"Unexpected GitLab dependencies response: {type(payload)!r}"
            )
        alerts: list[SecurityAlertData] = []
        for dep in payload:
            if not isinstance(dep, dict):
                continue
            for vuln in dep.get("vulnerabilities") or []:
                alerts.append(_map_dependency_alert(dep, vuln))
        return alerts

    async def get_pipelines(
        self,
        project_id: int | str,
        *,
        max_pipelines: int,
        per_page: int = 100,
    ) -> list[GitLabPipelineData]:
        if max_pipelines <= 0:
            return []
        params: dict[str, Any] = {"order_by": "updated_at", "sort": "desc"}
        path = f"/projects/{project_id}/pipelines"
        operation = f"{_PIPELINES_FAMILY_PREFIX}:GET /projects/{{id}}/pipelines"
        raw_items = await self._get_gitlab_list(
            path,
            operation=operation,
            params=params,
            per_page=per_page,
            paginate=max_pipelines > per_page,
            max_items=max_pipelines,
        )
        return [_map_pipeline(item) for item in raw_items[:max_pipelines]]

    async def get_deployment_releases(
        self,
        project_id: int | str,
        *,
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        return await self._get_gitlab_list(
            f"/projects/{project_id}/releases",
            operation=f"{_DEPLOYMENTS_FAMILY_PREFIX}:GET /projects/{{id}}/releases",
            params={},
            per_page=per_page,
            paginate=False,
            max_items=per_page,
        )

    async def get_deployments(
        self,
        project_id: int | str,
        *,
        max_deployments: int,
        per_page: int | None = None,
    ) -> list[GitLabDeploymentData]:
        if max_deployments <= 0:
            return []
        effective_per_page = per_page or min(max_deployments, 100)
        raw_items = await self._get_gitlab_list(
            f"/projects/{project_id}/deployments",
            operation=f"{_DEPLOYMENTS_FAMILY_PREFIX}:GET /projects/{{id}}/deployments",
            params={"order_by": "created_at", "sort": "desc"},
            per_page=effective_per_page,
            paginate=False,
            max_items=max_deployments,
        )
        return [_map_deployment(item) for item in raw_items[:max_deployments]]

    async def get_commits(
        self,
        project_id: int | str,
        *,
        max_commits: int | None,
        since: datetime | None = None,
        until: datetime | None = None,
        per_page: int = 100,
    ) -> list[GitLabCommitData]:
        if max_commits is not None and max_commits <= 0:
            return []
        params: dict[str, Any] = {}
        if since is not None:
            params["since"] = _format_gitlab_window_datetime(since)
        if until is not None:
            params["until"] = _format_gitlab_window_datetime(until)

        effective_per_page = min(max_commits, per_page) if max_commits else per_page
        encoded_project_id = _encode_project_id(project_id)
        path = f"/projects/{encoded_project_id}/repository/commits"
        operation = f"{_PROJECT_FAMILY_PREFIX}:GET /projects/{{id}}/repository/commits"
        if max_commits is None:
            raw_items = await self._core.paginate_page_param(
                path,
                operation=operation,
                params=params,
                per_page=effective_per_page,
                max_pages=10_000,
            )
            return [_map_commit(item) for item in raw_items if isinstance(item, dict)]

        raw_items = await self._get_gitlab_list(
            path,
            operation=operation,
            params=params,
            per_page=effective_per_page,
            paginate=max_commits > effective_per_page,
            max_items=max_commits,
        )
        return [_map_commit(item) for item in raw_items[:max_commits]]

    async def get_commit_stats(
        self, project_id: int | str, commit_sha: str
    ) -> GitLabCommitStatsData:
        encoded_project_id = _encode_project_id(project_id)
        encoded_sha = urllib.parse.quote(str(commit_sha), safe="")
        response = await self._core.request(
            "GET",
            f"/projects/{encoded_project_id}/repository/commits/{encoded_sha}",
            operation=(
                f"{_PROJECT_FAMILY_PREFIX}:GET "
                "/projects/{id}/repository/commits/{sha}"
            ),
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise APIException(f"Unexpected GitLab commit response: {type(payload)!r}")
        return _map_commit_stats(commit_sha, payload)

    async def get_file_contents(
        self,
        project_full_path: str,
        paths: list[str],
        *,
        ref: str = "HEAD",
        batch_size: int = 50,
        max_bytes: int | None = 1_000_000,
    ) -> dict[str, str]:
        """Fetch text contents for many files via batched GraphQL blob queries.

        Mirrors ``GitLabConnector.get_file_contents`` (``connectors/gitlab.py``,
        FROZEN) field-for-field: GitLab's GraphQL API resolves multiple blobs
        natively through ``repository.blobs(ref:, paths:)``; ``rawTextBlob``
        is null for binary blobs, so those (and missing paths) are omitted
        from the result. When ``max_bytes`` is set, a cheap ``rawSize``-only
        pass filters oversized blobs first so their text never crosses the
        wire.

        Per-chunk resilience mirrors ``get_commit_stats``'s per-commit caller
        contract (CHAOS-2814/CS13 precedent): a ``RateLimitException`` from
        any chunk PROPAGATES (an exhausted rate limit must fail the sync,
        not silently truncate file coverage); any other chunk failure
        degrades -- a failed size-pass chunk falls back to an unfiltered
        text fetch, a failed text-pass chunk is logged and skipped,
        preserving earlier chunks' results.

        :param project_full_path: Project full path (``group/project``).
        :param paths: Repository-relative file paths.
        :param ref: Git reference the paths are resolved against.
        :param batch_size: Number of blobs to resolve per GraphQL request.
        :param max_bytes: Skip blobs larger than this (None disables).
        :return: Mapping of path -> file text for blobs with usable text.
        """
        if not paths:
            return {}

        eligible = paths
        if max_bytes is not None:
            eligible = []
            for start in range(0, len(paths), batch_size):
                chunk = paths[start : start + batch_size]
                try:
                    nodes = await self._graphql_blobs(
                        project_full_path, ref, chunk, "path rawSize"
                    )
                except RateLimitException:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Size pass failed for %d paths in %s (%s); "
                        "fetching text without size filter",
                        len(chunk),
                        project_full_path,
                        exc,
                    )
                    eligible.extend(chunk)
                    continue
                for node in nodes:
                    path = node.get("path")
                    raw_size = node.get("rawSize")
                    if not path:
                        continue
                    if raw_size is not None and int(raw_size) > max_bytes:
                        continue
                    eligible.append(path)

        contents: dict[str, str] = {}
        for start in range(0, len(eligible), batch_size):
            chunk = eligible[start : start + batch_size]
            try:
                nodes = await self._graphql_blobs(
                    project_full_path, ref, chunk, "path rawTextBlob"
                )
            except RateLimitException:
                raise
            except Exception as exc:
                logger.warning(
                    "Content fetch failed for %d paths in %s: %s",
                    len(chunk),
                    project_full_path,
                    exc,
                )
                continue
            for node in nodes:
                text = node.get("rawTextBlob")
                path = node.get("path")
                if path and text is not None:
                    contents[path] = text

        return contents

    async def _graphql_blobs(
        self,
        project_full_path: str,
        ref: str,
        paths: list[str],
        fields: str,
    ) -> list[dict[str, Any]]:
        """Resolve one chunk of blobs via GitLab GraphQL.

        Mirrors ``GitLabConnector._graphql_blobs`` (``connectors/gitlab.py``,
        FROZEN): one POST per chunk to the instance's GraphQL endpoint
        (``{base_url}/api/graphql`` -- distinct from the REST v4 base every
        other method on this client targets). Routed through the SAME
        ``InstrumentedRESTCore`` -- retry, rate-limit classification, and
        usage recording included -- because httpx treats an absolute URL
        passed to a base-url-configured client as an override, not a join,
        so ``self._graphql_url`` reaches the GraphQL endpoint unchanged
        regardless of the core's REST ``base_url``. Labeled with the
        ``project:`` prefix like ``get_commits``/``get_commit_stats`` above:
        ``providers/gitlab/budget.py`` buckets FILES/BLAME datasets under
        the existing ``project`` route family (CHAOS-2815/CS14), not a new
        one.
        """
        query = (
            "query($fullPath: ID!, $ref: String!, $paths: [String!]!) {\n"
            "  project(fullPath: $fullPath) {\n"
            "    repository {\n"
            "      blobs(ref: $ref, paths: $paths) {\n"
            f"        nodes {{ {fields} }}\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}"
        )
        response = await self._core.request(
            "POST",
            self._graphql_url,
            operation=f"{_PROJECT_FAMILY_PREFIX}:POST /api/graphql blobs",
            json={
                "query": query,
                "variables": {
                    "fullPath": project_full_path,
                    "ref": ref,
                    "paths": paths,
                },
            },
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise APIException(f"Unexpected GitLab GraphQL response: {type(payload)!r}")
        errors = payload.get("errors")
        if errors:
            messages = "; ".join(
                str(item.get("message", item)) if isinstance(item, dict) else str(item)
                for item in errors
            )
            raise APIException(f"GitLab GraphQL errors: {messages}")
        project_data = (payload.get("data") or {}).get("project") or {}
        repository = project_data.get("repository") or {}
        nodes = (repository.get("blobs") or {}).get("nodes")
        return [node for node in nodes or [] if isinstance(node, dict)]

    async def get_file_blame(
        self,
        project_id: int | str,
        file_path: str,
        *,
        ref: str = "HEAD",
    ) -> GitLabFileBlame:
        """Fetch normalized GitLab blame ranges for one file.

        Mirrors ``GitLabRESTClient.get_file_blame``
        (``connectors/utils/rest.py``, FROZEN): ``GET
        /projects/{id}/repository/files/{path}/blame`` returns GitLab's raw
        ``{"lines": [...], "commit": {...}}`` ranges, and this provider
        normalizes them to a GitLab-owned DTO before the processor sees them.
        The endpoint returns ranges in file order with a ``lines`` array per
        range; because the documented response shape does not include explicit
        line numbers, the provider assigns cumulative line numbers across the
        returned ranges. GitLab's blame endpoint
        returns the full per-file breakdown in one response (no pagination
        concept), so this is a single request. Errors -- INCLUDING a 404
        (no blame for this ref/path) -- propagate; callers handle
        best-effort per-file (pre-existing behavior, mirroring
        ``get_pipeline_test_report``).
        """
        encoded_project_id = _encode_project_id(project_id)
        encoded_path = urllib.parse.quote(file_path, safe="")
        response = await self._core.request(
            "GET",
            f"/projects/{encoded_project_id}/repository/files/{encoded_path}/blame",
            operation=(
                f"{_PROJECT_FAMILY_PREFIX}:GET "
                "/projects/{id}/repository/files/{path}/blame"
            ),
            params={"ref": ref},
        )
        payload = response.json()
        if not isinstance(payload, list):
            raise APIException(f"Unexpected GitLab blame response: {type(payload)!r}")
        return _map_file_blame(file_path, payload)

    async def get_deployment_merge_requests(
        self, project_id: int | str, sha: str
    ) -> list[dict[str, Any]]:
        encoded_sha = urllib.parse.quote(str(sha), safe="")
        return await self._get_gitlab_list(
            f"/projects/{project_id}/repository/commits/{encoded_sha}/merge_requests",
            operation=(
                f"{_DEPLOYMENTS_FAMILY_PREFIX}:GET "
                "/projects/{id}/repository/commits/{sha}/merge_requests"
            ),
            params={},
            per_page=100,
            paginate=False,
            max_items=100,
        )

    async def iter_merge_requests(
        self,
        project_id: int | str,
        *,
        state: str = "all",
        per_page: int = 100,
        max_pages: int = 10_000,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        """List GitLab merge requests newest-updated first.

        Mirrors the processor's legacy ``GitLabRESTClient.get_merge_requests``
        loop: ``state=all``, ``order_by=updated_at``, ``sort=desc``, and
        GitLab page/per_page pagination. ``max_items`` is reserved for callers
        that intentionally cap the scan (the legacy ``connector.get_merge_requests``
        helper); the full sync path exhausts ``X-Next-Page``.
        """
        encoded_project_id = _encode_project_id(project_id)
        path = f"/projects/{encoded_project_id}/merge_requests"
        operation = (
            f"{_MERGE_REQUESTS_FAMILY_PREFIX}:GET /projects/{{id}}/merge_requests"
        )
        params = {"state": state, "order_by": "updated_at", "sort": "desc"}
        if max_items is not None:
            if max_items <= 0:
                return []
            effective_per_page = min(max_items, per_page)
            return await self._get_gitlab_list(
                path,
                operation=operation,
                params=params,
                per_page=effective_per_page,
                paginate=max_items > effective_per_page,
                max_items=max_items,
            )
        payload = await self._core.paginate_page_param(
            path,
            operation=operation,
            params=params,
            per_page=per_page,
            max_pages=max_pages,
        )
        return [item for item in payload if isinstance(item, dict)]

    async def get_merge_requests_page(
        self,
        project_id: int | str,
        *,
        page: int,
        state: str = "all",
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        encoded_project_id = _encode_project_id(project_id)
        response = await self._core.request(
            "GET",
            f"/projects/{encoded_project_id}/merge_requests",
            operation=(
                f"{_MERGE_REQUESTS_FAMILY_PREFIX}:GET /projects/{{id}}/merge_requests"
            ),
            params={
                "state": state,
                "order_by": "updated_at",
                "sort": "desc",
                "page": page,
                "per_page": per_page,
            },
        )
        payload = response.json()
        if not isinstance(payload, list):
            raise APIException(
                f"Unexpected GitLab merge requests response: {type(payload)!r}"
            )
        return [item for item in payload if isinstance(item, dict)]

    async def iter_mr_commits(
        self,
        project_id: int | str,
        iid: int | str,
        *,
        per_page: int = 100,
        max_pages: int = 10_000,
    ) -> list[dict[str, Any]]:
        """List raw commits attached to one merge request.

        Added with the MR-family migration for parity with the frozen
        ``GitLabConnector.get_merge_request_commits`` helper. No processor path
        consumes it today, but keeping the canonical method ready prevents new
        python-gitlab usage while the connector remains frozen until CS17.
        """
        encoded_project_id = _encode_project_id(project_id)
        encoded_iid = urllib.parse.quote(str(iid), safe="")
        payload = await self._core.paginate_page_param(
            f"/projects/{encoded_project_id}/merge_requests/{encoded_iid}/commits",
            operation=(
                f"{_MERGE_REQUESTS_FAMILY_PREFIX}:GET "
                "/projects/{id}/merge_requests/{iid}/commits"
            ),
            params={},
            per_page=per_page,
            max_pages=max_pages,
        )
        return [item for item in payload if isinstance(item, dict)]

    async def iter_mr_notes(
        self,
        project_id: int | str,
        iid: int | str,
        *,
        per_page: int = 100,
        max_pages: int = 10_000,
    ) -> list[dict[str, Any]]:
        """Fetch every note page for one merge request.

        The processor treats notes as the authoritative review-event source, so
        this method exhausts pagination instead of preserving the frozen REST
        client's one-page convenience helper.
        """
        encoded_project_id = _encode_project_id(project_id)
        encoded_iid = urllib.parse.quote(str(iid), safe="")
        payload = await self._core.paginate_page_param(
            f"/projects/{encoded_project_id}/merge_requests/{encoded_iid}/notes",
            operation=(
                f"{_NOTES_FAMILY_PREFIX}:GET "
                "/projects/{id}/merge_requests/{iid}/notes"
            ),
            params={"sort": "asc", "order_by": "created_at"},
            per_page=per_page,
            max_pages=max_pages,
        )
        return [item for item in payload if isinstance(item, dict)]

    async def get_mr_approvals(
        self, project_id: int | str, iid: int | str
    ) -> dict[str, Any]:
        """Fetch raw approvals for one merge request.

        GitLab's approvals endpoint is a single object response, not a list.
        Tier/permission errors propagate to the caller, which decides whether a
        non-rate failure is best-effort for its dataset.
        """
        encoded_project_id = _encode_project_id(project_id)
        encoded_iid = urllib.parse.quote(str(iid), safe="")
        response = await self._core.request(
            "GET",
            f"/projects/{encoded_project_id}/merge_requests/{encoded_iid}/approvals",
            operation=(
                f"{_MERGE_REQUESTS_FAMILY_PREFIX}:GET "
                "/projects/{id}/merge_requests/{iid}/approvals"
            ),
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise APIException(
                f"Unexpected GitLab MR approvals response: {type(payload)!r}"
            )
        return payload

    async def iter_pipelines_since(
        self,
        project_id: int | str,
        *,
        since: datetime | None = None,
        per_page: int = 100,
        max_pages: int = 30,
    ) -> list[dict[str, Any]]:
        """Raw (unmapped) pipeline list dicts, newest-``updated_at`` first,
        optionally server-side filtered to ``updated_after=since`` -- mirrors
        the legacy python-gitlab ``gl_project.pipelines.list(order_by=
        "updated_at", sort="desc", updated_after=...)`` call in
        ``processors/gitlab.py::_fetch_gitlab_test_reports_sync`` (CHAOS-2773
        CS12).

        Returns RAW dicts rather than :class:`GitLabPipelineData` because the
        ``tests`` family's caller needs the ``ref`` field for default-branch
        filtering (the narrower dataclass built for the ``cicd``/``pipelines``
        families does not carry it), and needs a scan bounded by PAGES rather
        than by matching-item count -- the legacy lazy python-gitlab generator
        stopped once enough REF-MATCHING pipelines were found, but this
        core's paginators are eager, so ``max_pages`` intentionally leaves
        headroom above a single-branch scan of ``MAX_RUNS_PER_SYNC`` pipelines
        so branch filtering downstream doesn't silently shrink the effective
        search window. See :meth:`get_pipelines` for the mapped,
        item-capped sibling the ``cicd``/``pipelines`` families use.
        """
        params: dict[str, Any] = {"order_by": "updated_at", "sort": "desc"}
        if since is not None:
            params["updated_after"] = since.isoformat()
        return await self._core.paginate_page_param(
            f"/projects/{project_id}/pipelines",
            operation=f"{_TESTS_FAMILY_PREFIX}:GET /projects/{{id}}/pipelines",
            params=params,
            per_page=per_page,
            max_pages=max_pages,
        )

    async def get_pipeline_test_report(
        self, project_id: int | str, pipeline_id: int | str
    ) -> dict[str, Any]:
        """Fetch GitLab's native parsed JUnit test report for a pipeline.

        Mirrors ``GitLabRESTClient.get_pipeline_test_report``
        (``connectors/utils/rest.py``, FROZEN) field-for-field: ``GET
        /projects/{id}/pipelines/{pipeline_id}/test_report`` returns
        already-parsed test suites/cases JSON, so no artifact download or
        XML parsing is needed for GitLab pass/fail/duration metrics
        (CHAOS-2370). Errors -- INCLUDING a 404 (no report for this
        pipeline) -- propagate, matching the connector's own ``get()``
        contract; callers handle best-effort (pre-existing behavior:
        ``processors/gitlab.py`` wraps this in a broad ``except Exception``
        per pipeline).
        """
        response = await self._core.request(
            "GET",
            f"/projects/{project_id}/pipelines/{pipeline_id}/test_report",
            operation=(
                f"{_TESTS_FAMILY_PREFIX}:GET "
                "/projects/{id}/pipelines/{id}/test_report"
            ),
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise APIException(
                f"Unexpected GitLab test report response: {type(payload)!r}"
            )
        return payload

    async def iter_pipeline_jobs(
        self,
        project_id: int | str,
        pipeline_id: int | str,
        *,
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        """List jobs for a pipeline -- SINGLE PAGE only. This is a
        pre-existing connector quirk (``GitLabRESTClient.get_list`` never
        looped on ``page``/``X-Next-Page``), preserved here for strict
        parity with the ``tests``-family coverage/artifact-discovery call
        site in ``processors/gitlab.py::_fetch_gitlab_test_reports_sync``
        (CHAOS-2773 CS12 -- default is parity, not a behavior change, same
        philosophy as CS10's security pathfinder). Deliberately does NOT
        pass ``include_retried`` -- unlike ``GitLabCIAdapter``'s own jobs
        fetch for the ``job_runs`` rows, the legacy coverage/artifact scan
        never requested retried jobs either.
        """
        return await self._get_gitlab_list(
            f"/projects/{project_id}/pipelines/{pipeline_id}/jobs",
            operation=(
                f"{_TESTS_FAMILY_PREFIX}:GET /projects/{{id}}/pipelines/{{id}}/jobs"
            ),
            params={},
            per_page=per_page,
            paginate=False,
            max_items=per_page,
        )

    async def download_job_artifact(
        self,
        project_id: int | str,
        job_id: int | str,
        *,
        max_bytes: int = 100 * 1024 * 1024,
    ) -> bytes:
        """Download a job's artifacts ZIP (for coverage / fallback JUnit).

        Mirrors ``GitLabRESTClient.download_job_artifacts``
        (``connectors/utils/rest.py``, FROZEN): returns ``b""`` when the job
        has no artifacts or they have expired (404), matching the
        connector's own best-effort, convenience-empty contract. Unlike the
        legacy ``requests``-streamed download, the shared
        ``InstrumentedRESTCore`` buffers the full response before this
        method can inspect its size (CS1 exposes no streaming primitive) --
        the byte cap is therefore enforced POST-fetch: an oversized payload
        is discarded (empty bytes returned, warning logged) rather than
        aborting the transfer early. The pre-CS12 memory-cap intent
        (CHAOS-2370) is preserved; only the enforcement point moves.
        """
        operation = f"{_TESTS_FAMILY_PREFIX}:GET /projects/{{id}}/jobs/{{id}}/artifacts"
        try:
            response = await self._core.request(
                "GET",
                f"/projects/{project_id}/jobs/{job_id}/artifacts",
                operation=operation,
                raw_redirect=True,
            )
        except NotFoundException:
            return b""
        if response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("Location")
            if not location:
                raise APIException(
                    f"GitLab artifact redirect for job {job_id} omitted Location"
                )
            response = await self._core.request_unauthenticated(
                location,
                operation=f"{operation} follow",
            )
            if response.status_code in {404, 410}:
                return b""
            if response.status_code >= 400:
                raise APIException(
                    "GitLab artifact redirect download failed for job "
                    f"{job_id}: HTTP {response.status_code}"
                )
        content = response.content
        if len(content) > max_bytes:
            logger.warning(
                "GitLab job artifact for job %s exceeds %d byte cap; discarding",
                job_id,
                max_bytes,
            )
            return b""
        return content

    async def _get_gitlab_list(
        self,
        path: str,
        *,
        operation: str,
        params: dict[str, Any],
        per_page: int,
        paginate: bool,
        max_items: int,
    ) -> list[dict[str, Any]]:
        if paginate:
            max_pages = max(1, (max_items + per_page - 1) // per_page)
            payload = await self._core.paginate_page_param(
                path,
                operation=operation,
                params=params,
                per_page=per_page,
                max_pages=max_pages,
            )
            return [item for item in payload if isinstance(item, dict)][:max_items]

        response = await self._core.request(
            "GET",
            path,
            operation=operation,
            params={**params, "page": 1, "per_page": per_page},
        )
        payload = response.json()
        if not isinstance(payload, list):
            raise APIException(
                f"Unexpected GitLab list response for {operation}: {type(payload)!r}"
            )
        return [item for item in payload if isinstance(item, dict)][:max_items]

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._core.drain_usage_observations()

    async def close(self) -> None:
        await self._core.close()

    async def __aenter__(self) -> GitLabCodeClient:
        await self._core.__aenter__()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()


__all__ = [
    "GitLabCodeClient",
    "GitLabCommitData",
    "GitLabCommitStatsData",
    "GitLabDeploymentData",
    "GitLabPipelineData",
    "GitLabProjectData",
]
