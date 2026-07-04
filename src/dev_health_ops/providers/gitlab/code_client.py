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
from typing import Any

import httpx

from dev_health_ops.connectors.models import SecurityAlertData
from dev_health_ops.exceptions import APIException, NotFoundException
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
]
