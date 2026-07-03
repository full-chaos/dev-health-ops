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
Later GitLab-wave changesets (CS11 pipelines+deployments, CS12 tests, ...)
copy this shape.

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

Every operation this client issues carries the explicit ``"security:"``
family prefix (CHAOS-2773 CS1's ``OperationResolver`` prefix short-circuit,
``providers/usage.py``) so usage resolution is deterministic by
construction rather than by marker-substring tuning -- see
``providers/gitlab/budget.py``'s ``security`` route family.
"""

from __future__ import annotations

import logging
import urllib.parse
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


class GitLabCodeClient:
    """Canonical GitLab code-dataset client (CHAOS-2773 CS10 pathfinder).

    Currently covers the "security" dataset (vulnerability findings +
    dependency-scan alerts). Built on the shared ``InstrumentedRESTCore`` so
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
            logger.debug(
                "GitLab vulnerability findings unavailable for project %s: %s",
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
            logger.debug(
                "GitLab dependencies unavailable for project %s: %s", project_id, exc
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

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return self._core.drain_usage_observations()

    async def close(self) -> None:
        await self._core.close()

    async def __aenter__(self) -> GitLabCodeClient:
        await self._core.__aenter__()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()


__all__ = ["GitLabCodeClient"]
