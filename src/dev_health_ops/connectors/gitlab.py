import gitlab
from gitlab.exceptions import GitlabAuthenticationError, GitlabError

from dev_health_ops.connectors.base import RateLimitException
from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
)
from dev_health_ops.connectors.utils import (
    GitLabRESTClient,
    match_project_pattern,
)

# match_project_pattern is re-exported for connectors/__init__.py (mirrors
# .github's match_repo_pattern); __all__ marks it an intentional export.
__all__ = ["GitLabConnector", "match_project_pattern"]


def _parse_retry_after_seconds(headers: object) -> float | None:
    if not isinstance(headers, dict):
        return None
    retry_after = headers.get("Retry-After")
    if retry_after is None:
        return None
    try:
        return max(0.0, float(retry_after))
    except (TypeError, ValueError):
        return None


class GitLabConnector:
    def __init__(
        self,
        url: str = "https://gitlab.com",
        private_token: str | None = None,
        per_page: int = 100,
        max_workers: int = 4,
        rest_timeout: int = 15,
    ) -> None:
        self.per_page = per_page
        self.max_workers = max_workers
        self.url = url
        self.private_token = private_token

        self.gitlab = gitlab.Gitlab(
            url=url,
            private_token=private_token,
            timeout=rest_timeout,
        )

        if private_token:
            try:
                self.gitlab.auth()
            except GitlabAuthenticationError as e:
                raise AuthenticationException(
                    f"GitLab authentication failed: {e}"
                ) from e

        # REST client for operations never migrated off the connector — still
        # used by live processor paths (incident label issues at
        # processors/gitlab.py:1111 and the CI adapter base_url at :1985).
        # Rides the retained connectors/utils/rest.py (CHAOS-2819).
        api_url = f"{url}/api/v4"
        self.rest_client = GitLabRESTClient(
            base_url=api_url,
            private_token=private_token,
            timeout=rest_timeout,
        )

    def _handle_gitlab_exception(self, e: Exception) -> None:
        if isinstance(e, GitlabAuthenticationError):
            raise AuthenticationException(f"GitLab authentication failed: {e}") from e
        if isinstance(e, GitlabError):
            if hasattr(e, "response_code"):
                if e.response_code == 429:
                    retry_after = _parse_retry_after_seconds(
                        getattr(e, "response_headers", None)
                    )
                    raise RateLimitException(
                        f"GitLab rate limit exceeded: {e}",
                        retry_after_seconds=retry_after,
                    ) from e
                if e.response_code == 404:
                    raise NotFoundException(
                        "GitLab resource not found (404). "
                        "This can also mean the token lacks access. "
                        f"Details: {e}"
                    ) from e
            raise APIException(f"GitLab API error: {e}") from e
        raise APIException(f"Unexpected error: {e}") from e

    def close(self) -> None:
        """No-op retained for live processor callers (CHAOS-2819)."""
        pass
