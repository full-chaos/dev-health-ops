"""
GraphQL client for GitHub API.

This module provides utilities for querying GitHub's GraphQL API,
particularly for operations not well-supported by PyGithub such as blame.
"""

import logging
import time
from collections.abc import Callable
from typing import Any

import requests

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.connectors.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


def _github_reset_delay_seconds(
    response: requests.Response,
) -> float | None:
    reset = response.headers.get("X-RateLimit-Reset")
    if not reset:
        return None
    try:
        reset_epoch = float(reset)
    except ValueError:
        return None

    return max(0.0, reset_epoch - time.time())


class GitHubGraphQLClient:
    """
    Client for GitHub GraphQL API v4.

    Provides methods to query GitHub's GraphQL API with automatic
    rate limit handling and error management.
    """

    GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

    def __init__(
        self,
        token: str | None = None,
        timeout: int = 30,
        token_provider: Callable[[], str] | None = None,
    ):
        """
        Initialize GitHub GraphQL client.

        :param token: GitHub personal access token.
        :param timeout: Request timeout in seconds.
        """
        if not token and token_provider is None:
            raise ValueError("GitHubGraphQLClient requires token or token_provider")
        self.token = token
        self.timeout = timeout
        self.token_provider = token_provider
        self.headers = {
            "Content-Type": "application/json",
        }

    def _headers(self) -> dict[str, str]:
        token = self.token_provider() if self.token_provider is not None else self.token
        return {
            **self.headers,
            "Authorization": f"Bearer {token}",
        }

    @retry_with_backoff(
        max_retries=5,
        initial_delay=1.0,
        max_delay=60.0,
        exceptions=(RateLimitException, APIException),
    )
    def query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute a GraphQL query.

        :param query: GraphQL query string.
        :param variables: Optional variables for the query.
        :return: Response data from GraphQL API.
        :raises AuthenticationException: If authentication fails.
        :raises RateLimitException: If rate limit is exceeded.
        :raises APIException: If API returns an error.
        """
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = requests.post(
                self.GRAPHQL_ENDPOINT,
                json=payload,
                headers=self._headers(),
                timeout=self.timeout,
            )

            # Check for HTTP errors
            if response.status_code == 401:
                raise AuthenticationException("GitHub authentication failed")
            elif response.status_code == 403:
                # Could be rate limit or other forbidden error
                rate_limit_remaining = response.headers.get("X-RateLimit-Remaining")
                if rate_limit_remaining == "0":
                    raise RateLimitException(
                        "GitHub API rate limit exceeded",
                        retry_after_seconds=_github_reset_delay_seconds(response),
                    )
                raise APIException(f"GitHub API forbidden: {response.text}")
            elif response.status_code != 200:
                raise APIException(
                    f"GitHub API error: {response.status_code} - {response.text}"
                )

            data = response.json()

            # Check for GraphQL errors
            if "errors" in data:
                error_messages = [e.get("message", str(e)) for e in data["errors"]]
                error_str = "; ".join(error_messages)
                raise APIException(f"GraphQL errors: {error_str}")

            return data.get("data", {})

        except requests.exceptions.Timeout as exc:
            raise APIException("Request timeout") from exc
        except requests.exceptions.RequestException as exc:
            raise APIException(f"Request failed: {exc}") from exc

    def get_blame(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str = "HEAD",
    ) -> dict[str, Any]:
        """
        Get blame information for a file using GitHub GraphQL API.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param path: File path within the repository.
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: Blame data from GraphQL API.
        """
        query = """
        query($owner: String!, $repo: String!, $path: String!, $ref: String!) {
          repository(owner: $owner, name: $repo) {
            object(expression: $ref) {
              ... on Commit {
                blame(path: $path) {
                  ranges {
                    startingLine
                    endingLine
                    commit {
                      oid
                      authoredDate
                      author {
                        name
                        email
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """

        variables = {
            "owner": owner,
            "repo": repo,
            "path": path,
            "ref": ref,
        }

        logger.debug(
            "Fetching blame for %s/%s:%s at ref %s",
            owner,
            repo,
            path,
            ref,
        )
        result = self.query(query, variables)

        return result

    def get_rate_limit(self) -> dict[str, Any]:
        """
        Get current rate limit status.

        :return: Rate limit information.
        """
        query = """
        {
          rateLimit {
            limit
            remaining
            resetAt
            used
          }
        }
        """

        return self.query(query)
