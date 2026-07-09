"""GitHub GraphQL query builders + response parsers (CHAOS-2773 CS7).

Relocates the file-contents and blame GraphQL shapes off the frozen
``connectors/utils/graphql.py::GitHubGraphQLClient`` (``get_blob_texts`` /
``get_blame``) onto the canonical ``providers/github/`` tree, per the epic's
provider-boundary contract: raw fetch/auth/pagination/retry/rate-limit stays
inside the provider, never in a processor.

This module owns ONLY pure query construction and response parsing --
transport (retry/backoff, 403 triage, usage recording) stays on
``GitHubCodeClient``'s single owned ``InstrumentedRESTCore``
(``providers/github/code_client.py``), which POSTs these queries via its
existing ``request()`` method (a relative ``/graphql`` path joins correctly
against the default ``https://api.github.com`` base -- the same base every
other CS3-CS6 code-client method already uses) exactly like its paginators
reuse ``request()`` for REST calls. This is deliberate: the epic's "one
``InstrumentedRESTCore`` per client, never a second transport primitive"
rule (see ``code_client.py``'s module docstring) means GraphQL support is
new QUERY/PARSE code, not a new transport.

Response-shape parity with the legacy client is pinned by
``tests/providers/test_github_graphql.py``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from dev_health_ops.connectors.models import BlameRange, FileBlame
from dev_health_ops.exceptions import APIException

logger = logging.getLogger(__name__)

GITHUB_GRAPHQL_PATH = "/graphql"
GITHUB_ENTERPRISE_GRAPHQL_PATH = "/api/graphql"


def github_graphql_url(rest_base_url: str) -> str:
    """Resolve the GitHub GraphQL endpoint from a REST API base URL.

    Public GitHub uses ``https://api.github.com/graphql``. GitHub Enterprise
    REST bases include ``/api/v3`` and GraphQL lives at sibling path
    ``/api/graphql`` on the same host.
    """
    parsed = urlsplit(rest_base_url.rstrip("/"))
    base_path = parsed.path.rstrip("/")
    path = (
        f"{base_path[: -len('/api/v3')]}/api/graphql"
        if base_path.endswith("/api/v3")
        else f"{base_path}/graphql"
    )
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


BLAME_QUERY = """
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


def blame_variables(owner: str, repo: str, path: str, ref: str) -> dict[str, str]:
    return {"owner": owner, "repo": repo, "path": path, "ref": ref}


def build_blob_texts_query(ref: str, paths: list[str]) -> str:
    """Build one query resolving up to ``len(paths)`` blobs via field
    aliases -- byte-for-byte port of ``connectors/utils/graphql.py::
    GitHubGraphQLClient.get_blob_texts``'s query construction."""
    fields = []
    for i, path in enumerate(paths):
        expression = json.dumps(f"{ref}:{path}")
        fields.append(
            f"f{i}: object(expression: {expression}) "
            "{ ... on Blob { text isBinary isTruncated } }"
        )
    return (
        "query($owner: String!, $repo: String!) {\n"
        "  repository(owner: $owner, name: $repo) {\n" + "\n".join(fields) + "\n  }\n}"
    )


def raise_for_graphql_errors(envelope: Mapping[str, Any], *, operation: str) -> None:
    """Raise ``APIException`` for a GraphQL-level ``errors`` array.

    A GraphQL application error rides on an HTTP 200 (no status-code
    classification catches it), so the transport core's retry/classification
    loop never sees it -- this must be checked explicitly by the caller after
    a successful ``request()``, mirroring ``connectors/utils/graphql.py``'s
    ``"errors" in data`` check.
    """
    errors = envelope.get("errors") if isinstance(envelope, Mapping) else None
    if not errors:
        return
    error_messages = [
        e.get("message", str(e)) if isinstance(e, Mapping) else str(e) for e in errors
    ]
    raise APIException(
        f"GitHub GraphQL errors on {operation}: {'; '.join(error_messages)}"
    )


def parse_blob_texts_response(
    payload: Mapping[str, Any], paths: list[str]
) -> dict[str, str | None]:
    """Byte-for-byte port of ``GitHubGraphQLClient.get_blob_texts``'s result
    parsing: binary/truncated/missing blobs resolve to ``None`` so callers
    can treat absence as "no usable text"."""
    repo_data = payload.get("repository") or {}
    contents: dict[str, str | None] = {}
    for i, path in enumerate(paths):
        blob = repo_data.get(f"f{i}") or {}
        text = blob.get("text")
        if blob.get("isBinary") or blob.get("isTruncated") or text is None:
            contents[path] = None
        else:
            contents[path] = text
    return contents


def _parse_blame_commit_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.debug("Failed to parse GitHub blame commit datetime: %s", value)
        return None


def parse_blame_response(payload: Mapping[str, Any], *, file_path: str) -> FileBlame:
    """Byte-for-byte port of ``connectors/github.py::GitHubConnector.
    get_file_blame``'s response parsing, including its per-call
    ``age_seconds`` calculation (measured against "now" at parse time)."""
    repo_data = payload.get("repository") or {}
    obj_data = repo_data.get("object") or {}
    blame_data = obj_data.get("blame") or {}
    ranges_data = blame_data.get("ranges") or []

    now = datetime.now(timezone.utc)
    ranges: list[BlameRange] = []
    for range_item in ranges_data:
        commit = range_item.get("commit") or {}
        author_info = commit.get("author") or {}
        authored_date = _parse_blame_commit_datetime(commit.get("authoredDate"))
        age_seconds = int((now - authored_date).total_seconds()) if authored_date else 0
        ranges.append(
            BlameRange(
                starting_line=range_item.get("startingLine", 0),
                ending_line=range_item.get("endingLine", 0),
                commit_sha=commit.get("oid", ""),
                author=author_info.get("name", "Unknown"),
                author_email=author_info.get("email", ""),
                age_seconds=age_seconds,
            )
        )
    return FileBlame(file_path=file_path, ranges=ranges)


__all__ = [
    "BLAME_QUERY",
    "GITHUB_ENTERPRISE_GRAPHQL_PATH",
    "GITHUB_GRAPHQL_PATH",
    "blame_variables",
    "build_blob_texts_query",
    "github_graphql_url",
    "parse_blame_response",
    "parse_blob_texts_response",
    "raise_for_graphql_errors",
]
