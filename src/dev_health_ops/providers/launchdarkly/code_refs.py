from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import httpx

from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.metrics.schemas import FeatureFlagLinkRecord
from dev_health_ops.work_graph.ids import generate_feature_flag_id, generate_file_id

logger = logging.getLogger(__name__)

_BASE_URL = "https://app.launchdarkly.com/api/v2"
LD_CODE_REFERENCE_CONFIDENCE = 0.95


@dataclass(frozen=True)
class LaunchDarklyCodeReference:
    flag_key: str
    project_key: str
    repo_name: str
    repo_source_link: str | None
    branch_name: str
    branch_head: str | None
    file_path: str
    starting_line_number: int
    lines: str
    aliases: tuple[str, ...] = ()

    @property
    def evidence(self) -> str:
        return (
            f"ld_code_ref:{self.repo_name}:{self.branch_name}:"
            f"{self.file_path}:L{self.starting_line_number}"
        )

    @property
    def file_target_id(self) -> str:
        return f"{self.repo_name}:{self.file_path}"

    def repo_match_keys(self) -> tuple[str, ...]:
        keys: list[str] = []
        if self.repo_source_link:
            parsed = urlparse(self.repo_source_link)
            path = parsed.path.strip("/")
            if path.endswith(".git"):
                path = path[:-4]
            _append_key(keys, path)
            if path:
                _append_key(keys, path.rsplit("/", 1)[-1])
        _append_key(keys, self.repo_name)
        return tuple(keys)


def _append_key(keys: list[str], value: str | None) -> None:
    key = (value or "").strip().strip("/").lower()
    if key and key not in keys:
        keys.append(key)


def _normalize_path(path: str, branch_name: str) -> str:
    normalized = path.strip().lstrip("/")
    branch_prefix = f"{branch_name.strip().strip('/')}/" if branch_name else ""
    if branch_prefix and normalized.startswith(branch_prefix):
        normalized = normalized[len(branch_prefix) :]
    return normalized


def _raise_for_status(response: httpx.Response) -> None:
    status = response.status_code
    if status == 401:
        raise AuthenticationException("LaunchDarkly authentication failed")
    if status == 429:
        retry_after = response.headers.get("Retry-After")
        raise RateLimitException(
            "LaunchDarkly rate limit exceeded",
            retry_after_seconds=float(retry_after) if retry_after else None,
        )
    if status == 403:
        raise APIException(f"LaunchDarkly code references forbidden: {response.text}")
    if status == 404:
        raise APIException(f"LaunchDarkly code references not found: {response.url}")
    if status >= 500:
        raise APIException(
            f"LaunchDarkly code references server error: {status} - {response.text}"
        )
    if status >= 400:
        raise APIException(
            f"LaunchDarkly code references API error: {status} - {response.text}"
        )


class LaunchDarklyCodeReferencesClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = _BASE_URL,
        timeout: int = 30,
        max_retries: int = 5,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={"Authorization": self.api_key},
                timeout=self.timeout,
            )
        return self._client

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        client = await self._get_client()
        delay = 1.0
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = await client.request(method, path, params=params)
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < self.max_retries - 1:
                        retry_after = response.headers.get("Retry-After")
                        wait_seconds = float(retry_after) if retry_after else delay
                        logger.warning(
                            "LaunchDarkly %d on %s (attempt %d/%d), retrying in %.1fs",
                            response.status_code,
                            sanitize_for_log(path),
                            attempt + 1,
                            self.max_retries,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        delay = min(delay * 2, 60.0)
                        continue
                _raise_for_status(response)
                return response.json()
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 60.0)
                    continue
                raise APIException(
                    f"LaunchDarkly code references request failed: {exc}"
                ) from exc
        raise APIException("LaunchDarkly code references request failed") from last_exc

    async def list_default_branch_references(
        self,
        *,
        project_key: str,
        flag_key: str | None = None,
    ) -> list[LaunchDarklyCodeReference]:
        params: dict[str, Any] = {
            "withReferencesForDefaultBranch": "1",
            "projKey": project_key,
        }
        if flag_key:
            params["flagKey"] = flag_key
        data = await self._request("GET", "/code-refs/repositories", params=params)
        refs = parse_code_reference_repositories(data)
        logger.info(
            "Fetched %d LaunchDarkly code references for project %s",
            len(refs),
            sanitize_for_log(project_key),
        )
        return refs

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> LaunchDarklyCodeReferencesClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


def parse_code_reference_repositories(
    data: dict[str, Any],
) -> list[LaunchDarklyCodeReference]:
    refs: list[LaunchDarklyCodeReference] = []
    for repo in data.get("items") or []:
        if not isinstance(repo, dict):
            continue
        repo_name = str(repo.get("name") or "").strip()
        if not repo_name:
            continue
        source_link = repo.get("sourceLink")
        for branch in repo.get("branches") or []:
            if not isinstance(branch, dict):
                continue
            branch_name = str(branch.get("name") or repo.get("defaultBranch") or "")
            branch_head = branch.get("head")
            for reference in branch.get("references") or []:
                if not isinstance(reference, dict):
                    continue
                path = str(reference.get("path") or "")
                normalized_path = _normalize_path(path, branch_name)
                if not normalized_path:
                    continue
                for hunk in reference.get("hunks") or []:
                    if not isinstance(hunk, dict):
                        continue
                    flag_key = str(hunk.get("flagKey") or "").strip()
                    project_key = str(hunk.get("projKey") or "").strip()
                    if not flag_key or not project_key:
                        continue
                    refs.append(
                        LaunchDarklyCodeReference(
                            flag_key=flag_key,
                            project_key=project_key,
                            repo_name=repo_name,
                            repo_source_link=str(source_link) if source_link else None,
                            branch_name=branch_name,
                            branch_head=str(branch_head) if branch_head else None,
                            file_path=normalized_path,
                            starting_line_number=int(
                                hunk.get("startingLineNumber") or 0
                            ),
                            lines=str(hunk.get("lines") or ""),
                            aliases=tuple(str(a) for a in hunk.get("aliases") or ()),
                        )
                    )
    return refs


def index_repo_rows(repo_rows: list[dict[str, Any]]) -> dict[str, uuid.UUID]:
    indexed: dict[str, uuid.UUID] = {}
    for row in repo_rows:
        repo_id = row.get("id")
        repo_name = str(row.get("repo") or "").strip().strip("/")
        if not repo_id or not repo_name:
            continue
        repo_uuid = uuid.UUID(str(repo_id))
        keys = [repo_name, repo_name.rsplit("/", 1)[-1]]
        for key in keys:
            _append_repo_index(indexed, key, repo_uuid)
    return indexed


def _append_repo_index(
    indexed: dict[str, uuid.UUID], key: str, repo_id: uuid.UUID
) -> None:
    normalized = key.strip().strip("/").lower()
    if normalized and normalized not in indexed:
        indexed[normalized] = repo_id


def resolve_repo_id(
    ref: LaunchDarklyCodeReference,
    repo_index: dict[str, uuid.UUID],
) -> uuid.UUID | None:
    for key in ref.repo_match_keys():
        repo_id = repo_index.get(key)
        if repo_id is not None:
            return repo_id
    return None


def build_code_reference_links(
    refs: list[LaunchDarklyCodeReference],
    *,
    org_id: str,
    repo_index: dict[str, uuid.UUID],
    pr_ids_by_repo_path: dict[tuple[str, str], set[str]],
    now: datetime | None = None,
) -> tuple[list[FeatureFlagLinkRecord], list[dict[str, Any]]]:
    synced_at = now or datetime.now(tz=timezone.utc)
    links: list[FeatureFlagLinkRecord] = []
    edges: list[dict[str, Any]] = []
    seen_links: set[tuple[str, str, str]] = set()
    seen_edges: set[tuple[str, str, str]] = set()

    for ref in refs:
        flag_id = generate_feature_flag_id(
            org_id, "launchdarkly", ref.project_key, ref.flag_key
        )
        repo_id = resolve_repo_id(ref, repo_index)
        if repo_id is not None:
            file_targets: list[tuple[str, uuid.UUID | None]] = [
                (generate_file_id(repo_id, ref.file_path), repo_id)
            ]
        else:
            file_targets = [(ref.file_target_id, None)]

        for target_id, target_repo_id in file_targets:
            link_key = (ref.flag_key, "file", target_id)
            if link_key not in seen_links:
                seen_links.add(link_key)
                links.append(_make_link(ref, "file", target_id, synced_at, org_id))
            if target_repo_id is not None:
                edge_key = (flag_id, "file", target_id)
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append(
                        {
                            "flag_id": flag_id,
                            "target_type": "file",
                            "target_id": target_id,
                            "repo_id": target_repo_id,
                            "evidence": ref.evidence,
                        }
                    )

        if repo_id is None:
            continue
        for pr_id in sorted(
            pr_ids_by_repo_path.get((str(repo_id), ref.file_path), set())
        ):
            link_key = (ref.flag_key, "pr", pr_id)
            if link_key not in seen_links:
                seen_links.add(link_key)
                links.append(_make_link(ref, "pr", pr_id, synced_at, org_id))
            edge_key = (flag_id, "pr", pr_id)
            if edge_key not in seen_edges:
                seen_edges.add(edge_key)
                edges.append(
                    {
                        "flag_id": flag_id,
                        "target_type": "pr",
                        "target_id": pr_id,
                        "repo_id": repo_id,
                        "evidence": ref.evidence,
                    }
                )

    return links, edges


def _make_link(
    ref: LaunchDarklyCodeReference,
    target_type: str,
    target_id: str,
    synced_at: datetime,
    org_id: str,
) -> FeatureFlagLinkRecord:
    return FeatureFlagLinkRecord(
        flag_key=ref.flag_key,
        target_type=target_type,
        target_id=target_id,
        provider="launchdarkly",
        link_source="native",
        link_type="code_reference",
        evidence_type="ld_code_ref",
        confidence=LD_CODE_REFERENCE_CONFIDENCE,
        valid_from=synced_at,
        valid_to=None,
        last_synced=synced_at,
        org_id=org_id,
    )
