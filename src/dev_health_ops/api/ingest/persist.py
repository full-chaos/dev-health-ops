"""Persist deserialized ingest items to ClickHouse."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.processors.release_ref import get_release_ref_enrichment
from dev_health_ops.providers.operational_migration import (
    IssueIncidentSource,
    map_issue_incidents,
    write_operational_batch,
)
from dev_health_ops.storage.clickhouse import ClickHouseStore

logger = logging.getLogger(__name__)

_INGEST_SETTINGS = {
    "async_insert": 1,
    "wait_for_async_insert": 1,
    "async_insert_busy_timeout_ms": int(
        os.getenv("INGEST_ASYNC_INSERT_TIMEOUT_MS", "200")
    ),
}


def _get_ingest_settings() -> dict:
    if os.getenv("INGEST_ASYNC_INSERT", "1") == "0":
        return {}
    return dict(_INGEST_SETTINGS)


def _repo_id_from_url(repo_url: str) -> uuid.UUID:
    """Derive deterministic repo_id from repo_url (same as ClickHouseStore)."""
    return uuid.uuid5(uuid.NAMESPACE_URL, repo_url)


async def persist_items(entity_type: str, items: list[dict[str, Any]]) -> int:
    """Persist a batch of deserialized ingest items to ClickHouse.

    Returns number of items persisted.
    """
    ch_url = os.getenv("CLICKHOUSE_URI") or os.getenv("DATABASE_URI") or ""
    if not ch_url:
        logger.warning("No ClickHouse URI configured, skipping persistence")
        return 0

    settings = _get_ingest_settings()
    async with ClickHouseStore(ch_url, settings=settings) as store:
        if entity_type == "commits":
            await _persist_commits(store, items)
        elif entity_type == "pull-requests":
            await _persist_pull_requests(store, items)
        elif entity_type == "work-items":
            await _persist_work_items(store, items)
        elif entity_type == "deployments":
            await _persist_deployments(store, items)
        elif entity_type == "incidents":
            await _persist_incidents(store, items)
        else:
            logger.warning("Unknown entity type for persistence: %s", entity_type)
            return 0
    return len(items)


async def _persist_commits(store: ClickHouseStore, items: list[dict[str, Any]]) -> None:
    rows: list[dict[str, Any]] = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        item["repo_id"] = _repo_id_from_url(repo_url)
        rows.append(item)
    if rows:
        await store.insert_git_commit_data(rows)


async def _persist_pull_requests(
    store: ClickHouseStore, items: list[dict[str, Any]]
) -> None:
    rows: list[dict[str, Any]] = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        item["repo_id"] = _repo_id_from_url(repo_url)
        item.pop("reviews", None)
        rows.append(item)
    if rows:
        await store.insert_git_pull_requests(rows)


async def _persist_work_items(
    store: ClickHouseStore, items: list[dict[str, Any]]
) -> None:
    rows: list[dict[str, Any]] = []
    for item in items:
        item.pop("_repo_url", None)
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        rows.append(item)
    if rows:
        await store.insert_work_items(rows)


async def _persist_deployments(
    store: ClickHouseStore, items: list[dict[str, Any]]
) -> None:
    rows: list[dict[str, Any]] = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        item["repo_id"] = _repo_id_from_url(repo_url)
        enrichment = get_release_ref_enrichment(item, "generic")
        item["release_ref"] = item.get("release_ref") or enrichment.release_ref
        item["release_ref_confidence"] = (
            item.get("release_ref_confidence")
            if item.get("release_ref_confidence") is not None
            else enrichment.confidence
        )
        rows.append(item)
    if rows:
        await store.insert_deployments(rows)


async def _persist_incidents(
    store: ClickHouseStore, items: list[dict[str, Any]]
) -> None:
    sources: list[IssueIncidentSource] = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        org_id = str(item.pop("_org_id", "") or "").strip()
        item.pop("_ingestion_id", None)
        started_at = item.get("started_at")
        if not isinstance(started_at, datetime):
            raise ValueError("incident ingest requires started_at")
        resolved_at = item.get("resolved_at")
        if resolved_at is not None and not isinstance(resolved_at, datetime):
            raise ValueError("incident ingest resolved_at must be a datetime")
        if not org_id or not repo_url:
            raise ValueError("incident ingest requires org_id and repo_url")
        repo_id = _repo_id_from_url(repo_url)
        source_version_at = resolved_at or started_at
        if source_version_at.tzinfo is None:
            source_version_at = source_version_at.replace(tzinfo=timezone.utc)
        sources.append(
            IssueIncidentSource(
                org_id=org_id,
                provider="external",
                provider_instance_id="legacy-repository-ingest",
                repo_id=repo_id,
                repo_full_name=repo_url,
                external_id=str(item.get("incident_id") or ""),
                issue_number=None,
                source_url=None,
                labels=(),
                raw_status=str(item.get("status") or "") or None,
                title=str(item.get("incident_id") or ""),
                description=None,
                created_at=started_at,
                resolved_at=resolved_at,
                source_version_at=source_version_at,
            )
        )
    if sources:
        await write_operational_batch(store, map_issue_incidents(sources))
