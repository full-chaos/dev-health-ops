"""Persist deserialized ingest items to ClickHouse."""

from __future__ import annotations

import logging
import os
import uuid

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


async def persist_items(entity_type: str, items: list[dict]) -> int:
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


async def _persist_commits(store: ClickHouseStore, items: list[dict]) -> None:
    rows = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        item["repo_id"] = _repo_id_from_url(repo_url)
        rows.append(item)
    if rows:
        await store.insert_git_commit_data(rows)


async def _persist_pull_requests(store: ClickHouseStore, items: list[dict]) -> None:
    rows = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        item["repo_id"] = _repo_id_from_url(repo_url)
        item.pop("reviews", None)
        rows.append(item)
    if rows:
        await store.insert_git_pull_requests(rows)


async def _persist_work_items(store: ClickHouseStore, items: list[dict]) -> None:
    rows = []
    for item in items:
        item.pop("_repo_url", None)
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        rows.append(item)
    if rows:
        await store.insert_work_items(rows)


async def _persist_deployments(store: ClickHouseStore, items: list[dict]) -> None:
    rows = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        item["repo_id"] = _repo_id_from_url(repo_url)
        rows.append(item)
    if rows:
        await store.insert_deployments(rows)


async def _persist_incidents(store: ClickHouseStore, items: list[dict]) -> None:
    rows = []
    for item in items:
        repo_url = item.pop("_repo_url", "")
        item.pop("_org_id", None)
        item.pop("_ingestion_id", None)
        item["repo_id"] = _repo_id_from_url(repo_url)
        rows.append(item)
    if rows:
        await store.insert_incidents(rows)
