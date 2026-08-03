"""Canonical persisted-row encoder for native repository syncs."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


def _normalize_uuid(value: Any) -> uuid.UUID:
    if value is None:
        raise ValueError("UUID value is required")
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _normalize_datetime(value: Any) -> Any:
    if value is None or not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def repository_json_or_none(value: Any) -> str | None:
    """Encode repository JSON fields in the cross-runtime canonical form."""
    if value is None:
        return None
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


def build_repository_insert_row(
    repo: object,
    *,
    synced_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the exact row passed by ``ClickHouseStore.insert_repo``."""
    normalized_synced_at = _normalize_datetime(synced_at or datetime.now(timezone.utc))
    created_at = (
        _normalize_datetime(getattr(repo, "created_at", None)) or normalized_synced_at
    )
    tags = getattr(repo, "repo_tags", None)
    if tags is None:
        tags = getattr(repo, "tags", None)
    source_id = getattr(repo, "source_id", None)
    return {
        "id": str(_normalize_uuid(getattr(repo, "id", None))),
        "repo": getattr(repo, "repo"),
        "ref": getattr(repo, "ref", None),
        "created_at": created_at,
        "settings": repository_json_or_none(getattr(repo, "settings", None)),
        "tags": repository_json_or_none(tags),
        "provider": getattr(repo, "provider", None) or "unknown",
        "last_synced": normalized_synced_at,
        "source_id": str(_normalize_uuid(source_id)) if source_id is not None else None,
    }
