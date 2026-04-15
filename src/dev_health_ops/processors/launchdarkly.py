"""
LaunchDarkly processor — normalizes flag and audit-log data into
canonical feature-flag records for sink persistence.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

try:
    from dev_health_ops.metrics.schemas import (
        FeatureFlagEventRecord,
        FeatureFlagRecord,
    )
except ImportError:
    # Will be available when branches merge; define compatible namedtuples
    class FeatureFlagRecord(NamedTuple):  # type: ignore[no-redef]
        org_id: str
        flag_key: str
        flag_name: str
        project_key: str
        kind: str
        status: str
        tags: list[str]
        created_at: datetime | None
        source: str
        dedupe_key: str

    class FeatureFlagEventRecord(NamedTuple):  # type: ignore[no-redef]
        org_id: str
        flag_key: str
        event_kind: str
        actor: str
        timestamp: datetime | None
        description: str
        source: str
        source_event_id: str
        dedupe_key: str


_EVENT_KIND_MAP: dict[str, str] = {
    "createFlag": "create",
    "updateFlag": "update",
    "toggleFlag": "toggle",
    "updateFlagVariations": "rule",
    "updateFlagDefaultRule": "rollout",
}

_SOURCE = "launchdarkly"


def _parse_iso(value: str | int | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        except (OSError, ValueError):
            return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def normalize_flags(
    flags: list[dict[str, Any]],
    org_id: str,
) -> list[FeatureFlagRecord]:
    """Map raw LaunchDarkly flag dicts to ``FeatureFlagRecord`` instances."""
    records: list[FeatureFlagRecord] = []
    for flag in flags:
        key = flag.get("key", "")
        env_statuses = flag.get("environments", {})
        status = "active"
        if env_statuses:
            first_env = next(iter(env_statuses.values()), {})
            if isinstance(first_env, dict):
                on = first_env.get("on")
                status = "active" if on else "inactive"

        records.append(
            FeatureFlagRecord(
                org_id=org_id,
                flag_key=key,
                flag_name=flag.get("name", key),
                project_key=flag.get("_projectKey", ""),
                kind=flag.get("kind", "boolean"),
                status=status,
                tags=flag.get("tags", []),
                created_at=_parse_iso(flag.get("creationDate")),
                source=_SOURCE,
                dedupe_key=f"ld:flag:{org_id}:{key}",
            )
        )
    logger.info("Normalized %d flags for org %s", len(records), org_id)
    return records


def normalize_audit_events(
    events: list[dict[str, Any]],
    org_id: str,
) -> list[FeatureFlagEventRecord]:
    """Map raw LaunchDarkly audit-log entries to ``FeatureFlagEventRecord`` instances."""
    records: list[FeatureFlagEventRecord] = []
    for entry in events:
        raw_kind = entry.get("kind", "")
        event_kind = _EVENT_KIND_MAP.get(raw_kind, raw_kind)

        entry_id = str(entry.get("_id", ""))
        member = entry.get("member", {}) or {}
        actor = member.get("email", "") or member.get("_id", "")

        target = entry.get("target", {}) or {}
        flag_key = ""
        if target:
            resources = target.get("resources", [])
            for res in resources:
                if not isinstance(res, str):
                    continue
                if "/flags/" in res:
                    flag_key = res.rsplit("/flags/", 1)[-1]
                    break
                if ":flag/" in res:
                    flag_key = res.rsplit(":flag/", 1)[-1]
                    break
        if not flag_key:
            name = entry.get("name", "")
            if name:
                flag_key = name

        records.append(
            FeatureFlagEventRecord(
                org_id=org_id,
                flag_key=flag_key,
                event_kind=event_kind,
                actor=str(actor),
                timestamp=_parse_iso(entry.get("date")),
                description=entry.get("description", ""),
                source=_SOURCE,
                source_event_id=entry_id,
                dedupe_key=entry_id,
            )
        )
    logger.info("Normalized %d audit events for org %s", len(records), org_id)
    return records
