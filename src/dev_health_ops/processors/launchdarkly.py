"""
LaunchDarkly processor — normalizes flag and audit-log data into
canonical feature-flag records for sink persistence.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.metrics.schemas import (
    FeatureFlagEventRecord,
    FeatureFlagRecord,
)

logger = logging.getLogger(__name__)


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
        records.append(
            FeatureFlagRecord(
                provider=_SOURCE,
                flag_key=key,
                project_key=flag.get("_projectKey") or None,
                repo_id=None,
                environment="",
                flag_type=flag.get("kind", "boolean"),
                created_at=_parse_iso(flag.get("creationDate")),
                archived_at=None,
                last_synced=datetime.now(tz=timezone.utc),
                org_id=org_id,
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

        now = datetime.now(tz=timezone.utc)
        records.append(
            FeatureFlagEventRecord(
                event_type=event_kind,
                flag_key=flag_key,
                environment="",
                repo_id=None,
                actor_type=str(actor) if actor else None,
                prev_state=None,
                next_state=None,
                event_ts=_parse_iso(entry.get("date")) or now,
                ingested_at=now,
                source_event_id=entry_id or None,
                dedupe_key=entry_id,
                org_id=org_id,
            )
        )
    logger.info("Normalized %d audit events for org %s", len(records), org_id)
    return records
