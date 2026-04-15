"""GitLab feature-flag normalization helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dev_health_ops.metrics.schemas import FeatureFlagEventRecord, FeatureFlagRecord


def _parse_gitlab_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _extract_environment_scopes(flag: dict[str, Any]) -> list[str]:
    scopes: list[str] = []
    for strategy in flag.get("strategies") or []:
        for scope in strategy.get("scopes") or []:
            env_scope = str(scope.get("environment_scope") or "").strip()
            if env_scope and env_scope not in scopes:
                scopes.append(env_scope)
    return scopes or [""]


def normalize_gitlab_feature_flags(
    flags: list[dict[str, Any]],
    *,
    project_key: str,
    org_id: str,
    repo_id: Any = None,
) -> list[FeatureFlagRecord]:
    now = datetime.now(timezone.utc)
    records: list[FeatureFlagRecord] = []
    for flag in flags:
        scopes = _extract_environment_scopes(flag)
        created_at = _parse_gitlab_datetime(flag.get("created_at")) or now
        for environment in scopes:
            records.append(
                FeatureFlagRecord(
                    provider="gitlab",
                    flag_key=str(flag.get("name") or flag.get("key") or ""),
                    project_key=project_key,
                    repo_id=repo_id,
                    environment=environment,
                    flag_type=str(flag.get("version") or "new_version_flag"),
                    created_at=created_at,
                    archived_at=None,
                    last_synced=now,
                    org_id=org_id,
                )
            )
    return records


def snapshot_gitlab_feature_flag_events(
    flags: list[dict[str, Any]],
    *,
    project_key: str,
    org_id: str,
    repo_id: Any = None,
    observed_at: datetime | None = None,
) -> list[FeatureFlagEventRecord]:
    now = observed_at or datetime.now(timezone.utc)
    records: list[FeatureFlagEventRecord] = []
    for flag in flags:
        flag_key = str(flag.get("name") or flag.get("key") or "")
        state = "on" if bool(flag.get("active")) else "off"
        event_ts = _parse_gitlab_datetime(flag.get("updated_at")) or now
        for environment in _extract_environment_scopes(flag):
            records.append(
                FeatureFlagEventRecord(
                    event_type="toggle",
                    flag_key=flag_key,
                    environment=environment,
                    repo_id=repo_id,
                    actor_type="snapshot",
                    prev_state=None,
                    next_state=state,
                    event_ts=event_ts,
                    ingested_at=now,
                    source_event_id=flag_key or None,
                    dedupe_key=f"gitlab:{project_key}:{flag_key}:{environment}:{state}",
                    org_id=org_id,
                )
            )
    return records
