"""Shared normalization utilities for provider modules."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, List, Optional, Sequence, Tuple

from dev_health_ops.models.work_items import (
    WorkItemReopenEvent,
    WorkItemStatusTransition,
)
from dev_health_ops.utils.datetime import to_utc

__all__ = [
    "to_utc",
    "parse_iso_datetime",
    "parse_jira_datetime",
    "priority_from_labels",
    "detect_reopen_events_from_transitions",
]

PRIORITY_LABEL_MAP = {
    "priority::critical": ("critical", "expedite"),
    "priority::high": ("high", "fixed_date"),
    "priority::medium": ("medium", "standard"),
    "priority::low": ("low", "intangible"),
    "critical": ("critical", "expedite"),
    "blocker": ("critical", "expedite"),
    "urgent": ("critical", "expedite"),
    "high": ("high", "fixed_date"),
    "medium": ("medium", "standard"),
    "low": ("low", "intangible"),
    "p0": ("critical", "expedite"),
    "p1": ("high", "fixed_date"),
    "p2": ("medium", "standard"),
    "p3": ("low", "intangible"),
    "p4": ("low", "intangible"),
    "priority-critical": ("critical", "expedite"),
    "priority-high": ("high", "fixed_date"),
    "priority-medium": ("medium", "standard"),
    "priority-low": ("low", "intangible"),
    "critical-priority": ("critical", "expedite"),
    "high-priority": ("high", "fixed_date"),
    "medium-priority": ("medium", "standard"),
    "low-priority": ("low", "intangible"),
}


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    """Parse ISO 8601 datetime (GitHub/GitLab Z-suffix format) to UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    raw = str(value).strip()
    if not raw:
        return None

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def parse_jira_datetime(value: Any) -> Optional[datetime]:
    """Parse Jira datetime (handles +0000 offset format without colon) to UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    raw = str(value).strip()
    if not raw:
        return None

    raw = raw.replace("Z", "+00:00")

    if re.search(r"[+-]\d{4}$", raw):
        raw = raw[:-2] + ":" + raw[-2:]

    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def priority_from_labels(labels: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
    """Extract (priority_raw, service_class) from label strings, or (None, None)."""
    for label in labels:
        normalized = label.strip().lower()
        if normalized in PRIORITY_LABEL_MAP:
            return PRIORITY_LABEL_MAP[normalized]
    return None, None


def detect_reopen_events_from_transitions(
    work_item_id: str,
    transitions: List[WorkItemStatusTransition],
) -> List[WorkItemReopenEvent]:
    """Detect reopens: transitions from terminal (done/canceled) to non-terminal status."""
    terminal_statuses = {"done", "canceled"}
    non_terminal_statuses = {"todo", "in_progress"}

    reopen_events: List[WorkItemReopenEvent] = []

    for t in transitions:
        from_status = t.from_status
        to_status = t.to_status

        if from_status in terminal_statuses and to_status in non_terminal_statuses:
            reopen_events.append(
                WorkItemReopenEvent(
                    work_item_id=work_item_id,
                    occurred_at=t.occurred_at,
                    from_status=from_status,
                    to_status=to_status,
                    from_status_raw=t.from_status_raw,
                    to_status_raw=t.to_status_raw,
                    actor=t.actor,
                )
            )

    return reopen_events
