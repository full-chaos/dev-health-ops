from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusCategory,
    WorkItemStatusTransition,
    WorkItemType,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping

logger = logging.getLogger(__name__)

LINEAR_PRIORITY_MAP: Dict[int, Tuple[str, str]] = {
    0: ("none", "intangible"),
    1: ("urgent", "expedite"),
    2: ("high", "fixed_date"),
    3: ("medium", "standard"),
    4: ("low", "intangible"),
}

LINEAR_STATE_TYPE_MAP: Dict[str, WorkItemStatusCategory] = {
    "backlog": "backlog",
    "unstarted": "todo",
    "started": "in_progress",
    "completed": "done",
    "canceled": "canceled",
    "cancelled": "canceled",
}


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get(obj: Any, *keys: str) -> Any:
    for key in keys:
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            obj = getattr(obj, key, None)
    return obj


def _priority_from_linear(
    priority: Optional[int],
) -> Tuple[Optional[str], Optional[str]]:
    if priority is None:
        return (None, None)
    return LINEAR_PRIORITY_MAP.get(priority, (None, None))


def _status_from_state_type(state_type: Optional[str]) -> WorkItemStatusCategory:
    if not state_type:
        return "unknown"
    return LINEAR_STATE_TYPE_MAP.get(state_type.lower(), "unknown")


def _type_from_labels(labels: List[str]) -> WorkItemType:
    label_lower = [l.lower() for l in labels]
    if "bug" in label_lower or "type:bug" in label_lower:
        return "bug"
    if "incident" in label_lower:
        return "incident"
    if "epic" in label_lower:
        return "epic"
    if "story" in label_lower or "feature" in label_lower:
        return "story"
    if "chore" in label_lower or "maintenance" in label_lower:
        return "chore"
    return "task"


def linear_issue_to_work_item(
    *,
    issue: Dict[str, Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    history: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    identifier = _get(issue, "identifier") or ""
    work_item_id = f"linear:{identifier}"

    title = _get(issue, "title") or ""
    description = _get(issue, "description")
    priority = _get(issue, "priority")
    estimate = _get(issue, "estimate")

    created_at = _to_utc(_parse_iso(_get(issue, "createdAt"))) or datetime.now(
        timezone.utc
    )
    updated_at = _to_utc(_parse_iso(_get(issue, "updatedAt"))) or created_at
    started_at = _to_utc(_parse_iso(_get(issue, "startedAt")))
    completed_at = _to_utc(_parse_iso(_get(issue, "completedAt")))
    canceled_at = _to_utc(_parse_iso(_get(issue, "canceledAt")))
    due_date = _to_utc(_parse_iso(_get(issue, "dueDate")))

    closed_at = completed_at or canceled_at

    state = _get(issue, "state") or {}
    state_name = _get(state, "name")
    state_type = _get(state, "type")

    label_nodes = _get(issue, "labels", "nodes") or []
    labels = [_get(l, "name") for l in label_nodes if _get(l, "name")]

    normalized_status = status_mapping.normalize_status(
        provider="linear",
        status_raw=state_name,
        labels=labels,
        state=state_type,
    )
    if normalized_status == "unknown" and state_type:
        normalized_status = _status_from_state_type(state_type)

    normalized_type = status_mapping.normalize_type(
        provider="linear",
        type_raw=None,
        labels=labels,
    )
    if normalized_type == "unknown":
        normalized_type = _type_from_labels(labels)

    assignee_obj = _get(issue, "assignee")
    assignees: List[str] = []
    if assignee_obj:
        resolved = identity.resolve(
            provider="linear",
            email=_get(assignee_obj, "email"),
            username=None,
            display_name=_get(assignee_obj, "name"),
        )
        if resolved and resolved != "unknown":
            assignees.append(resolved)

    creator_obj = _get(issue, "creator")
    reporter = None
    if creator_obj:
        reporter = identity.resolve(
            provider="linear",
            email=_get(creator_obj, "email"),
            username=None,
            display_name=_get(creator_obj, "name"),
        )
        if reporter == "unknown":
            reporter = None

    url = _get(issue, "url")

    team = _get(issue, "team") or {}
    team_key = _get(team, "key")

    project = _get(issue, "project") or {}
    project_name = _get(project, "name")

    cycle = _get(issue, "cycle") or {}
    cycle_id = _get(cycle, "id")
    cycle_name = _get(cycle, "name") or _get(cycle, "number")
    sprint_id = f"linear:cycle:{cycle_id}" if cycle_id else None
    sprint_name = str(cycle_name) if cycle_name else None

    parent = _get(issue, "parent") or {}
    parent_identifier = _get(parent, "identifier")
    parent_id = f"linear:{parent_identifier}" if parent_identifier else None

    priority_raw, service_class = _priority_from_linear(priority)

    transitions = extract_linear_status_transitions(
        work_item_id=work_item_id,
        history=history or [],
        identity=identity,
    )

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="linear",
        repo_id=None,
        project_key=team_key,
        project_id=project_name,
        title=str(title),
        description=str(description) if description else None,
        type=normalized_type,
        status=normalized_status,
        status_raw=state_name,
        assignees=assignees,
        reporter=reporter,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        due_at=due_date,
        labels=labels,
        story_points=float(estimate) if estimate else None,
        sprint_id=sprint_id,
        sprint_name=sprint_name,
        parent_id=parent_id,
        epic_id=None,
        url=url,
        priority_raw=priority_raw,
        service_class=service_class,
    )
    return work_item, transitions


def linear_cycle_to_sprint(cycle: Dict[str, Any]) -> Sprint:
    cycle_id = _get(cycle, "id") or ""
    sprint_id = f"linear:cycle:{cycle_id}"

    name = _get(cycle, "name")
    number = _get(cycle, "number")
    if not name and number:
        name = f"Cycle {number}"

    starts_at = _to_utc(_parse_iso(_get(cycle, "startsAt")))
    ends_at = _to_utc(_parse_iso(_get(cycle, "endsAt")))
    completed_at = _to_utc(_parse_iso(_get(cycle, "completedAt")))

    progress = _get(cycle, "progress")
    if completed_at:
        state = "closed"
    elif progress is not None and progress > 0:
        state = "active"
    else:
        state = "future"

    return Sprint(
        provider="linear",
        sprint_id=sprint_id,
        name=name,
        state=state,
        started_at=starts_at,
        ended_at=ends_at,
        completed_at=completed_at,
    )


def linear_comment_to_interaction_event(
    *,
    comment: Dict[str, Any],
    work_item_id: str,
    identity: IdentityResolver,
) -> Optional[WorkItemInteractionEvent]:
    body = _get(comment, "body") or ""
    if not body:
        return None

    created_at = _to_utc(_parse_iso(_get(comment, "createdAt"))) or datetime.now(
        timezone.utc
    )

    user = _get(comment, "user") or {}
    actor = identity.resolve(
        provider="linear",
        email=_get(user, "email"),
        username=None,
        display_name=_get(user, "name"),
    )
    if actor == "unknown":
        actor = None

    return WorkItemInteractionEvent(
        work_item_id=work_item_id,
        provider="linear",
        interaction_type="comment",
        occurred_at=created_at,
        actor=actor,
        body_length=len(body),
    )


def extract_linear_status_transitions(
    *,
    work_item_id: str,
    history: List[Dict[str, Any]],
    identity: IdentityResolver,
) -> List[WorkItemStatusTransition]:
    transitions: List[WorkItemStatusTransition] = []

    for entry in history:
        from_state = _get(entry, "fromState")
        to_state = _get(entry, "toState")

        if not from_state and not to_state:
            continue

        from_state_name = _get(from_state, "name") if from_state else None
        from_state_type = _get(from_state, "type") if from_state else None
        to_state_name = _get(to_state, "name") if to_state else None
        to_state_type = _get(to_state, "type") if to_state else None

        if not to_state_name:
            continue

        occurred_at = _to_utc(_parse_iso(_get(entry, "createdAt"))) or datetime.now(
            timezone.utc
        )

        from_status = (
            _status_from_state_type(from_state_type) if from_state_type else "unknown"
        )
        to_status = (
            _status_from_state_type(to_state_type) if to_state_type else "unknown"
        )

        actor_obj = _get(entry, "actor")
        actor = None
        if actor_obj:
            actor = identity.resolve(
                provider="linear",
                email=_get(actor_obj, "email"),
                username=None,
                display_name=_get(actor_obj, "name"),
            )
            if actor == "unknown":
                actor = None

        transitions.append(
            WorkItemStatusTransition(
                work_item_id=work_item_id,
                provider="linear",
                occurred_at=occurred_at,
                from_status_raw=from_state_name,
                to_status_raw=to_state_name,
                from_status=from_status,
                to_status=to_status,
                actor=actor,
            )
        )

    return transitions


def detect_linear_reopen_events(
    *,
    work_item_id: str,
    history: List[Dict[str, Any]],
    identity: IdentityResolver,
) -> List[WorkItemReopenEvent]:
    reopen_events: List[WorkItemReopenEvent] = []

    for entry in history:
        from_state = _get(entry, "fromState")
        to_state = _get(entry, "toState")

        if not from_state or not to_state:
            continue

        from_state_type = _get(from_state, "type")
        to_state_type = _get(to_state, "type")

        from_completed = from_state_type in ("completed", "canceled", "cancelled")
        to_active = to_state_type in ("backlog", "unstarted", "started")

        if from_completed and to_active:
            occurred_at = _to_utc(_parse_iso(_get(entry, "createdAt"))) or datetime.now(
                timezone.utc
            )

            from_status = _status_from_state_type(from_state_type)
            to_status = _status_from_state_type(to_state_type)

            actor_obj = _get(entry, "actor")
            actor = None
            if actor_obj:
                actor = identity.resolve(
                    provider="linear",
                    email=_get(actor_obj, "email"),
                    username=None,
                    display_name=_get(actor_obj, "name"),
                )
                if actor == "unknown":
                    actor = None

            reopen_events.append(
                WorkItemReopenEvent(
                    work_item_id=work_item_id,
                    occurred_at=occurred_at,
                    from_status=from_status,
                    to_status=to_status,
                    from_status_raw=_get(from_state, "name"),
                    to_status_raw=_get(to_state, "name"),
                    actor=actor,
                )
            )

    return reopen_events
