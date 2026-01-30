from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, List, Optional, Sequence, Tuple

from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping

logger = logging.getLogger(__name__)

# Priority labels -> (priority_raw, service_class)
_PRIORITY_LABEL_MAP = {
    "priority::critical": ("critical", "expedite"),
    "priority::high": ("high", "fixed_date"),
    "priority::medium": ("medium", "standard"),
    "priority::low": ("low", "intangible"),
    "critical": ("critical", "expedite"),
    "blocker": ("critical", "expedite"),
    "high": ("high", "fixed_date"),
    "medium": ("medium", "standard"),
    "low": ("low", "intangible"),
    "p0": ("critical", "expedite"),
    "p1": ("high", "fixed_date"),
    "p2": ("medium", "standard"),
    "p3": ("low", "intangible"),
    "p4": ("low", "intangible"),
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


def _priority_from_labels(labels: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract priority_raw and service_class from GitLab labels.

    Returns (priority_raw, service_class) or (None, None) if no match.
    """
    for label in labels:
        key = label.lower().strip()
        if key in _PRIORITY_LABEL_MAP:
            return _PRIORITY_LABEL_MAP[key]
    return (None, None)


def _get(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def gitlab_issue_to_work_item(
    *,
    issue: Any,
    project_full_path: str,
    repo_id: Optional[Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    label_events: Optional[Sequence[Any]] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    iid = int(_get(issue, "iid") or 0)
    work_item_id = f"gitlab:{project_full_path}#{iid}"

    title = _get(issue, "title") or ""
    description = _get(issue, "description")
    state = _get(issue, "state") or None  # opened/closed
    created_at = _to_utc(_parse_iso(_get(issue, "created_at"))) or datetime.now(
        timezone.utc
    )
    updated_at = _to_utc(_parse_iso(_get(issue, "updated_at"))) or created_at
    closed_at = _to_utc(_parse_iso(_get(issue, "closed_at")))

    labels = list(_get(issue, "labels") or [])
    labels = [str(lbl) for lbl in labels if lbl]

    normalized_status = status_mapping.normalize_status(
        provider="gitlab",
        status_raw=None,
        labels=labels,
        state=str(state) if state else None,
    )
    normalized_type = status_mapping.normalize_type(
        provider="gitlab",
        type_raw=None,
        labels=labels,
    )

    assignees: List[str] = []
    for a in _get(issue, "assignees") or []:
        assignees.append(
            identity.resolve(
                provider="gitlab",
                email=_get(a, "email"),
                username=_get(a, "username"),
                display_name=_get(a, "name"),
            )
        )

    author_obj = _get(issue, "author")
    reporter = None
    if author_obj is not None:
        reporter = identity.resolve(
            provider="gitlab",
            email=_get(author_obj, "email"),
            username=_get(author_obj, "username"),
            display_name=_get(author_obj, "name"),
        )

    url = _get(issue, "web_url") or _get(issue, "url")

    # Best-effort transitions from label events + state.
    transitions: List[WorkItemStatusTransition] = []
    started_at = None
    completed_at = None

    if label_events:

        def _ev_dt(ev: Any) -> datetime:
            return _to_utc(_parse_iso(_get(ev, "created_at"))) or datetime.min.replace(
                tzinfo=timezone.utc
            )

        prev_status = "unknown"
        for ev in sorted(list(label_events), key=_ev_dt):
            action = str(_get(ev, "action") or "").lower()
            label = _get(ev, "label") or {}
            label_name = _get(label, "name") or _get(ev, "label_name")
            if not label_name:
                continue
            label_name = str(label_name)
            occurred_at = _to_utc(_parse_iso(_get(ev, "created_at"))) or created_at

            if action not in {"add", "remove"}:
                continue
            mapped = status_mapping.normalize_status(
                provider="gitlab",
                status_raw=None,
                labels=[label_name] if action == "add" else (),
                state=None,
            )
            if mapped == "unknown":
                continue
            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=work_item_id,
                    provider="gitlab",
                    occurred_at=occurred_at,
                    from_status_raw=None,
                    to_status_raw=label_name,
                    from_status=prev_status,  # type: ignore[arg-type]
                    to_status=mapped,
                    actor=None,
                )
            )
            prev_status = mapped

        for t in transitions:
            if started_at is None and t.to_status == "in_progress":
                started_at = t.occurred_at
            if completed_at is None and t.to_status in {"done", "canceled"}:
                completed_at = t.occurred_at
                break

    if completed_at is None and closed_at is not None:
        completed_at = closed_at

    weight = _get(issue, "weight")
    story_points = float(weight) if weight is not None else None

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="gitlab",
        repo_id=repo_id,
        project_key=None,
        # For work tracking metrics, treat the GitLab project path as the "project" scope.
        project_id=str(project_full_path)
        if project_full_path
        else (str(_get(issue, "project_id")) if _get(issue, "project_id") else None),
        title=str(title),
        description=str(description) if description else None,
        type=normalized_type,
        status=normalized_status,
        status_raw=str(state) if state else None,
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        labels=labels,
        story_points=story_points,
        sprint_id=str(_get(_get(issue, "milestone"), "id"))
        if _get(issue, "milestone")
        else None,
        sprint_name=_get(_get(issue, "milestone"), "title")
        if _get(issue, "milestone")
        else None,
        url=url,
    )
    return work_item, transitions


def gitlab_mr_to_work_item(
    *,
    mr: Any,
    project_full_path: str,
    repo_id: Optional[Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    state_events: Optional[Sequence[Any]] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    """
    Convert a GitLab merge request to a normalized WorkItem.

    MRs are treated as work items with type "merge_request".
    State events are used for transitions (opened->merged/closed).
    """
    iid = int(_get(mr, "iid") or 0)
    work_item_id = f"gitlab:{project_full_path}!{iid}"  # ! for MRs

    title = _get(mr, "title") or ""
    description = _get(mr, "description")
    state = _get(mr, "state") or None  # opened/merged/closed
    created_at = _to_utc(_parse_iso(_get(mr, "created_at"))) or datetime.now(
        timezone.utc
    )
    updated_at = _to_utc(_parse_iso(_get(mr, "updated_at"))) or created_at
    merged_at = _to_utc(_parse_iso(_get(mr, "merged_at")))
    closed_at = _to_utc(_parse_iso(_get(mr, "closed_at")))

    labels = list(_get(mr, "labels") or [])
    labels = [str(lb) for lb in labels if lb]

    # MRs use state-based status
    status_raw = str(state) if state else "unknown"
    if status_raw == "merged":
        normalized_status = "done"
    elif status_raw == "closed":
        normalized_status = "canceled"
    elif status_raw == "opened":
        normalized_status = "in_progress"
    else:
        normalized_status = "unknown"

    # Priority from labels
    priority_raw, service_class = _priority_from_labels(labels)

    assignees: List[str] = []
    for a in _get(mr, "assignees") or []:
        assignees.append(
            identity.resolve(
                provider="gitlab",
                email=_get(a, "email"),
                username=_get(a, "username"),
                display_name=_get(a, "name"),
            )
        )

    author_obj = _get(mr, "author")
    reporter = None
    if author_obj is not None:
        reporter = identity.resolve(
            provider="gitlab",
            email=_get(author_obj, "email"),
            username=_get(author_obj, "username"),
            display_name=_get(author_obj, "name"),
        )

    url = _get(mr, "web_url") or _get(mr, "url")

    # Transitions from state events
    transitions: List[WorkItemStatusTransition] = []
    started_at = created_at  # MRs start when opened
    completed_at = merged_at or closed_at

    if state_events:
        prev_status = "unknown"
        for ev in sorted(
            state_events,
            key=lambda e: (
                _to_utc(_parse_iso(_get(e, "created_at")))
                or datetime.min.replace(tzinfo=timezone.utc)
            ),
        ):
            ev_state = str(_get(ev, "state") or "").lower()
            occurred_at = _to_utc(_parse_iso(_get(ev, "created_at"))) or created_at

            if ev_state == "merged":
                to_status = "done"
            elif ev_state == "closed":
                to_status = "canceled"
            elif ev_state == "opened":
                to_status = "in_progress"
            elif ev_state == "reopened":
                to_status = "in_progress"
            else:
                continue

            user_obj = _get(ev, "user")
            actor = None
            if user_obj:
                actor = identity.resolve(
                    provider="gitlab",
                    email=_get(user_obj, "email"),
                    username=_get(user_obj, "username"),
                    display_name=_get(user_obj, "name"),
                )

            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=work_item_id,
                    provider="gitlab",
                    occurred_at=occurred_at,
                    from_status_raw=None,
                    to_status_raw=ev_state,
                    from_status=prev_status,
                    to_status=to_status,
                    actor=actor,
                )
            )
            prev_status = to_status

    weight = _get(mr, "weight")
    story_points = float(weight) if weight is not None else None

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="gitlab",
        repo_id=repo_id,
        project_key=None,
        project_id=str(project_full_path) if project_full_path else None,
        title=str(title),
        description=str(description) if description else None,
        type="merge_request",
        status=normalized_status,
        status_raw=status_raw,
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at or merged_at,
        labels=labels,
        story_points=story_points,
        sprint_id=str(_get(_get(mr, "milestone"), "id"))
        if _get(mr, "milestone")
        else None,
        sprint_name=_get(_get(mr, "milestone"), "title")
        if _get(mr, "milestone")
        else None,
        url=url,
        priority_raw=priority_raw,
        service_class=service_class,
    )
    return work_item, transitions


def detect_gitlab_reopen_events(
    *,
    work_item_id: str,
    state_events: Sequence[Any],
    identity: IdentityResolver,
) -> List[WorkItemReopenEvent]:
    """
    Detect reopen events from GitLab resource_state_events.

    A reopen occurs when state changes to "reopened".
    """
    events: List[WorkItemReopenEvent] = []
    for ev in state_events:
        ev_state = str(_get(ev, "state") or "").lower()
        if ev_state != "reopened":
            continue

        occurred_at = _to_utc(_parse_iso(_get(ev, "created_at")))
        if not occurred_at:
            continue

        user_obj = _get(ev, "user")
        actor = None
        if user_obj:
            actor = identity.resolve(
                provider="gitlab",
                email=_get(user_obj, "email"),
                username=_get(user_obj, "username"),
                display_name=_get(user_obj, "name"),
            )

        events.append(
            WorkItemReopenEvent(
                work_item_id=work_item_id,
                occurred_at=occurred_at,
                from_status="done",  # Reopen implies was closed/done
                to_status="in_progress",
                from_status_raw="closed",
                to_status_raw="reopened",
                actor=actor,
            )
        )
    return events


def gitlab_note_to_interaction_event(
    *,
    note: Any,
    work_item_id: str,
    identity: IdentityResolver,
) -> Optional[WorkItemInteractionEvent]:
    """
    Convert a GitLab note (comment/discussion) to an interaction event.

    System notes are excluded unless they indicate meaningful work.
    """
    if _get(note, "system"):
        # Skip system-generated notes (label changes, assignments, etc.)
        return None

    body = _get(note, "body") or ""
    occurred_at = _to_utc(_parse_iso(_get(note, "created_at")))
    if not occurred_at:
        return None

    author_obj = _get(note, "author")
    actor = None
    if author_obj:
        actor = identity.resolve(
            provider="gitlab",
            email=_get(author_obj, "email"),
            username=_get(author_obj, "username"),
            display_name=_get(author_obj, "name"),
        )

    return WorkItemInteractionEvent(
        work_item_id=work_item_id,
        provider="gitlab",
        interaction_type="comment",
        occurred_at=occurred_at,
        actor=actor,
        body_length=len(body),
    )


# Regex patterns for GitLab issue references
_GITLAB_ISSUE_REF_PATTERN = re.compile(
    r"(?:^|[^\w])(?:(?P<project>[\w/-]+)?#(?P<iid>\d+))",
    re.MULTILINE,
)
_BLOCKING_KEYWORDS = {"blocks", "blocked by", "is blocked by", "blocking"}


def extract_gitlab_dependencies(
    *,
    work_item_id: str,
    issue: Any,
    project_full_path: str,
    linked_issues: Optional[Sequence[Any]] = None,
) -> List[WorkItemDependency]:
    """
    Extract dependency edges from GitLab issue links and description.

    GitLab has explicit issue links (via API) and implicit references in description.
    """
    dependencies: List[WorkItemDependency] = []
    seen_targets: set[str] = set()

    # Process explicit links from API
    if linked_issues:
        for link in linked_issues:
            link_type = str(_get(link, "link_type") or "relates_to").lower()
            target_iid = _get(link, "iid")

            if not target_iid:
                continue

            # Build target work_item_id
            # Linked issues might be from same or different project
            refs = _get(link, "references")
            target_path = (
                _get(refs, "full") if isinstance(refs, dict) else None
            ) or project_full_path
            if target_path and "#" in str(target_path):
                # Extract project from "group/project#123" format
                target_path = str(target_path).split("#")[0]

            target_id = f"gitlab:{target_path}#{target_iid}"
            if target_id in seen_targets:
                continue
            seen_targets.add(target_id)

            # Map GitLab link types
            if link_type in {"blocks", "is_blocked_by"}:
                relationship = "blocks" if link_type == "blocks" else "blocked_by"
            else:
                relationship = "relates_to"

            dependencies.append(
                WorkItemDependency(
                    source_work_item_id=work_item_id,
                    target_work_item_id=target_id,
                    relationship_type=relationship,
                    relationship_type_raw=link_type,
                )
            )

    # Parse description for implicit references (best effort)
    description = _get(issue, "description") or ""
    if description:
        for match in _GITLAB_ISSUE_REF_PATTERN.finditer(str(description)):
            ref_project = match.group("project") or project_full_path
            ref_iid = match.group("iid")
            if not ref_iid:
                continue

            target_id = f"gitlab:{ref_project}#{ref_iid}"
            if target_id in seen_targets or target_id == work_item_id:
                continue
            seen_targets.add(target_id)

            # Check if reference is near blocking keywords
            start = max(0, match.start() - 50)
            context = description[start : match.end()].lower()
            relationship = "relates_to"
            relationship_raw = "description_reference"
            for kw in _BLOCKING_KEYWORDS:
                if kw in context:
                    relationship = "blocked_by" if "by" in kw else "blocks"
                    relationship_raw = kw
                    break

            dependencies.append(
                WorkItemDependency(
                    source_work_item_id=work_item_id,
                    target_work_item_id=target_id,
                    relationship_type=relationship,
                    relationship_type_raw=relationship_raw,
                )
            )

    return dependencies


def gitlab_milestone_to_sprint(
    *,
    milestone: Any,
    project_full_path: str,
) -> Sprint:
    """
    Convert a GitLab milestone to a Sprint model.

    GitLab milestones serve as sprint boundaries.
    """
    ms_id = _get(milestone, "id")
    title = _get(milestone, "title") or ""
    state = _get(milestone, "state") or "active"

    start_date = _to_utc(_parse_iso(_get(milestone, "start_date")))
    due_date = _to_utc(_parse_iso(_get(milestone, "due_date")))

    # Determine sprint state
    if state == "closed":
        sprint_state = "closed"
    elif state == "active":
        sprint_state = "active"
    else:
        sprint_state = "future"

    return Sprint(
        sprint_id=f"gitlab:{project_full_path}:milestone:{ms_id}",
        provider="gitlab",
        name=str(title),
        state=sprint_state,
        started_at=start_date,
        ended_at=due_date,
        completed_at=due_date if state == "closed" else None,
    )


def enrich_work_item_with_priority(
    work_item: WorkItem,
    labels: Sequence[str],
) -> WorkItem:
    """
    Enrich a WorkItem with priority_raw and service_class from labels.

    Returns a new WorkItem with updated fields.
    """
    if work_item.priority_raw is not None:
        return work_item

    priority_raw, service_class = _priority_from_labels(labels)
    if priority_raw is None:
        return work_item

    return WorkItem(
        work_item_id=work_item.work_item_id,
        provider=work_item.provider,
        repo_id=work_item.repo_id,
        project_key=work_item.project_key,
        project_id=work_item.project_id,
        title=work_item.title,
        description=work_item.description,
        type=work_item.type,
        status=work_item.status,
        status_raw=work_item.status_raw,
        assignees=work_item.assignees,
        reporter=work_item.reporter,
        created_at=work_item.created_at,
        updated_at=work_item.updated_at,
        started_at=work_item.started_at,
        completed_at=work_item.completed_at,
        closed_at=work_item.closed_at,
        labels=work_item.labels,
        story_points=work_item.story_points,
        sprint_id=work_item.sprint_id,
        sprint_name=work_item.sprint_name,
        url=work_item.url,
        priority_raw=priority_raw,
        service_class=service_class,
    )


def gitlab_epic_to_work_item(
    *,
    epic: Any,
    group_full_path: str,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    state_events: Optional[Sequence[Any]] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    """Convert a GitLab Epic to a normalized WorkItem. Epics are group-level."""
    iid = int(_get(epic, "iid") or 0)
    work_item_id = f"gitlab:{group_full_path}:epic:{iid}"

    title = _get(epic, "title") or ""
    description = _get(epic, "description")
    state = _get(epic, "state") or None
    created_at = _to_utc(_parse_iso(_get(epic, "created_at"))) or datetime.now(
        timezone.utc
    )
    updated_at = _to_utc(_parse_iso(_get(epic, "updated_at"))) or created_at
    closed_at = _to_utc(_parse_iso(_get(epic, "closed_at")))
    start_date = _to_utc(_parse_iso(_get(epic, "start_date")))
    due_date = _to_utc(_parse_iso(_get(epic, "due_date")))

    labels = list(_get(epic, "labels") or [])
    labels = [str(lbl) for lbl in labels if lbl]

    normalized_status = status_mapping.normalize_status(
        provider="gitlab",
        status_raw=None,
        labels=labels,
        state=str(state) if state else None,
    )

    author_obj = _get(epic, "author")
    reporter = None
    if author_obj is not None:
        reporter = identity.resolve(
            provider="gitlab",
            email=_get(author_obj, "email"),
            username=_get(author_obj, "username"),
            display_name=_get(author_obj, "name"),
        )

    url = _get(epic, "web_url") or _get(epic, "url")

    transitions: List[WorkItemStatusTransition] = []
    started_at = start_date
    completed_at = closed_at

    if state_events:
        prev_status = "unknown"
        for ev in sorted(
            state_events,
            key=lambda e: (
                _to_utc(_parse_iso(_get(e, "created_at")))
                or datetime.min.replace(tzinfo=timezone.utc)
            ),
        ):
            ev_state = str(_get(ev, "state") or "").lower()
            occurred_at = _to_utc(_parse_iso(_get(ev, "created_at"))) or created_at

            if ev_state == "closed":
                to_status = "done"
            elif ev_state == "opened":
                to_status = "todo"
            elif ev_state == "reopened":
                to_status = "todo"
            else:
                continue

            user_obj = _get(ev, "user")
            actor = None
            if user_obj:
                actor = identity.resolve(
                    provider="gitlab",
                    email=_get(user_obj, "email"),
                    username=_get(user_obj, "username"),
                    display_name=_get(user_obj, "name"),
                )

            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=work_item_id,
                    provider="gitlab",
                    occurred_at=occurred_at,
                    from_status_raw=None,
                    to_status_raw=ev_state,
                    from_status=prev_status,
                    to_status=to_status,
                    actor=actor,
                )
            )
            prev_status = to_status

    priority_raw, service_class = _priority_from_labels(labels)

    parent_epic_id = _get(epic, "parent_id") or _get(epic, "parent_iid")
    parent_id = None
    if parent_epic_id:
        parent_id = f"gitlab:{group_full_path}:epic:{parent_epic_id}"

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="gitlab",
        repo_id=None,
        project_key=None,
        project_id=str(group_full_path),
        title=str(title),
        description=str(description) if description else None,
        type="epic",
        status=normalized_status,
        status_raw=str(state) if state else None,
        assignees=[],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        due_at=due_date,
        labels=labels,
        story_points=None,
        sprint_id=None,
        sprint_name=None,
        url=url,
        priority_raw=priority_raw,
        service_class=service_class,
        epic_id=parent_id,
    )
    return work_item, transitions


def build_epic_id_for_issue(
    *,
    issue: Any,
    group_full_path: str,
) -> Optional[str]:
    """Build epic_id for an issue that belongs to an epic."""
    epic = _get(issue, "epic")
    if epic is None:
        return None

    epic_iid = _get(epic, "iid")
    if epic_iid is None:
        return None

    epic_group = _get(epic, "group_id") or group_full_path
    return f"gitlab:{epic_group}:epic:{epic_iid}"
