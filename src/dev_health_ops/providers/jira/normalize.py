from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from dev_health_ops.models.work_items import (
    Sprint,
    Worklog,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping

if TYPE_CHECKING:
    from atlassian import JiraChangelogEvent, JiraIssue, JiraSprint, JiraWorklog


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        # Jira commonly uses "+0000" offsets (no colon); normalize for fromisoformat.
        raw = raw.replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", raw):
            raw = raw[:-2] + ":" + raw[-2:]
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_field(issue: Any, field_name: str) -> Any:
    if isinstance(issue, dict):
        fields = issue.get("fields") if isinstance(issue.get("fields"), dict) else None
        if fields is None:
            return None
        return fields.get(field_name)
    fields = getattr(issue, "fields", None)
    if fields is None:
        return None
    return getattr(fields, field_name, None)


def _parse_sprint(value: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Jira sprint fields vary by instance. Best-effort parsing:
    - list of strings with "id=...,name=..."
    - list of dict-like objects
    - single string
    """
    if not value:
        return None, None
    sprint = None
    if isinstance(value, list):
        sprint = value[-1] if value else None
    else:
        sprint = value

    if isinstance(sprint, dict):
        sid = sprint.get("id")
        name = sprint.get("name")
        return str(sid) if sid is not None else None, str(name) if name else None

    raw = str(sprint)
    # Typical Jira string contains "id=123,name=Sprint 1,".
    sid_match = re.search(r"\bid=(\d+)\b", raw)
    name_match = re.search(r"\bname=([^,\\]]+)", raw)
    sid = sid_match.group(1) if sid_match else None
    name = name_match.group(1).strip() if name_match else None
    return sid, name


def _service_class_from_priority(priority_raw: Optional[str]) -> str:
    if not priority_raw:
        return "standard"
    normalized = str(priority_raw).strip().lower()
    expedite_markers = ("highest", "critical", "blocker", "urgent", "p0", "p1")
    background_markers = ("low", "lowest", "p4", "p5")

    def _matches_marker(text: str, markers: Tuple[str, ...]) -> bool:
        """
        Return True if `text` matches any marker exactly or contains it as a whole word.

        This avoids substring false-positives such as:
        - "p10" matching "p1"
        - "below" matching "low"
        """
        for marker in markers:
            if text == marker:
                return True
            pattern = r"\b" + re.escape(marker) + r"\b"
            if re.search(pattern, text):
                return True
        return False

    if _matches_marker(normalized, expedite_markers):
        return "expedite"
    if _matches_marker(normalized, background_markers):
        return "background"
    return "standard"


def _normalize_relationship_type(raw_value: Optional[str]) -> str:
    if not raw_value:
        return "other"
    normalized = str(raw_value).strip().lower()
    # Check "blocked by" relationships first, using word boundaries to avoid
    # accidental matches on longer words and to capture direction explicitly.
    if re.search(r"\b(?:is\s+)?blocked by\b", normalized):
        return "blocked_by"
    # Then check for "block"/"blocks" as standalone words to avoid matching
    # unrelated strings like "blocker" or "blocking".
    if re.search(r"\bblocks?\b", normalized):
        return "blocks"
    if "relate" in normalized:
        return "relates"
    if "duplicate" in normalized:
        return "duplicates"
    return "other"


def extract_jira_issue_dependencies(
    *,
    issue: Any,
    work_item_id: str,
) -> List[WorkItemDependency]:
    links = _get_field(issue, "issuelinks") or []
    dependencies: List[WorkItemDependency] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        link_type = link.get("type") or {}
        if not isinstance(link_type, dict):
            link_type = {}
        outward_raw = link_type.get("outward") or link_type.get("name")
        inward_raw = link_type.get("inward") or link_type.get("name")

        outward_issue = link.get("outwardIssue")
        inward_issue = link.get("inwardIssue")

        if outward_issue:
            target_key = (
                outward_issue.get("key") if isinstance(outward_issue, dict) else None
            )
            if target_key:
                raw = str(outward_raw or "")
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=work_item_id,
                        target_work_item_id=f"jira:{target_key}",
                        relationship_type=_normalize_relationship_type(raw),
                        relationship_type_raw=raw,
                    )
                )
        if inward_issue:
            source_key = (
                inward_issue.get("key") if isinstance(inward_issue, dict) else None
            )
            if source_key:
                raw = str(inward_raw or "")
                dependencies.append(
                    WorkItemDependency(
                        source_work_item_id=f"jira:{source_key}",
                        target_work_item_id=work_item_id,
                        relationship_type=_normalize_relationship_type(raw),
                        relationship_type_raw=raw,
                    )
                )
    return dependencies


def detect_reopen_events(
    *,
    work_item_id: str,
    transitions: List[WorkItemStatusTransition],
) -> List[WorkItemReopenEvent]:
    events: List[WorkItemReopenEvent] = []
    for transition in transitions:
        if transition.from_status in {
            "done",
            "canceled",
        } and transition.to_status not in {
            "done",
            "canceled",
        }:
            events.append(
                WorkItemReopenEvent(
                    work_item_id=work_item_id,
                    occurred_at=transition.occurred_at,
                    from_status=transition.from_status,
                    to_status=transition.to_status,
                    from_status_raw=transition.from_status_raw,
                    to_status_raw=transition.to_status_raw,
                    actor=transition.actor,
                )
            )
    return events


def jira_comment_to_interaction_event(
    *,
    work_item_id: str,
    comment: Any,
    identity: IdentityResolver,
) -> Optional[WorkItemInteractionEvent]:
    if not isinstance(comment, dict):
        return None
    created_at = _parse_datetime(comment.get("created"))
    if not created_at:
        return None
    author_obj = comment.get("author") or {}
    actor = None
    if author_obj:
        actor = identity.resolve(
            provider="jira",
            email=author_obj.get("emailAddress"),
            account_id=author_obj.get("accountId"),
            display_name=author_obj.get("displayName"),
        )
    body = comment.get("body")
    body_length = len(body) if isinstance(body, str) else 0
    return WorkItemInteractionEvent(
        work_item_id=work_item_id,
        provider="jira",
        interaction_type="comment",
        occurred_at=created_at,
        actor=actor if actor and actor != "unknown" else None,
        body_length=body_length,
    )


def jira_sprint_payload_to_model(payload: Any) -> Optional[Sprint]:
    if not isinstance(payload, dict):
        return None
    sprint_id = payload.get("id")
    if sprint_id is None:
        return None
    return Sprint(
        provider="jira",
        sprint_id=str(sprint_id),
        name=str(payload.get("name")) if payload.get("name") else None,
        state=str(payload.get("state")) if payload.get("state") else None,
        started_at=_parse_datetime(payload.get("startDate")),
        ended_at=_parse_datetime(payload.get("endDate")),
        completed_at=_parse_datetime(payload.get("completeDate")),
    )


def jira_issue_to_work_item(
    *,
    issue: Any,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    repo_id: Optional[Any] = None,
    story_points_field: Optional[str] = None,
    sprint_field: Optional[str] = None,
    epic_link_field: Optional[str] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    """
    Normalize a Jira issue into a WorkItem and status transitions.

    `story_points_field`, `sprint_field`, and `epic_link_field` are instance-specific.
    If not provided, environment variables are used:
    - JIRA_STORY_POINTS_FIELD
    - JIRA_SPRINT_FIELD
    - JIRA_EPIC_LINK_FIELD
    """
    story_points_field = story_points_field or os.getenv("JIRA_STORY_POINTS_FIELD")
    sprint_field = sprint_field or os.getenv("JIRA_SPRINT_FIELD") or "customfield_10020"
    epic_link_field = epic_link_field or os.getenv("JIRA_EPIC_LINK_FIELD")

    key = (
        issue.get("key") if isinstance(issue, dict) else getattr(issue, "key", None)
    ) or ""
    work_item_id = f"jira:{key}"

    project = _get_field(issue, "project")
    if isinstance(project, dict):
        project_key = project.get("key")
        project_id = project.get("id")
    else:
        project_key = getattr(project, "key", None) if project else None
        project_id = getattr(project, "id", None) if project else None

    title = _get_field(issue, "summary") or ""
    description = _get_field(issue, "description")

    status_obj = _get_field(issue, "status")
    if isinstance(status_obj, dict):
        status_raw = status_obj.get("name")
        status_category_key = (
            (status_obj.get("statusCategory") or {})
            if isinstance(status_obj.get("statusCategory"), dict)
            else {}
        ).get("key")
    else:
        status_raw = getattr(status_obj, "name", None) if status_obj else None
        status_category_key = (
            getattr(getattr(status_obj, "statusCategory", None), "key", None)
            if status_obj
            else None
        )

    issue_type_obj = _get_field(issue, "issuetype")
    if isinstance(issue_type_obj, dict):
        type_raw = issue_type_obj.get("name")
    else:
        type_raw = getattr(issue_type_obj, "name", None) if issue_type_obj else None

    labels = list(_get_field(issue, "labels") or [])

    priority_obj = _get_field(issue, "priority")
    if isinstance(priority_obj, dict):
        priority_raw = priority_obj.get("name")
    else:
        priority_raw = getattr(priority_obj, "name", None) if priority_obj else None

    due_at = _parse_datetime(_get_field(issue, "duedate"))
    service_class = _service_class_from_priority(priority_raw)

    normalized_status = status_mapping.normalize_status(
        provider="jira",
        status_raw=status_raw,
        labels=labels,
    )
    # Jira statusCategory=done is a strong hint that the issue is completed even if the status name is custom.
    if normalized_status in {
        "unknown",
        "todo",
        "in_progress",
        "in_review",
        "blocked",
        "backlog",
    }:
        if str(status_category_key or "").lower() == "done":
            normalized_status = "done"
    normalized_type = status_mapping.normalize_type(
        provider="jira",
        type_raw=type_raw,
        labels=labels,
    )

    assignees: List[str] = []
    assignee_obj = _get_field(issue, "assignee")
    if assignee_obj is not None:
        assignees.append(
            identity.resolve(
                provider="jira",
                email=getattr(assignee_obj, "emailAddress", None),
                account_id=getattr(assignee_obj, "accountId", None),
                display_name=getattr(assignee_obj, "displayName", None),
            )
        )

    reporter_obj = _get_field(issue, "reporter")
    reporter = None
    if reporter_obj is not None:
        reporter = identity.resolve(
            provider="jira",
            email=getattr(reporter_obj, "emailAddress", None),
            account_id=getattr(reporter_obj, "accountId", None),
            display_name=getattr(reporter_obj, "displayName", None),
        )

    created_at = _parse_datetime(_get_field(issue, "created")) or datetime.now(
        timezone.utc
    )
    updated_at = _parse_datetime(_get_field(issue, "updated")) or created_at
    closed_at = _parse_datetime(_get_field(issue, "resolutiondate"))

    url = None
    if isinstance(issue, dict):
        url = issue.get("self")
    elif hasattr(issue, "self"):
        url = getattr(issue, "self", None)

    story_points = None
    if story_points_field:
        raw_points = _get_field(issue, story_points_field)
        try:
            story_points = float(raw_points) if raw_points is not None else None
        except Exception:
            story_points = None

    sprint_id = None
    sprint_name = None
    if sprint_field:
        sprint_id, sprint_name = _parse_sprint(_get_field(issue, sprint_field))

    parent_id = None
    parent_obj = _get_field(issue, "parent")
    if parent_obj is not None:
        parent_id = f"jira:{getattr(parent_obj, 'key', None) or ''}" or None

    epic_id = None
    if epic_link_field:
        epic_val = _get_field(issue, epic_link_field)
        if epic_val:
            epic_id = f"jira:{str(epic_val)}"

    # Changelog transitions for started/completed derivation.
    transitions: List[WorkItemStatusTransition] = []
    if isinstance(issue, dict):
        changelog = (
            issue.get("changelog") if isinstance(issue.get("changelog"), dict) else None
        )
        histories = changelog.get("histories") if changelog else None
    else:
        changelog = getattr(issue, "changelog", None)
        histories = getattr(changelog, "histories", None) if changelog else None
    if histories:
        # Jira returns newest-first sometimes; sort by created timestamp.
        def _hist_dt(h: Any) -> datetime:
            created = (
                h.get("created") if isinstance(h, dict) else getattr(h, "created", None)
            )
            return _parse_datetime(created) or datetime.min.replace(tzinfo=timezone.utc)

        for hist in sorted(list(histories), key=_hist_dt):
            occurred_at = (
                _parse_datetime(
                    hist.get("created")
                    if isinstance(hist, dict)
                    else getattr(hist, "created", None)
                )
                or created_at
            )
            author_obj = (
                hist.get("author")
                if isinstance(hist, dict)
                else getattr(hist, "author", None)
            )
            actor = None
            if author_obj is not None:
                actor = identity.resolve(
                    provider="jira",
                    email=author_obj.get("emailAddress")
                    if isinstance(author_obj, dict)
                    else getattr(author_obj, "emailAddress", None),
                    account_id=author_obj.get("accountId")
                    if isinstance(author_obj, dict)
                    else getattr(author_obj, "accountId", None),
                    display_name=author_obj.get("displayName")
                    if isinstance(author_obj, dict)
                    else getattr(author_obj, "displayName", None),
                )
            items = (
                hist.get("items")
                if isinstance(hist, dict)
                else getattr(hist, "items", None)
            )
            for item in items or []:
                field_name = (
                    item.get("field")
                    if isinstance(item, dict)
                    else getattr(item, "field", "")
                )
                if str(field_name or "").lower() != "status":
                    continue
                from_raw = (
                    item.get("fromString")
                    if isinstance(item, dict)
                    else getattr(item, "fromString", None)
                )
                to_raw = (
                    item.get("toString")
                    if isinstance(item, dict)
                    else getattr(item, "toString", None)
                )
                from_norm = status_mapping.normalize_status(
                    provider="jira", status_raw=from_raw, labels=labels
                )
                to_norm = status_mapping.normalize_status(
                    provider="jira", status_raw=to_raw, labels=labels
                )
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=work_item_id,
                        provider="jira",
                        occurred_at=occurred_at,
                        from_status_raw=from_raw,
                        to_status_raw=to_raw,
                        from_status=from_norm,
                        to_status=to_norm,
                        actor=actor,
                    )
                )

    started_at = None
    completed_at = None
    for t in transitions:
        if started_at is None and t.to_status == "in_progress":
            started_at = t.occurred_at
        if completed_at is None and t.to_status in {"done", "canceled"}:
            completed_at = t.occurred_at
            break

    # Fallback for closed issues with no changelog.
    if completed_at is None and normalized_status in {"done", "canceled"}:
        completed_at = closed_at or updated_at

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="jira",
        repo_id=repo_id,
        project_key=str(project_key) if project_key else None,
        project_id=str(project_id) if project_id else None,
        title=str(title),
        description=str(description) if description else None,
        type=normalized_type,
        status=normalized_status,
        status_raw=str(status_raw) if status_raw else None,
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        labels=[str(lbl) for lbl in labels if lbl],
        story_points=story_points,
        sprint_id=sprint_id,
        sprint_name=sprint_name,
        parent_id=parent_id,
        epic_id=epic_id,
        url=url,
        priority_raw=str(priority_raw) if priority_raw else None,
        service_class=service_class,
        due_at=due_at,
    )
    return work_item, transitions


def canonical_jira_issue_to_work_item(
    *,
    issue: "JiraIssue",
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    repo_id: Optional[Any] = None,
) -> WorkItem:
    work_item_id = f"jira:{issue.key}"

    normalized_status = status_mapping.normalize_status(
        provider="jira",
        status_raw=issue.status,
        labels=list(issue.labels),
    )
    normalized_type = status_mapping.normalize_type(
        provider="jira",
        type_raw=issue.issue_type,
        labels=list(issue.labels),
    )

    assignees: List[str] = []
    if issue.assignee is not None:
        resolved = identity.resolve(
            provider="jira",
            email=issue.assignee.email,
            account_id=issue.assignee.account_id,
            display_name=issue.assignee.display_name,
        )
        if resolved and resolved != "unknown":
            assignees.append(resolved)

    reporter = None
    if issue.reporter is not None:
        reporter = identity.resolve(
            provider="jira",
            email=issue.reporter.email,
            account_id=issue.reporter.account_id,
            display_name=issue.reporter.display_name,
        )
        if reporter == "unknown":
            reporter = None

    created_at = _parse_datetime(issue.created_at) or datetime.now(timezone.utc)
    updated_at = _parse_datetime(issue.updated_at) or created_at
    resolved_at = _parse_datetime(issue.resolved_at)

    sprint_id = issue.sprint_ids[0] if issue.sprint_ids else None

    return WorkItem(
        work_item_id=work_item_id,
        provider="jira",
        repo_id=repo_id,
        project_key=issue.project_key,
        project_id=None,
        title=issue.key,
        description=None,
        type=normalized_type,
        status=normalized_status,
        status_raw=issue.status,
        assignees=assignees,
        reporter=reporter,
        created_at=created_at,
        updated_at=updated_at,
        started_at=None,
        completed_at=resolved_at,
        closed_at=resolved_at,
        labels=list(issue.labels),
        story_points=issue.story_points,
        sprint_id=sprint_id,
        sprint_name=None,
        parent_id=None,
        epic_id=None,
        url=None,
        priority_raw=None,
        service_class="standard",
        due_at=None,
    )


def canonical_changelog_to_transitions(
    *,
    issue_key: str,
    changelog_events: List["JiraChangelogEvent"],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    labels: List[str],
) -> List[WorkItemStatusTransition]:
    work_item_id = f"jira:{issue_key}"
    transitions: List[WorkItemStatusTransition] = []

    sorted_events = sorted(changelog_events, key=lambda e: e.created_at)

    for event in sorted_events:
        occurred_at = _parse_datetime(event.created_at) or datetime.now(timezone.utc)

        actor = None
        if event.author is not None:
            actor = identity.resolve(
                provider="jira",
                email=event.author.email,
                account_id=event.author.account_id,
                display_name=event.author.display_name,
            )
            if actor == "unknown":
                actor = None

        for item in event.items:
            if item.field.lower() != "status":
                continue

            from_norm = status_mapping.normalize_status(
                provider="jira",
                status_raw=item.from_string,
                labels=labels,
            )
            to_norm = status_mapping.normalize_status(
                provider="jira",
                status_raw=item.to_string,
                labels=labels,
            )
            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=work_item_id,
                    provider="jira",
                    occurred_at=occurred_at,
                    from_status_raw=item.from_string,
                    to_status_raw=item.to_string,
                    from_status=from_norm,
                    to_status=to_norm,
                    actor=actor,
                )
            )

    return transitions


def derive_started_completed_from_transitions(
    transitions: List[WorkItemStatusTransition],
    normalized_status: str,
    resolved_at: Optional[datetime],
    updated_at: datetime,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    started_at = None
    completed_at = None

    for t in transitions:
        if started_at is None and t.to_status == "in_progress":
            started_at = t.occurred_at
        if completed_at is None and t.to_status in {"done", "canceled"}:
            completed_at = t.occurred_at
            break

    if completed_at is None and normalized_status in {"done", "canceled"}:
        completed_at = resolved_at or updated_at

    return started_at, completed_at


def canonical_worklog_to_model(
    *,
    issue_key: str,
    worklog: "JiraWorklog",
    identity: IdentityResolver,
) -> Worklog:
    author = None
    if worklog.author is not None:
        author = identity.resolve(
            provider="jira",
            email=worklog.author.email,
            account_id=worklog.author.account_id,
            display_name=worklog.author.display_name,
        )
        if author == "unknown":
            author = None

    started_at = _parse_datetime(worklog.started_at) or datetime.now(timezone.utc)
    created_at = _parse_datetime(worklog.created_at) or started_at
    updated_at = _parse_datetime(worklog.updated_at) or created_at

    return Worklog(
        work_item_id=f"jira:{issue_key}",
        provider="jira",
        worklog_id=worklog.worklog_id,
        author=author,
        started_at=started_at,
        time_spent_seconds=worklog.time_spent_seconds,
        created_at=created_at,
        updated_at=updated_at,
    )


def canonical_sprint_to_model(
    *,
    sprint: "JiraSprint",
) -> Sprint:
    started_at = _parse_datetime(sprint.start_at)
    ended_at = _parse_datetime(sprint.end_at)
    completed_at = _parse_datetime(sprint.complete_at)

    return Sprint(
        provider="jira",
        sprint_id=sprint.id,
        name=sprint.name,
        state=sprint.state,
        started_at=started_at,
        ended_at=ended_at,
        completed_at=completed_at,
    )
