from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusCategory,
    WorkItemStatusTransition,
    WorkItemType,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.normalize_common import to_utc as _to_utc
from dev_health_ops.providers.normalize_helpers import get_nested as _get
from dev_health_ops.providers.status_mapping import StatusMapping

logger = logging.getLogger(__name__)

LINEAR_PRIORITY_MAP: dict[int, tuple[str, str]] = {
    0: ("none", "intangible"),
    1: ("urgent", "expedite"),
    2: ("high", "fixed_date"),
    3: ("medium", "standard"),
    4: ("low", "intangible"),
}

LINEAR_STATE_TYPE_MAP: dict[str, WorkItemStatusCategory] = {
    "backlog": "backlog",
    "unstarted": "todo",
    "started": "in_progress",
    "completed": "done",
    "canceled": "canceled",
    "cancelled": "canceled",
}


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _priority_from_linear(
    priority: int | None,
) -> tuple[str | None, str | None]:
    if priority is None:
        return (None, None)
    return LINEAR_PRIORITY_MAP.get(priority, (None, None))


def _status_from_state_type(state_type: str | None) -> WorkItemStatusCategory:
    if not state_type:
        return "unknown"
    return LINEAR_STATE_TYPE_MAP.get(state_type.lower(), "unknown")


# Linear's GitHub/GitLab integration records a linked PR/MR as an issue
# *attachment* whose ``url`` points at the PR/MR. This is the authoritative
# link (Linear -> source control), unlike a PR body/branch which often only
# references the issue in a bot comment. The URL host+path is provider-agnostic:
# GitHub uses ``/{owner}/{repo}/pull/{n}`` (any host, incl. Enterprise),
# GitLab uses ``/{group/.../project}/-/merge_requests/{n}`` (incl. self-hosted).
_GITHUB_PR_URL_RE = re.compile(r"^/(?P<repo>.+?)/pull/(?P<num>\d+)/?$")
_GITLAB_MR_URL_RE = re.compile(r"^/(?P<project>.+?)/-/merge_requests/(?P<num>\d+)/?$")


def _work_item_id_from_pr_url(url: str | None) -> str | None:
    """Map a linked PR/MR URL to the matching work-item id, or None.

    Returns ``ghpr:{owner}/{repo}#{n}`` for a GitHub PR and
    ``gitlab:{project_path}!{n}`` for a GitLab MR — the same ids those
    providers mint — so the edge resolves directly to the PR/MR work item.
    """
    if not url:
        return None
    try:
        path = urlsplit(str(url)).path
    except ValueError:
        return None
    if not path:
        return None
    gh = _GITHUB_PR_URL_RE.match(path)
    if gh:
        return f"ghpr:{gh.group('repo')}#{gh.group('num')}"
    gl = _GITLAB_MR_URL_RE.match(path)
    if gl:
        return f"gitlab:{gl.group('project')}!{gl.group('num')}"
    return None


def extract_linear_dependencies(
    *, issue: Any, work_item_id: str
) -> list[WorkItemDependency]:
    """Emit PR/MR -> Linear-issue edges from an issue's linked attachments.

    The PR/MR is the EDGE SOURCE and the Linear issue (which carries the team)
    is the TARGET, so the linked-issue inheritance resolver attributes the
    unassigned PR/MR to this issue's team. Provider-agnostic via URL parsing;
    non-PR attachments (Figma, Slack, plain links) are ignored.
    """
    deps: list[WorkItemDependency] = []
    seen: set[str] = set()
    attachments = _get(issue, "attachments", "nodes") or []
    for att in attachments:
        pr_id = _work_item_id_from_pr_url(_get(att, "url"))
        if not pr_id or pr_id in seen:
            continue
        seen.add(pr_id)
        deps.append(
            WorkItemDependency(
                source_work_item_id=pr_id,
                target_work_item_id=work_item_id,
                relationship_type="relates_to",
                relationship_type_raw="linear_attachment",
            )
        )
    # The attachments connection is fetched as a single page; if it is
    # truncated, a PR/MR link beyond the page is silently missed. Surface that
    # so a missed link is observable rather than a silent attribution gap.
    if _get(issue, "attachments", "pageInfo", "hasNextPage"):
        logger.warning(
            "Linear issue %s has more attachments than fetched; a PR/MR link "
            "may be missed for team inheritance.",
            work_item_id,
        )
    return deps


def _type_from_labels(labels: list[str]) -> WorkItemType:
    label_lower = [lbl.lower() for lbl in labels]
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
    issue: dict[str, Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    history: list[dict[str, Any]] | None = None,
) -> tuple[WorkItem, list[WorkItemStatusTransition]]:
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
    labels = [_get(node, "name") for node in label_nodes if _get(node, "name")]

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
    assignees: list[str] = []
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


def linear_cycle_to_sprint(cycle: dict[str, Any]) -> Sprint:
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
    comment: dict[str, Any],
    work_item_id: str,
    identity: IdentityResolver,
) -> WorkItemInteractionEvent | None:
    body = _get(comment, "body") or ""
    if not body:
        return None

    created_at = _to_utc(_parse_iso(_get(comment, "createdAt"))) or datetime.now(
        timezone.utc
    )

    user = _get(comment, "user") or {}
    actor: str | None = identity.resolve(
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
    history: list[dict[str, Any]],
    identity: IdentityResolver,
) -> list[WorkItemStatusTransition]:
    transitions: list[WorkItemStatusTransition] = []

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
    history: list[dict[str, Any]],
    identity: IdentityResolver,
) -> list[WorkItemReopenEvent]:
    reopen_events: list[WorkItemReopenEvent] = []

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
