from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
import logging

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


def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _labels_from_nodes(nodes: Any) -> List[str]:
    labels: List[str] = []
    for node in nodes or []:
        name = (
            (node or {}).get("name")
            if isinstance(node, dict)
            else getattr(node, "name", None)
        )
        if name:
            labels.append(str(name))
    return labels


def _update_transitions_work_item_id(
    transitions: List[WorkItemStatusTransition],
    work_item_id: str,
) -> List[WorkItemStatusTransition]:
    """
    Update work_item_id in a list of transitions.

    Creates new WorkItemStatusTransition instances with the updated work_item_id
    while preserving all other fields.
    """
    return [
        WorkItemStatusTransition(
            work_item_id=work_item_id,
            provider=t.provider,
            occurred_at=t.occurred_at,
            from_status_raw=t.from_status_raw,
            to_status_raw=t.to_status_raw,
            from_status=t.from_status,
            to_status=t.to_status,
            actor=t.actor,
        )
        for t in transitions
    ]


def github_issue_to_work_item(
    *,
    issue: Any,
    repo_full_name: str,
    repo_id: Optional[Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    events: Optional[Sequence[Any]] = None,
    project_status_raw: Optional[str] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    number = int(getattr(issue, "number", 0) or 0)
    work_item_id = f"gh:{repo_full_name}#{number}"

    title = getattr(issue, "title", "") or ""
    description = getattr(issue, "body", None)
    state = getattr(issue, "state", None)
    created_at = _to_utc(getattr(issue, "created_at", None)) or datetime.now(
        timezone.utc
    )
    updated_at = _to_utc(getattr(issue, "updated_at", None)) or created_at
    closed_at = _to_utc(getattr(issue, "closed_at", None))

    labels = [getattr(lbl, "name", None) for lbl in getattr(issue, "labels", []) or []]
    labels = [str(lbl) for lbl in labels if lbl]

    # If the issue is in a Project with a status field, prefer that as status_raw.
    status_raw = project_status_raw
    normalized_status = status_mapping.normalize_status(
        provider="github",
        status_raw=status_raw,
        labels=() if status_raw else labels,
        state=str(state) if state else None,
    )
    normalized_type = status_mapping.normalize_type(
        provider="github",
        type_raw=None,
        labels=labels,
    )

    assignees: List[str] = []
    for a in getattr(issue, "assignees", []) or []:
        assignees.append(
            identity.resolve(
                provider="github",
                email=getattr(a, "email", None),
                username=getattr(a, "login", None),
                display_name=getattr(a, "name", None),
            )
        )

    reporter_obj = getattr(issue, "user", None)
    reporter = None
    if reporter_obj is not None:
        reporter = identity.resolve(
            provider="github",
            email=getattr(reporter_obj, "email", None),
            username=getattr(reporter_obj, "login", None),
            display_name=getattr(reporter_obj, "name", None),
        )

    url = getattr(issue, "html_url", None) or getattr(issue, "url", None)

    # Best-effort transitions from issue events (label add/remove, closed/reopened).
    transitions: List[WorkItemStatusTransition] = []
    started_at = None
    completed_at = None
    if events:
        # Events are returned newest-first by PyGithub; sort by created_at.
        def _ev_dt(ev: Any) -> datetime:
            return _to_utc(getattr(ev, "created_at", None)) or datetime.min.replace(
                tzinfo=timezone.utc
            )

        prev_status = "unknown"
        for ev in sorted(list(events), key=_ev_dt):
            event_type = str(getattr(ev, "event", "") or "").lower()
            occurred_at = _to_utc(getattr(ev, "created_at", None)) or created_at

            if event_type in {"closed", "reopened"}:
                to_status = "done" if event_type == "closed" else "todo"
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=work_item_id,
                        provider="github",
                        occurred_at=occurred_at,
                        from_status_raw=None,
                        to_status_raw=event_type,
                        from_status=prev_status,  # type: ignore[arg-type]
                        to_status=to_status,  # type: ignore[arg-type]
                        actor=None,
                    )
                )
                prev_status = to_status  # type: ignore[assignment]
                continue

            if event_type not in {"labeled", "unlabeled"}:
                continue

            label_obj = getattr(ev, "label", None)
            label_name = getattr(label_obj, "name", None)
            if not label_name:
                continue
            label_name = str(label_name)

            mapped = status_mapping.normalize_status(
                provider="github",
                status_raw=None,
                labels=[label_name] if event_type == "labeled" else (),
                state=None,
            )
            if mapped == "unknown":
                continue

            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=work_item_id,
                    provider="github",
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

    # Fallback: closed_at implies done.
    if completed_at is None and closed_at is not None:
        completed_at = closed_at

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="github",
        repo_id=repo_id,
        project_key=None,
        # For work tracking metrics, treat the repo as the "project" scope.
        project_id=str(repo_full_name) if repo_full_name else None,
        title=str(title),
        description=str(description) if description else None,
        type=normalized_type,
        status=normalized_status,
        status_raw=str(status_raw) if status_raw else (str(state) if state else None),
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        labels=labels,
        url=url,
    )

    return work_item, transitions


def github_project_v2_item_to_work_item(
    *,
    item_node: Dict[str, Any],
    project_scope_id: Optional[str] = None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
) -> Tuple[Optional[WorkItem], List[WorkItemStatusTransition]]:
    """
    Normalize a Projects v2 item node into a WorkItem with status transitions.

    Parses field changes to reconstruct status/phase transition history.
    """
    content = item_node.get("content") or {}
    typename = content.get("__typename")

    # Extract a status, iteration, and estimate values from field values.
    status_raw = None
    iteration_title = None
    iteration_id = None
    estimate = None

    for fv in (item_node.get("fieldValues") or {}).get("nodes") or []:
        fv_typename = (fv or {}).get("__typename")
        field = (fv or {}).get("field") or {}
        field_name = str(field.get("name") or "").strip().lower()

        if fv_typename == "ProjectV2ItemFieldSingleSelectValue":
            if field_name == "status":
                status_raw = fv.get("name")

        elif fv_typename == "ProjectV2ItemFieldIterationValue":
            # GitHub Iterations
            if "iteration" in field_name or "sprint" in field_name:
                iteration_title = fv.get("title")
                iteration_id = fv.get("id")  # internal node id

        elif fv_typename == "ProjectV2ItemFieldNumberValue":
            # Estimates / Points
            if field_name in {"estimate", "points", "story points", "size"}:
                try:
                    estimate = float(fv.get("number") or 0)
                except (ValueError, TypeError):
                    logging.getLogger(__name__).debug(
                        "Failed to parse numeric estimate from value %r",
                        fv.get("number"),
                    )

    # Parse field changes to create status transitions
    transitions: List[WorkItemStatusTransition] = []

    # Extract transitions from changes
    for change in (item_node.get("changes") or {}).get("nodes") or []:
        field = change.get("field") or {}
        field_name = str(field.get("name") or "").strip().lower()

        # Only track status and phase field changes
        if field_name not in {"status", "phase"}:
            continue

        prev_val = (change.get("previousValue") or {}).get("name")
        new_val = (change.get("newValue") or {}).get("name")
        occurred_at = _parse_iso(change.get("createdAt"))

        if not occurred_at or not new_val:
            continue

        # Map to normalized statuses
        from_status = status_mapping.normalize_status(
            provider="github",
            status_raw=str(prev_val) if prev_val else None,
            labels=(),
            state=None,
        )
        to_status = status_mapping.normalize_status(
            provider="github",
            status_raw=str(new_val) if new_val else None,
            labels=(),
            state=None,
        )

        # Get actor
        actor_obj = change.get("actor") or {}
        actor = (
            identity.resolve(
                provider="github",
                email=None,
                username=actor_obj.get("login"),
                display_name=None,
            )
            if actor_obj.get("login")
            else None
        )

        # We'll set work_item_id later when we know the content type
        transitions.append(
            WorkItemStatusTransition(
                work_item_id="",  # Placeholder, will be updated
                provider="github",
                occurred_at=_to_utc(occurred_at) or datetime.now(timezone.utc),
                from_status_raw=str(prev_val) if prev_val else None,
                to_status_raw=str(new_val) if new_val else None,
                from_status=from_status,
                to_status=to_status,
                actor=actor if actor and actor != "unknown" else None,
            )
        )

    if typename == "Issue":
        repo_full_name = ((content.get("repository") or {}).get("nameWithOwner")) or ""
        number = int(content.get("number") or 0)
        work_item_id = (
            f"gh:{repo_full_name}#{number}"
            if repo_full_name and number
            else f"ghproj:{item_node.get('id')}"
        )
        labels = _labels_from_nodes(((content.get("labels") or {}).get("nodes")) or [])
        assignees = []
        for a in ((content.get("assignees") or {}).get("nodes")) or []:
            assignees.append(
                identity.resolve(
                    provider="github",
                    email=a.get("email"),
                    username=a.get("login"),
                    display_name=a.get("name"),
                )
            )
        author = content.get("author") or {}
        reporter = identity.resolve(
            provider="github",
            email=author.get("email"),
            username=author.get("login"),
            display_name=author.get("name"),
        )
        created_at = _to_utc(_parse_iso(content.get("createdAt"))) or datetime.now(
            timezone.utc
        )
        updated_at = _to_utc(_parse_iso(content.get("updatedAt"))) or created_at
        closed_at = _to_utc(_parse_iso(content.get("closedAt")))
        state = content.get("state")

        normalized_status = status_mapping.normalize_status(
            provider="github",
            status_raw=str(status_raw) if status_raw else None,
            labels=() if status_raw else labels,
            state=str(state) if state else None,
        )
        normalized_type = status_mapping.normalize_type(
            provider="github",
            type_raw=None,
            labels=labels,
        )
        completed_at = closed_at if closed_at else None

        # Update work_item_id in transitions
        transitions = _update_transitions_work_item_id(transitions, work_item_id)

        description = content.get("body")
        return WorkItem(
            work_item_id=work_item_id,
            provider="github",
            repo_id=None,
            project_key=None,
            project_id=str(project_scope_id or repo_full_name)
            if (project_scope_id or repo_full_name)
            else None,
            title=str(content.get("title") or ""),
            description=str(description) if description else None,
            type=normalized_type,
            status=normalized_status,
            status_raw=str(status_raw)
            if status_raw
            else (str(state) if state else None),
            assignees=[a for a in assignees if a and a != "unknown"],
            reporter=reporter if reporter and reporter != "unknown" else None,
            created_at=created_at,
            updated_at=updated_at,
            started_at=None,
            completed_at=completed_at,
            closed_at=closed_at,
            labels=labels,
            story_points=estimate,
            sprint_id=iteration_id,
            sprint_name=iteration_title,
            url=content.get("url"),
        ), transitions

    if typename == "DraftIssue":
        created_at = _to_utc(_parse_iso(content.get("createdAt"))) or datetime.now(
            timezone.utc
        )
        updated_at = _to_utc(_parse_iso(content.get("updatedAt"))) or created_at
        normalized_status = status_mapping.normalize_status(
            provider="github",
            status_raw=str(status_raw) if status_raw else None,
            labels=(),
            state=None,
        )

        work_item_id = f"ghproj:{item_node.get('id')}"

        # Update work_item_id in transitions
        transitions = _update_transitions_work_item_id(transitions, work_item_id)

        description = content.get("body")
        return WorkItem(
            work_item_id=work_item_id,
            provider="github",
            repo_id=None,
            project_key=None,
            project_id=str(project_scope_id) if project_scope_id else None,
            title=str(content.get("title") or ""),
            description=str(description) if description else None,
            type="issue",
            status=normalized_status,
            status_raw=str(status_raw) if status_raw else None,
            assignees=[],
            reporter=None,
            created_at=created_at,
            updated_at=updated_at,
            started_at=None,
            completed_at=None,
            closed_at=None,
            labels=[],
            story_points=estimate,
            sprint_id=iteration_id,
            sprint_name=iteration_title,
            url=None,
        ), transitions

    return None, []


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # GitHub returns RFC3339 with Z.
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def github_pr_to_work_item(
    *,
    pr: Any,
    repo_full_name: str,
    repo_id: Optional[Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    events: Optional[Sequence[Any]] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    """
    Normalize a GitHub pull request to a WorkItem with status transitions.

    Args:
        pr: PyGithub PullRequest object
        repo_full_name: e.g. "owner/repo"
        repo_id: optional internal repo ID
        status_mapping: StatusMapping instance
        identity: IdentityResolver instance
        events: optional list of PR events (labeled/unlabeled/closed/reopened)

    Returns:
        Tuple of (WorkItem, list of WorkItemStatusTransition)
    """
    number = int(getattr(pr, "number", 0) or 0)
    work_item_id = f"ghpr:{repo_full_name}#{number}"

    title = getattr(pr, "title", "") or ""
    description = getattr(pr, "body", None)
    state = getattr(pr, "state", None)
    merged = getattr(pr, "merged", False)
    created_at = _to_utc(getattr(pr, "created_at", None)) or datetime.now(timezone.utc)
    updated_at = _to_utc(getattr(pr, "updated_at", None)) or created_at
    closed_at = _to_utc(getattr(pr, "closed_at", None))
    merged_at = _to_utc(getattr(pr, "merged_at", None))

    labels = [getattr(lbl, "name", None) for lbl in getattr(pr, "labels", []) or []]
    labels = [str(lbl) for lbl in labels if lbl]

    # Determine status based on state and merge status
    if merged or merged_at:
        status_raw = "merged"
        normalized_status = "done"
    elif state == "closed":
        status_raw = "closed"
        normalized_status = "canceled"
    elif state == "open":
        status_raw = "open"
        # Check labels to see if it's in review, draft, etc.
        draft = getattr(pr, "draft", False)
        if draft:
            normalized_status = "todo"
        else:
            normalized_status = "in_progress"
    else:
        status_raw = str(state) if state else None
        normalized_status = status_mapping.normalize_status(
            provider="github",
            status_raw=status_raw,
            labels=labels,
            state=str(state) if state else None,
        )

    # PRs are always type "pr"
    normalized_type = "pr"

    assignees: List[str] = []
    for assignee in getattr(pr, "assignees", []) or []:
        assignees.append(
            identity.resolve(
                provider="github",
                email=getattr(assignee, "email", None),
                username=getattr(assignee, "login", None),
                display_name=getattr(assignee, "name", None),
            )
        )

    reporter_obj = getattr(pr, "user", None)
    reporter = None
    if reporter_obj is not None:
        reporter = identity.resolve(
            provider="github",
            email=getattr(reporter_obj, "email", None),
            username=getattr(reporter_obj, "login", None),
            display_name=getattr(reporter_obj, "name", None),
        )

    url = getattr(pr, "html_url", None) or getattr(pr, "url", None)

    # Build transitions from events
    transitions: List[WorkItemStatusTransition] = []
    started_at = created_at  # PRs are in progress when opened
    completed_at = None

    if events:

        def _ev_dt(ev: Any) -> datetime:
            return _to_utc(getattr(ev, "created_at", None)) or datetime.min.replace(
                tzinfo=timezone.utc
            )

        prev_status = "in_progress"  # PRs start as in_progress
        # Track if we've seen a merged event to determine closed status correctly
        merged_in_history = False

        for ev in sorted(list(events), key=_ev_dt):
            event_type = str(getattr(ev, "event", "") or "").lower()
            occurred_at = _to_utc(getattr(ev, "created_at", None)) or created_at

            if event_type == "merged":
                merged_in_history = True
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=work_item_id,
                        provider="github",
                        occurred_at=occurred_at,
                        from_status_raw=None,
                        to_status_raw="merged",
                        from_status=prev_status,  # type: ignore[arg-type]
                        to_status="done",
                        actor=None,
                    )
                )
                prev_status = "done"
            elif event_type == "closed":
                # Use merged_in_history to determine status at the time of close
                to_status = "done" if merged_in_history else "canceled"
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=work_item_id,
                        provider="github",
                        occurred_at=occurred_at,
                        from_status_raw=None,
                        to_status_raw=event_type,
                        from_status=prev_status,  # type: ignore[arg-type]
                        to_status=to_status,  # type: ignore[arg-type]
                        actor=None,
                    )
                )
                prev_status = to_status  # type: ignore[assignment]
            elif event_type == "reopened":
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=work_item_id,
                        provider="github",
                        occurred_at=occurred_at,
                        from_status_raw=None,
                        to_status_raw="reopened",
                        from_status=prev_status,  # type: ignore[arg-type]
                        to_status="in_progress",
                        actor=None,
                    )
                )
                prev_status = "in_progress"

    # Set completed_at based on final status
    if merged_at:
        completed_at = merged_at
    elif closed_at and normalized_status in {"done", "canceled"}:
        completed_at = closed_at

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="github",
        repo_id=repo_id,
        project_key=None,
        project_id=str(repo_full_name) if repo_full_name else None,
        title=str(title),
        description=str(description) if description else None,
        type=normalized_type,
        status=normalized_status,
        status_raw=status_raw,
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        labels=labels,
        url=url,
    )

    return work_item, transitions


def github_comment_to_interaction_event(
    *,
    comment: Any,
    work_item_id: str,
    identity: IdentityResolver,
) -> Optional[WorkItemInteractionEvent]:
    """
    Convert a GitHub comment to a WorkItemInteractionEvent.

    Args:
        comment: PyGithub IssueComment or PullRequestComment object
        work_item_id: work item identifier
        identity: IdentityResolver instance

    Returns:
        WorkItemInteractionEvent or None if invalid
    """
    comment_id = getattr(comment, "id", None)
    if not comment_id:
        return None

    created_at = _to_utc(getattr(comment, "created_at", None))
    if not created_at:
        return None

    user = getattr(comment, "user", None)
    actor = None
    if user:
        actor = identity.resolve(
            provider="github",
            email=getattr(user, "email", None),
            username=getattr(user, "login", None),
            display_name=getattr(user, "name", None),
        )

    body = getattr(comment, "body", "") or ""

    return WorkItemInteractionEvent(
        work_item_id=work_item_id,
        provider="github",
        interaction_type="comment",
        occurred_at=created_at,
        actor=actor if actor and actor != "unknown" else None,
        body_length=len(body),
        last_synced=datetime.now(timezone.utc),
    )


def github_milestone_to_sprint(
    *,
    milestone: Any,
    repo_full_name: str,
) -> Sprint:
    """
    Convert a GitHub milestone to a Sprint.

    Args:
        milestone: PyGithub Milestone object
        repo_full_name: e.g. "owner/repo"

    Returns:
        Sprint object
    """
    milestone_id = str(getattr(milestone, "id", 0) or getattr(milestone, "number", 0))
    sprint_id = f"ghms:{repo_full_name}:{milestone_id}"

    title = getattr(milestone, "title", "") or ""
    created_at = _to_utc(getattr(milestone, "created_at", None)) or datetime.now(
        timezone.utc
    )
    due_on = _to_utc(getattr(milestone, "due_on", None))
    state = getattr(milestone, "state", "open")

    return Sprint(
        provider="github",
        sprint_id=sprint_id,
        name=title,
        state="closed" if state == "closed" else "active",
        started_at=created_at,
        ended_at=due_on,
        completed_at=due_on if state == "closed" else None,
        last_synced=datetime.now(timezone.utc),
    )


def detect_github_reopen_events(
    *,
    work_item_id: str,
    events: Sequence[Any],
    identity: IdentityResolver,
) -> List[WorkItemReopenEvent]:
    """
    Detect reopen events from GitHub issue/PR events.

    Args:
        work_item_id: work item identifier
        events: list of issue/PR events from GitHub API
        identity: IdentityResolver instance

    Returns:
        List of WorkItemReopenEvent objects
    """
    reopen_events: List[WorkItemReopenEvent] = []

    for ev in events:
        event_type = str(getattr(ev, "event", "") or "").lower()
        if event_type != "reopened":
            continue

        occurred_at = _to_utc(getattr(ev, "created_at", None))
        if not occurred_at:
            continue

        actor_obj = getattr(ev, "actor", None)
        actor = None
        if actor_obj:
            actor = identity.resolve(
                provider="github",
                email=getattr(actor_obj, "email", None),
                username=getattr(actor_obj, "login", None),
                display_name=getattr(actor_obj, "name", None),
            )

        reopen_events.append(
            WorkItemReopenEvent(
                work_item_id=work_item_id,
                occurred_at=occurred_at,
                from_status="done",  # Was closed before reopen
                to_status="todo",  # Reopened to todo
                from_status_raw="closed",
                to_status_raw="reopened",
                actor=actor if actor and actor != "unknown" else None,
                last_synced=datetime.now(timezone.utc),
            )
        )

    return reopen_events


# Regex patterns for parsing GitHub-style references from issue/PR body
_GITHUB_ISSUE_REF_PATTERN = re.compile(
    r"(?:^|[^\S\r\n])(?:depends\s+on|blocked\s+by|blocks|fixes|closes|resolves)\s*:?\s*"
    r"(?:#(\d+)|(?:(?:https?://)?github\.com/)?([a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+)#(\d+))",
    re.IGNORECASE | re.MULTILINE,
)


def extract_github_dependencies(
    *,
    work_item_id: str,
    issue_or_pr: Any,
    repo_full_name: str,
) -> List[WorkItemDependency]:
    """
    Extract dependencies from GitHub issue/PR body text.

    Parses common patterns like:
    - "depends on #123"
    - "blocked by owner/repo#456"
    - "blocks #789"

    Args:
        work_item_id: source work item identifier
        issue_or_pr: PyGithub Issue or PullRequest object
        repo_full_name: e.g. "owner/repo"

    Returns:
        List of WorkItemDependency objects
    """
    dependencies: List[WorkItemDependency] = []
    body = getattr(issue_or_pr, "body", "") or ""

    for match in _GITHUB_ISSUE_REF_PATTERN.finditer(body):
        keyword = match.group(0).strip().lower()

        # Determine direction and dependency type
        if any(k in keyword for k in ["depends on", "blocked by"]):
            dep_type = "blocks"  # The referenced issue blocks this one
        elif "blocks" in keyword:
            dep_type = "is_blocked_by"  # This issue blocks the referenced one
        elif any(k in keyword for k in ["fixes", "closes", "resolves"]):
            dep_type = "relates_to"
        else:
            dep_type = "relates_to"

        # Parse reference
        same_repo_num = match.group(1)
        cross_repo = match.group(2)
        cross_repo_num = match.group(3)

        if same_repo_num:
            target_id = f"gh:{repo_full_name}#{same_repo_num}"
        elif cross_repo and cross_repo_num:
            target_id = f"gh:{cross_repo}#{cross_repo_num}"
        else:
            continue

        dependencies.append(
            WorkItemDependency(
                source_work_item_id=work_item_id,
                target_work_item_id=target_id,
                relationship_type=dep_type,
                relationship_type_raw=dep_type,
                last_synced=datetime.now(timezone.utc),
            )
        )

    return dependencies


# Priority label mapping - maps label to (priority_raw, service_class)
_PRIORITY_LABELS = {
    "priority::critical": ("critical", "expedite"),
    "priority::high": ("high", "fixed_date"),
    "priority::medium": ("medium", "standard"),
    "priority::low": ("low", "intangible"),
    "p0": ("critical", "expedite"),
    "p1": ("high", "fixed_date"),
    "p2": ("medium", "standard"),
    "p3": ("low", "intangible"),
    "priority-critical": ("critical", "expedite"),
    "priority-high": ("high", "fixed_date"),
    "priority-medium": ("medium", "standard"),
    "priority-low": ("low", "intangible"),
    "critical": ("critical", "expedite"),
    "urgent": ("critical", "expedite"),
    "high-priority": ("high", "fixed_date"),
    "low-priority": ("low", "intangible"),
}


def _priority_from_labels(labels: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract priority_raw and service_class from GitHub labels.

    Returns (priority_raw, service_class) or (None, None) if no match.
    """
    for label in labels:
        normalized = label.lower().strip()
        if normalized in _PRIORITY_LABELS:
            return _PRIORITY_LABELS[normalized]
    return (None, None)


def enrich_work_item_with_priority(
    work_item: WorkItem,
    labels: Sequence[str],
) -> WorkItem:
    """
    Enrich a WorkItem with priority and service_class from labels.

    Args:
        work_item: WorkItem to enrich
        labels: list of label strings

    Returns:
        WorkItem with priority/service_class set (or original if no match)
    """
    priority, service_class = _priority_from_labels(labels)
    if priority is None:
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
        priority_raw=priority,
        service_class=service_class,
        url=work_item.url,
    )
