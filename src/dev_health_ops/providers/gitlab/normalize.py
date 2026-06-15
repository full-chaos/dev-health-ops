from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from datetime import datetime, timezone
from uuid import UUID

from dev_health_ops.models.ai_attribution import (
    AIAttributionRecord,
    AIAttributionSignal,
)
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusCategory,
    WorkItemStatusTransition,
)
from dev_health_ops.providers._ai_detection import (
    AuthorInfo,
    detect_from_author,
    detect_from_branch_name,
    detect_from_commit_trailers,
    detect_from_pr_body,
    detect_from_pr_labels,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.normalize_common import (
    parse_iso_datetime as _parse_iso,
)
from dev_health_ops.providers.normalize_common import (
    priority_from_labels as _priority_from_labels,
)
from dev_health_ops.providers.normalize_common import (
    to_utc as _to_utc,
)
from dev_health_ops.providers.normalize_helpers import get_attr as _get
from dev_health_ops.providers.status_mapping import StatusMapping

logger = logging.getLogger(__name__)


def gitlab_issue_to_work_item(
    *,
    issue: object,
    project_full_path: str,
    repo_id: UUID | None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    label_events: Sequence[object] | None = None,
) -> tuple[WorkItem, list[WorkItemStatusTransition]]:
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

    assignees: list[str] = []
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
    transitions: list[WorkItemStatusTransition] = []
    started_at = None
    completed_at = None

    if label_events:

        def _ev_dt(ev: object) -> datetime:
            return _to_utc(_parse_iso(_get(ev, "created_at"))) or datetime.min.replace(
                tzinfo=timezone.utc
            )

        prev_status: WorkItemStatusCategory = "unknown"
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
                    from_status=prev_status,
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
    mr: object,
    project_full_path: str,
    repo_id: UUID | None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    state_events: Sequence[object] | None = None,
) -> tuple[WorkItem, list[WorkItemStatusTransition]]:
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
    normalized_status: WorkItemStatusCategory
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

    assignees: list[str] = []
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
    transitions: list[WorkItemStatusTransition] = []
    started_at = created_at  # MRs start when opened
    completed_at = merged_at or closed_at

    if state_events:
        prev_status: WorkItemStatusCategory = "unknown"
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
                to_status: WorkItemStatusCategory = "done"
            elif ev_state == "closed":
                to_status = "canceled"
            elif ev_state == "opened" or ev_state == "reopened":
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
    state_events: Sequence[object],
    identity: IdentityResolver,
) -> list[WorkItemReopenEvent]:
    """
    Detect reopen events from GitLab resource_state_events.

    A reopen occurs when state changes to "reopened".
    """
    events: list[WorkItemReopenEvent] = []
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
    note: object,
    work_item_id: str,
    identity: IdentityResolver,
) -> WorkItemInteractionEvent | None:
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

# Cross-provider issue keys (Linear/Jira style, e.g. CHAOS-2400, PROJ-12) that a
# GitLab issue/MR description references when the tracked work lives outside
# GitLab. Emitted as provider-neutral ``extkey:`` edges and matched to the real
# Linear/Jira work item at team-inheritance time so the GitLab item can borrow
# that issue's team — the same cross-provider recovery the GitHub path provides.
_GITLAB_EXTERNAL_KEY_BODY_PATTERN = re.compile(
    r"(depends\s+on|blocked\s+by|blocks|fixes|closes|resolves|relates\s+to|"
    r"part\s+of|see)\s*:?\s*([A-Za-z]{2,}-\d+)\b",
    re.IGNORECASE,
)


def _gitlab_external_key_relationship(keyword: str) -> str:
    """Blocking words → blocking relationship (non-inheritable); else relates_to."""
    kw = keyword.strip().lower()
    if kw == "blocks":
        return "blocks"
    if kw in {"blocked by", "depends on"}:
        return "blocked_by"
    return "relates_to"


def extract_gitlab_dependencies(
    *,
    work_item_id: str,
    issue: object,
    project_full_path: str,
    linked_issues: Sequence[object] | None = None,
) -> list[WorkItemDependency]:
    """
    Extract dependency edges from GitLab issue links and description.

    GitLab has explicit issue links (via API) and implicit references in description.
    """
    dependencies: list[WorkItemDependency] = []
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

    # Cross-provider links: external (Linear/Jira) issue keys referenced via a
    # magic word in the description. Emitted as ``extkey:KEY`` so a GitLab item
    # can inherit the team of an issue tracked in another provider.
    if description:
        seen_external: set[str] = set()
        for match in _GITLAB_EXTERNAL_KEY_BODY_PATTERN.finditer(str(description)):
            key = match.group(2).strip().upper()
            if not key or key in seen_external:
                continue
            seen_external.add(key)
            dependencies.append(
                WorkItemDependency(
                    source_work_item_id=work_item_id,
                    target_work_item_id=f"extkey:{key}",
                    relationship_type=_gitlab_external_key_relationship(match.group(1)),
                    relationship_type_raw="external_issue_key",
                )
            )

    return dependencies


def gitlab_milestone_to_sprint(
    *,
    milestone: object,
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
    epic: object,
    group_full_path: str,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    state_events: Sequence[object] | None = None,
) -> tuple[WorkItem, list[WorkItemStatusTransition]]:
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

    transitions: list[WorkItemStatusTransition] = []
    started_at = start_date
    completed_at = closed_at

    if state_events:
        prev_status: WorkItemStatusCategory = "unknown"
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
                to_status: WorkItemStatusCategory = "done"
            elif ev_state == "opened" or ev_state == "reopened":
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
    issue: object,
    group_full_path: str,
) -> str | None:
    """Build epic_id for an issue that belongs to an epic."""
    epic = _get(issue, "epic")
    if epic is None:
        return None

    epic_iid = _get(epic, "iid")
    if epic_iid is None:
        return None

    epic_group = _get(epic, "group_id") or group_full_path
    return f"gitlab:{epic_group}:epic:{epic_iid}"


# ---------------------------------------------------------------------------
# AI attribution detection — wired into the GitLab normalize path
# ---------------------------------------------------------------------------


def detect_mr_attributions(
    *,
    mr: object,
) -> list[AIAttributionSignal]:
    """Detect all AI attribution signals from a GitLab merge request.

    Mirrors :func:`providers.github.normalize.detect_pr_attributions` so the
    SAME downstream write path (``ProviderBatch.ai_attributions`` →
    ``write_ai_attribution``) persists GitLab attribution into
    ``ai_governance_coverage_daily``.

    Runs every provider-agnostic detector and returns the full list of raw
    signals.  Signals are not collapsed — all are returned for raw
    persistence; precedence is resolved at read time by the storage layer.

    Sources checked (in precedence order, but all persisted regardless):
        1. MR labels (highest confidence)
        2. MR author (bot detection)
        3. Commit trailers in MR description (squash merges expose them here)
        4. MR source branch name (weak)
        5. MR description body (weak)

    Note on commit-level trailers:
        Full commit message traversal requires an extra API call and is not
        wired here — the normalize path receives the MR object only.  Squash
        workflows typically surface commit trailers in the MR description.

    Args:
        mr: GitLab merge request object (REST attribute object or dict).

    Returns:
        List of :class:`AIAttributionSignal` objects (may be empty).
    """
    signals: list[AIAttributionSignal] = []

    # 1. MR labels (GitLab labels are plain strings)
    labels = [str(lbl) for lbl in (_get(mr, "labels") or []) if lbl]
    signals.extend(detect_from_pr_labels(labels))

    # 2. MR author (GitLab user objects expose ``username``/``name``; bots set
    #    ``bot=True`` rather than GitHub's ``type``/``app_slug``).
    author_obj = _get(mr, "author")
    if author_obj is not None:
        login = str(_get(author_obj, "username") or "")
        if login:
            is_bot = bool(_get(author_obj, "bot"))
            author_signal = detect_from_author(
                AuthorInfo(
                    login=login,
                    user_type="Bot" if is_bot else None,
                    app_slug=None,
                )
            )
            if author_signal is not None:
                signals.append(author_signal)

    # 3. Commit trailers from MR description
    #    Squash/rebase merges often surface commit messages in the description.
    description = str(_get(mr, "description") or "")
    signals.extend(detect_from_commit_trailers(description))

    # 4. Source branch name (weak signal)
    source_branch = _get(mr, "source_branch")
    if source_branch:
        branch_signal = detect_from_branch_name(str(source_branch))
        if branch_signal is not None:
            signals.append(branch_signal)

    # 5. MR description text (keyword matching — weak signal)
    #    Only run if a trailer signal hasn't already fired from the same text
    #    (avoids double-counting the same description body).
    trailer_fired = any(s.source.value == "commit_trailer" for s in signals)
    if not trailer_fired and description:
        body_signal = detect_from_pr_body(description)
        if body_signal is not None:
            signals.append(body_signal)

    return signals


def gitlab_mr_ai_attributions(
    *,
    mr: object,
    project_full_path: str,
    org_id: UUID,
    repo_id: UUID | None,
) -> list[AIAttributionRecord]:
    """Promote a GitLab MR's AI attribution signals to persisted records.

    This is the single mapping from a merge request to
    :class:`AIAttributionRecord` so that BOTH ingestion paths — the live
    ``metrics.work_items.fetch_gitlab_work_items`` sync entrypoint and
    :class:`GitLabProvider` — emit byte-identical records into
    ``ai_governance_coverage_daily`` via ``write_ai_attribution``.

    ``subject_id`` is the bare MR ``iid`` as a string (``str(iid)``) — the
    SAME shape the governance/impact read paths join against, where
    ``ai_attribution.subject_id = toString(git_pull_requests.number)`` and the
    GitLab processor stores ``git_pull_requests.number = int(mr.iid)`` (see
    :mod:`dev_health_ops.processors.gitlab` and
    :mod:`dev_health_ops.audit.ai_governance.loaders`). A prefixed work-item id
    such as ``gitlab:{path}!{iid}`` would NEVER match that join, leaving
    ``human_reviewed`` NULL for every GitLab AI MR and fabricating
    high-severity "missing review" governance violations (CHAOS-2379 round-2).
    The synthetic fixtures use the same ``str(pr.number)`` contract.

    GitLab MR ``iid`` (and GitHub PR ``number``) is only unique WITHIN a repo,
    so two repos in one org can both carry attribution for ``subject_id="1"``.
    The bare-iid join is safe because EVERY read path additionally constrains
    ``attr.repo_id = pr.repo_id`` and the ``ai_attribution_resolved`` view
    resolves per ``(org_id, subject_type, repo_id, subject_id)`` (migration
    043, CHAOS-2379 round-3) — both repos' rows survive the resolve and each
    matches only its own repo's PR. ``repo_id`` therefore disambiguates the
    repo-local id; we do NOT repo-qualify ``subject_id`` itself (that would
    break the ``toString(pr.number)`` join used by every loader).

    ``observed_at`` is a real provider timestamp — never a fabricated
    ingest-time value — derived from the MR's ``created_at``, falling back to
    the MR ``updated_at`` and only then to ``now()`` when the provider omits
    both.

    Args:
        mr: GitLab merge request object (REST attribute object or dict).
        project_full_path: Project path used to build the canonical subject id.
        org_id: Owning organization (tenant scope) — required; records are
            never written with a blank tenant.
        repo_id: Repository UUID for this project (may be ``None``).

    Returns:
        List of :class:`AIAttributionRecord` (may be empty).
    """
    signals = detect_mr_attributions(mr=mr)
    if not signals:
        return []

    iid = int(_get(mr, "iid") or 0)
    # Governance/impact loaders join `ai_attribution.subject_id =
    # toString(git_pull_requests.number)`, and the GitLab processor stores
    # `git_pull_requests.number = int(mr.iid)`. The subject id MUST therefore be
    # the bare iid string — a prefixed `gitlab:{path}!{iid}` work-item id would
    # never join and would fabricate "missing review" policy failures.
    # iid is repo-local; cross-repo collisions are disambiguated by repo_id in
    # every read-path join AND in the ai_attribution_resolved partition
    # (migration 043), so both repos' rows survive — see this function's
    # docstring (CHAOS-2379 round-3).
    subject_id = str(iid)
    observed_at = (
        _to_utc(_parse_iso(_get(mr, "created_at")))
        or _to_utc(_parse_iso(_get(mr, "updated_at")))
        or datetime.now(timezone.utc)
    )

    return [
        AIAttributionRecord.from_signal(
            sig,
            org_id=org_id,
            provider="gitlab",
            subject_type="pull_request",
            subject_id=subject_id,
            repo_id=repo_id,
            observed_at=observed_at,
        )
        for sig in signals
    ]
