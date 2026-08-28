"""Live Python oracle for CHAOS-4365 item 1b's donor-walk GATING parity.

This pair does NOT compare the full team_repo_ownership derivation (that has
no Python side to compare against -- it is a new, Go-only producer). It
compares the narrower, genuinely shared surface: the EDGE-SELECTION rules
Go's ``buildDonorProjectIDResolver`` (team_repo_ownership_derivation.go) was
built to mirror verbatim from the existing linked-issue resolver,
``compute_work_items.py::build_linked_issue_team_resolver`` -- only
inheritance-safe relationship types transfer a team, the latest edge per
(source, target) pair wins, a cross-provider ``extkey:KEY`` target resolves
via a Linear/Jira key index (dropped if ambiguous), and multiple donor
candidates resolve to the lexicographically smallest canonical target. A
future drift between the two implementations of this shared gating logic
(see docs/contribute/architecture/team-attribution.md's Sec 0.6) fails this
oracle, not just the Go-only unit tests in
team_repo_ownership_derivation_test.go.

Every project-ownership candidate in a case is registered under the SAME
provider as every work item ("linear"), sidestepping the OTHER, unrelated
provider-matching semantics `_context_candidates` layers on top of
`team_project_ownership` for the 9-tier attribution ladder -- provider
matching is not part of what this oracle tests.
"""

from __future__ import annotations

import contextlib
import io
from datetime import datetime, timezone
from typing import Any, Literal

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.metrics.compute_work_items import (
        TeamAttributionCandidate,
        TeamAttributionContext,
        build_linked_issue_team_resolver,
        resolve_team_attribution,
    )
    from dev_health_ops.models.work_items import WorkItem, WorkItemDependency

_PROVIDER: Literal["jira", "github", "gitlab", "linear"] = "linear"


def _time(value: str | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _work_item(raw: dict[str, Any]) -> WorkItem:
    return WorkItem(
        work_item_id=str(raw["work_item_id"]),
        provider=_PROVIDER,
        title=str(raw["work_item_id"]),
        type="issue",
        status="todo",
        status_raw="open",
        project_id=raw.get("project_id") or None,
        created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 8, 4, tzinfo=timezone.utc),
    )


def _dependency(raw: dict[str, Any]) -> WorkItemDependency:
    return WorkItemDependency(
        source_work_item_id=str(raw["source_work_item_id"]),
        target_work_item_id=str(raw["target_work_item_id"]),
        relationship_type=str(raw["relationship_type"]),
        relationship_type_raw=str(raw["relationship_type"]),
        last_synced=_time(raw.get("last_synced")),
    )


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    context = TeamAttributionContext()
    for link in case.get("project_ownership") or []:
        key = (_PROVIDER, str(link["project_id"]))
        context.project_by_id.setdefault(key, []).append(
            TeamAttributionCandidate(
                source="project_ownership",
                team_id=str(link["team_id"]),
                team_name=str(link["team_id"]),
                confidence="high",
                evidence="oracle",
                is_primary=1,
                specificity=100,
            )
        )
    work_items = [_work_item(raw) for raw in case.get("work_items") or []]
    dependencies = [_dependency(raw) for raw in case.get("dependencies") or []]

    linked = build_linked_issue_team_resolver(
        work_items=work_items,
        dependencies=dependencies,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    subject_id = str(case["source_work_item_id"])
    subject = next(item for item in work_items if item.work_item_id == subject_id)
    team_id, _team_name, _candidates = resolve_team_attribution(
        subject,
        team_resolver=None,
        project_key_resolver=None,
        linked_issue_resolver=linked,
        attribution_context=context,
    )
    return {"team_id": team_id}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="sync/team-repo-ownership/donor-walk",
        build_row=_build_row,
        # The wrapper exposes resolve_team_attribution's winning team_id
        # under one protocol key; there is no narrower field set that could
        # hide a production result.
        reflected_fields=lambda: frozenset({"team_id"}),
    )
)
