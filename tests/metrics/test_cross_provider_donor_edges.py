"""CHAOS-3978: cross-provider donor edges must reach the inheriting writer.

`ghpr:...#1794 --relates_to--> linear:CHAOS-3914` (``relationship_type_raw``
``linear_attachment``) is minted EXCLUSIVELY by the Linear sync, from a Linear
attachment. The GitHub work-items writer never mints a ``linear:`` target at
all, so a fresh-edges-only donor load cannot see it, and the PR is re-stamped
``unassigned`` on every run despite a valid, teamed donor. Prod: 85 items on
2026-08-23, up from 82 on 2026-08-20.

The PRODUCTION fix for this ticket is in the Go writer
(``internal/providersync``), which had never read ``work_item_dependencies``.
The Python path reaches the correct answer ALREADY, because CHAOS-4112's
stored-edge union is provider-agnostic -- but nothing pinned that, and nothing
pinned the two runtimes to the same pruning key. Both runtimes write
``work_item_team_attributions`` for the same items, so:

* if this file's cross-provider case regresses, Python re-breaks the same 85
  items the Go fix repairs, and the last writer decides;
* if the PRUNING KEY here drifts from Go's, CHAOS-4112 is undone from whichever
  side moved -- a coarser key (per item) deletes stored edges whose extractor
  never ran; a finer one (per full edge identity) never prunes at all.

These are pure-Python tests with a fake sink, so they run in CI's unit tier.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from dev_health_ops.metrics.compute_work_items import (
    build_linked_issue_team_resolver,
    resolve_team_attribution,
)
from dev_health_ops.metrics.job_work_items import _merge_stored_inheritable_edges
from dev_health_ops.models.work_items import WorkItem, WorkItemDependency
from dev_health_ops.providers.teams import ProjectKeyTeamResolver

ORG = "org-3978"
NOW = datetime(2026, 8, 23, tzinfo=timezone.utc)
# The prod pair, verbatim from the ticket's evidence.
PR_ID = "ghpr:full-chaos/dev-health-ops#1794"
ISSUE_ID = "linear:CHAOS-3914"
ATTACHMENT_RAW = "linear_attachment"


def _work_item(
    work_item_id: str,
    provider: Literal["jira", "github", "gitlab", "linear"],
    **kw: Any,
) -> WorkItem:
    defaults: dict[str, Any] = dict(
        title="t",
        type="task",
        status="done",
        status_raw="Done",
        created_at=NOW - timedelta(days=30),
        updated_at=NOW,
        started_at=NOW - timedelta(days=2),
        completed_at=NOW,
        closed_at=NOW,
        labels=[],
    )
    defaults.update(kw)
    return WorkItem(work_item_id=work_item_id, provider=provider, **defaults)


def _edge(
    source: str,
    target: str,
    relationship_type: str = "relates_to",
    *,
    raw: str = ATTACHMENT_RAW,
    last_synced: datetime | None = None,
) -> WorkItemDependency:
    return WorkItemDependency(
        source_work_item_id=source,
        target_work_item_id=target,
        relationship_type=relationship_type,
        relationship_type_raw=raw,
        last_synced=NOW if last_synced is None else last_synced,
        org_id=ORG,
    )


def _stored_row(dep: WorkItemDependency) -> dict[str, Any]:
    """A stored edge as ClickHouse returns it (``SELECT *``)."""
    return {
        "source_work_item_id": dep.source_work_item_id,
        "target_work_item_id": dep.target_work_item_id,
        "relationship_type": dep.relationship_type,
        "relationship_type_raw": dep.relationship_type_raw,
        "relationship_semantics_version": dep.relationship_semantics_version,
        "last_synced": dep.last_synced,
        "org_id": dep.org_id,
    }


class _FakeSink:
    """``query_dicts`` stand-in that honours the read's bounds.

    It filters on the parameters the production query filters on, so a lookup
    that stopped being keyed (dropping ``source_ids`` or ``rel_types``) would
    change what these tests see rather than passing silently.
    """

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        source_ids = set(params.get("source_ids") or [])
        rel_types = set(params.get("rel_types") or [])
        return [
            row
            for row in self._rows
            if row["source_work_item_id"] in source_ids
            and row["relationship_type"] in rel_types
            and row["org_id"] == params.get("org_id")
        ]


def _pk_resolver() -> ProjectKeyTeamResolver:
    return ProjectKeyTeamResolver(
        project_key_to_team={"fullchaos": ("fullchaos", "Fullchaos")}
    )


def _github_run_items() -> list[WorkItem]:
    """What a GitHub sync run holds: the PR, and nothing Linear."""
    return [_work_item(PR_ID, "github")]


def _linear_donor() -> WorkItem:
    """The donor as the preload loads it: a Linear issue with a native team."""
    return _work_item(ISSUE_ID, "linear", native_team_key="fullchaos")


def _resolve_pr(
    dependencies: list[WorkItemDependency],
    *,
    donor: WorkItem | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve the PR through the real inheritance path.

    Returns (team_id, team_name, primary_source).
    """
    pk = _pk_resolver()
    items = _github_run_items() + ([donor] if donor is not None else [])
    resolver = build_linked_issue_team_resolver(
        work_items=items,
        dependencies=dependencies,
        project_key_resolver=pk,
    )
    pr = next(item for item in items if item.work_item_id == PR_ID)
    team_id, team_name, marked = resolve_team_attribution(
        pr, None, pk, linked_issue_resolver=resolver
    )
    primary = next((record.source for record in marked if record.is_primary), None)
    return team_id, team_name, primary


def test_fixture_genuinely_crosses_providers() -> None:
    """ANTI-VACUITY. A same-provider fixture would pass every assertion below
    while proving nothing about this ticket."""
    pr, donor = _github_run_items()[0], _linear_donor()
    assert pr.provider == "github"
    assert donor.provider == "linear"
    assert pr.provider != donor.provider
    assert pr.work_item_id.startswith("ghpr:")
    assert donor.work_item_id.startswith("linear:")
    # The edge points PR -> issue: the PR is the SOURCE, which is why bounding
    # the stored-edge read to this run's items as the edge source reaches it.
    edge = _edge(PR_ID, ISSUE_ID)
    assert edge.source_work_item_id == pr.work_item_id
    assert edge.target_work_item_id == donor.work_item_id
    assert edge.relationship_type_raw == ATTACHMENT_RAW


def test_red_control_without_the_stored_edge_the_pr_has_no_team() -> None:
    """The defect, expressed as the pre-fix input: donor present, edge absent
    from this run's fresh window, PR team-less."""
    team_id, _, primary = _resolve_pr([], donor=_linear_donor())
    assert team_id is None, "fixture no longer reproduces the team-less PR"
    assert primary != "linked_issue"


def test_cross_provider_stored_edge_donates_a_team() -> None:
    """The 85-item shape: the ONLY edge lives in the store, minted by the
    Linear sync, and the GitHub-side recompute still inherits the team."""
    sink = _FakeSink(
        [_stored_row(_edge(PR_ID, ISSUE_ID, last_synced=NOW - timedelta(days=40)))]
    )
    merged: dict[tuple[str, str, str], Any] = {}  # this run's fresh window: empty

    added = _merge_stored_inheritable_edges(sink, ORG, _github_run_items(), merged)

    assert added == 1, "the cross-provider stored edge was not unioned in"
    team_id, team_name, primary = _resolve_pr(
        list(merged.values()), donor=_linear_donor()
    )
    assert (team_id, team_name) == ("fullchaos", "Fullchaos")
    assert primary == "linked_issue"


def test_a_github_run_emitting_its_own_edges_still_keeps_the_attachment_edge() -> None:
    """The realistic run: GitHub's own extractors DID produce edges for this PR
    this run (an ``external_issue_key`` from the body). That is no evidence
    about the Linear-attachment producer, so the stored edge must survive."""
    sink = _FakeSink(
        [_stored_row(_edge(PR_ID, ISSUE_ID, last_synced=NOW - timedelta(days=40)))]
    )
    fresh = _edge(
        PR_ID, "extkey:OTHER-1", "external_issue_key", raw="external_issue_key"
    )
    merged = {
        (
            fresh.source_work_item_id,
            fresh.target_work_item_id,
            fresh.relationship_type,
        ): fresh
    }

    added = _merge_stored_inheritable_edges(sink, ORG, _github_run_items(), merged)

    assert added == 1
    team_id, _, primary = _resolve_pr(list(merged.values()), donor=_linear_donor())
    assert (team_id, primary) == ("fullchaos", "linked_issue")


def test_pruning_key_is_source_plus_relationship_type_raw() -> None:
    """CHAOS-4112 anti-regression, and the Python half of the cross-runtime key
    pin. Each arm fails under exactly one drift of the key shape."""
    stored = _edge(PR_ID, ISSUE_ID, last_synced=NOW - timedelta(days=40))
    rows = [_stored_row(stored)]

    # (a) SAME item + SAME raw re-emitted this run -> that extractor ran and
    # did not re-emit this link: a removal. Pruned.
    same_provenance = _edge(PR_ID, "linear:CHAOS-9999")
    merged = {
        (
            same_provenance.source_work_item_id,
            same_provenance.target_work_item_id,
            same_provenance.relationship_type,
        ): same_provenance
    }
    assert (
        _merge_stored_inheritable_edges(
            _FakeSink(rows), ORG, _github_run_items(), merged
        )
        == 0
    ), "stored edge survived its own extractor's re-run (key too fine)"

    # (b) SAME item, DIFFERENT raw -> a different, independently-gated
    # extractor ran. Keeping the stored edge is what protects the linkback
    # population; pruning here is the per-item drift.
    other_provenance = _edge(
        PR_ID, "extkey:CHAOS-1", "external_issue_key", raw="external_issue_key"
    )
    merged = {
        (
            other_provenance.source_work_item_id,
            other_provenance.target_work_item_id,
            other_provenance.relationship_type,
        ): other_provenance
    }
    assert (
        _merge_stored_inheritable_edges(
            _FakeSink(rows), ORG, _github_run_items(), merged
        )
        == 1
    ), "an unrelated extractor pruned the stored edge (key too coarse)"

    # (c) DIFFERENT item, same raw -> no evidence about THIS item's links.
    other_item = _edge("ghpr:full-chaos/dev-health-ops#1795", "linear:CHAOS-2")
    merged = {
        (
            other_item.source_work_item_id,
            other_item.target_work_item_id,
            other_item.relationship_type,
        ): other_item
    }
    assert (
        _merge_stored_inheritable_edges(
            _FakeSink(rows), ORG, _github_run_items(), merged
        )
        == 1
    ), "another item's provenance pruned this item's stored edge"


def test_fresh_edge_stays_authoritative_for_its_own_identity() -> None:
    """A stored row must never displace a fresh row for the same
    (source, target, relationship_type)."""
    fresh = _edge(PR_ID, ISSUE_ID, raw="github_comment_linear_url")
    merged = {(PR_ID, ISSUE_ID, "relates_to"): fresh}
    sink = _FakeSink(
        [_stored_row(_edge(PR_ID, ISSUE_ID, last_synced=NOW - timedelta(days=40)))]
    )

    assert _merge_stored_inheritable_edges(sink, ORG, _github_run_items(), merged) == 0
    assert merged[(PR_ID, ISSUE_ID, "relates_to")] is fresh


def test_stored_edge_read_stays_bounded_to_this_runs_items() -> None:
    """The read must be a keyed lookup, never a history scan: bounded to this
    run's items as the edge SOURCE and to the inheritable relationship types."""
    sink = _FakeSink([_stored_row(_edge(PR_ID, ISSUE_ID))])
    merged: dict[tuple[str, str, str], Any] = {}

    _merge_stored_inheritable_edges(sink, ORG, _github_run_items(), merged)

    assert len(sink.calls) == 1
    sql, params = sink.calls[0]
    assert "work_item_dependencies" in sql
    assert params["org_id"] == ORG
    assert params["source_ids"] == [PR_ID]
    assert set(params["rel_types"]) == {
        "relates_to",
        "relates",
        "duplicates",
        "external_issue_key",
    }
