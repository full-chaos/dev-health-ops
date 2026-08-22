"""CHAOS-4112: team inheritance must not decay across recomputes.

Attribution rows are recomputed and re-stamped on every run. The donor preload
for linked-issue inheritance used to consider THIS RUN'S fresh edges only, so
once a PR's ``relates_to`` edge fell out of the sync window every later
recompute rebuilt that PR as ``unassigned`` -- superseding its own earlier,
correct ``linked_issue`` attribution. The prod probe found 106 team-less GitHub
items carrying a CHAOS-#### ref, all 106 with a stored ``relates_to`` edge, 69
of them pointing at a Linear issue attributed ``native_team`` -> Fullchaos.
Not staleness: donor and dependent were stamped 88 seconds apart in one pass
(linear:CHAOS-3678 vs ghpr:...#1662). The recency of the EDGE decided.

The fix unions the STORED inheritable edges for the items being recomputed
with the fresh ones and lets the existing ``latest_edge`` collapse settle
conflicts by ``last_synced``.

These are pure-Python tests with a fake sink, so they run in CI's unit tier --
unlike anything marked ``clickhouse``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import pytest

from dev_health_ops.metrics.compute_work_items import (
    build_linked_issue_team_resolver,
    resolve_team_attribution,
)
from dev_health_ops.metrics.job_work_items import (
    _merge_stored_inheritable_edges,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemDependency
from dev_health_ops.providers.teams import ProjectKeyTeamResolver

ORG = "org-4112"
NOW = datetime(2026, 8, 11, tzinfo=timezone.utc)
DONOR_ID = "linear:CHAOS-3678"
DEPENDENT_ID = "ghpr:full-chaos/dev-health-ops#1662"


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
        started_at=NOW - timedelta(days=1),
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
    last_synced: datetime = NOW,
    raw: str | None = None,
) -> WorkItemDependency:
    return WorkItemDependency(
        source_work_item_id=source,
        target_work_item_id=target,
        relationship_type=relationship_type,
        relationship_type_raw=raw or relationship_type,
        last_synced=last_synced,
        org_id=ORG,
    )


def _edge_row(dep: WorkItemDependency) -> dict[str, Any]:
    """A stored row as ClickHouse returns it (``SELECT *``)."""
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
    """Stands in for the ClickHouse sink's ``query_dicts``.

    Records the parameters it was asked for, so tests can assert the lookup
    stays BOUNDED rather than scanning history.
    """

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        fail: bool = False,
        fail_times: int = 0,
    ):
        self._rows = rows or []
        self._fail = fail
        self._fail_times = fail_times
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        if self._fail:
            raise RuntimeError("clickhouse unavailable")
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("transient clickhouse blip")
        source_ids = set(params.get("source_ids") or [])
        rel_types = set(params.get("rel_types") or [])
        return [
            row
            for row in self._rows
            if row["source_work_item_id"] in source_ids
            and row["relationship_type"] in rel_types
            and row["org_id"] == params.get("org_id")
        ]


def _donor_and_dependent() -> list[WorkItem]:
    return [
        _work_item(DONOR_ID, "linear", native_team_key="fullchaos"),
        _work_item(DEPENDENT_ID, "github"),
    ]


def _team_resolvers() -> ProjectKeyTeamResolver:
    return ProjectKeyTeamResolver(
        project_key_to_team={"fullchaos": ("fullchaos", "Fullchaos")}
    )


def _resolved_team(
    work_items: list[WorkItem],
    dependencies: list[WorkItemDependency],
) -> tuple[str | None, str | None]:
    """Resolve the DEPENDENT's team through the real inheritance path."""
    pk = _team_resolvers()
    resolver = build_linked_issue_team_resolver(
        work_items=work_items,
        dependencies=dependencies,
        project_key_resolver=pk,
    )
    dependent = next(w for w in work_items if w.work_item_id == DEPENDENT_ID)
    team_id, team_name, _marked = resolve_team_attribution(
        dependent, None, pk, linked_issue_resolver=resolver
    )
    return team_id, team_name


def test_red_control_fresh_edges_only_decays_to_unassigned() -> None:
    """RED CONTROL: this is the bug, expressed as the pre-fix input.

    The edge has aged out of the sync window, so the fresh `dependencies` list
    is empty. With ONLY those edges -- the old behaviour -- the dependent
    resolves to no team even though the donor is right there, attributed. If
    this ever stops reproducing, the tests below are vacuous.
    """
    work_items = _donor_and_dependent()
    team_id, _ = _resolved_team(work_items, dependencies=[])
    assert team_id is None, "fixture no longer reproduces the decay"

    # The SAME inputs plus the stored edge do resolve a team.
    team_id, team_name = _resolved_team(
        work_items, dependencies=[_edge(DEPENDENT_ID, DONOR_ID)]
    )
    assert (team_id, team_name) == ("fullchaos", "Fullchaos")


def test_stored_edge_is_unioned_so_attribution_survives_recompute() -> None:
    """The fix: the edge is absent from the fresh window but present in the
    store, and the dependent keeps the team it already had."""
    work_items = _donor_and_dependent()
    stored = _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW - timedelta(days=90))
    sink = _FakeSink([_edge_row(stored)])

    merged: dict[tuple[str, str, str], Any] = {}  # fresh window: no edges
    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 1
    team_id, team_name = _resolved_team(work_items, list(merged.values()))
    assert (team_id, team_name) == ("fullchaos", "Fullchaos")


def test_removed_edge_does_not_keep_donating() -> None:
    """INPUT SYMMETRY: the store is the only source of stored edges, so an
    edge the store no longer has must not resurrect. Seeded with an empty
    store and an empty window, the dependent stays unassigned -- the union
    never invents an edge."""
    work_items = _donor_and_dependent()
    sink = _FakeSink([])  # the edge has been removed from the store

    merged: dict[tuple[str, str, str], Any] = {}
    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 0
    assert merged == {}
    assert _resolved_team(work_items, list(merged.values()))[0] is None


def test_fresh_edge_wins_over_the_stored_row_for_the_same_key() -> None:
    """A fresh edge is authoritative for its own key and must not be replaced
    by the stored copy, which carries an older ``last_synced``."""
    work_items = _donor_and_dependent()
    fresh = _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW)
    stale_row = _edge_row(
        _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW - timedelta(days=365))
    )
    sink = _FakeSink([stale_row])

    key = (fresh.source_work_item_id, fresh.target_work_item_id, "relates_to")
    merged: dict[tuple[str, str, str], Any] = {key: fresh}
    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 0
    assert merged[key] is fresh


def test_retyped_relationship_stops_the_old_type_donating() -> None:
    """The stale-edge case that motivated the fresh-edges-only rule.

    A relationship retyped ``relates_to`` -> ``blocked_by`` comes back fresh
    under the new type -- and a new ``relationship_type_raw``, so the pruning
    proof (which is per provenance, deliberately) does NOT cover the stored
    row and keeps it. ``latest_edge``'s recency collapse is what settles it:
    the fresh blocking edge is newer and wins, and blocking relationships
    never donate a team. This is the case that motivated the fresh-edges-only
    rule, and it still holds.
    """
    work_items = _donor_and_dependent()
    stored_row = _edge_row(
        _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW - timedelta(days=90))
    )
    fresh_block = _edge(DEPENDENT_ID, DONOR_ID, "blocked_by", last_synced=NOW)
    sink = _FakeSink([stored_row])

    merged: dict[tuple[str, str, str], Any] = {
        (DEPENDENT_ID, DONOR_ID, "blocked_by"): fresh_block
    }
    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    # A retype changes relationship_type_raw, so the stored row is NOT pruned
    # -- the pruning proof is per provenance and this is a different one. That
    # is why `latest_edge` remains the backstop: it collapses the pair by
    # last_synced and the fresh blocking edge wins.
    assert added == 1
    assert len(merged) == 2
    assert _resolved_team(work_items, list(merged.values()))[0] is None


def test_lookup_is_bounded_to_recomputed_items_and_inheritable_types() -> None:
    """Never a full-history scan: the query is keyed on this run's items as
    the edge SOURCE and on the inheritable relationship types only."""
    work_items = _donor_and_dependent()
    sink = _FakeSink([])
    _merge_stored_inheritable_edges(sink, ORG, work_items, {})

    assert len(sink.calls) == 1
    sql, params = sink.calls[0]
    assert params["org_id"] == ORG
    assert params["source_ids"] == sorted([DONOR_ID, DEPENDENT_ID])
    assert set(params["rel_types"]) == {
        "relates_to",
        "relates",
        "duplicates",
        "external_issue_key",
    }
    # Blocking types routinely span teams and must never be requested.
    assert "blocks" not in params["rel_types"]
    assert "blocked_by" not in params["rel_types"]
    assert "source_work_item_id IN" in sql
    assert "relationship_type IN" in sql


def test_store_failure_degrades_to_the_sync_window() -> None:
    """Telemetry and recovery must never fail the run they observe."""
    work_items = _donor_and_dependent()
    fresh = _edge(DEPENDENT_ID, DONOR_ID)
    key = (DEPENDENT_ID, DONOR_ID, "relates_to")
    merged: dict[tuple[str, str, str], Any] = {key: fresh}

    added = _merge_stored_inheritable_edges(
        _FakeSink(fail=True), ORG, work_items, merged
    )

    assert added == 0
    assert merged == {key: fresh}  # the fresh window survives untouched


@pytest.mark.parametrize(
    ("org_id", "items"),
    [("", "some"), (ORG, "none")],
)
def test_no_lookup_without_org_or_items(org_id: str, items: str) -> None:
    sink = _FakeSink([])
    work_items = _donor_and_dependent() if items == "some" else []
    assert _merge_stored_inheritable_edges(sink, org_id, work_items, {}) == 0
    assert sink.calls == []


# --------------------------------------------------------------------------
# Downgrade telemetry (standing order: the decay signature is never silent)
# --------------------------------------------------------------------------


class _RecordingCounter:
    def __init__(self) -> None:
        self.increments: list[dict[str, str]] = []

    def labels(self, **values: str) -> _RecordingCounter:
        self._pending = values
        return self

    def inc(self, amount: float = 1) -> None:
        self.increments.append(self._pending)


def _attribution(
    work_item_id: str,
    source: str,
    *,
    team_id: str | None,
    is_primary: int = 1,
    provider: str = "github",
) -> Any:
    from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

    return WorkItemTeamAttributionRecord(
        work_item_id=work_item_id,
        provider=provider,
        source=source,
        is_primary=is_primary,
        confidence="high",
        evidence="",
        computed_at=NOW,
        team_id=team_id,
        team_name=("Fullchaos" if team_id else None),
        org_id=ORG,
    )


@pytest.fixture
def counter(monkeypatch: pytest.MonkeyPatch) -> _RecordingCounter:
    import dev_health_ops.metrics.job_work_items as job

    recording = _RecordingCounter()
    monkeypatch.setattr(job, "ATTRIBUTION_DOWNGRADES_TOTAL", recording)
    return recording


def test_downgrade_is_counted_and_warned(
    counter: _RecordingCounter, caplog: pytest.LogCaptureFixture
) -> None:
    """The decay signature: an item whose primary attribution came from a real
    team source resolves to `unassigned` on a later run."""
    import logging

    from dev_health_ops.metrics.job_work_items import _report_attribution_downgrades

    prior = {DEPENDENT_ID: ("linked_issue", "github")}
    new = [_attribution(DEPENDENT_ID, "unassigned", team_id=None)]

    with caplog.at_level(logging.WARNING):
        assert _report_attribution_downgrades(prior, new) == 1

    assert counter.increments == [
        {"provider": "github", "previous_source": "linked_issue"}
    ]
    assert any(
        "DOWNGRADED" in r.message or "DOWNGRADED" in r.getMessage()
        for r in caplog.records
    )
    assert any(DEPENDENT_ID in r.getMessage() for r in caplog.records)


def test_recovery_is_not_a_downgrade(counter: _RecordingCounter) -> None:
    """unassigned -> teamed is the fix working, not a data loss."""
    from dev_health_ops.metrics.job_work_items import _report_attribution_downgrades

    # An item with no prior TEAMED attribution is absent from `prior` by
    # construction (the loader filters team-less rows out).
    assert (
        _report_attribution_downgrades(
            {}, [_attribution(DEPENDENT_ID, "linked_issue", team_id="fullchaos")]
        )
        == 0
    )
    assert counter.increments == []


def test_precedence_change_between_teamed_sources_is_not_a_downgrade(
    counter: _RecordingCounter,
) -> None:
    """linked_issue -> native_team keeps a team; nothing was lost."""
    from dev_health_ops.metrics.job_work_items import _report_attribution_downgrades

    prior = {DEPENDENT_ID: ("linked_issue", "github")}
    new = [_attribution(DEPENDENT_ID, "native_team", team_id="fullchaos")]

    assert _report_attribution_downgrades(prior, new) == 0
    assert counter.increments == []


def test_teamless_row_with_no_team_id_counts_as_a_downgrade(
    counter: _RecordingCounter,
) -> None:
    """A row can name a source but carry no team; that is still a loss."""
    from dev_health_ops.metrics.job_work_items import _report_attribution_downgrades

    prior = {DEPENDENT_ID: ("native_team", "linear")}
    new = [_attribution(DEPENDENT_ID, "repo_ownership", team_id=None)]

    assert _report_attribution_downgrades(prior, new) == 1
    assert counter.increments == [
        {"provider": "github", "previous_source": "native_team"}
    ]


def test_non_primary_rows_are_ignored(counter: _RecordingCounter) -> None:
    """compute stamps every candidate; only the primary is the attribution."""
    from dev_health_ops.metrics.job_work_items import _report_attribution_downgrades

    prior = {DEPENDENT_ID: ("linked_issue", "github")}
    new = [_attribution(DEPENDENT_ID, "unassigned", team_id=None, is_primary=0)]

    assert _report_attribution_downgrades(prior, new) == 0
    assert counter.increments == []


def test_prior_loader_ignores_teamless_and_stale_rows() -> None:
    """Only a row that actually carried a team can be downgraded FROM, and the
    query must use the latest-primary fence -- without it an older candidate
    masquerades as the current attribution and manufactures a phantom
    downgrade."""
    from dev_health_ops.metrics.job_work_items import (
        _load_prior_primary_attributions,
    )

    rows = [
        {
            "work_item_id": DEPENDENT_ID,
            "source": "linked_issue",
            "provider": "github",
            "team_id": "fullchaos",
        },
        {
            "work_item_id": "ghpr:other#1",
            "source": "unassigned",
            "provider": "github",
            "team_id": "",
        },
    ]

    class _Sink:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def query_dicts(self, sql: str, params: dict[str, Any]):
            self.calls.append((sql, params))
            return rows

    sink = _Sink()
    prior = _load_prior_primary_attributions(sink, ORG, [DEPENDENT_ID, "ghpr:other#1"])

    assert prior == {DEPENDENT_ID: ("linked_issue", "github")}
    sql, params = sink.calls[0]
    assert "FROM work_item_team_attributions FINAL" in sql
    assert "is_primary = 1" in sql
    assert "max(computed_at)" in sql
    assert params["ids"] == sorted([DEPENDENT_ID, "ghpr:other#1"])


def test_prior_loader_degrades_when_the_store_is_unavailable() -> None:
    from dev_health_ops.metrics.job_work_items import (
        _load_prior_primary_attributions,
    )

    assert (
        _load_prior_primary_attributions(_FakeSink(fail=True), ORG, [DEPENDENT_ID])
        == {}
    )


# --------------------------------------------------------------------------
# The stored-edge read is load-bearing for attribution, so its failure mode
# matters as much as its success path.
# --------------------------------------------------------------------------


def test_transient_read_failure_is_retried_and_recovers() -> None:
    """A blip on the first attempt must not drop the run back to
    sync-window-only inheritance -- that would silently recreate the exact
    decay this change exists to prevent."""
    work_items = _donor_and_dependent()
    stored = _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW - timedelta(days=90))
    sink = _FakeSink([_edge_row(stored)], fail_times=1)

    merged: dict[tuple[str, str, str], Any] = {}
    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert len(sink.calls) == 2, "the read must be retried once"
    assert added == 1
    assert _resolved_team(work_items, list(merged.values()))[0] == "fullchaos"


def test_persistent_read_failure_is_counted_not_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """When both attempts fail, inheritance falls back to the sync window for
    this run. That is the pre-fix behaviour and it is self-healing -- the next
    successful run finds the stored edge again -- but it is a degraded window
    and must be measurable, not just logged.
    """
    import logging

    import dev_health_ops.metrics.job_work_items as job

    failures = _RecordingCounter()
    monkeypatch.setattr(job, "STORED_EDGE_LOAD_FAILURES_TOTAL", failures)

    work_items = _donor_and_dependent()
    sink = _FakeSink(fail=True)
    merged: dict[tuple[str, str, str], Any] = {}

    with caplog.at_level(logging.WARNING):
        added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 0
    assert len(sink.calls) == 2, "both attempts must be made before giving up"
    assert failures.increments == [{"org_id": ORG}]
    assert any("limited to the sync window" in r.getMessage() for r in caplog.records)

    # And the consequence is stated plainly: with an empty fresh window the
    # dependent does resolve teamless for this run. This is the accepted,
    # transient cost of an unavailable store -- pinned so it is a known
    # behaviour rather than a surprise.
    assert _resolved_team(work_items, list(merged.values()))[0] is None


def test_link_removed_upstream_stops_donating() -> None:
    """NO-RESURRECTION control, in the form the data model actually supports.

    Providers RE-EXTRACT an item's links every sync and stamp them
    ``last_synced=now``, so a link still present upstream reappears in this
    run's fresh edges. Here the PR was re-synced and DID produce a fresh edge
    (to an unrelated item), proving the extractor ran -- so the stored donor
    edge, which was not re-emitted, is a link the provider removed. It must
    stop donating, and the dependent must fall back to no team rather than
    inheriting forever from a dead link.
    """
    work_items = _donor_and_dependent()
    removed = _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW - timedelta(days=400))
    sink = _FakeSink([_edge_row(removed)])

    # This run re-synced the PR and extracted a DIFFERENT link, so the
    # extractor demonstrably ran.
    still_present = _edge(DEPENDENT_ID, "linear:CHAOS-9999", last_synced=NOW)
    merged: dict[tuple[str, str, str], Any] = {
        (DEPENDENT_ID, "linear:CHAOS-9999", "relates_to"): still_present
    }

    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 0, "a removed link must not be resurrected from the store"
    assert (DEPENDENT_ID, DONOR_ID, "relates_to") not in merged
    assert _resolved_team(work_items, list(merged.values()))[0] is None


def test_item_not_resynced_keeps_its_stored_edge() -> None:
    """The decay case, and why the removal rule cannot swallow it.

    This item produced NO fresh edges, so there is no evidence the extractor
    ran for it -- it simply fell outside the sync window. "Link removed" and
    "this sync path never extracts dependencies" are indistinguishable here,
    so the stored edge is kept and the attribution survives.
    """
    work_items = _donor_and_dependent()
    stored = _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW - timedelta(days=400))
    sink = _FakeSink([_edge_row(stored)])

    merged: dict[tuple[str, str, str], Any] = {}  # no fresh edges at all
    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 1
    assert _resolved_team(work_items, list(merged.values()))[0] == "fullchaos"


def test_removal_evidence_is_per_item_not_global() -> None:
    """Another item having fresh edges says nothing about THIS item's sync.

    The donor was re-synced and produced an edge; the dependent was not. The
    dependent's stored edge must still donate -- otherwise one busy item in
    the batch would strip inheritance from every quiet one.
    """
    work_items = _donor_and_dependent()
    stored = _edge(DEPENDENT_ID, DONOR_ID, last_synced=NOW - timedelta(days=400))
    sink = _FakeSink([_edge_row(stored)])

    # Fresh edge belongs to the DONOR, a different source item.
    merged: dict[tuple[str, str, str], Any] = {
        (DONOR_ID, "linear:CHAOS-9999", "relates_to"): _edge(
            DONOR_ID, "linear:CHAOS-9999", last_synced=NOW
        )
    }

    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 1
    assert _resolved_team(work_items, list(merged.values()))[0] == "fullchaos"


def test_fresh_body_edge_does_not_delete_a_stored_comment_edge() -> None:
    """The pruning proof must be per PROVENANCE, not per item.

    GitHub edges come from two extractors: the PR body, always parsed, and
    Linear linkback comments, gated by GITHUB_FETCH_COMMENTS and capped by
    GITHUB_COMMENTS_LIMIT. A run where comments are disabled, capped or
    failing still produces a fresh BODY edge. Treating that as proof the whole
    dependency snapshot is complete would delete the stored
    `github_comment_linear_url` edge and un-attribute the PR -- decaying
    precisely the Linear-linkback population this ticket protects.

    The stored comment edge must survive, and the team with it.
    """
    work_items = _donor_and_dependent()
    stored_comment_edge = _edge(
        DEPENDENT_ID,
        DONOR_ID,
        last_synced=NOW - timedelta(days=200),
        raw="github_comment_linear_url",
    )
    sink = _FakeSink([_edge_row(stored_comment_edge)])

    # This run parsed the body and found a different link. Comments did not run.
    fresh_body_edge = _edge(
        DEPENDENT_ID, "linear:CHAOS-9999", last_synced=NOW, raw="external_issue_key"
    )
    merged: dict[tuple[str, str, str], Any] = {
        (DEPENDENT_ID, "linear:CHAOS-9999", "relates_to"): fresh_body_edge
    }

    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 1, "a body-only sync must not prune a comment-derived edge"
    assert _resolved_team(work_items, list(merged.values()))[0] == "fullchaos"


def test_same_extractor_rerunning_does_prune_its_own_removed_edge() -> None:
    """The other half: when the SAME extractor re-ran and dropped the link,
    the stored edge is a genuine removal and must stop donating."""
    work_items = _donor_and_dependent()
    stored_comment_edge = _edge(
        DEPENDENT_ID,
        DONOR_ID,
        last_synced=NOW - timedelta(days=200),
        raw="github_comment_linear_url",
    )
    sink = _FakeSink([_edge_row(stored_comment_edge)])

    # Comments DID run this time and captured a different Linear issue.
    fresh_comment_edge = _edge(
        DEPENDENT_ID,
        "linear:CHAOS-9999",
        last_synced=NOW,
        raw="github_comment_linear_url",
    )
    merged: dict[tuple[str, str, str], Any] = {
        (DEPENDENT_ID, "linear:CHAOS-9999", "relates_to"): fresh_comment_edge
    }

    added = _merge_stored_inheritable_edges(sink, ORG, work_items, merged)

    assert added == 0, "the same extractor re-ran and dropped this link"
    assert _resolved_team(work_items, list(merged.values()))[0] is None
