"""No-LLM membership backfill with run_id / completion-marker protocol (CHAOS-2439/2433).

The daily scheduled job must NOT re-run LLM categorization (cost + category
drift). Instead it cheaply PROJECTS ``work_unit_membership`` from the theme /
subcategory distributions ALREADY persisted in ``work_unit_investments`` by the
post-sync LLM materializer.

Per org, with NO categorizer/LLM call:

1. Rebuild the work-graph connected components from ``work_graph_edges`` (reuse
   ``_build_components`` + ``work_unit_id`` so the unit hashing matches the
   materializer exactly).
2. Read the LATEST ``work_unit_investments`` row per ``work_unit_id`` (argMax on
   ``computed_at``, the same latest-per-unit semantics as
   ``api/queries/work_unit_investments.py``) for those unit ids to recover the
   persisted ``theme_distribution_json`` / ``subcategory_distribution_json``.
3. Generate a fresh ``run_id`` for this backfill run.
4. Project membership rows via the SHARED ``build_membership_records`` helper, so
   the rows are byte-for-byte identical (except run_id / computed_at) to what the
   LLM materializer would emit for the same distributions.
5. Write ALL membership rows first, THEN write one ``work_unit_membership_runs``
   row (the completion marker) as the LAST step (CHAOS-2433 protocol).  The
   marker is published whenever the org HAS work-graph components, even when the
   run emits ZERO membership rows (all components churned) — an empty complete
   run correctly retires the previous run's stale rows (no tombstones).  Only a
   genuine no-component org publishes no marker.  A repo-SCOPED run never
   publishes the org-wide marker (it would blank other repos for unscoped
   reads); it relies on the org-wide daily run to publish.

RUN_ID PROTOCOL (CHAOS-2433):
  Every backfill run generates its own ``run_id`` (uuid hex). ALL membership rows
  written in this run carry that run_id. The completion marker is written to
  ``work_unit_membership_runs`` only AFTER all membership rows are persisted.
  A run with membership rows but no marker is incomplete (in-flight or crashed)
  and is INVISIBLE to readers. The resolver selects
  ``argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id=?``
  and scopes membership reads to that run_id.

NO TOMBSTONES:
  The run_id protocol makes tombstones unnecessary. A node absent from the latest
  complete run simply has no membership rows in that run, and the resolver treats
  it exactly as "no membership" (annotation null, not filterable). This is cleaner
  than the tombstone sentinel and eliminates the is_dominant=0 sentinel edge case.

"LATEST COMPLETE INVESTMENT RUN":
  The backfill projects from the most recently persisted row per work_unit_id in
  ``work_unit_investments`` (argMax on computed_at). If a work_unit_id has no row
  (edges churned since last LLM run), that component is skipped — it contributes
  no membership rows to this backfill run. Since the run_id protocol hides all
  rows not in the latest complete run, churned nodes are automatically invisible
  with no special tombstone action required.

  DESIGN NOTE (for reviewers): we intentionally use argMax(computed_at) per
  work_unit_id rather than gating on a separate "investments run completion
  marker" table.  work_unit_investments is written atomically per component by
  the materializer (one INSERT per batch), and its categorization_run_id is not
  globally consistent across component batches (components are processed in
  parallel).  The argMax per work_unit_id is the established semantics used by
  the investment query API; projecting from that is consistent with what the
  resolver already reads.  A future CHAOS ticket can add an investments run
  marker if needed; for now, argMax is correct and sufficient.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.metrics.schemas import WorkUnitMembershipRunRecord
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.factory import create_sink
from dev_health_ops.work_graph.investment.membership import (
    NodeKey,
    build_membership_records,
)
from dev_health_ops.work_graph.investment.queries import fetch_work_graph_edges
from dev_health_ops.work_graph.investment.utils import work_unit_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MembershipBackfillConfig:
    dsn: str
    org_id: str | None = None
    repo_ids: list[str] | None = None

    @property
    def is_org_wide(self) -> bool:
        """True when this run covers the WHOLE org (no repo scoping).

        Only an org-wide run may publish an org-wide completion marker
        (CHAOS-2433 finding #2): a repo-scoped run that wrote a marker would
        become the org's "latest complete run" while containing only that
        scope's rows, blanking every other repo's membership for unscoped
        reads.  A scoped run therefore writes its rows but NOT a marker,
        relying on a subsequent org-wide run (the daily backfill is org-wide)
        to publish.
        """
        return not self.repo_ids


def _build_components_for_backfill(
    edges: list[dict[str, Any]],
) -> list[list[NodeKey]]:
    """Connected-component node lists from work_graph_edges.

    Mirrors ``materialize._build_components`` (same union-find over
    source/target endpoints) but returns only the per-component node lists — the
    backfill does not need the component edges. Kept local to avoid importing the
    heavy ``materialize`` module (and its LLM provider deps) into the worker.
    """
    adjacency: dict[NodeKey, list[NodeKey]] = {}
    for edge in edges:
        source = (str(edge.get("source_type")), str(edge.get("source_id")))
        target = (str(edge.get("target_type")), str(edge.get("target_id")))
        adjacency.setdefault(source, []).append(target)
        adjacency.setdefault(target, []).append(source)

    visited: set[NodeKey] = set()
    components: list[list[NodeKey]] = []
    for node in adjacency:
        if node in visited:
            continue
        stack = [node]
        visited.add(node)
        component_nodes: list[NodeKey] = []
        while stack:
            current = stack.pop()
            component_nodes.append(current)
            for neighbor in adjacency.get(current, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        components.append(component_nodes)
    return components


def _fetch_latest_distributions(
    sink: BaseMetricsSink,
    *,
    work_unit_ids: list[str],
    org_id: str,
) -> dict[str, dict[str, Any]]:
    """Return {work_unit_id -> {theme/ subcategory distribution + status}}.

    Reads the LATEST row per ``work_unit_id`` (argMax on ``computed_at``), the
    same latest-per-unit semantics as ``api/queries/work_unit_investments.py``.
    Org-scoped. Returns only the unit ids that actually have a row.
    """
    if not work_unit_ids:
        return {}
    query = """
        SELECT
            work_unit_id,
            argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
            argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution_json,
            argMax(categorization_status, computed_at) AS categorization_status
        FROM work_unit_investments
        WHERE org_id = %(org_id)s
          AND work_unit_id IN %(work_unit_ids)s
        GROUP BY org_id, work_unit_id
    """
    rows = sink.query_dicts(
        query,
        {"org_id": org_id, "work_unit_ids": work_unit_ids},
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        wid = str(row.get("work_unit_id") or "")
        if wid:
            result[wid] = row
    return result


def _as_distribution(value: Any) -> dict[str, float]:
    """Coerce a ClickHouse Map(String, Float64) cell into a plain dict."""
    if isinstance(value, dict):
        return {str(k): float(v) for k, v in value.items()}
    return {}


def backfill_memberships(config: MembershipBackfillConfig) -> dict[str, int]:
    """Project work_unit_membership from existing work_unit_investments (no LLM).

    Uses the run_id / completion-marker protocol (CHAOS-2433): all membership
    rows for this run are written first, then the completion marker is written
    last.  Nodes in components with no persisted investment row are skipped —
    they contribute no rows to this run and are therefore invisible to readers
    (no tombstones needed).

    Returns a stats dict: components seen, units matched (had a persisted
    categorization), units skipped (churned component / never categorized), and
    total membership rows written.
    """
    sink = create_sink(config.dsn)
    org_id = config.org_id or ""
    try:
        sink.ensure_schema()

        edges = fetch_work_graph_edges(sink, repo_ids=config.repo_ids, org_id=org_id)
        components = _build_components_for_backfill(edges)
        if not components:
            logger.info(
                "Membership backfill: no work graph components for org=%s", org_id
            )
            return {
                "components": 0,
                "matched": 0,
                "skipped": 0,
                "memberships": 0,
            }

        # Map each current work_unit_id -> its node list.
        unit_nodes_by_id: dict[str, list[NodeKey]] = {}
        for nodes in components:
            unit_nodes = list(dict.fromkeys(nodes))
            uid = work_unit_id(unit_nodes)
            unit_nodes_by_id[uid] = unit_nodes

        distributions = _fetch_latest_distributions(
            sink,
            work_unit_ids=list(unit_nodes_by_id.keys()),
            org_id=org_id,
        )

        # A single run_id for the entire backfill run.  All rows carry this id;
        # the completion marker is written last (CHAOS-2433 protocol).
        backfill_run_id = uuid.uuid4().hex
        computed_at = datetime.now(timezone.utc)
        from dev_health_ops.metrics.schemas import (
            WorkUnitMembershipRecord,  # noqa: F401
        )

        membership_records: list[WorkUnitMembershipRecord] = []
        matched = 0
        skipped = 0
        for uid, unit_nodes in unit_nodes_by_id.items():
            persisted = distributions.get(uid)
            if persisted is None:
                # No persisted investment row for this component (edges churned
                # since last LLM run).  Skip — the run_id protocol makes these
                # nodes invisible without tombstones: they have no rows in the
                # new run, so readers won't see them once the new run completes.
                skipped += 1
                continue
            matched += 1
            membership_records.extend(
                build_membership_records(
                    unit_nodes=unit_nodes,
                    work_unit_id=uid,
                    theme_distribution=_as_distribution(
                        persisted.get("theme_distribution_json")
                    ),
                    subcategory_distribution=_as_distribution(
                        persisted.get("subcategory_distribution_json")
                    ),
                    categorization_status=str(
                        persisted.get("categorization_status") or ""
                    ),
                    computed_at=computed_at,
                    org_id=org_id,
                    run_id=backfill_run_id,
                )
            )

        # Write ALL membership rows first, THEN the completion marker.
        # A run with membership rows but no marker is incomplete and invisible
        # to readers.
        if membership_records:
            sink.write_work_unit_memberships(membership_records)

        # Publish the completion marker (CHAOS-2433).
        #
        # FINDING #1 (empty-but-complete run MUST supersede): we reached here
        # only when the org HAS work-graph components (the `if not components`
        # early-return above handles the genuine no-op org).  An ALL-SKIPPED
        # run — every current component churned past its last categorization —
        # legitimately has ZERO membership rows.  It MUST still publish a marker
        # so it becomes the latest complete run and the stale rows from the
        # PREVIOUS complete run stop matching (the no-tombstone design relies on
        # an empty complete run to retire churned nodes).  We therefore write the
        # marker whenever components were evaluated, even with zero rows.  Only a
        # genuine no-component org (handled above) writes no marker.
        #
        # FINDING #2 (scoped runs must not publish an org-wide marker): a
        # repo-scoped backfill writes its rows but does NOT publish the org-wide
        # marker — otherwise it would become the org's latest complete run while
        # covering only that scope, blanking other repos for unscoped reads.
        #
        # ROUND-3 FINDING #1 (marker completed_at = COMPLETION time, not start):
        # the marker's completed_at must reflect when the run FINISHED publishing
        # (after all rows are persisted), NOT the run-start ``computed_at``.
        # Readers pick argMax(run_id, completed_at); if two runs overlap, the one
        # that FINISHES last must win.  Stamping the run-start time would let a
        # run that started earlier but finished later publish an OLDER completed_at
        # and lose to a run that finished first — the exact concurrency race the
        # protocol must prevent.  The membership ROWS keep their run ``computed_at``;
        # only the MARKER carries this completion timestamp.
        marker_completed_at = datetime.now(timezone.utc)
        if config.is_org_wide:
            sink.write_membership_run(
                WorkUnitMembershipRunRecord(
                    org_id=org_id,
                    run_id=backfill_run_id,
                    completed_at=marker_completed_at,
                )
            )
            # RETENTION (CHAOS-2433 round-5 — unbounded growth): migration 049
            # keeps run_id in the dedup key so old generations are NOT collapsed.
            # Without pruning, every org-wide projection (daily beat + post-sync)
            # adds a full copy of the org's memberships forever. Right after the
            # new marker lands, prune to the latest 2 COMPLETE runs per org. This
            # only ever deletes MARKERED runs (the candidate set comes from
            # work_unit_membership_runs), so a markerless in-flight run is never
            # touched. keep=2 leaves the current + one prior so an overlap reader
            # against the immediately-previous complete run is not pulled out from
            # under it.
            try:
                sink.prune_membership_runs(org_id, keep=2)
            except Exception:
                # Retention is best-effort: a prune failure must not fail the
                # projection (the marker is already published and correct). The
                # next run's prune is idempotent and will catch up.
                logger.warning(
                    "Membership run retention failed for org=%s (non-fatal); "
                    "the next projection's prune will catch up",
                    org_id,
                    exc_info=True,
                )
        else:
            logger.info(
                "Membership backfill org=%s is repo-scoped (repos=%s) — wrote "
                "%d rows but NOT publishing an org-wide completion marker "
                "(CHAOS-2433); the org-wide daily backfill publishes",
                org_id,
                config.repo_ids,
                len(membership_records),
            )

        logger.info(
            "Membership backfill org=%s: components=%d matched=%d skipped=%d "
            "memberships=%d run_id=%s (no LLM)",
            org_id,
            len(components),
            matched,
            skipped,
            len(membership_records),
            backfill_run_id,
        )
        return {
            "components": len(components),
            "matched": matched,
            "skipped": skipped,
            "memberships": len(membership_records),
        }
    finally:
        sink.close()
