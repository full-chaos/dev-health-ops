"""Resolver for work graph edge queries."""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Any

from dev_health_ops.api.services.identity import looks_like_uuid

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.inputs import WorkGraphEdgeFilterInput
from ..models.outputs import (
    PageInfo,
    WorkGraphArtifactRow,
    WorkGraphArtifactsResult,
    WorkGraphEdgeResult,
    WorkGraphEdgesResult,
    WorkGraphEdgeType,
    WorkGraphFlowResult,
    WorkGraphFlowRow,
    WorkGraphNodeType,
    WorkGraphProvenance,
)
from ._membership_run_scope import (
    LATEST_COMPLETE_RUN_SUBQUERY as _LATEST_COMPLETE_RUN_SUBQUERY,
)
from ._membership_run_scope import LEGACY_NODE_MAX_JOIN as _LEGACY_NODE_MAX_JOIN
from ._membership_run_scope import RUN_SCOPE_PREDICATE as _RUN_SCOPE_PREDICATE

logger = logging.getLogger(__name__)

_OPAQUE_HEX_ID_RE = re.compile(r"^[0-9a-f]{24,}$", re.IGNORECASE)

# PR ids stored in work_graph_edges use the format "{repo_uuid}#pr{number}".
# This pattern is not a bare UUID so it slips past looks_like_uuid(), but it
# is not human-readable either — it must be resolved to the PR title.
_PR_EDGE_ID_RE = re.compile(
    r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})#pr(\d+)$",
    re.IGNORECASE,
)

# Incident status → customer-facing label.  Unknown statuses fall back to a
# neutral "Incident" label so raw enum strings never reach customer copy.
_INCIDENT_STATUS_LABELS: dict[str, str] = {
    "open": "Open",
    "triggered": "Triggered",
    "acknowledged": "Acknowledged",
    "investigating": "Investigating",
    "resolved": "Resolved",
    "closed": "Closed",
}


def _uses_scoped_partial(filters: WorkGraphEdgeFilterInput | None) -> bool:
    return bool(_partial_repo_ids(filters))


def _partial_repo_ids(filters: WorkGraphEdgeFilterInput | None) -> list[str]:
    if filters is None or not filters.allow_scoped_partial or not filters.repo_ids:
        return []
    return sorted({str(repo_id) for repo_id in filters.repo_ids if repo_id})


def _membership_run_subquery(filters: WorkGraphEdgeFilterInput | None) -> str:
    if not _uses_scoped_partial(filters):
        return _LATEST_COMPLETE_RUN_SUBQUERY
    return """
        SELECT run_id AS latest_run_id
        FROM work_unit_membership_scoped_runs
        WHERE org_id = %(org_id)s
          AND scope_kind = 'repo'
          AND scope_id IN %(scoped_repo_ids)s
        GROUP BY run_id
        HAVING uniqExact(scope_id) = %(scoped_repo_count)s
        ORDER BY max(completed_at) DESC, run_id DESC
        LIMIT 1
    """


def _add_membership_scope_params(
    params: dict[str, Any], filters: WorkGraphEdgeFilterInput | None
) -> None:
    scoped_repo_ids = _partial_repo_ids(filters)
    if not scoped_repo_ids:
        return
    params["scoped_repo_ids"] = scoped_repo_ids
    params["scoped_repo_count"] = len(scoped_repo_ids)


def _incident_label(status: str) -> str:
    """Map a raw incident status string to a normalised customer-facing label."""
    return _INCIDENT_STATUS_LABELS.get(status.lower(), "Incident")


def _map_node_type(value: str) -> WorkGraphNodeType:
    try:
        return WorkGraphNodeType(value.lower())
    except ValueError:
        return WorkGraphNodeType.ISSUE


def _map_edge_type(value: str) -> WorkGraphEdgeType:
    try:
        return WorkGraphEdgeType(value.lower())
    except ValueError:
        return WorkGraphEdgeType.RELATES


def _map_provenance(value: str) -> WorkGraphProvenance:
    try:
        return WorkGraphProvenance(value.lower())
    except ValueError:
        return WorkGraphProvenance.HEURISTIC


def _display_name_for(
    entity_id: str, resolved: dict[str, str] | None = None
) -> str | None:
    """A7/A8: pass through human-readable ids; return None for unresolvable UUIDs.

    Resolution priority:
    1. Lookup-resolved name (from batch lookup) — takes precedence.
    2. Human-readable pass-through for non-UUID, non-hex identifiers
       (e.g. PROJ-123, INC-001, deploy-xyz).
    3. None for bare UUIDs, opaque hex strings, and UUID-based PR ids that
       were not resolved — the client renders a controlled Unresolved badge
       rather than leaking a raw UUID (A8).
    """
    raw = str(entity_id).strip()
    if not raw:
        return None
    # Lookup-resolved names (from batch lookup) take precedence.
    if resolved and raw in resolved:
        return resolved[raw]
    # UUID-based PR ids are not human-readable even though they do not match
    # the bare-UUID regex (they carry a "#pr{N}" suffix).
    if _PR_EDGE_ID_RE.match(raw):
        return None
    # Bare UUIDs and opaque hex strings are not human-readable.
    if looks_like_uuid(raw) or _OPAQUE_HEX_ID_RE.match(raw):
        return None
    # Human-readable id — pass through verbatim.
    return raw


async def _batch_resolve_display_names(
    client: Any,
    org_id: str,
    rows: list[dict[str, Any]],
) -> dict[str, str]:
    """Resolve display names for UUID-derived ids in one query per entity type.

    Collects unresolved source/target ids across the edge page, grouped by
    entity type, then issues ONE ClickHouse query per type (no N+1).
    org_id is included in every join predicate to prevent cross-tenant leaks.

    Returns a mapping {entity_id -> display_name} for all successfully
    resolved ids.  Any ids absent from the returned dict remain unresolved
    and will surface as None (→ client Unresolved badge).
    """
    from dev_health_ops.api.queries.client import query_dicts

    resolved: dict[str, str] = {}

    # Collect ids that need lookup, partitioned by entity type.
    pr_ids: set[str] = set()  # "{uuid}#pr{N}" format
    deployment_ids: set[str] = set()  # bare UUID deployment ids
    incident_ids: set[str] = set()  # bare UUID incident ids

    for row in rows:
        for id_field, type_field in (
            ("source_id", "source_type"),
            ("target_id", "target_type"),
        ):
            entity_id = str(row.get(id_field) or "").strip()
            entity_type = str(row.get(type_field) or "").lower()
            if not entity_id:
                continue

            is_pr_format = bool(_PR_EDGE_ID_RE.match(entity_id))
            is_bare_uuid = looks_like_uuid(entity_id)
            is_opaque_hex = bool(_OPAQUE_HEX_ID_RE.match(entity_id))

            # Opaque hex ids (feature_flag hashes, etc.) are not resolvable.
            if is_opaque_hex:
                continue
            # Only collect ids that need a table lookup.
            if not (is_pr_format or is_bare_uuid):
                continue

            if is_pr_format or entity_type == "pr":
                pr_ids.add(entity_id)
            elif entity_type == "deployment" and is_bare_uuid:
                deployment_ids.add(entity_id)
            elif entity_type == "incident" and is_bare_uuid:
                incident_ids.add(entity_id)

    # --- PRs: one query against git_pull_requests -------------------------
    if pr_ids:
        # Only "{uuid}#pr{N}" ids can be resolved; bare UUID pr ids cannot.
        pr_lookups: dict[str, tuple[str, int]] = {}
        repo_uuids: set[str] = set()
        for pr_id in pr_ids:
            m = _PR_EDGE_ID_RE.match(pr_id)
            if m:
                repo_uuid = m.group(1).lower()
                pr_num = int(m.group(2))
                pr_lookups[pr_id] = (repo_uuid, pr_num)
                repo_uuids.add(repo_uuid)

        if pr_lookups and repo_uuids:
            pr_numbers = sorted({pr_num for _, pr_num in pr_lookups.values()})
            try:
                pr_rows = await query_dicts(
                    client,
                    """
                    SELECT toString(repo_id) AS repo_id, number, title
                    FROM git_pull_requests FINAL
                    WHERE org_id = %(org_id)s
                      AND toString(repo_id) IN %(repo_ids)s
                      AND number IN %(pr_numbers)s
                    """,
                    {
                        "org_id": org_id,
                        "repo_ids": sorted(repo_uuids),
                        "pr_numbers": pr_numbers,
                    },
                )
                # Build (repo_id_lower, number) → title lookup.
                pr_title_map: dict[tuple[str, int], str] = {}
                for r in pr_rows:
                    repo_id = str(r.get("repo_id") or "").lower()
                    number = int(r.get("number") or 0)
                    # Keep variable name distinct from the outer loop's `resolved_title`
                    # to avoid mypy seeing str | None rebind the earlier `str` annotation.
                    row_title = str(r.get("title") or "").strip()
                    if repo_id and number and row_title:
                        pr_title_map[(repo_id, number)] = row_title

                for pr_id, (repo_uuid, pr_num) in pr_lookups.items():
                    resolved_title: str | None = pr_title_map.get((repo_uuid, pr_num))
                    if resolved_title:
                        resolved[pr_id] = resolved_title
            except Exception:
                logger.warning("PR display-name lookup failed", exc_info=True)

    # --- Deployments: one query against deployments -----------------------
    if deployment_ids:
        dep_ids = sorted(deployment_ids)
        try:
            dep_rows = await query_dicts(
                client,
                """
                SELECT deployment_id, environment
                FROM deployments FINAL
                WHERE org_id = %(org_id)s
                  AND deployment_id IN %(dep_ids)s
                """,
                {"org_id": org_id, "dep_ids": dep_ids},
            )
            for r in dep_rows:
                dep_id = str(r.get("deployment_id") or "")
                env = str(r.get("environment") or "").strip()
                # Only store a label when we have a meaningful environment string.
                # Empty env → omit from resolved so _display_name_for returns None
                # (Unresolved badge) rather than leaking the raw UUID (A8).
                if dep_id and env:
                    resolved[dep_id] = f"{env} deploy"
        except Exception:
            logger.warning("Deployment display-name lookup failed", exc_info=True)

    # --- Incidents: one query against incidents ---------------------------
    if incident_ids:
        inc_ids = sorted(incident_ids)
        try:
            inc_rows = await query_dicts(
                client,
                """
                SELECT incident_id, status
                FROM incidents FINAL
                WHERE org_id = %(org_id)s
                  AND incident_id IN %(inc_ids)s
                """,
                {"org_id": org_id, "inc_ids": inc_ids},
            )
            for r in inc_rows:
                inc_id = str(r.get("incident_id") or "")
                status = str(r.get("status") or "").strip()
                # Empty status → omit from resolved (→ Unresolved badge, not raw UUID).
                # Known statuses are normalised to customer-facing labels; unknown
                # statuses map to the neutral "Incident" label via _incident_label().
                if inc_id and status:
                    resolved[inc_id] = f"incident ({_incident_label(status)})"
        except Exception:
            logger.warning("Incident display-name lookup failed", exc_info=True)

    return resolved


def _row_to_edge(
    row: dict[str, Any],
    resolved: dict[str, str] | None = None,
    membership: dict[tuple[str, str], dict[str, str]] | None = None,
) -> WorkGraphEdgeResult:
    source_id = str(row.get("source_id", ""))
    target_id = str(row.get("target_id", ""))
    source_type_raw = str(row.get("source_type", "issue"))
    target_type_raw = str(row.get("target_type", "issue"))

    theme: str | None = None
    subcategory: str | None = None

    # edge.theme / edge.subcategory ALWAYS report the node's DOMINANT category
    # (the is_dominant row), on BOTH the filtered and unfiltered paths. A theme
    # filter only selects WHICH edges are shown — it never changes what the
    # annotation reports — so a mixed-membership unit matched under its secondary
    # theme still reports its dominant theme here (not the requested filter
    # value). Precedence: ISSUE endpoint > PR endpoint > other endpoint types;
    # both endpoints are checked and the issue wins when types differ. Fields
    # stay None when neither endpoint has membership data.
    if membership is not None:
        endpoint_preference = [
            (source_type_raw, source_id),
            (target_type_raw, target_id),
        ]
        # Sort so issue endpoints are tried before pr, and pr before others.
        _type_rank = {"issue": 0, "pr": 1}
        endpoint_preference.sort(key=lambda ep: _type_rank.get(ep[0].lower(), 2))
        for ep_type, ep_id in endpoint_preference:
            m = membership.get((ep_type, ep_id))
            if m:
                theme = m.get("dominant_theme") or None
                subcategory = m.get("dominant_subcategory") or None
                break

    return WorkGraphEdgeResult(
        edge_id=str(row.get("edge_id", "")),
        source_type=_map_node_type(source_type_raw),
        source_id=source_id,
        source_display_name=_display_name_for(source_id, resolved),
        target_type=_map_node_type(target_type_raw),
        target_id=target_id,
        target_display_name=_display_name_for(target_id, resolved),
        edge_type=_map_edge_type(str(row.get("edge_type", "relates"))),
        provenance=_map_provenance(str(row.get("provenance", "heuristic"))),
        confidence=float(row.get("confidence", 0.0)),
        evidence=str(row.get("evidence", "")),
        repo_id=str(row.get("repo_id")) if row.get("repo_id") else None,
        provider=str(row.get("provider")) if row.get("provider") else None,
        theme=theme,
        subcategory=subcategory,
    )


# work_unit_membership is multi-membership (one row per node-category) on a
# ReplacingMergeTree keyed by (org, node, category_kind, category, run_id).
#
# RUN_ID / COMPLETION-MARKER PROTOCOL (CHAOS-2433):
# Every membership write (materializer or backfill) stamps a single run_id on
# ALL rows of the run, then writes ONE completion-marker row to
# work_unit_membership_runs (org_id, run_id, completed_at) as the LAST step.
# A run is COMPLETE only when its marker exists. Readers select the latest
# complete run for the org:
#
#   argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id=?
#
# and scope ALL membership reads to rows whose run_id equals that value.
#
# This protocol fixes three failure modes at once (superseding per-node
# max(computed_at)):
#   1. CONCURRENCY RACE: a materializer in-flight has written some membership
#      rows but not its marker; the prior COMPLETE backfill run is still visible.
#      The materializer's marker write atomically switches readers to the new run.
#   2. SPLIT/MERGE STALE: nodes from a prior component that are absent from the
#      latest complete run simply have no rows in that run — not filterable and
#      annotation returns None, exactly "no membership row" semantics.
#   3. PARTIAL-WRITE DIVERGENCE: a run with rows but no marker is never selected.
#
# NO TOMBSTONES: tombstones (category='') are not needed because a churned node
# that is absent from the latest complete run has no rows in scope — the reader
# sees exactly the same result as "node has no membership" without any sentinel.
#
# When NO complete run exists for the org (table empty or all runs incomplete),
# readers treat it as no membership at all — degraded state if investments exist.
#
# LEGACY ROLLOUT (CHAOS-2433 finding #3, corrected in the final review):
# Migration 048 seeds ONE synthetic "legacy" marker per org that already had
# membership rows before migration 047 (run_id defaulted to '' on those rows,
# and no real run had published a marker yet).  The legacy marker uses the
# reserved run_id _LEGACY_RUN_ID and completed_at = max(existing computed_at).
# When the selected latest run IS the legacy marker, readers must match the
# PRE-EXISTING rows (run_id = '').
#
# OVER-MATCH FIX (final review HIGH): a blanket "match ALL run_id='' rows" is
# WRONG.  The OLD pre-protocol table key was (org, node, category_kind, category)
# with run_id NOT in the key, so a node that was dominant category A at T1 and
# re-materialized to dominant category B at T2 has BOTH rows persisted (A and B
# are different category values -> different keys -> ReplacingMergeTree never
# deduped them).  The OLD reader scoped each node to its latest row via
# max(computed_at) per (org, node).  A blanket run_id='' match resurfaces the
# stale A alongside the current B (wrong filters + multiple is_dominant rows per
# node) during the rollout window.  We therefore restore the EXACT old per-node
# semantics for the legacy run ONLY: scope each node to its latest computed_at
# among its run_id='' rows.  For REAL run_ids, run_id equality is unchanged (the
# protocol guarantees one generation per run_id, so no per-node-max is needed).
# As soon as a real org-wide run publishes a marker, its completed_at exceeds the
# legacy marker's, argMax selects it, and the legacy path retires automatically.
#
# The four scoping constants (_LEGACY_RUN_ID, _LATEST_COMPLETE_RUN_SUBQUERY,
# _LEGACY_NODE_MAX_JOIN, _RUN_SCOPE_PREDICATE) are imported at the top of this
# module from ``_membership_run_scope`` — shared with the work-unit
# team-attribution resolver so the two membership readers cannot drift (a partial
# reimplementation drops results for migrated tenants — CHAOS-2608 codex finding).


async def _batch_resolve_membership(
    client: Any,
    org_id: str,
    rows: list[dict[str, Any]],
    filters: WorkGraphEdgeFilterInput | None = None,
) -> dict[tuple[str, str], dict[str, str]]:
    """Batch-lookup the dominant theme/subcategory per edge endpoint in ONE query.

    Returns {(node_type, node_id) -> {"dominant_theme": ..., "dominant_subcategory": ...}}
    built from the is_dominant=1 rows of the latest COMPLETE run (scoped via
    work_unit_membership_runs — see _LATEST_COMPLETE_RUN_SUBQUERY). Nodes absent
    from the latest complete run return no entry (annotation null). Scoped to
    org_id.  When no complete run exists, returns {} (no annotation).
    """
    from dev_health_ops.api.queries.client import query_dicts

    # Collect unique (node_type, node_id) pairs from both endpoints of every edge.
    endpoints: set[tuple[str, str]] = set()
    for row in rows:
        for id_field, type_field in (
            ("source_id", "source_type"),
            ("target_id", "target_type"),
        ):
            node_id = str(row.get(id_field) or "").strip()
            node_type = str(row.get(type_field) or "").strip()
            if node_id and node_type:
                endpoints.add((node_type, node_id))

    if not endpoints:
        return {}

    node_types = sorted({t for t, _ in endpoints})
    node_ids = sorted({i for _, i in endpoints})
    params: dict[str, Any] = {
        "org_id": org_id,
        "node_types": node_types,
        "node_ids": node_ids,
    }
    _add_membership_scope_params(params, filters)

    try:
        membership_rows = await query_dicts(
            client,
            f"""
            WITH latest_run AS ({_membership_run_subquery(filters)})
            SELECT
                m.node_type AS node_type,
                m.node_id AS node_id,
                m.category_kind AS category_kind,
                m.category AS category
            FROM work_unit_membership AS m
            INNER JOIN latest_run ON 1 = 1
            {_LEGACY_NODE_MAX_JOIN}
            WHERE m.org_id = %(org_id)s
              AND m.node_type IN %(node_types)s
              AND m.node_id IN %(node_ids)s
              AND m.is_dominant = 1
              AND latest_run.latest_run_id != ''
              AND ({_RUN_SCOPE_PREDICATE})
            """,
            params,
        )
    except Exception as exc:
        # Only the EXPECTED recognized state — work_unit_membership does not
        # exist yet (rolling deploy / pre-migration) — degrades to "no
        # annotation" (edges still returned, theme/subcategory null). This
        # mirrors the narrowed filtered-path handling. Every OTHER error
        # (timeouts, auth, a DIFFERENT missing table, schema regressions)
        # re-raises so real failures surface loudly instead of being silently
        # served as null annotations. A node that genuinely has no membership
        # row is an empty result set, not an exception, and is unaffected.
        if _is_missing_membership_table_error(exc):
            logger.warning(
                "work_unit_membership table missing during annotation lookup "
                "for org %s — returning edges without theme annotation "
                "(CHAOS-2430).",
                org_id,
            )
            return {}
        raise

    result: dict[tuple[str, str], dict[str, str]] = {}
    for r in membership_rows:
        nt = str(r.get("node_type") or "")
        ni = str(r.get("node_id") or "")
        if not (nt and ni):
            continue
        entry = result.setdefault(
            (nt, ni), {"dominant_theme": "", "dominant_subcategory": ""}
        )
        kind = str(r.get("category_kind") or "")
        category = str(r.get("category") or "")
        if kind == "theme":
            entry["dominant_theme"] = category
        elif kind == "subcategory":
            entry["dominant_subcategory"] = category
    return result


def _theme_membership_exists_clause(filters: WorkGraphEdgeFilterInput | None) -> str:
    """SQL EXISTS clause: edge endpoint is a member of ALL requested categories.

    A correlated semi-join pushed INTO the edge query so the membership filter,
    the repo_id/edge_type/source filters, AND the LIMIT all execute in one
    ClickHouse plan (no unbounded Python IN set; the before-LIMIT guarantee
    holds because this lives in the edge WHERE). Run-scoping via
    work_unit_membership_runs (CHAOS-2433) ensures only the latest COMPLETE run's
    rows are considered. The uniqExact HAVING enforces "member of EVERY requested
    (kind, category)", so a theme+subcategory filter requires membership in BOTH.
    An edge matches when EITHER endpoint satisfies the subquery.
    """
    return f"""
        EXISTS (
            SELECT 1
            FROM work_unit_membership AS m
            INNER JOIN ({_membership_run_subquery(filters)}) AS latest_run
                ON 1 = 1
            {_LEGACY_NODE_MAX_JOIN}
            WHERE m.org_id = %(org_id)s
              AND latest_run.latest_run_id != ''
              AND ({_RUN_SCOPE_PREDICATE})
              AND (
                (m.node_type, m.node_id) = (work_graph_edges.source_type, work_graph_edges.source_id)
                OR (m.node_type, m.node_id) = (work_graph_edges.target_type, work_graph_edges.target_id)
              )
              AND (m.category_kind, m.category) IN %(category_tuples)s
            GROUP BY m.node_type, m.node_id
            HAVING uniqExact((m.category_kind, m.category)) = %(wanted_count)s
        )
    """


def _subcategory_parent_theme(subcategory: str) -> str | None:
    """Return the canonical parent theme of a subcategory, or None if unknown.

    Resolves via the canonical SUBCATEGORY_TO_THEME mapping, falling back to the
    ``"theme.sub"`` naming convention prefix when the subcategory is not in the
    map (defensive — keeps a future taxonomy addition from silently failing).
    """
    from dev_health_ops.investment_taxonomy import SUBCATEGORY_TO_THEME

    mapped = SUBCATEGORY_TO_THEME.get(subcategory)
    if mapped:
        return mapped
    prefix = subcategory.split(".", 1)[0]
    return prefix or None


def _theme_subcategory_conflict(theme: str | None, subcategory: str | None) -> bool:
    """True when theme + subcategory are BOTH set but cross taxonomy boundaries.

    A subcategory belongs to exactly one theme (canonical SUBCATEGORY_TO_THEME).
    If the caller supplies a theme AND a subcategory whose parent theme differs
    (e.g. via URL tampering, stale client state, or version skew), the requested
    intersection is impossible — no work unit can be a member of both. Callers
    must short-circuit to an empty result rather than build the (corrupting)
    cross-theme intersection (CHAOS-2430).
    """
    if not (theme and subcategory):
        return False
    return _subcategory_parent_theme(subcategory) != theme


def _build_theme_filter(
    theme: str | None,
    subcategory: str | None,
    filters: WorkGraphEdgeFilterInput | None,
) -> tuple[str | None, dict[str, Any]]:
    """Return (exists_clause_sql, params) for the active theme/subcategory filter.

    Returns (None, {}) when no theme filter is active. ``params`` carries the
    requested ``(category_kind, category)`` tuples and the count that the
    endpoint must match (1 for theme-only or subcategory-only, 2 for both).

    Callers MUST screen for ``_theme_subcategory_conflict`` first: a cross-theme
    theme+subcategory pair has no valid intersection and must short-circuit to an
    empty result rather than reach here.
    """
    wanted: list[tuple[str, str]] = []
    if theme:
        wanted.append(("theme", theme))
    if subcategory:
        wanted.append(("subcategory", subcategory))
    if not wanted:
        return None, {}
    return _theme_membership_exists_clause(filters), {
        "category_tuples": wanted,
        "wanted_count": len(wanted),
    }


# Wire value consumed by the web client to distinguish a transient rollout state
# (membership table not yet populated) from a genuine empty theme-filter result.
MEMBERSHIP_NOT_MATERIALIZED = "MEMBERSHIP_NOT_MATERIALIZED"


async def _detect_membership_degraded_reason(
    client: Any,
    org_id: str,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> str | None:
    """Return MEMBERSHIP_NOT_MATERIALIZED when a theme filter yielded nothing
    because NO complete membership run has been published for an org that HAS
    categorized work units, else None.

    Degraded iff: the org has >= 1 work_unit_investments row AND NO complete-run
    MARKER exists in work_unit_membership_runs for the org (the run_id /
    completion-marker protocol has not produced any complete run yet —
    CHAOS-2433).

    GATE ON MARKER EXISTENCE, NOT ROW COUNT (CHAOS-2433 round-2 finding #2):
    an intentionally-EMPTY complete run (the all-skipped supersede case — every
    current component churned past its last categorization) is a genuine
    "this org currently has no memberships" state, NOT "not materialized". It
    publishes a marker with zero membership rows. Keying the degraded signal on
    count(membership rows in latest run) > 0 would mislabel that valid empty
    complete run as a rollout failure. We therefore key on whether ANY complete
    marker exists for the org: a marker present (even with zero rows) means the
    pipeline HAS produced a complete run, so an empty filter result is a genuine
    empty (degraded_reason=None). Only the total absence of a marker (pre-migration
    / pre-first-run rollout window) while investments exist is degraded.

    Also emits the rollout warning log (observability retained). Never raises:
    on probe failure it returns None so the request is unaffected.
    """
    from dev_health_ops.api.queries.client import query_dicts

    scoped_repo_ids = _partial_repo_ids(filters)
    marker_sql = """
                    SELECT count()
                    FROM work_unit_membership_runs
                    WHERE org_id = %(org_id)s
                """
    params: dict[str, Any] = {"org_id": org_id}
    if scoped_repo_ids:
        marker_sql = """
                    SELECT count()
                    FROM (
                        SELECT run_id
                        FROM work_unit_membership_scoped_runs
                        WHERE org_id = %(org_id)s
                          AND scope_kind = 'repo'
                          AND scope_id IN %(scoped_repo_ids)s
                        GROUP BY run_id
                        HAVING uniqExact(scope_id) = %(scoped_repo_count)s
                    )
                """
        params["scoped_repo_ids"] = scoped_repo_ids
        params["scoped_repo_count"] = len(scoped_repo_ids)

    try:
        rows = await query_dicts(
            client,
            f"""
            SELECT
                ({marker_sql}) AS complete_run_markers,
                (
                    SELECT count()
                    FROM work_unit_investments
                    WHERE org_id = %(org_id)s
                ) AS investment_rows
            """,
            params,
        )
    except Exception:
        # Never let the degraded-state probe affect the request.
        logger.debug("membership-population probe failed", exc_info=True)
        return None

    if not rows:
        return None
    complete_run_markers = int(rows[0].get("complete_run_markers") or 0)
    investment_rows = int(rows[0].get("investment_rows") or 0)
    if complete_run_markers == 0 and investment_rows > 0:
        logger.warning(
            "Theme filter returned no edges and NO complete membership run "
            "marker exists for org %s while work_unit_investments has %d rows — "
            "the post-migration investment materialization rerun that publishes "
            "a work_unit_membership_runs completion marker has likely not run "
            "yet (CHAOS-2430/2433).",
            org_id,
            investment_rows,
        )
        return MEMBERSHIP_NOT_MATERIALIZED
    return None


_MEMBERSHIP_TABLES = frozenset(
    {
        "work_unit_membership",
        "work_unit_membership_runs",
        "work_unit_membership_scoped_runs",
    }
)

# ClickHouse names the actually-missing table in an "Unknown table ...
# identifier '<name>'" clause. The full error ALSO echoes the entire failing
# SQL (which mentions every table in the query), so a naive substring search for
# the table name matches even when a DIFFERENT table is missing. We therefore
# extract ONLY the quoted identifier from the dedicated clause and compare it.
_UNKNOWN_TABLE_IDENTIFIER_RE = re.compile(
    r"Unknown table(?: expression identifier)?\s+'([^']+)'",
    re.IGNORECASE,
)


def _unknown_table_names(text: str) -> set[str]:
    """Return the table identifiers ClickHouse reported as unknown.

    Matches the "Unknown table expression identifier '<name>'" / "Unknown table
    '<name>'" clauses only — NOT the echoed SQL — and strips any database
    qualifier (``db.table`` -> ``table``).
    """
    names: set[str] = set()
    for ident in _UNKNOWN_TABLE_IDENTIFIER_RE.findall(text):
        names.add(ident.split(".")[-1].strip("`"))
    return names


def _is_missing_membership_table_error(exc: BaseException) -> bool:
    """True ONLY when a ClickHouse missing-table (code 60) error names
    ``work_unit_membership`` OR ``work_unit_membership_runs`` as the unknown table.

    During a rolling deploy or before migration 047 has run, the filtered edge
    query or the degraded probe references these tables before they exist; the
    driver raises a DatabaseError carrying ``code == 60`` with an
    ``UNKNOWN_TABLE`` server message whose identifier clause names the offending
    table. We require the code-60/UNKNOWN_TABLE signal AND that the
    reported-unknown identifier IS one of the membership tables — parsed from the
    identifier clause, NOT the echoed SQL (which lists every table in the query).
    So a code-60 error for any OTHER table (e.g. a missing ``work_graph_edges``
    or another schema regression on the filtered path) re-raises and surfaces
    loudly instead of masquerading as the benign degraded state.
    """
    text = str(exc)
    is_unknown_table = (
        getattr(exc, "code", None) == 60
        or "UNKNOWN_TABLE" in text
        or "code: 60" in text
    )
    if not is_unknown_table:
        return False
    return bool(_unknown_table_names(text) & _MEMBERSHIP_TABLES)


def _empty_edges_result(
    degraded_reason: str | None = None,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> WorkGraphEdgesResult:
    """An empty, well-formed result — used for impossible filters and the
    degraded (membership-not-materialized) state."""
    return WorkGraphEdgesResult(
        edges=[],
        total_count=0,
        page_info=PageInfo(
            has_next_page=False,
            has_previous_page=False,
            start_cursor=None,
            end_cursor=None,
        ),
        degraded_reason=degraded_reason,
        is_partial=_uses_scoped_partial(filters),
        partial_scope="repo" if _uses_scoped_partial(filters) else None,
        partial_repo_ids=_partial_repo_ids(filters),
    )


def _empty_flow_result(
    degraded_reason: str | None = None,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> WorkGraphFlowResult:
    """An empty, well-formed flow aggregate — impossible filters + degraded state."""
    return WorkGraphFlowResult(
        rows=[],
        degraded_reason=degraded_reason,
        is_partial=_uses_scoped_partial(filters),
        partial_scope="repo" if _uses_scoped_partial(filters) else None,
        partial_repo_ids=_partial_repo_ids(filters),
    )


def _empty_artifacts_result(
    degraded_reason: str | None = None,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> WorkGraphArtifactsResult:
    """An empty, well-formed artifacts aggregate — impossible filters + degraded."""
    return WorkGraphArtifactsResult(
        rows=[],
        degraded_reason=degraded_reason,
        is_partial=_uses_scoped_partial(filters),
        partial_scope="repo" if _uses_scoped_partial(filters) else None,
        partial_repo_ids=_partial_repo_ids(filters),
    )


def _build_work_graph_where(
    filters: WorkGraphEdgeFilterInput | None,
    org_id: str,
    *,
    include_edge_filters: bool,
) -> tuple[str, dict[str, Any]]:
    """Build the shared WHERE clause + params for the edge list AND aggregates.

    ALWAYS applies the graph-wide filters: org_id (cross-tenant safety),
    repo_ids, and the theme/subcategory correlated-EXISTS semi-join. These
    describe the WHOLE graph, so both the capped edge list and the full-set
    aggregates (flow / artifacts) apply them identically.

    When ``include_edge_filters`` is True, ALSO applies the edge-list-only
    filters — source_type/target_type/edge_type/edge_types/node_id — which
    narrow WHICH edges a single view shows. The aggregates describe the whole
    graph and therefore pass ``include_edge_filters=False``.

    org_id is ALWAYS the first predicate (and lives inside the EXISTS join
    predicate too — see ``_theme_membership_exists_clause``). Callers MUST
    screen for ``_theme_subcategory_conflict`` first and short-circuit to an
    empty result; this builder assumes a non-conflicting theme/subcategory.
    """
    params: dict[str, Any] = {"org_id": org_id}
    where_clauses: list[str] = ["org_id = %(org_id)s"]
    _add_membership_scope_params(params, filters)

    theme_filter = filters.theme if filters else None
    subcategory_filter = filters.subcategory if filters else None

    if filters:
        if filters.repo_ids:
            where_clauses.append("repo_id IN %(repo_ids)s")
            params["repo_ids"] = filters.repo_ids

        if include_edge_filters:
            if filters.source_type:
                where_clauses.append("source_type = %(source_type)s")
                params["source_type"] = filters.source_type.value

            if filters.target_type:
                where_clauses.append("target_type = %(target_type)s")
                params["target_type"] = filters.target_type.value

            if filters.edge_type:
                where_clauses.append("edge_type = %(edge_type)s")
                params["edge_type"] = filters.edge_type.value

            # Plural edge_types: fetched BEFORE the cap so a tab's own edge
            # types can never be starved by an unrelated dominant type
            # (CHAOS-2442). ANDs with the singular edge_type when both given.
            if filters.edge_types:
                where_clauses.append("edge_type IN %(edge_types)s")
                params["edge_types"] = [e.value for e in filters.edge_types]

            if filters.node_id:
                where_clauses.append(
                    "(source_id = %(node_id)s OR target_id = %(node_id)s)"
                )
                params["node_id"] = filters.node_id

    # --- Server-side theme/subcategory filter (CHAOS-2430) --------------------
    # The membership constraint is pushed INTO the query as a correlated EXISTS
    # semi-join, so repo_id/edge filters AND the membership filter AND any LIMIT
    # all execute in ONE ClickHouse plan. This keeps the before-LIMIT guarantee
    # (a sparse theme's edges are never hidden behind the row cap) WITHOUT
    # round-tripping an unbounded matched-node set through Python. An edge
    # matches if EITHER endpoint is a member of the requested theme/subcategory
    # (multi-membership), with the theme+subcategory "both required" semantics
    # enforced inside the subquery.
    if theme_filter or subcategory_filter:
        exists_clause, theme_params = _build_theme_filter(
            theme_filter, subcategory_filter, filters
        )
        if exists_clause:
            where_clauses.append(exists_clause)
            params.update(theme_params)

    where_sql = f"WHERE {' AND '.join(where_clauses)}"
    return where_sql, params


async def resolve_work_graph_edges(
    context: GraphQLContext,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> WorkGraphEdgesResult:
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    limit = filters.limit if filters else 1000

    theme_filter = filters.theme if filters else None
    subcategory_filter = filters.subcategory if filters else None
    theme_filter_active = bool(theme_filter or subcategory_filter)

    # A theme + subcategory pair that cross taxonomy boundaries (the subcategory
    # belongs to a different theme) has no valid intersection — short-circuit to
    # an empty result instead of building the impossible cross-theme filter
    # (guards against URL tampering / stale client state / version skew).
    if _theme_subcategory_conflict(theme_filter, subcategory_filter):
        logger.info(
            "work_graph theme filter conflict: theme=%r does not own "
            "subcategory=%r — returning empty result",
            theme_filter,
            subcategory_filter,
        )
        return _empty_edges_result(filters=filters)

    where_sql, params = _build_work_graph_where(
        filters, org_id, include_edge_filters=True
    )
    params["limit"] = int(limit)

    # ORDER BY confidence DESC, edge_id ASC (CHAOS-2442) — GATED on a narrowing
    # filter being active. The table is sorted by (source_type, source_id,
    # edge_type, target_type, target_id), NOT confidence, so a GLOBAL relevance
    # sort would force ClickHouse to read the org's ENTIRE edge set and top-sort
    # before returning the first page — defeating early-LIMIT termination (a
    # timeout path for large tenants). We therefore only sort when the candidate
    # set is ALREADY bounded by a narrowing filter (edge_type(s)/source/target/
    # node_id/theme/subcategory/repo_ids). For the fully-unfiltered default
    # overview we emit NO ORDER BY, keeping the cap-bounded hot path — that's
    # acceptable because the Dependencies tab fetches its own edge_types and the
    # aggregates (work_graph_flow / work_graph_artifacts) are exact at any scale,
    # so the broad overview does not need the global sort to be useful.
    has_narrowing_filter = bool(
        filters
        and (
            filters.repo_ids
            or filters.source_type
            or filters.target_type
            or filters.edge_type
            or filters.edge_types
            or filters.node_id
            or filters.theme
            or filters.subcategory
        )
    )
    order_by_sql = (
        "ORDER BY confidence DESC, edge_id ASC" if has_narrowing_filter else ""
    )
    query = f"""
        SELECT
            edge_id,
            source_type,
            source_id,
            target_type,
            target_id,
            edge_type,
            toString(repo_id) AS repo_id,
            provider,
            provenance,
            confidence,
            evidence
        FROM work_graph_edges
        {where_sql}
        {order_by_sql}
        LIMIT %(limit)s
    """

    try:
        rows = await query_dicts(client, query, params)
    except Exception as exc:
        # During a rolling deploy / before the membership migration has run, the
        # filtered edge query's EXISTS subquery references work_unit_membership
        # before it exists. Return the controlled degraded state instead of a
        # 500 — the web client already handles MEMBERSHIP_NOT_MATERIALIZED. The
        # check is NARROW: only a code-60 error that names work_unit_membership
        # is downgraded. Any other missing-table / schema regression on this
        # path (e.g. a missing work_graph_edges) re-raises so it fails loudly
        # rather than masquerading as a benign empty state.
        if theme_filter_active and _is_missing_membership_table_error(exc):
            logger.warning(
                "work_unit_membership table missing for org %s during theme "
                "filter — returning degraded state (CHAOS-2430).",
                org_id,
            )
            return _empty_edges_result(MEMBERSHIP_NOT_MATERIALIZED, filters)
        raise

    # Degraded-state signal (CHAOS-2430 rollout): a theme filter that yields zero
    # edges is normally a legitimate empty state, but immediately after deploy it
    # can instead mean work_unit_membership has not been populated yet (the table
    # is empty until the next investment materialization rerun). Surface that as
    # degraded_reason=MEMBERSHIP_NOT_MATERIALIZED so the client distinguishes a
    # transient rollout state from a genuine empty result. No silent fallback —
    # edges stay [] either way.
    degraded_reason: str | None = None
    if theme_filter_active and not rows:
        degraded_reason = await _detect_membership_degraded_reason(
            client, org_id, filters
        )

    resolved = await _batch_resolve_display_names(client, org_id, rows)

    # Annotation ALWAYS reports each edge's dominant theme/subcategory (the
    # is_dominant rows), on both the filtered and unfiltered paths — the theme
    # filter only selects which edges are shown, never what is reported.
    membership = await _batch_resolve_membership(client, org_id, rows, filters)
    edges = [_row_to_edge(row, resolved, membership) for row in rows]

    return WorkGraphEdgesResult(
        edges=edges,
        total_count=len(edges),
        page_info=PageInfo(
            has_next_page=len(edges) == limit,
            has_previous_page=False,
            start_cursor=edges[0].edge_id if edges else None,
            end_cursor=edges[-1].edge_id if edges else None,
        ),
        degraded_reason=degraded_reason,
        is_partial=_uses_scoped_partial(filters),
        partial_scope="repo" if _uses_scoped_partial(filters) else None,
        partial_repo_ids=_partial_repo_ids(filters),
    )


async def resolve_work_graph_flow(
    context: GraphQLContext,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> WorkGraphFlowResult:
    """Per-node-type inflow/outflow counts over the FULL edge set (CHAOS-2442).

    A true server-side aggregate: it GROUPs the whole work graph by
    (source_type, target_type), so it reflects the REAL direction mix and is
    correct at any scale — NOT derived from the capped edge page (which, for the
    demo org, was dominated by `references` and collapsed Inflow/Outflow into a
    degenerate Issue-in / Commit-out split). Applies ONLY the graph-wide filters
    (org_id + repo_ids + theme/subcategory); edge-list-only filters do not apply.
    """
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    theme_filter = filters.theme if filters else None
    subcategory_filter = filters.subcategory if filters else None
    theme_filter_active = bool(theme_filter or subcategory_filter)

    if _theme_subcategory_conflict(theme_filter, subcategory_filter):
        logger.info(
            "work_graph_flow theme filter conflict: theme=%r does not own "
            "subcategory=%r — returning empty result",
            theme_filter,
            subcategory_filter,
        )
        return _empty_flow_result(filters=filters)

    where_sql, params = _build_work_graph_where(
        filters, org_id, include_edge_filters=False
    )

    # uniqExact(edge_id), NOT raw count(): work_graph_edges is a
    # ReplacingMergeTree(last_synced) whose logical dedup key is edge_id (the
    # deterministic hash of source_type/source_id/edge_type/target_type/
    # target_id). Un-merged retry/backfill duplicate physical rows are visible
    # until a background merge, so count() would inflate the direction mix.
    # edge_id is unique per logical edge and falls in exactly ONE
    # (source_type, target_type) group, so uniqExact(edge_id) is exact.
    query = f"""
        SELECT source_type, target_type, uniqExact(edge_id) AS cnt
        FROM work_graph_edges
        {where_sql}
        GROUP BY source_type, target_type
    """

    try:
        rows = await query_dicts(client, query, params)
    except Exception as exc:
        # Same NARROW degraded contract as resolve_work_graph_edges: only a
        # code-60 missing-membership-table error under an active theme filter is
        # downgraded; every other error propagates (project rule — no blanket
        # except/return-default).
        if theme_filter_active and _is_missing_membership_table_error(exc):
            logger.warning(
                "work_unit_membership table missing for org %s during "
                "work_graph_flow theme filter — returning degraded state "
                "(CHAOS-2442).",
                org_id,
            )
            return _empty_flow_result(MEMBERSHIP_NOT_MATERIALIZED, filters)
        raise

    inflow: dict[WorkGraphNodeType, int] = defaultdict(int)
    outflow: dict[WorkGraphNodeType, int] = defaultdict(int)
    for row in rows:
        cnt = int(row.get("cnt") or 0)
        source_type = _map_node_type(str(row.get("source_type", "")))
        target_type = _map_node_type(str(row.get("target_type", "")))
        outflow[source_type] += cnt
        inflow[target_type] += cnt

    flow_rows = [
        WorkGraphFlowRow(
            node_type=node_type,
            inflow=inflow.get(node_type, 0),
            outflow=outflow.get(node_type, 0),
        )
        for node_type in set(inflow) | set(outflow)
        if inflow.get(node_type, 0) > 0 or outflow.get(node_type, 0) > 0
    ]
    # Sort by total degree desc; node_type value as a deterministic tiebreaker.
    flow_rows.sort(key=lambda r: (-(r.inflow + r.outflow), r.node_type.value))

    degraded_reason: str | None = None
    if theme_filter_active and not flow_rows:
        degraded_reason = await _detect_membership_degraded_reason(
            client, org_id, filters
        )

    return WorkGraphFlowResult(
        rows=flow_rows,
        degraded_reason=degraded_reason,
        is_partial=_uses_scoped_partial(filters),
        partial_scope="repo" if _uses_scoped_partial(filters) else None,
        partial_repo_ids=_partial_repo_ids(filters),
    )


async def resolve_work_graph_artifacts(
    context: GraphQLContext,
    filters: WorkGraphEdgeFilterInput | None = None,
) -> WorkGraphArtifactsResult:
    """Top-N nodes by degree over the FULL edge set (CHAOS-2442 Artifacts tab).

    Degree = edges touching a node as SOURCE or TARGET, counted across the whole
    graph (a UNION ALL of source/target projections), so the ranking is correct
    at any scale rather than reflecting whatever happened to land in the capped
    edge page. Applies ONLY the graph-wide filters (org_id + repo_ids +
    theme/subcategory). ``filters.limit`` is the top-N (the web client passes 50;
    default 1000). Display names reuse the edge-list batch resolver / A7-A8
    fallback so a UUID that cannot be resolved surfaces as None (Unresolved
    badge), never a raw UUID.
    """
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    limit = filters.limit if filters else 1000

    theme_filter = filters.theme if filters else None
    subcategory_filter = filters.subcategory if filters else None
    theme_filter_active = bool(theme_filter or subcategory_filter)

    if _theme_subcategory_conflict(theme_filter, subcategory_filter):
        logger.info(
            "work_graph_artifacts theme filter conflict: theme=%r does not own "
            "subcategory=%r — returning empty result",
            theme_filter,
            subcategory_filter,
        )
        return _empty_artifacts_result(filters=filters)

    where_sql, params = _build_work_graph_where(
        filters, org_id, include_edge_filters=False
    )
    params["limit"] = int(limit)

    # The {where} (including the correlated theme EXISTS clause that references
    # work_graph_edges.{source,target}_{type,id}) appears TWICE — once per UNION
    # ALL branch. Both branches select FROM work_graph_edges so the correlated
    # references resolve, and because the param NAMES are identical in both
    # occurrences, the single params dict serves both.
    #
    # degree = uniqExact(edge_id), NOT raw count(): work_graph_edges is a
    # ReplacingMergeTree(last_synced) keyed logically on edge_id, so un-merged
    # duplicate physical rows would inflate a count(). uniqExact(edge_id) also
    # makes a self-loop (source==target, same node) count ONCE — the node's two
    # UNION ALL projections carry the SAME edge_id, so a self-referential edge
    # contributes degree 1 (one edge touching the node), not 2.
    query = f"""
        SELECT node_type, node_id, uniqExact(edge_id) AS degree, any(evidence) AS evidence
        FROM (
            SELECT source_type AS node_type, source_id AS node_id, edge_id, evidence
            FROM work_graph_edges
            {where_sql}
            UNION ALL
            SELECT target_type AS node_type, target_id AS node_id, edge_id, evidence
            FROM work_graph_edges
            {where_sql}
        )
        GROUP BY node_type, node_id
        ORDER BY degree DESC, node_id ASC
        LIMIT %(limit)s
    """

    try:
        rows = await query_dicts(client, query, params)
    except Exception as exc:
        if theme_filter_active and _is_missing_membership_table_error(exc):
            logger.warning(
                "work_unit_membership table missing for org %s during "
                "work_graph_artifacts theme filter — returning degraded state "
                "(CHAOS-2442).",
                org_id,
            )
            return _empty_artifacts_result(MEMBERSHIP_NOT_MATERIALIZED, filters)
        raise

    # Reuse the edge-list display-name batch resolver by shaping each aggregate
    # row as a pseudo-edge whose SOURCE endpoint is the node (target endpoint
    # blank so it is ignored). One query per entity type — no N+1.
    pseudo_rows = [
        {
            "source_id": str(row.get("node_id", "")),
            "source_type": str(row.get("node_type", "")),
            "target_id": "",
            "target_type": "",
        }
        for row in rows
    ]
    resolved = await _batch_resolve_display_names(client, org_id, pseudo_rows)

    artifact_rows = [
        WorkGraphArtifactRow(
            node_type=_map_node_type(str(row.get("node_type", ""))),
            node_id=str(row.get("node_id", "")),
            display_name=_display_name_for(str(row.get("node_id", "")), resolved),
            degree=int(row.get("degree") or 0),
            evidence=str(row["evidence"]) if row.get("evidence") else None,
        )
        for row in rows
    ]

    degraded_reason: str | None = None
    if theme_filter_active and not artifact_rows:
        degraded_reason = await _detect_membership_degraded_reason(
            client, org_id, filters
        )

    return WorkGraphArtifactsResult(
        rows=artifact_rows,
        degraded_reason=degraded_reason,
        is_partial=_uses_scoped_partial(filters),
        partial_scope="repo" if _uses_scoped_partial(filters) else None,
        partial_repo_ids=_partial_repo_ids(filters),
    )
