"""Bounded ClickHouse reader for Ask Dev status and observed-change facts.

Only canonical links are followed.  Current schema gaps (required CI/check
designation, consistent blocker direction, and project declared state)
remain explicit unknown/unavailable inputs to the deterministic rule service.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from dev_health_ops.api.graphql.resolvers._membership_run_scope import (
    LATEST_COMPLETE_RUN_SUBQUERY,
    LEGACY_NODE_MAX_JOIN,
    RUN_SCOPE_PREDICATE,
)
from dev_health_ops.api.queries.client import query_dicts

from ._project_identity import (
    project_identity_cte,
    project_identity_match,
)
from .contracts import ClaimKind, DevScope, DirectScope, FreshnessState
from .native_evidence import (
    SourceFreshnessPolicy,
    default_native_freshness_policies,
)
from .status_change_service import (
    MAX_STATUS_ASSESSMENT_ITEMS,
    ChangeCategory,
    ChangeWindow,
    CIFact,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    RawChangeSummary,
    RawStatusSnapshot,
    SourceReference,
    StatusFact,
)

NATIVE_STATUS_SOURCE_VERSION = "native-status-change.v1"
NATIVE_STATUS_QUERY_VERSION = "native-status-change-query.v1"
QUERY_TIMEOUT_SECONDS = 15

#: CHAOS-3301 addendum (Option B), structural closure. Every SQL constant in
#: this module that gates on ``{scope_type:String}`` must either carry a
#: ``'team'`` arm in its disjunction, or be named here — enforced by
#: ``tests/api/dev/test_chaos_3301_controls.py::
#: test_native_status_change_scope_type_disjunctions_are_total_over_team``,
#: which enumerates every ``scope_type``-gated SQL constant in this module by
#: introspection and fails if a new one is added without updating one of the
#: two.
#:
#: CHAOS-3303 lands real team arms for the nine sources whose org/repo arm
#: already meant "everything within ``repository_ids``, no further entity
#: filter" (``_PULL_REQUESTS_SQL``, ``_DEPLOYMENTS_SQL``, ``_TRANSITIONS_SQL``,
#: ``_RELATIONSHIPS_SQL``, and the five ``_*_CHANGES_SQL`` delivery
#: projections): once ``_authorized_repository_ids`` re-derives a team's
#: owned repositories from ``team_repo_ownership`` at query time (never
#: carried on the wire, per the addendum), that arm's existing semantics --
#: "no single named entity, just the repository set" -- is exactly what a
#: team subject wants, so adding ``'team'`` to the same disjunction member is
#: sufficient; no new join or control-flow branch is required. ``_CI_SQL``,
#: ``_CI_ACCEPTANCE_SQL``, and ``_INCIDENTS_SQL`` are not ``scope_type``-gated
#: at all (they filter by ``pr_numbers``/``deployment_ids`` derived from the
#: above), so they already work correctly for team scope once those upstream
#: sources do.
#:
#: The remaining two sources stay not-applicable to a team subject, for the
#: same structural reason organization/repository scope never reaches them
#: either (see ``status_snapshot``'s own guard: both are only ever queried
#: when ``scope.direct_scope in {ISSUE, PROJECT}`` or a work-unit's member
#: issues): ``_WORK_ITEMS_SQL`` produces a single declared/children
#: completion tree, and ``_BLOCKERS_SQL`` blocks a single target entity --
#: neither concept maps onto a team cohort of repositories. A team's overall
#: ``StatusResultState`` no longer requires a declared status either (see
#: ``StatusChangeService.status_snapshot``'s ``declared_optional`` set),
#: exactly mirroring how PROJECT/WORK_UNIT scope already treats "no single
#: declared item" as expected, not a gap.
TEAM_NOT_APPLICABLE_SOURCES: frozenset[str] = frozenset(
    {
        "_WORK_ITEMS_SQL",
        "_BLOCKERS_SQL",
    }
)

_BLOCKER_PROJECTION_RULE_VERSION = "canonical-blocks.v2"

#: CHAOS-3303 round 2 (Codex HIGH, 2026-08-02): ``team_repo_ownership`` rows
#: can represent mere *provider_access* to a repository -- a parent team can
#: have access to a repo whose canonical primary work-item attribution
#: belongs to a *different* (e.g. child) team. Repository co-location is not
#: team ownership. Every fact-level team arm below joins through this exact
#: canonical-primary snapshot instead of the coarser ``repository_ids``
#: bound, so a parent team with broader repo access never inherits a child
#: team's canonically-owned facts. Mirrors
#: ``api/graphql/resolvers/team_attribution.py``'s own ``is_primary = 1`` +
#: latest-``computed_at``-snapshot pattern verbatim (not re-derived): a work
#: item's *latest compute* is scoped by ``(work_item_id, computed_at) IN
#: (SELECT work_item_id, max(computed_at) ... GROUP BY work_item_id)``
#: because ``compute_work_item_team_attributions`` appends every candidate
#: of one compute run and never deletes prior ones -- without this bound, a
#: retired candidate from an earlier compute could survive ``FINAL`` (its
#: RMT key includes ``source``, so a superseded row is a distinct key, not
#: replaced) and reappear as a stale extra ``is_primary`` row.
#:
#: Plain (non-f-string) text spliced into each query with ``+`` at the exact
#: point it's needed -- every ``{name:Type}`` placeholder here is
#: interpreted by clickhouse-connect server-side, never by Python string
#: formatting (see ``api/queries/client.py``'s server-placeholder detection),
#: so verbatim splicing is safe and avoids doubling every brace an f-string
#: embedding would require across ~500 lines of surrounding SQL.
_TEAM_OWNED_WORK_ITEM_IDS_SUBQUERY = """
    SELECT work_item_id
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND team_id = {team_id:String}
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String}
            AND computed_at <= {as_of:DateTime64(3, 'UTC')}
          GROUP BY work_item_id
      )
"""

_WORK_UNIT_MEMBERSHIP_WATERMARK_SQL = """
SELECT max(completed_at) AS last_synced
FROM work_unit_membership_runs
WHERE org_id = {org_id:String}
  AND completed_at <= {as_of:DateTime64(3, 'UTC')}
HAVING count() > 0
"""

_WORK_UNIT_MEMBERS_SQL = f"""
WITH latest_run AS (
{LATEST_COMPLETE_RUN_SUBQUERY.replace("%(org_id)s", "{org_id:String}")}
)
SELECT m.node_type, m.node_id, max(m.computed_at) AS last_synced
FROM work_unit_membership AS m FINAL
INNER JOIN latest_run ON 1 = 1
{LEGACY_NODE_MAX_JOIN.replace("%(org_id)s", "{org_id:String}")}
WHERE m.org_id = {{org_id:String}}
  AND m.work_unit_id = {{entity_id:String}}
  AND latest_run.latest_run_id != ''
  AND ({RUN_SCOPE_PREDICATE})
GROUP BY m.node_type, m.node_id
ORDER BY m.node_type, m.node_id
LIMIT {{limit:UInt32}}
"""

#: CHAOS-3374: every project-scoped fact arm below (plus ``PROJECT_REPOSITORIES_SQL``)
#: joins through the catalog's OWN ``projects`` row for ``{entity_id:String}`` instead
#: of comparing ``work_items.project_id``/``project_key`` against the catalog id
#: directly. The two are NOT the same value space for every provider:
#: ``team_autoimport_jira._project_id`` mints a Jira project's catalog id as
#: ``f"{org_id}:jira:{project_key}"`` (workers/team_autoimport_jira.py:106-107)
#: while ``providers/jira/normalize.py:517-518`` writes the RAW Jira id/key onto
#: ``work_items`` -- so ``project_id = {entity_id:String} OR project_key =
#: {entity_id:String}`` can never match a Jira row: neither side of the OR is ever
#: the prefixed catalog id. Linear's catalog id IS the raw ``work_items.project_id``
#: (see ``team_autoimport_linear._linear_project_records``'s own docstring), which
#: is why Linear worked and Jira didn't with the identical predicate.
#:
#: The fix resolves the catalog row's ``provider`` and (nullable) ``project_key``
#: through this shared CTE and requires BOTH ``work_items.provider = catalog_provider``
#: and (``work_items.project_id = {entity_id:String}`` OR the two raw
#: ``project_key`` values match) -- so a Jira project matches by its own raw key, a
#: Linear project keeps matching by the raw id it always used, and neither can ever
#: match the other's rows even if a raw key/id value collided (``provider`` is a
#: real ``work_items`` column -- CHAOS-3374 requires this explicitly: GitLab's own
#: ``work_items.project_id`` is a bare repo full path, e.g. "group/project", which
#: could otherwise coincidentally equal another provider's project id/key string in
#: the same org). CHAOS-3380 is that case made real, and (round 2, Codex HIGH)
#: mints the catalog identity Jira-style rather than Linear-style for exactly
#: the reason this paragraph names: a GitLab project's PATH is mutable
#: (rename, group transfer) while Linear's/Jira's own ids are not.
#: ``team_autoimport_gitlab.py``'s catalog ``id`` is GitLab's own IMMUTABLE
#: numeric project id, prefixed like Jira's (``f"{org_id}:gitlab:{numeric_id}"``);
#: ``project_key`` carries the CURRENT ``path_with_namespace``, refreshed on
#: every discovery run.
#:
#: CHAOS-3380 round 3 (Codex HIGH -- incremental sync strands historical
#: GitLab rows): unlike Jira, ``providers/gitlab/normalize.py`` does NOT
#: write anything onto ``work_items.project_key`` -- it stays empty for
#: EVERY GitLab row, old and new alike (an earlier revision of this comment
#: had normalize.py mirror the path onto ``project_key`` too; reverted --
#: pointless once the arm below exists, and it would have made "did this row
#: sync after the cutover" a THIRD identity dimension for no reason).
#: ``work_items.project_id`` is, and always has been, the raw path for every
#: GitLab issue/MR row regardless of when it was synced -- ``updated_after``
#: incremental syncs never rewrite a row that has not itself changed, so
#: "before this ticket" and "after" rows are IDENTICAL in shape. The two arms
#: above (raw id match, key-to-key match) both miss this: GitLab's entity id
#: is prefixed (never equals any raw ``project_id``) and its ``project_key``
#: is always empty (never satisfies ``catalog_project_key != ''`` on its own
#: side). A THIRD arm closes it: ``{alias}project_id = catalog_project_key``
#: (guarded by the same ``catalog_project_key != ''``) -- the work item's raw
#: path against the catalog's CURRENT path, independent of when the row was
#: synced. Provider-guarded like the other two, so it cannot cross-match:
#: a Jira row's ``project_id`` is always empty (``providers/jira/normalize``
#: never sets it), so it can never equal a non-empty ``catalog_project_key``;
#: a Linear native project row's ``project_key`` is empty (this arm is a
#: no-op for it), but a Linear TEAM-derived catalog row's ``project_key`` IS
#: a real value (the team key) -- this arm still cannot false-match it in
#: practice, since no Linear work item's ``project_id`` (Linear's own project
#: UUID) is ever going to literally equal a short team-key string, but the
#: claim "Linear's project_key is always empty" from the previous revision of
#: this comment was not true of every catalog row Linear writes, only the
#: native-project ones -- corrected here rather than left as a latent false
#: assumption (see ``ask_dev_project_subject_oracle.py`` for where believing
#: it uncritically actually mattered).
#:
#: ``_project_identity_cte``/``_project_identity_match`` both take an
#: ``entity_param`` (default ``"entity_id"``, this module's own parameter
#: name everywhere below) so ``native_evidence.py`` can reuse the EXACT same
#: predicate text keyed off ITS OWN differently-named scope parameter
#: (``scope_entity_id``) instead of hand-copying a variant that could drift
#: (CHAOS-3380 round 3, Codex HIGH -- native_evidence project search/expand
#: compared work_items directly against a prefixed catalog id and could never
#: match a real row for any provider using the prefixed-id model, not just
#: GitLab).
#:
#: ``LEFT JOIN`` (every arm that also serves 'issue'/'work_unit'/'team'/
#: 'organization'/'repository' scope types): a project-identity lookup miss must
#: never blank out those OTHER scope types' rows -- only the project arm's own
#: predicate is gated (``ifNull(catalog_provider, '') != ''`` in
#: ``_project_identity_match``). ``PROJECT_REPOSITORIES_SQL`` is project-scope-only
#: and uses ``INNER JOIN``: an unresolvable identity there means the derivation has
#: nothing to derive, which IS the correct fail-closed answer (an empty repository
#: set), not a silent bypass of the join.
#:
#: Codex adversarial review (MEDIUM, 2026-08-04), two findings against the first
#: cut of this CTE, both fixed here rather than accepted as residual risk:
#:
#: 1. ``projects``' ReplacingMergeTree key is ``(org_id, provider, id)``, NOT
#:    ``(org_id, id)`` -- ``id`` alone is only unique WITHIN one provider, not
#:    across providers in the same org. A same-org, same-id row minted by two
#:    different providers (today only a coincidence; nothing enforces cross-
#:    provider id uniqueness) survives ``FINAL`` as TWO rows, since they differ
#:    on the provider component of the key. The original ``LIMIT 1`` (no
#:    ``ORDER BY``) would then pick one of them *nondeterministically*, and the
#:    provider guard would faithfully protect the WRONG provider's identity.
#:    Fixed by aggregating with no ``GROUP BY`` (the whole filtered set is one
#:    implicit group) and admitting a result only when ``count() = 1`` --
#:    exactly one row for this ``(org_id, id)``, at any provider. Two or more
#:    (or zero) both correctly yield an empty CTE, which every call site above
#:    already treats as an unresolvable identity (fail closed).
#: 2. The authorized-entity catalog's own committing query
#:    (``scope_catalog.ClickHouseAuthorizedEntityCatalog._query_for``,
#:    ``EntityKind.PROJECT``) filters ``is_active = 1`` -- a project retired
#:    AFTER a scope was committed against it (a newer ReplacingMergeTree row
#:    with ``is_active = 0``) must not keep answering here just because this
#:    CTE re-reads the same table without that filter. Added explicitly rather
#:    than assumed from ``FINAL`` alone, since retirement is a data state
#:    (``is_active``), not a version-ordering property ``FINAL`` enforces on
#:    its own.
#: Both helpers live in ``_project_identity.py`` (CHAOS-3380 round 3) so
#: ``native_evidence.py`` can reuse them without a circular import (this
#: module already imports FROM ``native_evidence.py``). Re-exported here
#: under their historical private names -- every internal usage below, and
#: the test assertions against ``native_status_change._PROJECT_IDENTITY_CTE``
#: / ``native_status_change._project_identity_match``, keep working
#: unchanged.
_project_identity_cte = project_identity_cte
_project_identity_match = project_identity_match

#: The default-parameterized text, used verbatim by every arm in THIS module
#: (all of which key off ``{entity_id:String}``) -- a plain string constant so
#: existing ``_PROJECT_IDENTITY_CTE + "..."`` splices and ``in`` assertions
#: keep working unchanged. ``native_evidence.py`` calls
#: ``project_identity_cte("scope_entity_id")`` directly instead, for its own
#: differently-named parameter.
_PROJECT_IDENTITY_CTE = _project_identity_cte()


def _project_scope_arm(alias: str = "") -> str:
    """The full ``scope_type = 'project'`` disjunction arm, identity-scoped."""
    return (
        "({scope_type:String} = 'project'\n"
        f"      AND {_project_identity_match(alias)})"
    )


_PROJECT_LINKED_WORK_ITEMS_CTE = (
    _PROJECT_IDENTITY_CTE
    + """, linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  INNER JOIN work_items AS item FINAL
    ON item.org_id = link.org_id
   AND item.repo_id = link.repo_id
   AND item.work_item_id = link.work_item_id
  LEFT JOIN project ON 1 = 1
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR """
    + _project_scope_arm("item.")
    + """
      OR ({scope_type:String} = 'team' AND link.work_item_id IN ("""
    + _TEAM_OWNED_WORK_ITEM_IDS_SUBQUERY
    + """
      ))
    )
)"""
)


_WORK_ITEMS_SQL = (
    "WITH "
    + _PROJECT_IDENTITY_CTE
    + """
SELECT toString(repo_id) AS repository_id, work_item_id, title, status,
       parent_id, project_id, project_key, updated_at, last_synced
FROM work_items FINAL
LEFT JOIN project ON 1 = 1
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND updated_at <= {as_of:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'issue'
      AND (work_item_id = {entity_id:String} OR parent_id = {entity_id:String}))
    OR """
    + _project_scope_arm()
    + """
    OR ({scope_type:String} = 'work_unit'
      AND work_item_id IN {member_issue_ids:Array(String)})
  )
ORDER BY (work_item_id = {entity_id:String}) DESC, updated_at DESC, work_item_id
LIMIT {limit:UInt32}
"""
)

#: CHAOS-3368: the project's own DECLARED lifecycle state / target date
#: (``projects.state``/``projects.target_date``, migration 073 -- see
#: ``metrics.schemas.ProjectRecord``'s own docstring: "the provider's OWN
#: lifecycle vocabulary, stored verbatim"). Read with the same RMT
#: discipline every other catalog read in this module uses: ``FINAL`` for
#: the current row, ``org_id`` in the WHERE, ``is_active = 1`` so a
#: retired project row does not resurrect a stale declared state. Guarded
#: by an explicit ``HAVING count() = 1`` for the same reason CHAOS-3374's
#: ``_PROJECT_IDENTITY_CTE`` guards its own read of this table: the
#: ReplacingMergeTree key is ``(org_id, provider, id)``, not ``(org_id,
#: id)``, so a same-org, same-``id`` row minted by two different
#: providers survives ``FINAL`` as two rows and an unguarded ``LIMIT 1``
#: would pick one nondeterministically. This read is deliberately
#: narrower than that CTE: it never resolves which provider a project
#: SUBJECT belongs to (CHAOS-3374's turf) -- it only reads the declared-
#: state columns of a subject the scope has ALREADY resolved and
#: authorized, so an ambiguous match here is grounds to render the
#: declared-state facts absent (fail closed), never to guess a provider.
#: Selects ``last_synced`` under that exact alias so ``_read``'s existing
#: watermark/freshness plumbing applies unchanged -- no bespoke handling
#: needed here.
#:
#: Codex adversarial review (MEDIUM, 2026-08-04): ``FINAL`` collapses the
#: RMT to its single CURRENT row per key -- there is no history left to read
#: here, so ``updated_at <= {as_of:DateTime64(3, 'UTC')}`` (mirroring every
#: other as-of-bounded read in this module, e.g. ``_WORK_ITEMS_SQL``) is not
#: a narrowing filter, it is the difference between "this project's
#: declared state as of the requested instant" and "whatever it is RIGHT
#: NOW, mislabeled as of an earlier instant". A project updated after
#: ``as_of`` is excluded entirely (``count() = 0`` -> absent facts) rather
#: than answered with its current, not-yet-true-at-as_of state -- this
#: ticket deliberately does not attempt a real history representation; an
#: as-of snapshot of a since-changed declared state is simply unavailable.
#: CHAOS-3377 residual defect (live acceptance probe, 2026-08-04): the
#: aggregate below was originally aliased ``AS updated_at`` -- the SAME name
#: as the raw column the ``WHERE`` clause filters on two lines down.
#: ClickHouse resolves a bare ``WHERE`` identifier against a same-named
#: ``SELECT`` alias in preference to the source column, so ``WHERE
#: updated_at <= {as_of:...}`` was silently rewritten to filter on
#: ``any(updated_at)`` -- an aggregate function, which ``WHERE`` (unlike
#: ``HAVING``) can never contain. The result was ``Code: 184
#: (ILLEGAL_AGGREGATION)`` raised on EVERY invocation of this query,
#: unconditionally (never a timestamp-skew or evidence-cap artifact).
#: ``_read`` (below) catches that exception and reports the ``projects``
#: source as merely "unavailable" -- indistinguishable, from every caller's
#: perspective, from a genuinely absent declared state, which is exactly why
#: this went unnoticed: the declared-state clause simply never rendered, for
#: any project, on any run. Aliased to ``declared_updated_at`` instead so the
#: ``WHERE`` clause's ``updated_at`` reference stays bound to the raw
#: column -- the fix is the rename alone; ``state``/``target_date``/
#: ``last_synced`` are left untouched (not aggregation-clause hazards, since
#: nothing filters on them) so ``_read``'s own generic
#: ``row.get("last_synced")`` watermark lookup keeps working unchanged.
_PROJECT_DECLARED_FACTS_SQL = """
SELECT any(state) AS state,
       any(target_date) AS target_date,
       any(updated_at) AS declared_updated_at,
       any(last_synced) AS last_synced
FROM projects FINAL
WHERE org_id = {org_id:String} AND id = {entity_id:String} AND is_active = 1
  AND updated_at <= {as_of:DateTime64(3, 'UTC')}
HAVING count() = 1
"""

_BLOCKER_WATERMARK_SQL = """
SELECT if(
         countIf(scope_repo_id IS NULL) > 0,
         maxIf(last_completed, scope_repo_id IS NULL),
         min(last_completed)
       ) AS last_synced
FROM (
  SELECT scope_repo_id, max(completed_at) AS last_completed
  FROM work_graph_projection_runs
  WHERE org_id = {org_id:String}
    AND projection_name = 'issue_blockers'
    AND rule_version = {blocker_rule_version:String}
    AND completed_at <= {as_of:DateTime64(3, 'UTC')}
    AND (scope_repo_id IS NULL OR toString(scope_repo_id) IN {repository_ids:Array(String)})
  GROUP BY scope_repo_id
)
HAVING countIf(scope_repo_id IS NULL) > 0
    OR countDistinctIf(toString(scope_repo_id), scope_repo_id IS NOT NULL)
       = length({repository_ids:Array(String)})
"""

#: CHAOS-3377 residual defect (Codex adversarial review HIGH, live
#: acceptance probe 2026-08-04): this query used to ``SELECT`` directly off
#: the ``edge``/``blocker``/``blocked`` join with no ``GROUP BY`` -- one row
#: PER MATCHING EDGE, not per blocker. A blocker whose ``blocks`` edges
#: target several blocked issues in the SAME project scope therefore
#: returned its own entity ``N`` times (once per edge), and ``ORDER BY`` /
#: ``LIMIT`` applied to those MULTIPLIED rows -- so a blocker with more
#: edges than the page ``limit`` could fill the ENTIRE result page by
#: itself, silently crowding out every other, genuinely distinct blocker
#: (and the evidence minted from it) with no way for anything downstream
#: (the renderer's own defense-in-depth dedup included) to recover what the
#: page never contained. ``matched_edges`` now holds the pre-collapse,
#: one-row-per-edge result the query used to return directly; the outer
#: ``SELECT ... GROUP BY entity_id`` collapses it to one row per blocker
#: BEFORE ``ORDER BY``/``LIMIT`` ever run, so a page of ``limit`` rows can
#: hold up to ``limit`` DISTINCT blockers again, exactly as intended.
#: ``max()`` (not ``any()``) for the three non-key columns: ``display_label``
#: and ``status`` are properties of the ONE blocker work-item row joined
#: repeatedly (identical across every duplicate, so any aggregate is
#: equivalent in practice, but ``max()`` is deterministic even if that ever
#: stops being true), while ``observed_at`` (a ``greatest()`` of two
#: timestamps per edge) genuinely differs per edge and must take the
#: latest, matching this query's own pre-existing "most recently observed"
#: intent for ``ORDER BY``.
_BLOCKERS_SQL = (
    "WITH "
    + _PROJECT_IDENTITY_CTE
    + """, matched_edges AS (
  SELECT blocker.work_item_id AS entity_id,
         blocker.title AS display_label,
         blocker.status AS status,
         greatest(blocker.updated_at, edge.event_ts) AS observed_at,
         greatest(blocker.last_synced, edge.last_synced) AS last_synced
  FROM work_graph_edges AS edge FINAL
  INNER JOIN work_items AS blocker FINAL
    ON blocker.org_id = edge.org_id AND blocker.work_item_id = edge.source_id
  INNER JOIN work_items AS blocked FINAL
    ON blocked.org_id = edge.org_id AND blocked.work_item_id = edge.target_id
  LEFT JOIN project ON 1 = 1
  WHERE edge.org_id = {org_id:String}
    AND edge.source_type = 'issue'
    AND edge.target_type = 'issue'
    AND edge.edge_type = 'blocks'
    AND edge.provenance = 'native'
    AND toString(blocker.repo_id) IN {repository_ids:Array(String)}
    AND toString(blocked.repo_id) IN {repository_ids:Array(String)}
    AND edge.event_ts <= {as_of:DateTime64(3, 'UTC')}
    AND blocker.updated_at <= {as_of:DateTime64(3, 'UTC')}
    AND blocked.updated_at <= {as_of:DateTime64(3, 'UTC')}
    AND (
      ({scope_type:String} = 'issue' AND edge.target_id = {entity_id:String})
      OR """
    + _project_scope_arm("blocked.")
    + """
      OR ({scope_type:String} = 'work_unit'
        AND edge.target_id IN {member_issue_ids:Array(String)})
    )
)
SELECT entity_id,
       max(display_label) AS display_label,
       max(status) AS status,
       max(observed_at) AS observed_at,
       max(last_synced) AS last_synced
FROM matched_edges
GROUP BY entity_id
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""
)

_PULL_REQUESTS_SQL = (
    "WITH "
    + _PROJECT_IDENTITY_CTE
    + """, linked AS (
  SELECT toString(link.repo_id) AS repository_id, link.pr_number
  FROM work_graph_issue_pr AS link FINAL
  LEFT JOIN work_items AS item FINAL
    ON item.repo_id = link.repo_id AND item.work_item_id = link.work_item_id
  LEFT JOIN project ON 1 = 1
  WHERE link.org_id = {org_id:String}
    AND item.org_id = {org_id:String}
    AND toString(link.repo_id) IN {repository_ids:Array(String)}
    AND (
      ({scope_type:String} = 'issue' AND link.work_item_id = {entity_id:String})
      OR """
    + _project_scope_arm("item.")
    + """
      OR ({scope_type:String} = 'work_unit'
        AND link.work_item_id IN {member_issue_ids:Array(String)})
      OR ({scope_type:String} = 'team'
        AND link.work_item_id IN ("""
    + _TEAM_OWNED_WORK_ITEM_IDS_SUBQUERY
    + """
        ))
    )
), latest_reviews AS (
  SELECT toString(repo_id) AS repository_id, number, reviewer,
         argMax(state, (submitted_at, last_synced, review_id)) AS state,
         max(submitted_at) AS submitted_at
  FROM git_pull_request_reviews FINAL
  WHERE org_id = {org_id:String}
    AND toString(repo_id) IN {repository_ids:Array(String)}
  GROUP BY repository_id, number, reviewer
), reviews AS (
  SELECT repository_id, number,
         multiIf(
           countIf(upper(state) = 'CHANGES_REQUESTED') > 0,
           'CHANGES_REQUESTED',
           countIf(upper(state) = 'APPROVED') > 0,
           'APPROVED',
           argMax(state, submitted_at)
         ) AS review_state,
         countIf(upper(state) = 'CHANGES_REQUESTED') AS changes_requested
  FROM latest_reviews
  GROUP BY repository_id, number
)
SELECT toString(pr.repo_id) AS repository_id, pr.number,
       concat(toString(pr.repo_id), '#pr', toString(pr.number)) AS entity_id,
       ifNull(pr.title, concat('Pull request #', toString(pr.number))) AS display_label,
       ifNull(pr.state, 'unknown') AS state,
       reviews.review_state AS review_state,
       ifNull(reviews.changes_requested, 0) AS changes_requested,
       isNotNull(pr.merged_at) AS merged,
       coalesce(pr.merged_at, pr.closed_at, pr.created_at) AS observed_at,
       pr.last_synced
FROM git_pull_requests AS pr FINAL
LEFT JOIN reviews
  ON reviews.repository_id = toString(pr.repo_id) AND reviews.number = pr.number
WHERE pr.org_id = {org_id:String}
  AND toString(pr.repo_id) IN {repository_ids:Array(String)}
  AND pr.created_at <= {as_of:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND pr.number = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project', 'team')
      AND (toString(pr.repo_id), pr.number) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} = 'work_unit'
      AND (
        (toString(pr.repo_id), pr.number) IN
          (SELECT repository_id, pr_number FROM linked)
        OR concat(toString(pr.repo_id), '#pr', toString(pr.number))
          IN {member_pr_ids:Array(String)}
      ))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""
)

_CI_SQL = """
SELECT toString(repo_id) AS repository_id, run_id,
       concat(toString(repo_id), '#ci', run_id) AS entity_id,
       ifNull(pipeline_name, concat('CI run ', run_id)) AS display_label,
       ifNull(status, 'unknown') AS conclusion,
       ifNull(pr_number, 0) AS pr_number,
       coalesce(finished_at, started_at) AS observed_at, last_synced
FROM ci_pipeline_runs FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND ifNull(pr_number, 0) IN {pr_numbers:Array(UInt32)}
  AND started_at <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_CI_ACCEPTANCE_SQL = """
SELECT toString(repo_id) AS repository_id, run_id, check_key,
       concat(toString(repo_id), '#ci', run_id, '#check', check_key) AS entity_id,
       check_name AS display_label, requirement, result AS conclusion,
       ifNull(pr_number, 0) AS pr_number, observed_at, last_synced,
       provenance, rule_version, source_url
FROM ci_acceptance_checks FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND ifNull(pr_number, 0) IN {pr_numbers:Array(UInt32)}
  AND observed_at <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_DEPLOYMENTS_SQL = """
SELECT toString(repo_id) AS repository_id, deployment_id AS entity_id,
       concat('Deployment ', deployment_id) AS display_label,
       ifNull(status, 'unknown') AS status, environment,
       ifNull(pull_request_number, 0) AS pr_number,
       coalesce(deployed_at, finished_at, started_at, last_synced) AS observed_at,
       last_synced
FROM deployments FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND (
    ({scope_type:String} IN ('organization', 'repository'))
    -- CHAOS-3303 round 2 (Codex HIGH): a bare repository-membership arm
    -- would admit every deployment in the team's team_repo_ownership
    -- repos, including ones belonging to a different (e.g. child) team
    -- that merely shares the repo. A team-scoped deployment is admitted
    -- ONLY through an already team-owned PR (pr_numbers is derived from
    -- the now canonically-filtered _PULL_REQUESTS_SQL rows) -- the same
    -- rule every other scope narrower than organization/repository
    -- already follows via the unconditional arm below, made explicit here
    -- for 'team' rather than left implicit.
    OR ({scope_type:String} = 'team'
      AND ifNull(pull_request_number, 0) IN {pr_numbers:Array(UInt32)})
    OR ifNull(pull_request_number, 0) IN {pr_numbers:Array(UInt32)}
  )
  AND coalesce(deployed_at, finished_at, started_at, last_synced)
      <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

#: CHAOS-3303 round 4 (Codex HIGH, 2026-08-02): ``deployment_id`` is only
#: unique PER REPO in the schema -- filtering incident edges by a bare
#: ``edge.deployment_id IN {deployment_ids}`` list (even after the round-3
#: (repository_id, pr_number) pair filter correctly excludes a deployment
#: from ``deployments``) still matches ANY edge with that deployment_id in
#: ANY of the team's authorized repos, including one whose OWN deployment
#: happens to share the same id string as an admitted deployment in a
#: different repo. Admission must therefore be scoped by the exact
#: (repository_id, deployment_id) PAIR, mirroring the
#: work_unit_investments.py ``(work_unit_id, categorization_run_id) IN
#: {pairs:Array(Tuple(String, String))}`` idiom already used elsewhere in
#: this codebase for the same reason.
_INCIDENTS_SQL = """
SELECT incident.id AS entity_id, incident.title AS display_label,
       ifNull(incident.normalized_status, 'unknown') AS status,
       incident.resolved_at IS NULL AND incident.is_deleted = 0 AS active,
       coalesce(incident.source_event_at, incident.observed_at) AS observed_at,
       incident.last_synced
FROM operational_incidents AS incident FINAL
INNER JOIN work_graph_deployment_incident_edges AS edge FINAL
  ON edge.org_id = toUUIDOrZero(incident.org_id)
 AND edge.incident_id = incident.id
WHERE incident.org_id = {org_id:String}
  AND (toString(edge.repo_id), edge.deployment_id)
      IN {deployment_pairs:Array(Tuple(String, String))}
  AND coalesce(incident.source_event_at, incident.observed_at)
      <= {as_of:DateTime64(3, 'UTC')}
ORDER BY observed_at DESC, entity_id
LIMIT {limit:UInt32}
"""

_ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL = """
SELECT toString(id) AS repository_id
FROM repos FINAL
WHERE org_id = {org_id:String}
"""

#: CHAOS-3303 round 3 (Codex MEDIUM, 2026-08-02): a bare repository-level
#: existence check -- deliberately bypasses canonical work-item attribution
#: entirely (unlike every fact-level team arm above). Used ONLY to detect
#: "this team's accessible repositories are not actually empty" when every
#: canonically-attributed team read came back with nothing, so
#: status_snapshot can tell "insufficient attribution coverage" (repo-level
#: activity exists, none of it canonically assignable to this team) apart
#: from "this team genuinely has no activity in scope" -- the exclusion
#: itself (repo co-location != ownership) stays correct either way; only the
#: silent-completeness gap is being closed. Checks pull requests and
#: deployments only (the two root categories every other delivery fact --
#: CI, reviews, incidents -- derives from), so a genuinely empty root
#: category set implies the derived categories are empty too.
#: CHAOS-3303 round 4 (Codex MEDIUM, 2026-08-02): bounded by as_of, mirroring
#: the root _PULL_REQUESTS_SQL / _DEPLOYMENTS_SQL bounds exactly -- without
#: this, a pull request or deployment created strictly AFTER the snapshot's
#: as_of would still trip the probe, falsely degrading a historical
#: (as_of=t1) snapshot with activity that had not happened yet at t1.
_TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL = """
SELECT 1 AS found
FROM git_pull_requests FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND created_at <= {as_of:DateTime64(3, 'UTC')}
LIMIT 1
UNION ALL
SELECT 1 AS found
FROM deployments FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND coalesce(deployed_at, finished_at, started_at, last_synced)
      <= {as_of:DateTime64(3, 'UTC')}
LIMIT 1
"""

#: CHAOS-3303: re-derive a committed team subject's authorized repositories
#: from ``team_repo_ownership`` at query time (CHAOS-3301 addendum, Option
#: B -- never carried on the wire; ``DevScope.validate_direct_scope`` forbids
#: a team direct scope from carrying its own ``repositories`` list). Mirrors
#: the exact reviewed ``argMax(..., (updated_at, valid_from))`` /
#: ``valid_from``/``valid_to`` windowing pattern already used by
#: ``metrics.loaders.clickhouse.ClickHouseDataLoader.load_team_attribution_
#: context`` for the same table, scoped further to one ``team_id`` and
#: excluding pattern-matched-but-unresolved ownership rows (``repo_id IS
#: NULL`` -- a ``match_type='pattern'`` row that has not yet resolved to a
#: concrete repository contributes no queryable repository id).
#:
#: CHAOS-3375 (Codex adversarial review, HIGH): the first cut stopped there
#: and trusted every ``team_repo_ownership.repo_id`` outright, exactly the
#: mistake the adjacent ``PROJECT_REPOSITORIES_SQL`` derivation's own review
#: comment above documents for ``work_items.repo_id``. ClickHouse enforces no
#: foreign key, so a repository revoked from ``repos`` -- de-authorized, and
#: correctly invisible to the ORGANIZATION branch, which enumerates ``repos``
#: itself -- can keep a stale ``team_repo_ownership`` row and would have
#: become an admitted read bound for team scope alone. Every *real*
#: repository id must therefore still resolve through the same org-scoped
#: ``repos`` catalog. Verified against a live migrated ClickHouse: an
#: orphaned ownership row (repo_id absent from ``repos``) is admitted by the
#: query without this clause and excluded with it.
_TEAM_REPOSITORIES_SQL = """
SELECT toString(g.repo_id) AS repository_id
FROM (
  SELECT
    org_id,
    provider,
    repo_full_name,
    team_id,
    argMax(repo_id, (updated_at, valid_from)) AS repo_id
  FROM team_repo_ownership
  WHERE org_id = {org_id:String}
    AND team_id = {team_id:String}
    AND valid_from <= {as_of:DateTime64(3, 'UTC')}
    AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
  GROUP BY org_id, provider, repo_full_name, team_id
) AS g
WHERE g.repo_id IS NOT NULL
  AND toString(g.repo_id) IN (
    SELECT toString(id) FROM repos FINAL WHERE org_id = {org_id:String}
  )
"""

#: Re-derive one project's repository set from canonical work-item attribution.
#:
#: A committed PROJECT subject can never carry a repository list of its own,
#: exactly like the TEAM subject above: ``DevScope.repositories`` is only ever
#: populated for a REPOSITORY commit
#: (``scope_service.ScopeResolutionService.committed_scope_for`` /
#: ``_resolved_scope``), and the catalog row a project commit is built from
#: selects ``NULL AS repository_id``
#: (``scope_catalog.ClickHouseAuthorizedEntityCatalog._query_for``) because a
#: project spans repositories and has no repository dimension. So
#: ``_repository_ids(scope)`` returned an empty set for *every* production
#: project subject, and ``status_snapshot``/``change_summary`` took the
#: fail-closed "authorized repository set" branch before running a single
#: project query -- an empty snapshot for a project whose work items match.
#:
#: Bounded by the same ``org_id`` tenant boundary
#: ``_ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL`` enforces, by the same
#: ``updated_at <= as_of`` bound ``_WORK_ITEMS_SQL`` applies, and by the
#: byte-identical ``project_id``/``project_key`` disjunction every project SQL
#: arm uses (pinned by
#: ``test_derivation_matches_projects_exactly_as_the_queries_it_bounds_do``) --
#: so the derived set can never admit a repository the fact queries it bounds
#: would not themselves have drawn from.
#:
#: Codex adversarial review (HIGH, 2026-08-03): the first cut stopped there and
#: trusted every ``work_items.repo_id`` outright. ClickHouse enforces no
#: foreign key, so a repository revoked from ``repos`` -- de-authorized, and
#: correctly invisible to the ORGANIZATION branch, which enumerates ``repos``
#: itself -- can keep its ``work_items`` rows and would have become an admitted
#: read bound for project scope alone. Every *real* repository id must therefore
#: still resolve through the same org-scoped ``repos`` catalog.
#:
#: Provider-identity scoped (CHAOS-3374, superseding the prior "provider scope,
#: deliberate" ruling below it superseded a "clean and small" derivation-only
#: fix that would have made Jira *worse* -- a confident empty answer instead of
#: a disclosed fail-closed one -- for exactly the reason described in
#: ``_PROJECT_IDENTITY_CTE``'s own comment: ``team_autoimport_jira._project_id``
#: mints the catalog id as ``f"{org_id}:jira:{project_key}"`` while
#: ``providers/jira/normalize`` writes the raw Jira id/key onto ``work_items``.
#: Rather than leaving the mismatch in place, this arm now joins through the
#: catalog's own ``provider``/``project_key`` columns (``_project_identity_match``)
#: so a Jira project matches its own raw key, a Linear project keeps matching
#: the raw id it always used, and neither can match the other's (or a future
#: GitLab catalog row's) rows even on a coincidental key/id collision. Asserted
#: end to end by ``test_jira_shaped_project_resolves_via_provider_scoped_identity``
#: and the cross-provider collision coverage beside it; the derivation and
#: ``_WORK_ITEMS_SQL`` share ``_project_identity_match``'s exact text, pinned by
#: ``test_derivation_matches_projects_exactly_as_the_queries_it_bounds_do``.
#:
#: The zero UUID is admitted explicitly rather than by omitting that check.
#: It is not a repository at all: it is the sentinel
#: ``ClickHouseMetricsSink`` writes whenever a record has no repository
#: (``metrics/sinks/clickhouse/core.py`` -- ``row.repo_id or uuid.UUID(int=0)``),
#: which is every Linear work item (``providers/linear/normalize.py`` sets
#: ``repo_id=None``). It has no ``repos`` row and never will, so a bare
#: existence join would re-empty the set for the exact provider this fix exists
#: for -- while a blanket "skip the catalog" would have de-authorized nothing
#: and admitted everything. Naming the sentinel keeps both properties.
PROJECT_REPOSITORIES_SQL = (
    "WITH "
    + _PROJECT_IDENTITY_CTE
    + """
SELECT DISTINCT toString(repo_id) AS repository_id
FROM work_items FINAL
INNER JOIN project ON 1 = 1
WHERE org_id = {org_id:String}
  AND updated_at <= {as_of:DateTime64(3, 'UTC')}
  AND """
    + _project_identity_match()
    + """
  AND (
    toString(repo_id) = '00000000-0000-0000-0000-000000000000'
    OR toString(repo_id) IN (
      SELECT toString(id) FROM repos FINAL WHERE org_id = {org_id:String}
    )
  )
"""
)

_TRANSITIONS_SQL = (
    "WITH "
    + _PROJECT_IDENTITY_CTE
    + """
SELECT transition.work_item_id AS entity_id, item.title AS display_label,
       transition.from_status, transition.to_status,
       transition.occurred_at AS observed_at, transition.last_synced
FROM work_item_transitions AS transition FINAL
INNER JOIN work_items AS item FINAL
  ON item.org_id = transition.org_id
 AND item.repo_id = transition.repo_id
 AND item.work_item_id = transition.work_item_id
LEFT JOIN project ON 1 = 1
WHERE transition.org_id = {org_id:String}
  AND item.org_id = {org_id:String}
  AND toString(item.repo_id) IN {repository_ids:Array(String)}
  AND transition.occurred_at >= {start:DateTime64(3, 'UTC')}
  AND transition.occurred_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'issue' AND transition.work_item_id = {entity_id:String})
    OR """
    + _project_scope_arm("item.")
    + """
    OR ({scope_type:String} = 'team' AND transition.work_item_id IN ("""
    + _TEAM_OWNED_WORK_ITEM_IDS_SUBQUERY
    + """
    ))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, from_status, to_status
LIMIT {limit:UInt32}
"""
)

_RELATIONSHIPS_SQL = (
    "WITH "
    + _PROJECT_IDENTITY_CTE
    + """
SELECT edge_id AS change_id, source_type, source_id, edge_type,
       target_type, target_id, provenance, confidence,
       discovered_at AS observed_at, last_synced
FROM work_graph_edges FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repository_ids:Array(String)}
  AND discovered_at >= {start:DateTime64(3, 'UTC')}
  AND discovered_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} IN ('issue', 'pull_request')
      AND (source_id = {entity_id:String} OR target_id = {entity_id:String}))
    OR ({scope_type:String} = 'project' AND (
      source_id IN (
        SELECT work_item_id FROM work_items FINAL
        INNER JOIN project ON 1 = 1
        WHERE org_id = {org_id:String}
          AND toString(repo_id) IN {repository_ids:Array(String)}
          AND """
    + _project_identity_match()
    + """
      )
      OR target_id IN (
        SELECT work_item_id FROM work_items FINAL
        INNER JOIN project ON 1 = 1
        WHERE org_id = {org_id:String}
          AND toString(repo_id) IN {repository_ids:Array(String)}
          AND """
    + _project_identity_match()
    + """
      )
    ))
    OR ({scope_type:String} = 'team' AND (
      source_id IN ("""
    + _TEAM_OWNED_WORK_ITEM_IDS_SUBQUERY
    + """
      )
      OR target_id IN ("""
    + _TEAM_OWNED_WORK_ITEM_IDS_SUBQUERY
    + """
      )
    ))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, source_type, source_id, edge_type, target_type, target_id
LIMIT {limit:UInt32}
"""
)

_PULL_REQUEST_CHANGES_SQL = (
    "WITH "
    + _PROJECT_LINKED_WORK_ITEMS_CTE
    + """
SELECT concat(toString(pr.repo_id), '#pr', toString(pr.number), '#state#',
              if(isNotNull(pr.merged_at), 'merged',
                 if(isNotNull(pr.closed_at), 'closed', ifNull(pr.state, 'open')))) AS change_id,
       concat(toString(pr.repo_id), '#pr', toString(pr.number)) AS entity_id,
       ifNull(pr.title, concat('Pull request #', toString(pr.number))) AS display_label,
       if(isNotNull(pr.merged_at) OR isNotNull(pr.closed_at),
          CAST('open', 'Nullable(String)'), CAST(NULL, 'Nullable(String)')) AS before_value,
       if(isNotNull(pr.merged_at), 'merged',
          if(isNotNull(pr.closed_at), 'closed', ifNull(pr.state, 'open'))) AS after_value,
       coalesce(pr.merged_at, pr.closed_at, pr.created_at) AS observed_at,
       pr.last_synced
FROM git_pull_requests AS pr FINAL
WHERE pr.org_id = {org_id:String}
  AND toString(pr.repo_id) IN {repository_ids:Array(String)}
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND pr.number = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project', 'team')
      AND (toString(pr.repo_id), pr.number) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""
)

_REVIEW_CHANGES_SQL = (
    "WITH "
    + _PROJECT_LINKED_WORK_ITEMS_CTE
    + """
SELECT concat(toString(review.repo_id), '#pr', toString(review.number),
              '#review#', review.review_id) AS change_id,
       change_id AS entity_id,
       concat('Review by ', review.reviewer) AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       review.state AS after_value,
       review.submitted_at AS observed_at,
       review.last_synced
FROM git_pull_request_reviews AS review FINAL
WHERE review.org_id = {org_id:String}
  AND toString(review.repo_id) IN {repository_ids:Array(String)}
  AND review.submitted_at >= {start:DateTime64(3, 'UTC')}
  AND review.submitted_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND review.number = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project', 'team')
      AND (toString(review.repo_id), review.number) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""
)

_CI_CHANGES_SQL = (
    "WITH "
    + _PROJECT_LINKED_WORK_ITEMS_CTE
    + """
SELECT concat(toString(run.repo_id), '#ci#', run.run_id) AS change_id,
       change_id AS entity_id,
       ifNull(run.pipeline_name, concat('CI run ', run.run_id)) AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       ifNull(run.status, 'unknown') AS after_value,
       coalesce(run.finished_at, run.started_at) AS observed_at,
       run.last_synced
FROM ci_pipeline_runs AS run FINAL
WHERE run.org_id = {org_id:String}
  AND toString(run.repo_id) IN {repository_ids:Array(String)}
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request' AND ifNull(run.pr_number, 0) = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project', 'team')
      AND (toString(run.repo_id), ifNull(run.pr_number, 0)) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""
)

_DEPLOYMENT_CHANGES_SQL = (
    "WITH "
    + _PROJECT_LINKED_WORK_ITEMS_CTE
    + """
SELECT concat(toString(deployment.repo_id), '#deployment#',
              deployment.deployment_id) AS change_id,
       change_id AS entity_id,
       concat('Deployment ', deployment.deployment_id) AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       ifNull(deployment.status, 'unknown') AS after_value,
       coalesce(deployment.deployed_at, deployment.finished_at,
                deployment.started_at, deployment.last_synced) AS observed_at,
       deployment.last_synced
FROM deployments AS deployment FINAL
WHERE deployment.org_id = {org_id:String}
  AND toString(deployment.repo_id) IN {repository_ids:Array(String)}
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request'
      AND ifNull(deployment.pull_request_number, 0) = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project', 'team')
      AND (toString(deployment.repo_id), ifNull(deployment.pull_request_number, 0)) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""
)

_INCIDENT_CHANGES_SQL = (
    "WITH "
    + _PROJECT_LINKED_WORK_ITEMS_CTE
    + """
SELECT concat(incident.id, '#state#',
              ifNull(incident.normalized_status, 'unknown')) AS change_id,
       incident.id AS entity_id,
       incident.title AS display_label,
       CAST(NULL, 'Nullable(String)') AS before_value,
       ifNull(incident.normalized_status, 'unknown') AS after_value,
       edge.deployment_id AS deployment_id,
       edge.source AS relationship_source,
       edge.confidence AS relationship_confidence,
       coalesce(incident.resolved_at, incident.source_event_at,
                incident.observed_at) AS observed_at,
       incident.last_synced
FROM operational_incidents AS incident FINAL
INNER JOIN work_graph_deployment_incident_edges AS edge FINAL
  ON edge.org_id = toUUIDOrZero(incident.org_id)
 AND edge.incident_id = incident.id
INNER JOIN deployments AS deployment FINAL
  ON deployment.org_id = incident.org_id
 AND deployment.repo_id = edge.repo_id
 AND deployment.deployment_id = edge.deployment_id
WHERE incident.org_id = {org_id:String}
  AND deployment.org_id = {org_id:String}
  AND toString(edge.repo_id) IN {repository_ids:Array(String)}
  AND incident.is_deleted = 0
  AND observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}
  AND (
    ({scope_type:String} = 'pull_request'
      AND ifNull(deployment.pull_request_number, 0) = {pr_number:UInt32})
    OR ({scope_type:String} IN ('issue', 'project', 'team')
      AND (toString(deployment.repo_id), ifNull(deployment.pull_request_number, 0)) IN
          (SELECT repository_id, pr_number FROM linked))
    OR ({scope_type:String} IN ('organization', 'repository'))
  )
ORDER BY observed_at, entity_id, change_id
LIMIT {limit:UInt32}
"""
)


#: Bounded so a long-lived ``ClickHouseStatusChangeSource`` (e.g. a worker
#: process serving many orgs/teams over time) cannot accumulate an unbounded
#: cache -- mirrors ``metrics.service.MetricRequestCache``'s reviewed
#: bounded-``OrderedDict`` pattern.
_TEAM_REPOSITORY_CACHE_MAX_ENTRIES = 64


@dataclass(frozen=True, slots=True)
class TeamAttributionResult:
    """Distinguishes a genuinely empty cohort from a failed lookup.

    Codex finding (MEDIUM, 2026-08-02): the two must never collapse to the
    same caller-visible outcome. ``measured=False`` means the lookup itself
    failed (client/query error) -- cohort_size is UNKNOWABLE, not zero, and
    a caller must report every dimension unmeasured rather than suppress
    them as ``insufficient_cohort`` (which claims a real, measured zero).
    ``measured=True`` with an empty ``repository_ids`` is the genuine
    zero-cohort case, which correctly does suppress as
    ``insufficient_cohort``.
    """

    measured: bool
    repository_ids: tuple[str, ...] = ()


class ClickHouseStatusChangeSource:
    """Read only facts with server-owned repository bounds and query timeout."""

    def __init__(
        self,
        client: Any,
        *,
        policies: Mapping[str, SourceFreshnessPolicy] | None = None,
        now: datetime | None = None,
    ) -> None:
        self._client = client
        self._policies = dict(policies or default_native_freshness_policies())
        self._now = now
        # Codex finding (MEDIUM, 2026-08-02): TeamHealthService resolves its
        # own cohort attribution snapshot, and status_snapshot/change_summary
        # independently re-derive the same team's repositories internally
        # (via _authorized_repository_ids). When both calls share this same
        # ClickHouseStatusChangeSource instance (the natural production
        # wiring) and the SAME as_of, this cache turns the second lookup
        # into a hit instead of a second ClickHouse round trip -- "one
        # immutable attribution snapshot, reused" without changing
        # PlanExecutorRuntime's wire contract (a team DevScope cannot carry
        # its own repository list -- CHAOS-3301 addendum, ratified).
        self._team_repository_cache: OrderedDict[
            tuple[str, str, datetime], TeamAttributionResult
        ] = OrderedDict()

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> TeamAttributionResult:
        """Re-derive one team's owned repositories from ``team_repo_ownership``.

        Public (CHAOS-3303, Codex finding HIGH 2026-08-02): this is the
        canonical, verified team-attribution source -- the ONLY place a
        real cohort-size count for a team subject may come from. Exposed so
        ``TeamHealthService`` can resolve its own ``cohort_size`` from this
        exact query instead of trusting a caller-asserted integer (which
        could claim "cohort_size=25" for a team with zero real
        ``team_repo_ownership`` rows and get a fabricated healthy finding
        out of a fail-closed status source). ``_authorized_repository_ids``
        also delegates here for its own TEAM branch -- one source of truth
        for "what repositories does this team own right now", never two
        independently-drifting queries.

        A genuine query failure is never cached (a transient outage must
        not poison a later, successful call within the same evaluation);
        a successful (possibly empty) result is cached by the exact
        ``(org_id, team_id, as_of)`` key -- safe because the underlying
        predicate (``valid_from <= as_of AND (valid_to IS NULL OR valid_to
        > as_of)``) is a pure function of that key, not of wall-clock time.
        """

        cache_key = (org_id, team_id, as_of)
        cached = self._team_repository_cache.get(cache_key)
        if cached is not None:
            self._team_repository_cache.move_to_end(cache_key)
            return cached
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(
                    self._client,
                    _TEAM_REPOSITORIES_SQL,
                    {
                        "org_id": org_id,
                        "team_id": team_id,
                        "as_of": as_of.astimezone(UTC),
                    },
                )
        except Exception:
            return TeamAttributionResult(measured=False)
        result = TeamAttributionResult(
            measured=True,
            repository_ids=tuple(
                sorted(
                    {
                        str(row["repository_id"])
                        for row in rows
                        if row.get("repository_id")
                    }
                )
            ),
        )
        self._team_repository_cache[cache_key] = result
        while len(self._team_repository_cache) > _TEAM_REPOSITORY_CACHE_MAX_ENTRIES:
            self._team_repository_cache.popitem(last=False)
        return result

    async def _project_repository_ids(
        self, org_id: str, entity_id: str, *, as_of: datetime
    ) -> list[str]:
        """Repositories this project's canonical work items live in.

        A failed query returns an empty set, which lands the caller on the
        same fail-closed "authorized repository set" branch an unmeasured
        team attribution already takes -- an unreadable attribution source is
        never allowed to read as "this project spans no repositories".
        """

        if not entity_id:
            return []
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(
                    self._client,
                    PROJECT_REPOSITORIES_SQL,
                    {
                        "org_id": org_id,
                        "entity_id": entity_id,
                        "as_of": as_of.astimezone(UTC),
                    },
                )
        except Exception:
            return []
        return sorted(
            {str(row["repository_id"]) for row in rows if row.get("repository_id")}
        )

    async def _authorized_repository_ids(
        self, org_id: str, scope: DevScope, *, as_of: datetime
    ) -> list[str]:
        """Bound status/change reads to the server-owned repository set.

        Organization scope cannot serialize its full authorized repository
        set onto the wire (``DevScope.repositories`` is capped at 20 entries
        by the ask-dev/v1 contract), so it is re-derived here directly from
        the same ``org_id`` boundary every other native query already
        enforces (CHAOS-3255). This scales to any repository count without
        truncation or widening. Every other direct scope keeps using the
        scope's own bounded repository/entity refs.

        A team-filtered organization scope (``scope.team_ids`` set alongside
        ``direct_scope=ORGANIZATION``, a *filter*) is excluded from
        organization-native enumeration: no native query here applies a team
        filter, so deriving the full org repository set would silently widen
        a team-filtered request to every repository in the organization.
        Falling through to the caller's own (empty) bounded fields keeps the
        prior fail-closed "authorized repository set" behavior instead.

        A committed team *subject* (``direct_scope=TEAM``, checked first --
        distinct from the filter case above) instead re-derives its owned
        repositories from ``team_repo_ownership`` (CHAOS-3303, CHAOS-3301
        addendum Option B): a team direct scope can never carry its own
        ``repositories`` list (``DevScope.validate_direct_scope`` forbids
        it), so without this branch every team subject would always resolve
        to an empty repository set and take the fail-closed path below,
        regardless of real ownership data.

        A committed project *subject* (``direct_scope=PROJECT``) has the same
        structural problem for a different reason -- the catalog resolves
        projects with no repository dimension at all, so both
        ``scope.repositories`` and ``entity_refs[0].repository_id`` are always
        empty for a production project commit -- and is re-derived here from
        canonical work-item project attribution
        (``PROJECT_REPOSITORIES_SQL``). Without this branch every project
        subject failed closed below while its work items sat in ``work_items``
        matching the very ``project_id`` the scope committed.

        A *team-filtered* project scope (``direct_scope=PROJECT`` alongside
        ``scope.team_ids``, a filter -- ``DevScope`` permits the combination,
        and ``ScopeResolutionService._resolved_scope`` populates ``team_ids``
        from ``resolution.team_filters`` for any direct scope) is excluded
        from that derivation for exactly the reason the team-filtered
        ORGANIZATION case below is excluded, and the exclusion is checked
        first: no project SQL arm applies a team filter, so deriving the
        project's full repository set would silently answer a "project P, team
        A only" request with team B's work from the same project. Falling
        through to the caller's own (empty) bounded fields preserves the
        fail-closed behavior such a request already had before this branch
        existed. Codex adversarial review (HIGH, 2026-08-03).
        """
        if scope.direct_scope is DirectScope.PROJECT and not scope.team_ids:
            return await self._project_repository_ids(
                org_id, self._entity_id(scope), as_of=as_of
            )
        if scope.direct_scope is DirectScope.TEAM:
            # Unmeasured (failed lookup) and measured-but-empty both fall
            # through to the existing empty-repositories fail-closed branch
            # below -- N0's guarantee holds either way; the measured/failed
            # distinction matters to TeamHealthService's own cohort_size
            # resolution, not to this read-bounding helper.
            result = await self.team_repository_ids(
                org_id, scope.team_ids[0], as_of=as_of
            )
            return list(result.repository_ids)
        if scope.direct_scope is not DirectScope.ORGANIZATION or scope.team_ids:
            return self._repository_ids(scope)
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(
                    self._client,
                    _ORGANIZATION_AUTHORIZED_REPOSITORIES_SQL,
                    {"org_id": org_id},
                )
        except Exception:
            return []
        return sorted(
            {str(row["repository_id"]) for row in rows if row.get("repository_id")}
        )

    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot:
        repositories = await self._authorized_repository_ids(org_id, scope, as_of=as_of)
        entity_id = self._entity_id(scope)
        scope_type = scope.direct_scope.value
        warnings: list[str] = []
        source_refs: list[SourceReference] = []
        if not repositories:
            return RawStatusSnapshot(
                declared=None,
                source_refs=(self._unavailable_ref("authorized_repositories", scope),),
                warnings=(
                    "Status reads require the complete authorized repository set; scope was not widened.",
                ),
            )

        requested = min(limit, MAX_STATUS_ASSESSMENT_ITEMS)
        common = {
            "org_id": org_id,
            "repository_ids": repositories,
            "scope_type": scope_type,
            "entity_id": entity_id,
            "pr_number": self._pr_number(entity_id),
            "as_of": as_of.astimezone(UTC),
            "limit": requested,
            "member_issue_ids": [],
            "member_pr_ids": [],
            # CHAOS-3303 round 2: the canonical-primary-attribution team arms
            # (_PULL_REQUESTS_SQL, _TRANSITIONS_SQL, _RELATIONSHIPS_SQL, and
            # the five _*_CHANGES_SQL delivery projections) bind this
            # directly; harmless/unused for every other scope_type.
            "team_id": scope.team_ids[0]
            if scope.direct_scope is DirectScope.TEAM
            else "",
        }
        membership_source_truncated = False
        if scope.direct_scope is DirectScope.WORK_UNIT:
            marker_rows, membership_ref, warning = await self._read(
                "work_units",
                _WORK_UNIT_MEMBERSHIP_WATERMARK_SQL,
                common,
                scope,
            )
            source_refs.append(membership_ref)
            if warning:
                warnings.append(warning)
            if not marker_rows:
                return RawStatusSnapshot(
                    declared=None,
                    source_refs=tuple(source_refs),
                    warnings=tuple(
                        warnings
                        + ["canonical work-unit membership has no complete run"]
                    ),
                )
            # CHAOS-3297 s2 round 3 (codex HIGH): _WORK_UNIT_MEMBERS_SQL
            # mixes issue and PR members in ONE query sharing a single
            # LIMIT budget, then splits them post-fetch by node_type --
            # the same shared-budget-then-split shape as _WORK_ITEMS_SQL's
            # parent/child split. A plain post-split length check on
            # either member_issue_ids or member_pr_ids can stay under the
            # bound even when the true combined membership exceeds it
            # (e.g. 500 issues + 501 PRs = 1001 rows against a 1000 cap
            # drops the last PR, and neither 500-length list ever trips
            # its own >= 1000 check). Fetch one sentinel row beyond the
            # real budget, exactly as for work items, and record
            # truncation BEFORE the split.
            membership_requested = requested
            try:
                async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                    member_rows = await query_dicts(
                        self._client,
                        _WORK_UNIT_MEMBERS_SQL,
                        {**common, "limit": membership_requested + 1},
                    )
            except Exception:
                return RawStatusSnapshot(
                    declared=None,
                    source_refs=(self._unavailable_ref("work_units", scope),),
                    warnings=("work_units source unavailable",),
                )
            if len(member_rows) > membership_requested:
                membership_source_truncated = True
                member_rows = member_rows[:membership_requested]
            common["member_issue_ids"] = sorted(
                {
                    str(row.get("node_id") or "")
                    for row in member_rows
                    if str(row.get("node_type") or "").casefold() == "issue"
                    and row.get("node_id")
                }
            )
            common["member_pr_ids"] = sorted(
                {
                    str(row.get("node_id") or "")
                    for row in member_rows
                    if str(row.get("node_type") or "").casefold()
                    in {"pr", "pull_request"}
                    and row.get("node_id")
                }
            )
            if not common["member_issue_ids"] and not common["member_pr_ids"]:
                return RawStatusSnapshot(
                    declared=None,
                    source_refs=tuple(source_refs),
                    warnings=tuple(warnings),
                    membership_source_truncated=membership_source_truncated,
                )

        work_item_rows: list[dict[str, Any]] = []
        children_source_truncated = False
        if scope.direct_scope in {DirectScope.ISSUE, DirectScope.PROJECT} or (
            scope.direct_scope is DirectScope.WORK_UNIT
            and bool(common["member_issue_ids"])
        ):
            # CHAOS-3297 s2 round 2 (codex HIGH): _WORK_ITEMS_SQL fetches
            # the declared parent AND its children from ONE query sharing a
            # single LIMIT budget -- the parent consumes one row of that
            # budget, so a plain post-split ``len(children) >= limit``
            # check can never fire even when the true child set exceeds
            # the limit (limit=1000 -> 1000 rows returned -> 999 children
            # -> 999 < 1000, no truncation detected, though an older,
            # incomplete child past rank 1000 was silently dropped).
            # Request one sentinel row beyond the real budget: getting it
            # back proves the source had more matching rows than we asked
            # for, independent of how the parent/child split later divides
            # the (trimmed-back-down) result.
            work_items_requested = requested
            work_item_rows, ref, warning = await self._read(
                "work_items",
                _WORK_ITEMS_SQL,
                {**common, "limit": work_items_requested + 1},
                scope,
            )
            if len(work_item_rows) > work_items_requested:
                children_source_truncated = True
                work_item_rows = work_item_rows[:work_items_requested]
            source_refs.append(ref)
            if warning:
                warnings.append(warning)

        blocker_rows: list[dict[str, Any]] = []
        blocker_ref: SourceReference | None = None
        blockers_source_truncated = False
        if scope.direct_scope in {DirectScope.ISSUE, DirectScope.PROJECT} or (
            scope.direct_scope is DirectScope.WORK_UNIT
            and bool(common["member_issue_ids"])
        ):
            marker_rows, blocker_ref, warning = await self._read(
                "work_graph",
                _BLOCKER_WATERMARK_SQL,
                {
                    **common,
                    "blocker_rule_version": _BLOCKER_PROJECTION_RULE_VERSION,
                },
                scope,
            )
            if warning:
                warnings.append(warning)
            if marker_rows:
                try:
                    async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                        blocker_rows = await query_dicts(
                            self._client,
                            _BLOCKERS_SQL,
                            {**common, "limit": requested + 1},
                        )
                except Exception:
                    blocker_ref = self._unavailable_ref("work_graph", scope)
                    warnings.append("work_graph blocker source unavailable")
                else:
                    # CHAOS-3297 s2 round 5 (codex MEDIUM): sentinel, not a
                    # post-fetch length inference -- see _bounded_read.
                    if len(blocker_rows) > requested:
                        blockers_source_truncated = True
                        blocker_rows = blocker_rows[:requested]

        (
            pr_rows,
            pr_ref,
            warning,
            pull_requests_source_truncated,
        ) = await self._bounded_read(
            "pull_requests", _PULL_REQUESTS_SQL, common, scope, requested=requested
        )
        source_refs.append(pr_ref)
        if warning:
            warnings.append(warning)
        pr_pairs = {
            (str(row.get("repository_id") or ""), int(row.get("number") or 0))
            for row in pr_rows
        }
        pr_numbers = sorted({number for _, number in pr_pairs if number})

        ci_rows: list[dict[str, Any]] = []
        ci_acceptance_rows: list[dict[str, Any]] = []
        ci_acceptance_ref: SourceReference | None = None
        ci_source_truncated = False
        if pr_numbers:
            ci_rows, ci_ref, warning, ci_runs_truncated = await self._bounded_read(
                "ci_runs",
                _CI_SQL,
                {**common, "pr_numbers": pr_numbers},
                scope,
                requested=requested,
            )
            source_refs.append(ci_ref)
            if warning:
                warnings.append(warning)
            ci_rows = [
                row
                for row in ci_rows
                if (str(row.get("repository_id") or ""), int(row.get("pr_number") or 0))
                in pr_pairs
            ]
            (
                ci_acceptance_rows,
                ci_acceptance_ref,
                warning,
                ci_acceptance_truncated,
            ) = await self._bounded_read(
                "ci_acceptance_checks",
                _CI_ACCEPTANCE_SQL,
                {**common, "pr_numbers": pr_numbers},
                scope,
                requested=requested,
            )
            source_refs.append(ci_acceptance_ref)
            if warning:
                warnings.append(warning)
            ci_acceptance_rows = [
                row
                for row in ci_acceptance_rows
                if (str(row.get("repository_id") or ""), int(row.get("pr_number") or 0))
                in pr_pairs
            ]
            ci_rows = self._latest_ci_run_rows(ci_rows)
            latest_pipeline_runs = {
                self._ci_run_scope(row): str(row.get("run_id") or "") for row in ci_rows
            }
            ci_acceptance_rows = [
                row
                for row in self._latest_ci_run_rows(ci_acceptance_rows)
                if latest_pipeline_runs.get(
                    self._ci_run_scope(row), str(row.get("run_id") or "")
                )
                == str(row.get("run_id") or "")
            ]
            # CHAOS-3297 s2 round 5 (codex HIGH): truncation is measured on
            # the RAW fetched rows, before the pr_pairs filter and the
            # latest-run-per-PR collapse -- the global bound applies to
            # per-EVENT rows, not per-PR ones, so a high-churn PR can push
            # a different PR's latest (possibly failing) run out of the
            # fetch window entirely; the collapse can only pick a latest
            # run from what was actually fetched, never recover one that
            # never arrived.
            ci_source_truncated = ci_runs_truncated or ci_acceptance_truncated

        (
            deployment_rows,
            deployment_ref,
            warning,
            deployments_source_truncated,
        ) = await self._bounded_read(
            "deployments",
            _DEPLOYMENTS_SQL,
            {**common, "pr_numbers": pr_numbers},
            scope,
            requested=requested,
        )
        source_refs.append(deployment_ref)
        if warning:
            warnings.append(warning)
        if (
            scope.direct_scope not in {DirectScope.ORGANIZATION, DirectScope.REPOSITORY}
            and pr_ref.freshness is not FreshnessState.UNAVAILABLE
        ):
            # Codex finding (HIGH, 2026-08-02): _DEPLOYMENTS_SQL's
            # ifNull(pull_request_number, 0) IN {pr_numbers} arm matches on
            # a bare, cross-repository-flattened PR NUMBER list -- with
            # multiple repositories in scope (team, and in principle
            # organization/repository), a deployment in repo B for its own
            # unrelated PR #77 is wrongly admitted merely because repo A's
            # PR #77 (a genuinely different pull request) was. Re-derive
            # admission by the exact (repository_id, pr_number) PAIR,
            # mirroring the pair-filter ci_rows/ci_acceptance_rows already
            # apply just above. Organization/repository scope is exempt: its
            # own SQL arm legitimately admits every deployment regardless of
            # PR linkage (an unlinked scheduled/manual deploy has
            # pr_number=0, which would never match a real pair) -- for every
            # other scope, admission is entirely through PR linkage already,
            # so the pair check only ever tightens a real collision, never
            # excludes a legitimately-unlinked deployment. Also exempt when
            # the pull-request read itself failed (pr_ref UNAVAILABLE): an
            # empty pr_pairs from a genuine failure is not the same claim as
            # "this scope has zero pull requests", and applying the stricter
            # pair check there would wrongly strip deployments the SQL's own
            # (already-executed) pr_numbers filter had legitimately admitted.
            deployment_rows = [
                row
                for row in deployment_rows
                if (str(row.get("repository_id") or ""), int(row.get("pr_number") or 0))
                in pr_pairs
            ]
        # Codex round 4 (HIGH): pairs, not bare ids -- deployment_id alone
        # collides across repos (see _INCIDENTS_SQL's docstring above).
        deployment_pairs = [
            (str(row.get("repository_id") or ""), str(row.get("entity_id") or ""))
            for row in deployment_rows
        ]

        incident_rows: list[dict[str, Any]] = []
        incidents_source_truncated = False
        if deployment_pairs:
            (
                incident_rows,
                incident_ref,
                warning,
                incidents_source_truncated,
            ) = await self._bounded_read(
                "incidents",
                _INCIDENTS_SQL,
                {**common, "org_id": org_id, "deployment_pairs": deployment_pairs},
                scope,
                requested=requested,
            )
            source_refs.append(incident_ref)
            if warning:
                warnings.append(warning)

        declared, children = self._work_item_facts(work_item_rows, scope, source_refs)

        # CHAOS-3368: the project's own declared state/target date, additive
        # to (never mixed into) `declared`/`children` above -- those still
        # describe the derived work-item completion tree; this describes
        # what the provider itself declared for the project as a whole.
        # Only ever queried for PROJECT scope, mirroring every other
        # project-only read in this method (_BLOCKERS_SQL's own guard,
        # just above): an issue/work-unit/team/org/repository scope must
        # see byte-identical behavior to before this change.
        #
        # Codex adversarial review (HIGH, 2026-08-04): kept as TYPED scalars
        # here, never pre-joined into presentation text -- a renderer that
        # only ever sees "started; target date 2026-09-01" would have to
        # parse it back apart to use the pieces independently. Formatting
        # into a display fact is production_runtime.py's job, sourced from
        # these typed fields.
        declared_project_state: str | None = None
        declared_project_target_date: date | None = None
        declared_project_observed_at: datetime | None = None
        if scope.direct_scope is DirectScope.PROJECT:
            project_rows, project_ref, warning = await self._read(
                "projects", _PROJECT_DECLARED_FACTS_SQL, common, scope
            )
            source_refs.append(project_ref)
            if warning:
                warnings.append(warning)
            if project_rows:
                row = project_rows[0]
                state = str(row.get("state") or "").strip()
                target_date = row.get("target_date")
                if state or target_date is not None:
                    declared_project_state = state or None
                    declared_project_target_date = (
                        target_date if target_date is not None else None
                    )
                    declared_project_observed_at = self._datetime(
                        row.get("declared_updated_at"), as_of
                    )

        pull_requests = tuple(
            PullRequestFact(
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "Pull request"),
                state=str(row.get("state") or "unknown"),
                review_state=str(row["review_state"])
                if row.get("review_state")
                else None,
                changes_requested=int(row.get("changes_requested") or 0),
                merged=bool(row.get("merged")),
                observed_at=self._datetime(row.get("observed_at"), as_of),
                source_ref_id=pr_ref.ref_id,
                evidence_ref_ids=(),
                required=scope.direct_scope
                in {
                    DirectScope.ISSUE,
                    DirectScope.PROJECT,
                    DirectScope.WORK_UNIT,
                    DirectScope.PULL_REQUEST,
                },
            )
            for row in pr_rows
        )
        if scope.direct_scope is DirectScope.PULL_REQUEST and pull_requests:
            pr = pull_requests[0]
            declared = StatusFact(
                entity_type="pull_request",
                entity_id=pr.entity_id,
                display_label=pr.display_label,
                status=pr.state,
                observed_at=pr.observed_at,
                source_ref_id=pr.source_ref_id,
                evidence_ref_ids=pr.evidence_ref_ids,
            )

        gap_refs: list[SourceReference] = []
        if (
            scope.direct_scope is DirectScope.TEAM
            and not pull_requests
            and not deployment_rows
        ):
            # Codex finding (MEDIUM, 2026-08-02): the exclusion of
            # repository-co-located-but-not-canonically-owned facts is
            # correct (see the round-2 fix above), but a team whose
            # accessible repositories contain ONLY such unlinked activity
            # would otherwise resolve a clean READY/COMPLETE with zero
            # attributed facts and no disclosure -- indistinguishable from
            # a team with genuinely nothing happening. Distinguish the two:
            # if the team's repos have ANY pull-request or deployment
            # activity at all (checked here with NO attribution join, only
            # bare repository membership), the coverage gap must be
            # disclosed and the result must not read as a clean pass.
            try:
                async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                    unlinked_activity_rows = await query_dicts(
                        self._client, _TEAM_REPO_HAS_UNLINKED_ACTIVITY_SQL, common
                    )
            except Exception:
                # Codex round 4 (HIGH): a probe FAILURE must not collapse to
                # the same empty shape as "genuinely nothing found" -- that
                # silently restores the exact false-confidence
                # READY/COMPLETE state this probe exists to prevent. Treat
                # failure as unknown coverage, not clean coverage.
                gap_refs.append(
                    self._unavailable_ref("team_attribution_coverage", scope)
                )
                warnings.append(
                    "team attribution coverage probe failed; cannot rule "
                    "out unattributed repository activity, reporting "
                    "insufficient attribution coverage rather than a "
                    "genuinely empty team"
                )
            else:
                if unlinked_activity_rows:
                    gap_refs.append(
                        self._unavailable_ref("team_attribution_coverage", scope)
                    )
                    warnings.append(
                        "team-accessible repositories contain pull-request "
                        "or deployment activity that could not be "
                        "canonically attributed to this team; reporting "
                        "insufficient attribution coverage rather than a "
                        "genuinely empty team"
                    )
        acceptance_run_ids = {
            (str(row.get("repository_id") or ""), str(row.get("run_id") or ""))
            for row in ci_acceptance_rows
        }
        missing_classification_rows = [
            row
            for row in ci_rows
            if (str(row.get("repository_id") or ""), str(row.get("run_id") or ""))
            not in acceptance_run_ids
        ]
        if missing_classification_rows:
            warnings.append(
                "CI requirement classification is missing for one or more runs; green CI cannot prove required work ran."
            )
        ci_facts = [
            CIFact(
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "CI check"),
                conclusion=str(row.get("conclusion") or "unknown"),
                required=(
                    True
                    if row.get("requirement") == "required"
                    else False
                    if row.get("requirement") == "optional"
                    else None
                ),
                skipped_required_work=(
                    str(row.get("conclusion") or "").casefold() == "skipped"
                    if row.get("requirement") == "required"
                    else None
                ),
                observed_at=self._datetime(row.get("observed_at"), as_of),
                source_ref_id=ci_acceptance_ref.ref_id
                if ci_acceptance_ref is not None
                else ci_ref.ref_id,
                evidence_ref_ids=(),
            )
            for row in ci_acceptance_rows
        ]
        ci_facts.extend(
            CIFact(
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "CI run"),
                conclusion=str(row.get("conclusion") or "unknown"),
                required=None,
                skipped_required_work=None,
                observed_at=self._datetime(row.get("observed_at"), as_of),
                source_ref_id=ci_ref.ref_id,
                evidence_ref_ids=(),
            )
            for row in missing_classification_rows
        )

        return RawStatusSnapshot(
            declared=declared,
            children=children,
            declared_project_state=declared_project_state,
            declared_project_target_date=declared_project_target_date,
            declared_project_observed_at=declared_project_observed_at,
            blockers=tuple(
                StatusFact(
                    entity_type="issue",
                    entity_id=str(row.get("entity_id") or ""),
                    display_label=str(row.get("display_label") or "Work item"),
                    status=str(row.get("status") or "unknown"),
                    observed_at=self._datetime(row.get("observed_at"), as_of),
                    source_ref_id=blocker_ref.ref_id
                    if blocker_ref is not None
                    else "source:work-graph-unavailable",
                    evidence_ref_ids=(),
                    required=True,
                )
                for row in blocker_rows
            ),
            pull_requests=pull_requests,
            ci=tuple(ci_facts),
            deployments=tuple(
                DeploymentFact(
                    entity_id=str(row.get("entity_id") or ""),
                    display_label=str(row.get("display_label") or "Deployment"),
                    status=str(row.get("status") or "unknown"),
                    environment=str(row["environment"])
                    if row.get("environment")
                    else None,
                    required=True,
                    observed_at=self._datetime(row.get("observed_at"), as_of),
                    source_ref_id=deployment_ref.ref_id,
                    evidence_ref_ids=(),
                )
                for row in deployment_rows
            ),
            incidents=tuple(
                IncidentFact(
                    entity_id=str(row.get("entity_id") or ""),
                    display_label=str(row.get("display_label") or "Incident"),
                    status=str(row.get("status") or "unknown"),
                    active=bool(row.get("active")),
                    blocking=False,
                    observed_at=self._datetime(row.get("observed_at"), as_of),
                    source_ref_id=next(
                        ref.ref_id
                        for ref in source_refs
                        if ref.source_system == "incidents"
                    ),
                    evidence_ref_ids=(),
                )
                for row in incident_rows
            ),
            source_refs=tuple(
                source_refs
                + ([blocker_ref] if blocker_ref is not None else [])
                + gap_refs
            ),
            warnings=tuple(warnings),
            children_source_truncated=children_source_truncated,
            membership_source_truncated=membership_source_truncated,
            blockers_source_truncated=blockers_source_truncated,
            pull_requests_source_truncated=pull_requests_source_truncated,
            ci_source_truncated=ci_source_truncated,
            deployments_source_truncated=deployments_source_truncated,
            incidents_source_truncated=incidents_source_truncated,
        )

    async def change_summary(
        self,
        *,
        org_id: str,
        scope: DevScope,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
    ) -> RawChangeSummary:
        del comparison
        repositories = await self._authorized_repository_ids(
            org_id, scope, as_of=current.end
        )
        if not repositories:
            return RawChangeSummary(
                changes=(),
                source_refs=(self._unavailable_ref("authorized_repositories", scope),),
                warnings=("Observed-change scope was not widened.",),
            )
        entity_id = self._entity_id(scope)
        params = {
            "org_id": org_id,
            "repository_ids": repositories,
            "scope_type": scope.direct_scope.value,
            "entity_id": entity_id,
            "pr_number": self._pr_number(entity_id),
            "start": current.start.astimezone(UTC),
            "end": current.end.astimezone(UTC),
            "limit": min(limit, 100),
            "team_id": scope.team_ids[0]
            if scope.direct_scope is DirectScope.TEAM
            else "",
            # CHAOS-3303 round 3 (Codex HIGH): _TEAM_OWNED_WORK_ITEM_IDS_
            # SUBQUERY bounds its "latest compute" by as_of -- the natural
            # as_of for a change-summary window is its own end, the same
            # instant _authorized_repository_ids already resolved
            # `repositories` at (see the as_of=current.end call above).
            "as_of": current.end.astimezone(UTC),
        }
        transitions, transition_ref, transition_warning = await self._read(
            "work_items", _TRANSITIONS_SQL, params, scope
        )
        relationships, relationship_ref, relationship_warning = await self._read(
            "work_graph", _RELATIONSHIPS_SQL, params, scope
        )
        delivery_specs = (
            (
                "pull_requests",
                _PULL_REQUEST_CHANGES_SQL,
                ChangeCategory.PULL_REQUEST,
                "pull_request",
            ),
            ("reviews", _REVIEW_CHANGES_SQL, ChangeCategory.REVIEW, "review"),
            ("ci_runs", _CI_CHANGES_SQL, ChangeCategory.CI, "ci_run"),
            (
                "deployments",
                _DEPLOYMENT_CHANGES_SQL,
                ChangeCategory.DEPLOYMENT,
                "deployment",
            ),
            (
                "incidents",
                _INCIDENT_CHANGES_SQL,
                ChangeCategory.INCIDENT,
                "incident",
            ),
        )
        delivery_results = await asyncio.gather(
            *(
                self._read(source_system, sql, params, scope)
                for source_system, sql, _, _ in delivery_specs
            )
        )
        changes = [
            ObservedChange(
                change_id=self._change_id("status", row),
                category=ChangeCategory.STATUS,
                entity_type="issue",
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or "Work item"),
                before=str(row.get("from_status") or "unknown"),
                after=str(row.get("to_status") or "unknown"),
                observed_at=self._datetime(row.get("observed_at"), current.end),
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(transition_ref.ref_id,),
                evidence_ref_ids=(),
            )
            for row in transitions
        ]
        changes.extend(
            ObservedChange(
                change_id=str(row.get("change_id") or self._change_id("edge", row)),
                category=self._relationship_change_category(
                    str(row.get("edge_type") or "")
                ),
                entity_type=str(row.get("source_type") or "entity"),
                entity_id=str(row.get("source_id") or ""),
                display_label=(
                    f"{row.get('source_type')} {row.get('source_id')} "
                    f"{row.get('edge_type')} {row.get('target_type')} {row.get('target_id')}"
                ),
                before=None,
                after="present",
                observed_at=self._datetime(row.get("observed_at"), current.end),
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(
                    str(row.get("source_id") or ""),
                    str(row.get("edge_type") or "related_to"),
                    str(row.get("target_id") or ""),
                ),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(relationship_ref.ref_id,),
                evidence_ref_ids=(),
            )
            for row in relationships
        )
        source_refs = [transition_ref, relationship_ref]
        warnings = [
            warning
            for warning in (transition_warning, relationship_warning)
            if warning is not None
        ]
        for (
            (_, _, category, entity_type),
            (rows, source_ref, warning),
        ) in zip(delivery_specs, delivery_results, strict=True):
            source_refs.append(source_ref)
            if warning is not None:
                warnings.append(warning)
            changes.extend(
                self._delivery_changes(
                    rows,
                    category=category,
                    entity_type=entity_type,
                    source_ref_id=source_ref.ref_id,
                    fallback=current.end,
                )
            )
        changes.sort(key=self._observed_change_key)
        return RawChangeSummary(
            changes=tuple(changes[:limit]),
            source_refs=tuple(source_refs),
            warnings=tuple(warnings),
        )

    async def _read(
        self,
        source: str,
        sql: str,
        params: dict[str, Any],
        scope: DevScope,
    ) -> tuple[list[dict[str, Any]], SourceReference, str | None]:
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(self._client, sql, params)
        except Exception:
            return (
                [],
                self._unavailable_ref(source, scope),
                f"{source} source unavailable",
            )
        watermarks = [
            self._datetime(row.get("last_synced"), None)
            for row in rows
            if row.get("last_synced") is not None
        ]
        watermark = max(watermarks, default=None)
        ref = self._source_ref(source, scope, watermark)
        return rows, ref, None

    async def _bounded_read(
        self,
        source: str,
        sql: str,
        params: dict[str, Any],
        scope: DevScope,
        *,
        requested: int,
    ) -> tuple[list[dict[str, Any]], SourceReference, str | None, bool]:
        """``_read`` plus a limit+1 sentinel (CHAOS-3297 s2 round 5, codex
        MEDIUM): fetch one row beyond ``requested`` so truncation is
        detected from the source's own row count, never inferred from
        ``len(result) >= MAX_STATUS_ASSESSMENT_ITEMS`` -- that equality
        heuristic has two proven failure modes: it never fires when a
        query shares its LIMIT budget with another entity type sharing the
        row before a post-fetch split (rounds 2-3: _WORK_ITEMS_SQL,
        _WORK_UNIT_MEMBERS_SQL), and it fires on a false positive at
        exactly ``MAX_STATUS_ASSESSMENT_ITEMS`` legitimate, untruncated
        results (round 3). Trims back down to ``requested`` so every
        caller's row count is unaffected by the +1 probe.
        """
        rows, ref, warning = await self._read(
            source, sql, {**params, "limit": requested + 1}, scope
        )
        truncated = len(rows) > requested
        if truncated:
            rows = rows[:requested]
        return rows, ref, warning, truncated

    def _source_ref(
        self, source: str, scope: DevScope, watermark: datetime | None
    ) -> SourceReference:
        policy = self._policies.get(source)
        freshness = (
            policy.classify(watermark, now=self._now or datetime.now(UTC))
            if policy
            else FreshnessState.UNKNOWN
        )
        return SourceReference(
            ref_id=self._ref_id(source, scope, watermark),
            source_system=source,
            source_version=(
                f"{NATIVE_STATUS_SOURCE_VERSION}:"
                f"{policy.policy_version if policy else NATIVE_STATUS_QUERY_VERSION}"
            ),
            freshness=freshness,
            watermark=watermark,
            evidence_ref_ids=(),
        )

    def _unavailable_ref(self, source: str, scope: DevScope) -> SourceReference:
        return SourceReference(
            ref_id=self._ref_id(source, scope, None),
            source_system=source,
            source_version=NATIVE_STATUS_SOURCE_VERSION,
            freshness=FreshnessState.UNAVAILABLE,
            watermark=None,
            evidence_ref_ids=(),
        )

    @staticmethod
    def _work_item_facts(
        rows: list[dict[str, Any]],
        scope: DevScope,
        source_refs: list[SourceReference],
    ) -> tuple[StatusFact | None, tuple[StatusFact, ...]]:
        ref_id = next(
            (ref.ref_id for ref in source_refs if ref.source_system == "work_items"),
            "source:work-items-unavailable",
        )
        entity_id = ClickHouseStatusChangeSource._entity_id(scope)
        facts = tuple(
            StatusFact(
                entity_type="issue",
                entity_id=str(row.get("work_item_id") or ""),
                display_label=str(row.get("title") or "Work item"),
                status=str(row.get("status") or "unknown"),
                observed_at=ClickHouseStatusChangeSource._datetime(
                    row.get("updated_at"), scope.time_range.end
                ),
                source_ref_id=ref_id,
                evidence_ref_ids=(),
                # The provider-neutral v2 completion rule treats canonical direct
                # parent-child and project-work-item membership as required work.
                # Provider-specific labels, statuses, and issue-key prefixes never
                # change this policy.
                required=(
                    scope.direct_scope is DirectScope.PROJECT
                    or scope.direct_scope is DirectScope.WORK_UNIT
                    or str(row.get("parent_id") or "") == entity_id
                ),
            )
            for row in rows
        )
        declared = next((fact for fact in facts if fact.entity_id == entity_id), None)
        children = tuple(fact for fact in facts if fact is not declared)
        return declared, children

    @classmethod
    def _latest_ci_run_rows(cls, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Keep every row from the latest run for each repository and PR."""

        minimum = datetime.min.replace(tzinfo=UTC)
        latest: dict[tuple[str, int], tuple[datetime, datetime, str]] = {}
        for row in rows:
            order = (
                cls._datetime(row.get("observed_at"), minimum),
                cls._datetime(row.get("last_synced"), minimum),
                str(row.get("run_id") or ""),
            )
            scope = cls._ci_run_scope(row)
            if order > latest.get(scope, (minimum, minimum, "")):
                latest[scope] = order
        return [
            row
            for row in rows
            if str(row.get("run_id") or "") == latest[cls._ci_run_scope(row)][2]
        ]

    @staticmethod
    def _ci_run_scope(row: Mapping[str, Any]) -> tuple[str, int]:
        return (
            str(row.get("repository_id") or ""),
            int(row.get("pr_number") or 0),
        )

    @classmethod
    def _delivery_changes(
        cls,
        rows: list[dict[str, Any]],
        *,
        category: ChangeCategory,
        entity_type: str,
        source_ref_id: str,
        fallback: datetime,
    ) -> tuple[ObservedChange, ...]:
        return tuple(
            ObservedChange(
                change_id=str(
                    row.get("change_id")
                    or cls._change_id(
                        category.value,
                        {
                            "entity_id": row.get("entity_id"),
                            "after_value": row.get("after_value"),
                            "observed_at": row.get("observed_at"),
                        },
                    )
                ),
                category=category,
                entity_type=entity_type,
                entity_id=str(row.get("entity_id") or ""),
                display_label=str(row.get("display_label") or entity_type),
                before=(
                    str(row["before_value"])
                    if row.get("before_value") is not None
                    else None
                ),
                after=(
                    str(row["after_value"])
                    if row.get("after_value") is not None
                    else None
                ),
                observed_at=cls._datetime(row.get("observed_at"), fallback),
                claim_kind=(
                    ClaimKind.INFERRED
                    if str(row.get("relationship_source") or "").casefold()
                    == "heuristic"
                    else ClaimKind.OBSERVED
                ),
                relationship_chain=cls._delivery_relationship_chain(row, category),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(source_ref_id,),
                evidence_ref_ids=(),
                confidence=(
                    float(row["relationship_confidence"])
                    if row.get("relationship_confidence") is not None
                    else None
                ),
            )
            for row in rows
        )

    @staticmethod
    def _relationship_change_category(edge_type: str) -> ChangeCategory:
        normalized = edge_type.casefold()
        if normalized == "blocks":
            return ChangeCategory.BLOCKER
        if normalized in {"depends_on", "dependency", "is_blocked_by"}:
            return ChangeCategory.DEPENDENCY
        return ChangeCategory.RELATIONSHIP

    @staticmethod
    def _delivery_relationship_chain(
        row: Mapping[str, Any], category: ChangeCategory
    ) -> tuple[str, ...]:
        deployment_id = str(row.get("deployment_id") or "")
        incident_id = str(row.get("entity_id") or "")
        if category is ChangeCategory.INCIDENT and deployment_id and incident_id:
            return (deployment_id, "associated_with", incident_id)
        return ()

    @staticmethod
    def _observed_change_key(change: ObservedChange) -> tuple:
        return (
            change.observed_at,
            change.category.value,
            change.display_label.casefold(),
            change.change_id,
        )

    @staticmethod
    def _repository_ids(scope: DevScope) -> list[str]:
        return sorted(
            set(scope.repositories)
            | {
                ref.repository_id
                for ref in scope.entity_refs
                if ref.repository_id is not None
            }
        )

    @staticmethod
    def _entity_id(scope: DevScope) -> str:
        return scope.entity_refs[0].entity_id if scope.entity_refs else ""

    @staticmethod
    def _pr_number(entity_id: str) -> int:
        marker = "#pr"
        if marker not in entity_id:
            return 0
        try:
            return int(entity_id.rsplit(marker, 1)[1])
        except ValueError:
            return 0

    @staticmethod
    def _datetime(value: object, fallback: datetime | None) -> datetime:
        if isinstance(value, datetime):
            return (
                value.replace(tzinfo=UTC)
                if value.tzinfo is None
                else value.astimezone(UTC)
            )
        if value:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return (
                parsed.replace(tzinfo=UTC)
                if parsed.tzinfo is None
                else parsed.astimezone(UTC)
            )
        return fallback or datetime.now(UTC)

    @staticmethod
    def _ref_id(source: str, scope: DevScope, watermark: datetime | None) -> str:
        digest = hashlib.sha256(
            json.dumps(
                {
                    "source": source,
                    "scope": scope.model_dump(mode="json"),
                    "watermark": watermark.isoformat() if watermark else None,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()[:32]
        return f"status-source:{digest}"

    @staticmethod
    def _change_id(prefix: str, row: Mapping[str, object]) -> str:
        digest = hashlib.sha256(
            json.dumps(
                dict(row), sort_keys=True, default=str, separators=(",", ":")
            ).encode()
        ).hexdigest()[:32]
        return f"{prefix}:{digest}"
