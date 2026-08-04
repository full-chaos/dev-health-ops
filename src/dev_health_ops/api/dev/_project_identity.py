"""Shared provider-aware project identity CTE + match predicate.

Extracted out of ``native_status_change.py`` (CHAOS-3380 round 3, Codex
HIGH) so ``native_evidence.py`` can reuse the EXACT SAME predicate for its
own project-scoped ``work_items`` search/expand queries, instead of a
hand-copied variant that silently drifted from the one
``native_status_change.py`` actually uses (which is exactly what had
happened: ``native_evidence.py``'s ``work_items`` source compared
``project_id``/``project_key`` directly against the resolved entity id,
never through this identity join at all -- broken for any provider whose
catalog id isn't the raw value ``work_items`` carries, i.e. Jira since
CHAOS-3374 and GitLab since CHAOS-3380, not just GitLab).

A plain module with no dependency on either caller, specifically to avoid a
circular import: ``native_status_change.py`` already imports from
``native_evidence.py`` (``SourceFreshnessPolicy``,
``default_native_freshness_policies``), so ``native_evidence.py`` cannot
import identity helpers back out of ``native_status_change.py`` directly.
Both modules import from here instead; ``native_status_change.py``
re-exports these under its own historical private names
(``_PROJECT_IDENTITY_CTE`` / ``_project_identity_cte`` /
``_project_identity_match``) so every existing internal usage and test
assertion against ``native_status_change.<name>`` keeps working unchanged.
"""

from __future__ import annotations


def project_identity_cte(entity_param: str = "entity_id") -> str:
    """The ``project`` CTE: resolves one catalog row's provider + native key.

    ``entity_param`` is the ClickHouse query-parameter name (interpreted
    server-side by clickhouse-connect, never by Python string formatting)
    holding the id to look up -- ``native_status_change.py``'s own queries
    always use ``"entity_id"``; ``native_evidence.py``'s ``expand`` query
    uses ``"scope_entity_id"`` instead (a differently-named parameter for
    the SAME concept: the currently-authorized scope's committed entity).
    """
    return f"""project AS (
  SELECT any(provider) AS catalog_provider,
         any(ifNull(project_key, '')) AS catalog_project_key
  FROM projects FINAL
  WHERE org_id = {{org_id:String}} AND id = {{{entity_param}:String}} AND is_active = 1
  HAVING count() = 1
)"""


def project_identity_match(alias: str = "", entity_param: str = "entity_id") -> str:
    """Provider + native-key aware match against a work_items-shaped row.

    ``alias`` is the table prefix (e.g. ``"item."``, ``"blocked."``) for a
    joined reference; ``""`` for a bare, unaliased ``work_items`` FROM.
    ``entity_param`` must match whatever was passed to
    ``project_identity_cte`` for the SAME query.

    Three arms, all provider-guarded by the leading
    ``{alias}provider = catalog_provider``, so none of them can ever
    cross-match a different provider's row even on a coincidental
    id/key/path string collision:

    1. ``{alias}project_id = {{entity_param}}`` -- the raw-id space Linear's
       catalog id lives in (a Linear project's catalog id IS its own
       ``work_items.project_id``).
    2. ``{alias}project_key = catalog_project_key`` -- the native-key space
       Jira's catalog lives in (``providers/jira/normalize.py`` writes the
       raw Jira key onto ``work_items.project_key``; the catalog's
       ``project_key`` is that same raw key).
    3. ``{alias}project_id = catalog_project_key`` -- GitLab's compatibility
       arm (CHAOS-3380 round 3): GitLab's catalog id is an opaque, prefixed,
       IMMUTABLE numeric id (never equal to any raw ``work_items`` column),
       but ``work_items.project_id`` carries the raw, MUTABLE path for
       every GitLab row regardless of when it was synced, and the catalog's
       ``project_key`` carries that SAME current path -- so this arm is what
       makes both historical (pre-CHAOS-3380) and freshly-synced GitLab rows
       resolve identically, with no dependency on when a row was ingested.

       ACCEPTED RESIDUAL RISK (CHAOS-3380 round 4, Codex HIGH -- draw-the-line
       closure, not a fix): this arm matches on a MUTABLE value, so it can
       cross-attribute. The window: project A is renamed/transferred off a
       path; project B later claims that exact freed path; an OLD work item
       from A, never resynced since the rename (so its own ``project_id``
       still carries the vacated path), now satisfies this arm against B's
       CURRENT ``project_key`` and reads as B's history. Requires a specific
       provider-side event sequence (rename + reclaim + a stale, never-
       resynced row) -- not reachable from any app write path, and not
       something a request handler can trigger.
       ``ask_dev_project_subject_oracle.py`` DISCLOSES a row that resolves
       ONLY through this arm as ``kind="path_match_unverified"`` -- but that
       is an offline diagnostic, read by nobody at query time. PRODUCTION
       QUERIES (this predicate, live, inside every project-scoped fact arm
       and ``PROJECT_REPOSITORIES_SQL``) ACCEPT such a match same as any
       other; there is no query-time equivalent of the oracle's flag, and
       adding one by failing closed on a path-only match would disable
       GitLab project resolution entirely -- every GitLab work item is
       path-only today, so "fail closed on path-only" and "fail closed on
       GitLab" are the same statement. Two Codex rounds already ruled out
       the alternatives at the query layer; this is not a candidate for a
       fourth pass here. CHAOS-3383 (immutable numeric id threaded through
       ``work_items.project_id`` itself, plus a backfill of existing rows)
       is the filed, structural fix that retires this arm outright by
       removing the mutable value it depends on; until it lands, this
       residual is accepted and pinned by
       ``tests/test_ask_dev_gitlab_project_subject_live.py::
       test_path_reuse_cross_attribution_is_a_known_residual_until_chaos_3383``,
       whose assertion is written to FAIL once CHAOS-3383 closes this gap --
       forcing that implementer to consciously retire the pin rather than
       leave a stale one behind.
    """
    return (
        "ifNull(catalog_provider, '') != ''"
        f" AND {alias}provider = catalog_provider"
        f" AND ({alias}project_id = {{{entity_param}:String}}"
        f" OR (catalog_project_key != '' AND {alias}project_key = catalog_project_key)"
        f" OR (catalog_project_key != '' AND {alias}project_id = catalog_project_key))"
    )
