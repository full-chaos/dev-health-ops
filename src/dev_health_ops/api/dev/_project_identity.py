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
    """
    return (
        "ifNull(catalog_provider, '') != ''"
        f" AND {alias}provider = catalog_provider"
        f" AND ({alias}project_id = {{{entity_param}:String}}"
        f" OR (catalog_project_key != '' AND {alias}project_key = catalog_project_key)"
        f" OR (catalog_project_key != '' AND {alias}project_id = catalog_project_key))"
    )
