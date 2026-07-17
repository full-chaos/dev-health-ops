"""One-active-owner resolution for external customer-push ingestion (CHAOS-2695).

Single source of truth for CC5's per-provider instance matching (brief
decision 12 / master-spec CC5): the SAME predicates run at both enforcement
points — registration time (``api/admin/routers/customer_push.py``, 409) and
batch-accept time (``api/external_ingest/router.py``, 403). fullchaos_sync
ownership is DERIVED at read time from ``integrations``/``integration_sources``
(native sync's existing tables), never mirrored into
``external_ingest_sources`` (brief decision 11) — so a managed sync connected
AFTER a customer_push registration is still caught here on the next accept.

``instance`` matching is per-provider because ``integration_sources.external_id``
is NOT uniformly the human-readable name (master-spec CC5): GitHub stores
``owner/repo`` there, GitLab stores the NUMERIC project id (the path lives in
``full_name`` / ``metadata_.path_with_namespace``), and Linear stores a team
UUID or the literal ``"linear"`` org-wide placeholder that owns ALL teams.

See docs/architecture/external-ingest-idempotency-ownership.md.
"""

from __future__ import annotations

from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.ingest_auth import IngestSource, IngestSourceMode
from dev_health_ops.models.integrations import Integration, IntegrationSource

EffectiveMode = Literal["fullchaos_sync", "customer_push", "disabled", "unclaimed"]

__all__ = [
    "EffectiveMode",
    "find_active_managed_owner",
    "find_matching_managed_sources",
    "linear_is_org_wide_placeholder",
    "matches_instance",
    "resolve_effective_mode",
]


def linear_is_org_wide_placeholder(source: IntegrationSource) -> bool:
    """A Linear integration source that owns the WHOLE workspace (all teams).

    ``api/admin/routers/sync.py`` creates such rows with either an explicit
    ``org_wide_placeholder`` metadata flag or the literal external_id
    ``"linear"``; both spellings must match any Linear instance string.
    """
    metadata = source.metadata_ or {}
    if metadata.get("org_wide_placeholder") is True:
        return True
    return (source.external_id or "").strip().lower() == "linear"


def _candidate_set(*values: object) -> set[str]:
    return {v.strip().lower() for v in values if isinstance(v, str) and v.strip()}


def matches_instance(system: str, instance: str, source: IntegrationSource) -> bool:
    """CC5 per-provider matching (see docs/architecture/customer-push-authz.md).

    Comparisons are case-insensitive on BOTH sides (adversarial-review
    finding): GitHub repo full names, GitLab paths, and Jira/Linear keys are
    case-insensitive identifiers on their providers (no two managed entities
    can differ only by case), and sync stores them as the provider API
    happened to return them — an exact match would let a managed
    ``Acme/API`` row silently fail to block a customer-push ``acme/api``
    registration/accept, defeating one-active-owner.
    """
    inst = instance.strip().lower()
    if not inst:
        return False
    if system in ("github", "jira"):
        return inst in _candidate_set(source.external_id, source.full_name)
    if system == "gitlab":
        path_with_namespace = (source.metadata_ or {}).get("path_with_namespace")
        return inst in _candidate_set(
            source.full_name, path_with_namespace, source.external_id
        )
    if system == "linear":
        if linear_is_org_wide_placeholder(source):
            return True
        return inst in _candidate_set(source.external_id, source.full_name, source.name)
    return False


async def find_matching_managed_sources(
    session: AsyncSession,
    *,
    org_id: str,
    system: str,
    instance: str,
    entity_family: str = "legacy",
) -> list[tuple[IntegrationSource, bool]]:
    """All managed ``integration_sources`` rows matching (org, system, instance).

    Returns ``(source, parent_integration_is_active)`` pairs. ``custom``
    systems have no managed equivalent and always return ``[]``. The
    candidate query is org+provider-scoped with ``func.lower(...)`` on the
    provider (nearby sync-creation paths don't enforce lowercase provider
    values, so a mixed-case managed row must still be found); the
    per-provider instance predicate then filters in Python because GitLab's
    metadata-path and Linear's placeholder rules aren't expressible as one
    indexed SQL predicate.
    """
    if system == "custom":
        return []
    if entity_family == "operational" and system in {"github", "gitlab"}:
        return []

    candidate_rows = (
        await session.execute(
            select(IntegrationSource, Integration.is_active)
            .join(Integration, IntegrationSource.integration_id == Integration.id)
            .where(
                IntegrationSource.org_id == org_id,
                func.lower(IntegrationSource.provider) == system,
            )
        )
    ).all()
    return [
        (source, bool(integration_is_active))
        for source, integration_is_active in candidate_rows
        if matches_instance(system, instance, source)
    ]


async def find_active_managed_owner(
    session: AsyncSession,
    *,
    org_id: str,
    system: str,
    instance: str,
    entity_family: str = "legacy",
) -> IntegrationSource | None:
    """The managed source that ACTIVELY owns this instance, if any.

    "Active ownership" = the source row is ``is_enabled`` AND its parent
    ``Integration`` is ``is_active`` (post-critique CC5/CC14) — a source row
    left enabled under a since-deactivated integration no longer counts.
    """
    matches = await find_matching_managed_sources(
        session,
        org_id=org_id,
        system=system,
        instance=instance,
        entity_family=entity_family,
    )
    return next(
        (
            source
            for source, integration_is_active in matches
            if source.is_enabled and integration_is_active
        ),
        None,
    )


async def resolve_effective_mode(
    session: AsyncSession,
    *,
    org_id: str,
    system: str,
    instance: str,
    entity_family: str = "legacy",
) -> EffectiveMode:
    """Resolve the single active ingestion owner for (org, system, instance).

    Precedence (brief §6.5, post-critique CC5/CC14):

    1. An explicit ``external_ingest_sources`` row wins — ``disabled`` if
       the row is disabled (or mode=disabled), its mode otherwise — EXCEPT
       that a ``customer_push`` row is still overridden by a managed source
       that ACTIVELY owns the same instance (defense-in-depth, brief
       decision 12: nothing stops ``api/admin/routers/sync.py`` from
       connecting managed sync to the same repo AFTER registration).
    2. With no explicit row, an actively-owning managed source implies
       ``fullchaos_sync`` (legacy/native ownership, never registered here).
    3. Otherwise ``unclaimed``.

    On the data plane this runs after ``require_matching_source`` already
    bound the token to a registered write-eligible source, so ``unclaimed``/
    ``disabled`` are unreachable there in practice — the load-bearing outcome
    at accept time is the step-1 fullchaos_sync override. Callers map
    outcomes to errors per brief §7 (403 ``source_not_registered`` /
    ``source_disabled`` / ``source_owned_by_fullchaos_sync``).
    """
    explicit = (
        await session.execute(
            select(IngestSource).where(
                IngestSource.org_id == org_id,
                IngestSource.system == system,
                IngestSource.instance == instance,
                IngestSource.entity_family == entity_family,
            )
        )
    ).scalar_one_or_none()

    if explicit is not None:
        if not explicit.enabled or explicit.mode == IngestSourceMode.DISABLED.value:
            return "disabled"
        if explicit.mode == IngestSourceMode.FULLCHAOS_SYNC.value:
            return "fullchaos_sync"
        owner = await find_active_managed_owner(
            session,
            org_id=org_id,
            system=system,
            instance=instance,
            entity_family=entity_family,
        )
        if owner is not None:
            return "fullchaos_sync"
        return "customer_push"

    owner = await find_active_managed_owner(
        session,
        org_id=org_id,
        system=system,
        instance=instance,
        entity_family=entity_family,
    )
    if owner is not None:
        return "fullchaos_sync"
    return "unclaimed"
