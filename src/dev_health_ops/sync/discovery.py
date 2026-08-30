"""Source discovery service for the Integration model.

Populates ``integration_sources`` rows by reusing the existing provider
discovery in ``dev_health_ops.discovery.repos``.  No ``SyncConfiguration``
child rows are created — that is the whole point of this layer.

Stale handling policy
---------------------
Sources that are not returned by the latest discovery run are **not** deleted
and are **not** automatically disabled.  Their ``last_seen_at`` timestamp
simply stays old.  The planner (CHAOS-2511) already filters on ``is_enabled``
and can apply its own staleness heuristics.  Auto-disabling on absence would
be a destructive action that requires explicit operator intent; we document the
choice here rather than silently removing access.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from dev_health_ops.discovery.repos import discover_repos_for_config
from dev_health_ops.models.integrations import Integration, IntegrationSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_credentials(integration: Integration) -> dict[str, Any]:
    """Resolve credentials for *integration* into a flat mapping.

    Mirrors sync source discovery used by fan-out planning:
    - If the integration has a ``credential_id``, load the
      ``IntegrationCredential`` row and decrypt it via
      ``workers.task_utils._credential_mapping``.
    - Otherwise return an empty dict (anonymous / env-var-based auth).

    The session is not passed here because credential loading is synchronous
    and the caller already holds a sync session.
    """
    if integration.credential_id is None:
        return {}

    # Lazy import to avoid circular deps and heavy imports at module load time.
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import IntegrationCredential
    from dev_health_ops.workers.task_utils import _credential_mapping

    with get_postgres_session_sync() as cred_session:
        credential = (
            cred_session.query(IntegrationCredential)
            .filter(
                IntegrationCredential.id == integration.credential_id,
                IntegrationCredential.org_id == integration.org_id,
            )
            .one_or_none()
        )
    if credential is None:
        return {}
    return _credential_mapping(credential)


def _build_config_shim(integration: Integration, planner_config: Any | None) -> Any:
    """Build a minimal config-shim that ``discover_repos_for_config`` accepts.

    ``discover_repos_for_config`` expects an object with:
    - ``.provider`` (str)
    - ``.sync_options`` (dict | None)
    - ``.org_id`` (str | None) -- used only by the jira branch, to scope its
      per-host rate-limit gate to this org (codex review, CHAOS-4584 round 1
      P2: without it every org sharing a Jira host collapses onto the same
      ``rate_limit:jira:_:<host>`` key, so one org's 429 backoff throttles
      every other org on that host).

    The ``Integration.config`` column carries the same options that
    ``SyncConfiguration.sync_options`` used to carry (owner, search,
    all_repos, group, gitlab_url, etc.) -- normally identical to
    *planner_config*'s at creation time. For jira specifically, prefer
    *planner_config*'s CURRENT ``sync_options`` when available: codex review
    (CHAOS-4584 round 2, P1) -- ``PATCH /sync-configs/{id}`` only writes
    ``SyncConfiguration.sync_options`` (``Integration.config`` is kept in
    sync only for github, as a provider-specific special case), so an
    operator changing an explicitly-scoped jira project's ``project_key``
    would otherwise have discovery keep filtering by the stale value.
    """

    class _Shim:
        provider: str
        sync_options: dict[str, Any]
        org_id: str

    shim = _Shim()
    shim.provider = integration.provider or ""
    shim.sync_options = dict(integration.config or {})
    shim.org_id = integration.org_id
    if shim.provider == "jira" and planner_config is not None:
        shim.sync_options = dict(getattr(planner_config, "sync_options", None) or {})
    return shim


def _map_github_tuple(
    owner: str,
    repo_name: str,
    *,
    org_id: str,
    integration_id: uuid.UUID,
) -> dict[str, Any]:
    """Map a GitHub discovery tuple ``(owner, repo_name)`` to source fields."""
    full_name = f"{owner}/{repo_name}"
    return {
        "org_id": org_id,
        "integration_id": integration_id,
        "provider": "github",
        "source_type": "repository",
        "external_id": full_name,
        "name": repo_name,
        "full_name": full_name,
        "metadata_": {"owner": owner},
    }


def _map_gitlab_tuple(
    project_id: str,
    path_with_namespace: str,
    *,
    org_id: str,
    integration_id: uuid.UUID,
) -> dict[str, Any]:
    """Map a GitLab discovery tuple ``(project_id, path_with_namespace)`` to source fields.

    ``external_id`` is the numeric project_id (canonical GitLab identifier).
    ``full_name`` is the path_with_namespace slug.
    ``name`` is the last path segment (project name without group prefix).
    """
    name = path_with_namespace.rsplit("/", 1)[-1] if path_with_namespace else project_id
    return {
        "org_id": org_id,
        "integration_id": integration_id,
        "provider": "gitlab",
        "source_type": "project",
        "external_id": project_id,
        "name": name,
        "full_name": path_with_namespace,
        "metadata_": {"path_with_namespace": path_with_namespace},
    }


def _map_jira_tuple(
    project_key: str,
    project_name: str,
    project_type_key: str,
    *,
    org_id: str,
    integration_id: uuid.UUID,
    planner_managed_sync_config_id: str | None,
) -> dict[str, Any]:
    """Map a Jira discovery tuple ``(key, name, project_type_key)`` to source
    fields, shaped like the pre-existing hand-inserted proof rows (SUP/OPS,
    org 70d529e0): ``source_type='project'``, ``metadata.planner_managed_sync_config_id``
    set to the owning planner-managed config so the planner
    (``sync/trigger_routing.py::_planner_scoped_source_ids``) picks the row up."""
    metadata: dict[str, Any] = {"project_type_key": project_type_key}
    if planner_managed_sync_config_id:
        metadata["planner_managed_sync_config_id"] = planner_managed_sync_config_id
    return {
        "org_id": org_id,
        "integration_id": integration_id,
        "provider": "jira",
        "source_type": "project",
        "external_id": project_key,
        "name": project_name,
        "full_name": project_key,
        "metadata_": metadata,
    }


def _tuples_to_source_dicts(
    provider: str,
    tuples: list[tuple[str, ...]],
    *,
    org_id: str,
    integration_id: uuid.UUID,
    planner_managed_sync_config_id: str | None = None,
) -> list[dict[str, Any]]:
    """Convert raw discovery tuples to IntegrationSource field dicts."""
    result: list[dict[str, Any]] = []
    for t in tuples:
        if len(t) < 2:
            continue
        if provider == "github":
            result.append(
                _map_github_tuple(
                    t[0], t[1], org_id=org_id, integration_id=integration_id
                )
            )
        elif provider == "gitlab":
            result.append(
                _map_gitlab_tuple(
                    t[0], t[1], org_id=org_id, integration_id=integration_id
                )
            )
        elif provider == "jira":
            result.append(
                _map_jira_tuple(
                    t[0],
                    t[1],
                    t[2] if len(t) > 2 else "",
                    org_id=org_id,
                    integration_id=integration_id,
                    planner_managed_sync_config_id=planner_managed_sync_config_id,
                )
            )
    return result


def _planner_managed_config_for_integration(
    session: Session, integration_id: uuid.UUID
) -> Any | None:
    """The owning planner-managed parent ``SyncConfiguration`` for
    *integration_id*, or ``None``. Every integration has at most one such
    parent (``_assert_single_planner_parent_for_integration`` in
    ``api/admin/routers/sync.py`` enforces the invariant at write time)."""
    from dev_health_ops.models.settings import SyncConfiguration

    return (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.integration_id == integration_id,
            SyncConfiguration.planner_managed.is_(True),
            SyncConfiguration.parent_id.is_(None),
        )
        .one_or_none()
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def discover_sources_for_integration(
    session: Session,
    integration_id: uuid.UUID,
    *,
    auto_enable: bool = True,
) -> list[IntegrationSource]:
    """Discover provider sources for *integration_id* and upsert them.

    Calls the existing ``discover_repos_for_config`` provider discovery and
    writes the results into ``integration_sources`` rows keyed by the unique
    constraint ``(org_id, integration_id, provider, external_id)``.

    On re-discovery:
    - ``last_seen_at``, ``name``, and ``full_name`` are updated.
    - ``metadata_`` is merged: fresh discovery values win, while keys the
      discovery payload lacks (including ``planner_managed_sync_config_id``)
      are preserved.
    - ``is_enabled`` is **not** changed for existing rows (preserves operator
      intent).  Only brand-new sources get ``is_enabled = auto_enable``.
    - ``discovered_at`` is **not** changed for existing rows.

    No ``SyncConfiguration`` rows are created.

    Args:
        session: Synchronous SQLAlchemy session.
        integration_id: UUID of the Integration to discover sources for.
        auto_enable: Whether newly discovered sources are enabled by default.

    Returns:
        List of upserted ``IntegrationSource`` instances (all sources seen in
        this discovery run, both new and updated).
    """

    integration = session.get(Integration, integration_id)
    if integration is None:
        raise ValueError(f"Integration not found: {integration_id}")

    provider = (integration.provider or "").lower()
    planner_config = (
        _planner_managed_config_for_integration(session, integration_id)
        if provider == "jira"
        else None
    )
    planner_managed_sync_config_id = (
        str(planner_config.id) if planner_config is not None else None
    )

    config_shim = _build_config_shim(integration, planner_config)
    credentials = _resolve_credentials(integration)

    raw_tuples: list[tuple[str, ...]] = discover_repos_for_config(
        config_shim, credentials
    )

    source_dicts = _tuples_to_source_dicts(
        provider,
        raw_tuples,
        org_id=integration.org_id,
        integration_id=integration_id,
        planner_managed_sync_config_id=planner_managed_sync_config_id,
    )

    now = _now_utc()
    upserted: list[IntegrationSource] = []
    created_count = 0
    created_external_ids_lower: set[str] = set()
    discovered_external_ids_lower = {
        str(fields["external_id"]).lower() for fields in source_dicts
    }

    # codex review (CHAOS-4584 round 1, P2): isolate the upsert loop in a
    # SAVEPOINT so a DB failure partway through (e.g. a concurrent
    # unique-key race) only unwinds this savepoint -- never poisons the
    # caller's outer transaction. A caller invoking this via
    # ``AsyncSession.run_sync`` inside a "best-effort, must not fail"
    # request path (create_sync_config) can then catch the propagated
    # exception and continue using its session normally afterward.
    with session.begin_nested():
        for fields in source_dicts:
            external_id = fields["external_id"]

            base_query = session.query(IntegrationSource).filter(
                IntegrationSource.org_id == integration.org_id,
                IntegrationSource.integration_id == integration_id,
                IntegrationSource.provider == fields["provider"],
            )
            if provider == "jira":
                # codex review (CHAOS-4584 round 2, P1): Jira project keys
                # are case-insensitive server-side (always canonicalized to
                # uppercase in API responses), but an explicitly-scoped
                # config's source can carry whatever casing the operator
                # originally typed into ``project_key``
                # (``_non_git_source_rows`` stores it verbatim). Matching
                # case-insensitively here is what makes rediscovery update
                # that SAME row instead of inserting a case-variant
                # duplicate that then double-schedules the project.
                from sqlalchemy import func

                candidates = (
                    base_query.filter(
                        func.lower(IntegrationSource.external_id) == external_id.lower()
                    )
                    .order_by(
                        IntegrationSource.discovered_at.asc(),
                        IntegrationSource.id.asc(),
                    )
                    .all()
                )
                existing = None
                if candidates:
                    exact = [c for c in candidates if c.external_id == external_id]
                    existing = exact[0] if exact else candidates[0]
                    # codex review (CHAOS-4584 round 3, P1): a pre-existing
                    # case-variant pair (e.g. "eng" and "ENG") would make
                    # this filter match more than one row -- self-repair by
                    # folding every OTHER match into the surviving row
                    # instead of ever calling one_or_none() (which would
                    # raise MultipleResultsFound and 503 every future
                    # discovery run for this integration).
                    for dup in candidates:
                        if dup is existing:
                            continue
                        dup.is_enabled = False
                        dup.metadata_ = {
                            **(dup.metadata_ or {}),
                            "duplicate_of_external_id": existing.external_id,
                        }
            else:
                existing = base_query.filter(
                    IntegrationSource.external_id == external_id
                ).one_or_none()

            if existing is not None:
                # Update mutable fields; preserve is_enabled and discovered_at.
                existing.last_seen_at = now
                existing.name = fields["name"]
                existing.full_name = fields["full_name"]
                merged_metadata = {
                    **(existing.metadata_ or {}),
                    **fields["metadata_"],
                }
                if provider == "jira" and merged_metadata.pop(
                    "superseded_by_scope_change", None
                ):
                    # codex review (CHAOS-4584 round 4, P1): the project this
                    # row was superseded FOR was itself only a system-driven
                    # scope-change disable (never an operator's own), so if
                    # discovery just reconfirmed this external_id as the
                    # CURRENT scope again (e.g. an operator reverted
                    # project_key NEW -> OLD), re-enable it -- otherwise
                    # OLD<->NEW toggling would leave both rows disabled
                    # forever, producing zero sync units.
                    existing.is_enabled = True
                existing.metadata_ = merged_metadata
                upserted.append(existing)
            else:
                source = IntegrationSource(
                    org_id=fields["org_id"],
                    integration_id=fields["integration_id"],
                    provider=fields["provider"],
                    source_type=fields["source_type"],
                    external_id=external_id,
                    name=fields["name"],
                    full_name=fields["full_name"],
                    metadata_=fields["metadata_"],
                    is_enabled=auto_enable,
                    discovered_at=now,
                    last_seen_at=now,
                )
                session.add(source)
                upserted.append(source)
                created_count += 1
                created_external_ids_lower.add(external_id.lower())

        if provider == "jira" and planner_managed_sync_config_id:
            _supersede_stale_scoped_jira_sources(
                session,
                integration,
                config_shim,
                planner_managed_sync_config_id,
                discovered_external_ids_lower,
            )

        session.flush()

    if provider == "jira":
        existing_count = len(source_dicts) - created_count
        _record_jira_project_discovery(
            org_id=integration.org_id,
            integration_id=integration_id,
            discovered_count=len(source_dicts),
            created_count=created_count,
            existing_count=existing_count,
            has_planner_tag=planner_managed_sync_config_id is not None,
        )
        # codex review (CHAOS-4584 round 2, P1): the repo-limit cap must
        # apply to EVERY discovery entry point, not just config-creation
        # time -- enforcing it here means create_sync_config's inline
        # discovery call and the standalone
        # POST /integrations/{id}/discover endpoint both get it automatically.
        _rebalance_jira_sources_against_repo_limit(
            session,
            integration,
            created_external_ids_lower=created_external_ids_lower,
            discovered_external_ids_lower=discovered_external_ids_lower,
        )

    return upserted


def _supersede_stale_scoped_jira_sources(
    session: Session,
    integration: Integration,
    config_shim: Any,
    planner_managed_sync_config_id: str,
    discovered_external_ids_lower: set[str],
) -> None:
    """When an integration is explicitly scoped to one project
    (``sync_options.project_key``/``project_id``), disable any OTHER
    enabled source this same planner-managed config tagged that discovery
    did NOT just return (codex review, CHAOS-4584 round 3 P1).

    Without this, changing an explicitly-scoped config's project via
    ``PATCH /sync-configs/{id}`` (``project_key=OLD`` -> ``NEW``) leaves
    ``OLD`` enabled forever -- ``trigger_routing.py::_planner_scoped_source_ids``
    tags both rows to the same config, so the planner keeps syncing the
    project the operator just moved away from.

    Deliberately NOT applied to the "discover everything, no explicit
    scope" case: this module's documented stale-handling policy (top of
    file) is that a project temporarily absent from one discovery run is
    never auto-disabled there, and that must stay true. Only an explicit,
    single-project scope makes "not the current scope" a durable,
    operator-driven fact rather than a possibly-transient API omission.

    Also does nothing when *discovered_external_ids_lower* is empty (codex
    review, CHAOS-4584 round 4 P1): a genuinely EMPTY result set here is
    indistinguishable from a transient credential/API failure -- discovery
    already returns ``[]`` for an unresolvable credential (CHAOS-4584's own
    contract), so treating "found nothing" as "confirmed scope change" would
    silently zero out a previously-working, explicitly-scoped sync on a
    passing credential hiccup. Only a run that returned at least one real
    result is trusted enough to supersede anything.
    """
    sync_options = getattr(config_shim, "sync_options", None) or {}
    if not (sync_options.get("project_key") or sync_options.get("project_id")):
        return
    if not discovered_external_ids_lower:
        return

    enabled_sources = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.org_id == integration.org_id,
            IntegrationSource.integration_id == integration.id,
            IntegrationSource.provider == "jira",
            IntegrationSource.is_enabled.is_(True),
        )
        .all()
    )
    superseded = []
    for source in enabled_sources:
        if (source.metadata_ or {}).get(
            "planner_managed_sync_config_id"
        ) != planner_managed_sync_config_id:
            continue
        if source.external_id.lower() in discovered_external_ids_lower:
            continue
        source.is_enabled = False
        source.metadata_ = {
            **(source.metadata_ or {}),
            "superseded_by_scope_change": True,
        }
        superseded.append(source)

    if superseded:
        from dev_health_ops.metrics.prometheus import JIRA_PROJECT_DISCOVERY_TOTAL

        JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="superseded_by_scope_change").inc(
            len(superseded)
        )
        logger.info(
            "jira_project_discovery_superseded_by_scope_change",
            extra={
                "org_id": integration.org_id,
                "integration_id": str(integration.id),
                "superseded_count": len(superseded),
                "superseded_external_ids": [s.external_id for s in superseded],
            },
        )


def _acquire_repo_limit_lock(session: Session, org_id: str) -> None:
    """Sync-session mirror of ``api/admin/routers/sync.py``'s
    ``_acquire_repo_limit_create_lock`` -- a ``pg_advisory_xact_lock``
    scoped to *org_id*, held for the remainder of the CALLING transaction
    (released automatically at commit/rollback, never explicitly). No-op on
    a non-postgres backend (e.g. the sqlite-backed unit tests)."""
    import uuid as _uuid

    from sqlalchemy import text

    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    try:
        org_int = _uuid.UUID(org_id).int
    except ValueError:
        org_int = _uuid.uuid5(_uuid.NAMESPACE_URL, org_id).int
    lock_key = org_int & ((1 << 63) - 1)
    session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key}
    )


def _active_repo_usage_count_for_limit(session: Session, org_id: str) -> int:
    """Sync-session mirror of ``api/admin/routers/sync.py``'s async
    ``_active_repo_usage_count_for_limit`` -- same org-wide "legacy active
    configs + enabled planner-managed sources" count, needed here because
    this module only ever holds a synchronous ``Session``."""
    from dev_health_ops.models.settings import SyncConfiguration

    active_configs = (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.org_id == org_id,
            SyncConfiguration.is_active.is_(True),
        )
        .all()
    )
    planner_parent_ids = {
        config.id
        for config in active_configs
        if config.parent_id is None
        and bool(config.planner_managed)
        and config.integration_id is not None
    }
    planner_integration_ids = {
        config.integration_id
        for config in active_configs
        if config.id in planner_parent_ids
    }
    legacy_count = sum(
        1 for config in active_configs if config.id not in planner_parent_ids
    )
    if not planner_integration_ids:
        return legacy_count

    source_count = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.org_id == org_id,
            IntegrationSource.integration_id.in_(planner_integration_ids),
            IntegrationSource.is_enabled.is_(True),
        )
        .count()
    )
    return legacy_count + int(source_count or 0)


_CAP_MARKER_KEY = "capped_by_repo_limit"


def _rebalance_jira_sources_against_repo_limit(
    session: Session,
    integration: Integration,
    *,
    created_external_ids_lower: set[str],
    discovered_external_ids_lower: set[str],
) -> None:
    """Keep this jira integration's enabled source count within the org's
    ``max_repos`` allowance after a discovery run (codex review, CHAOS-4584
    round 1 P1, round 2 P1/P2, round 3 P1/P2).

    Only ever touches sources on *integration* -- never another provider's
    or another integration's rows.

    Capping prefers sources CREATED in this very discovery run
    (*created_external_ids_lower*) over ones that already existed and were
    already enabled: an org already at its limit must not have real,
    already-relied-upon sources silently disabled just because THIS run
    also discovered new ones (round 3 P1) -- that would violate this
    module's own "is_enabled is never changed for an existing row" contract
    in spirit even though the row being touched here is a different one.

    Disabling is marked with ``metadata_.capped_by_repo_limit`` so a later
    run -- once the org's allowance grows (a tier change, a higher
    ``limits_override``, or other sources on the org being disabled) -- can
    tell a cap-disabled row apart from one an operator deliberately
    disabled, and re-enable it (round 2 P2). Recovery only ever considers a
    row whose external_id was actually RETURNED by this discovery run
    (*discovered_external_ids_lower*, round 3 P2) -- a project Jira no
    longer reports (deleted, credential lost access) or the org's tier
    later downgraded past must never be silently re-enabled just because
    some OTHER project's headroom opened up.

    Serializes on the org's repo-limit advisory lock before counting or
    capping (codex review, CHAOS-4584 round 4 P1): two concurrent Jira
    discovery runs for DIFFERENT integrations in the same org could
    otherwise both count the org's pre-run usage before either's insert is
    visible, both pass the check, and together commit an over-limit source
    set. The same lock key `create_sync_config`'s repo-limit preflight
    already uses -- Postgres advisory locks are per-session-reentrant, so a
    caller that already holds it (creation-time discovery, still inside
    that same request's transaction) pays no extra cost.
    """
    from dev_health_ops.api.services.licensing import TierLimitService
    from dev_health_ops.metrics.prometheus import JIRA_PROJECT_DISCOVERY_TOTAL

    org_id = integration.org_id
    _acquire_repo_limit_lock(session, org_id)
    max_repos = TierLimitService(session).get_limit(uuid.UUID(org_id), "max_repos")

    if max_repos is not None:
        overflow = _active_repo_usage_count_for_limit(session, org_id) - int(max_repos)
        if overflow > 0:
            enabled_sources = (
                session.query(IntegrationSource)
                .filter(
                    IntegrationSource.org_id == org_id,
                    IntegrationSource.integration_id == integration.id,
                    IntegrationSource.is_enabled.is_(True),
                )
                .order_by(IntegrationSource.external_id.desc())
                .all()
            )
            newly_created = [
                s
                for s in enabled_sources
                if s.external_id.lower() in created_external_ids_lower
            ]
            pre_existing = [
                s
                for s in enabled_sources
                if s.external_id.lower() not in created_external_ids_lower
            ]
            capped = (newly_created + pre_existing)[:overflow]
            for source in capped:
                source.is_enabled = False
                source.metadata_ = {
                    **(source.metadata_ or {}),
                    _CAP_MARKER_KEY: True,
                }
            if capped:
                session.flush()
                JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="capped_by_repo_limit").inc(
                    len(capped)
                )
                logger.warning(
                    "jira_project_discovery_capped_by_repo_limit",
                    extra={
                        "org_id": org_id,
                        "integration_id": str(integration.id),
                        "capped_count": len(capped),
                        "capped_pre_existing_count": sum(
                            1 for s in capped if s in pre_existing
                        ),
                        "max_repos": max_repos,
                    },
                )
            return

    # Either unlimited (max_repos is None) or there's headroom: recover any
    # previously cap-disabled rows on THIS integration THAT DISCOVERY JUST
    # RECONFIRMED, up to that headroom.
    capped_rows = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.org_id == org_id,
            IntegrationSource.integration_id == integration.id,
            IntegrationSource.is_enabled.is_(False),
        )
        .order_by(IntegrationSource.external_id.asc())
        .all()
    )
    recoverable = [
        r
        for r in capped_rows
        if (r.metadata_ or {}).get(_CAP_MARKER_KEY)
        and r.external_id.lower() in discovered_external_ids_lower
    ]
    if not recoverable:
        return

    if max_repos is not None:
        headroom = int(max_repos) - _active_repo_usage_count_for_limit(session, org_id)
        recoverable = recoverable[: max(0, headroom)]
    if not recoverable:
        return

    for source in recoverable:
        source.is_enabled = True
        source.metadata_ = {
            k: v for k, v in (source.metadata_ or {}).items() if k != _CAP_MARKER_KEY
        }
    session.flush()
    JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="recovered_from_repo_limit_cap").inc(
        len(recoverable)
    )
    logger.info(
        "jira_project_discovery_recovered_from_repo_limit_cap",
        extra={
            "org_id": org_id,
            "integration_id": str(integration.id),
            "recovered_count": len(recoverable),
        },
    )


def _record_jira_project_discovery(
    *,
    org_id: str,
    integration_id: uuid.UUID,
    discovered_count: int,
    created_count: int,
    existing_count: int,
    has_planner_tag: bool,
) -> None:
    """Telemetry for Jira per-project source discovery (CHAOS-4584).

    Counts discovered/created/existing outcomes so "Jira has zero sources"
    is observable going forward instead of silently discovered only via a
    planner run coming back with ``total_units=0``. ``existing`` is a row
    discovery found that already had a source for that project key (e.g. a
    hand-inserted proof row, or a prior discovery run) -- team-lead's
    collision rule: those rows are left exactly as they are (no flip, no
    duplicate), so this label is what makes that visible without reading
    the DB.
    """
    from dev_health_ops.metrics.prometheus import JIRA_PROJECT_DISCOVERY_TOTAL

    if discovered_count == 0:
        JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="discovered_zero").inc()
    else:
        JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="discovered").inc(discovered_count)
        JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="created").inc(created_count)
        if existing_count:
            JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="existing").inc(existing_count)
    if not has_planner_tag:
        JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="skipped_no_planner_parent").inc()

    logger.info(
        "jira_project_discovery_completed",
        extra={
            "org_id": org_id,
            "integration_id": str(integration_id),
            "discovered_count": discovered_count,
            "created_count": created_count,
            "existing_count": existing_count,
            "has_planner_tag": has_planner_tag,
        },
    )


def set_source_enabled(
    session: Session,
    source_id: uuid.UUID,
    enabled: bool,
) -> IntegrationSource:
    """Enable or disable an ``IntegrationSource``.

    Args:
        session: Synchronous SQLAlchemy session.
        source_id: UUID of the IntegrationSource to update.
        enabled: New enabled state.

    Returns:
        The updated ``IntegrationSource``.

    Raises:
        ValueError: If the source is not found.
    """
    source = session.get(IntegrationSource, source_id)
    if source is None:
        raise ValueError(f"IntegrationSource not found: {source_id}")
    source.is_enabled = enabled
    if (source.metadata_ or {}).get(_CAP_MARKER_KEY):
        # ANY explicit operator enable/disable (codex review, CHAOS-4584
        # round 3 P2 + round 4 P1) supersedes the automatic-recovery
        # bookkeeping -- from this point it's an operator decision, not
        # something discovery's own repo-limit rebalancing should ever
        # touch again, in either direction.
        source.metadata_ = {
            k: v for k, v in (source.metadata_ or {}).items() if k != _CAP_MARKER_KEY
        }
    session.flush()
    return source


def list_sources(
    session: Session,
    integration_id: uuid.UUID,
    *,
    enabled_only: bool = False,
) -> list[IntegrationSource]:
    """List ``IntegrationSource`` rows for an integration.

    Args:
        session: Synchronous SQLAlchemy session.
        integration_id: UUID of the Integration.
        enabled_only: If True, return only enabled sources.

    Returns:
        List of ``IntegrationSource`` instances.
    """
    query = session.query(IntegrationSource).filter(
        IntegrationSource.integration_id == integration_id,
    )
    if enabled_only:
        query = query.filter(IntegrationSource.is_enabled.is_(True))
    return query.all()
