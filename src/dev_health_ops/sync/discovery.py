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

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from dev_health_ops.discovery.repos import discover_repos_for_config
from dev_health_ops.models.integrations import Integration, IntegrationSource

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_credentials(integration: Integration) -> dict[str, Any]:
    """Resolve credentials for *integration* into a flat mapping.

    Mirrors the pattern used in ``sync_batch.dispatch_batch_sync``:
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


def _build_config_shim(integration: Integration) -> Any:
    """Build a minimal config-shim that ``discover_repos_for_config`` accepts.

    ``discover_repos_for_config`` expects an object with:
    - ``.provider`` (str)
    - ``.sync_options`` (dict | None)

    The ``Integration.config`` column carries the same options that
    ``SyncConfiguration.sync_options`` used to carry (owner, search,
    all_repos, group, gitlab_url, etc.).
    """

    class _Shim:
        provider: str
        sync_options: dict[str, Any]

    shim = _Shim()
    shim.provider = integration.provider or ""
    shim.sync_options = dict(integration.config or {})
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


def _tuples_to_source_dicts(
    provider: str,
    tuples: list[tuple[str, ...]],
    *,
    org_id: str,
    integration_id: uuid.UUID,
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
    return result


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

    config_shim = _build_config_shim(integration)
    credentials = _resolve_credentials(integration)

    raw_tuples: list[tuple[str, ...]] = discover_repos_for_config(
        config_shim, credentials
    )

    provider = (integration.provider or "").lower()
    source_dicts = _tuples_to_source_dicts(
        provider,
        raw_tuples,
        org_id=integration.org_id,
        integration_id=integration_id,
    )

    now = _now_utc()
    upserted: list[IntegrationSource] = []

    for fields in source_dicts:
        external_id = fields["external_id"]

        existing = (
            session.query(IntegrationSource)
            .filter(
                IntegrationSource.org_id == integration.org_id,
                IntegrationSource.integration_id == integration_id,
                IntegrationSource.provider == fields["provider"],
                IntegrationSource.external_id == external_id,
            )
            .one_or_none()
        )

        if existing is not None:
            # Update mutable fields; preserve is_enabled and discovered_at.
            existing.last_seen_at = now
            existing.name = fields["name"]
            existing.full_name = fields["full_name"]
            existing.metadata_ = {**(existing.metadata_ or {}), **fields["metadata_"]}
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

    session.flush()
    return upserted


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
