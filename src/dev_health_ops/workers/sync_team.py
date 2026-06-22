from __future__ import annotations

import asyncio
import logging
from typing import Any

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


async def _discover_and_sync_all(org_id: str | None) -> dict:
    """Discover teams from every configured provider and run drift sync.

    Connections are scoped tightly to the DB work so a Postgres connection is
    never held idle-in-transaction across the slow external discovery calls:

    * Phase 1 (one short session): read + decrypt credentials for all providers.
    * Phase 2 (no session held): discover teams from each provider concurrently
      via ``asyncio.gather`` -- pure external network I/O, zero DB connections.
    * Phase 3 (one short session): persist drift sequentially.

    Peak demand is one connection per job, held only for the millisecond-scale
    reads/writes, while the slow per-provider network calls still overlap.
    """
    from dev_health_ops.api.services.configuration import (
        IntegrationCredentialsService,
        TeamDiscoveryService,
        TeamDriftSyncService,
    )
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.providers.team_capabilities import org_drift_capable_providers

    effective_org_id = org_id or ""
    providers = org_drift_capable_providers()

    # Phase 1: read + decrypt credentials in one short-lived session. These are
    # fast local reads; the connection is released before any network I/O.
    prepared: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    skipped: dict[str, dict[str, Any]] = {}
    async with get_postgres_session() as session:
        creds_svc = IntegrationCredentialsService(session, effective_org_id)
        for provider in providers:
            credential = await creds_svc.get(provider, "default")
            if credential is None:
                skipped[provider] = {"provider": provider, "skipped": "no_credential"}
                continue
            decrypted = await creds_svc.get_decrypted_credentials(provider, "default")
            if decrypted is None:
                skipped[provider] = {"provider": provider, "skipped": "no_decrypted"}
                continue
            config: dict[str, Any] = (
                credential.config if isinstance(credential.config, dict) else {}
            )
            prepared[provider] = (decrypted, config)

    # Phase 2: discover teams concurrently WITHOUT holding a DB connection.
    # discover_* perform external network I/O only, so a sessionless service is
    # safe and keeps the connection pool free during the slow calls.
    discovery_svc = TeamDiscoveryService(None, effective_org_id)

    async def _discover(provider: str) -> dict:
        decrypted, config = prepared[provider]
        try:
            if provider == "github":
                token = decrypted.get("token")
                org_name = config.get("org")
                if not isinstance(token, str) or not token:
                    return {"provider": provider, "skipped": "missing_config"}
                if not isinstance(org_name, str) or not org_name:
                    return {"provider": provider, "skipped": "missing_config"}
                teams = await discovery_svc.discover_github(
                    token=token, org_name=org_name
                )
            elif provider == "gitlab":
                token = decrypted.get("token")
                group_path = config.get("group")
                url = config.get("url", "https://gitlab.com")
                if not isinstance(token, str) or not token:
                    return {"provider": provider, "skipped": "missing_config"}
                if not isinstance(group_path, str) or not group_path:
                    return {"provider": provider, "skipped": "missing_config"}
                if not isinstance(url, str) or not url:
                    return {"provider": provider, "skipped": "missing_config"}
                gitlab_result = await discovery_svc.discover_gitlab(
                    token=token, group_path=group_path, url=url
                )
                # Truncation is logged server-side by the discovery walk;
                # drift sync only consumes the (possibly partial) teams.
                teams = gitlab_result.teams
            elif provider == "linear":
                api_key = decrypted.get("api_key") or decrypted.get("token")
                if not isinstance(api_key, str) or not api_key:
                    return {"provider": provider, "skipped": "missing_config"}
                teams = await discovery_svc.discover_linear(api_key=api_key)
            elif provider == "ms-teams":
                tenant_id = decrypted.get("tenant_id")
                client_id_val = decrypted.get("client_id")
                client_secret = decrypted.get("client_secret")
                if not isinstance(tenant_id, str) or not tenant_id:
                    return {"provider": provider, "skipped": "missing_config"}
                if not isinstance(client_id_val, str) or not client_id_val:
                    return {"provider": provider, "skipped": "missing_config"}
                if not isinstance(client_secret, str) or not client_secret:
                    return {"provider": provider, "skipped": "missing_config"}
                teams = await discovery_svc.discover_ms_teams(
                    tenant_id=tenant_id,
                    client_id=client_id_val,
                    client_secret=client_secret,
                )
            else:
                email = decrypted.get("email")
                api_token = decrypted.get("api_token") or decrypted.get("token")
                jira_url = config.get("url") or decrypted.get("url")
                if not isinstance(email, str) or not email:
                    return {"provider": provider, "skipped": "missing_config"}
                if not isinstance(api_token, str) or not api_token:
                    return {"provider": provider, "skipped": "missing_config"}
                if not isinstance(jira_url, str) or not jira_url:
                    return {"provider": provider, "skipped": "missing_config"}
                teams = await discovery_svc.discover_jira(
                    email=email, api_token=api_token, url=jira_url
                )
            return {"provider": provider, "teams": teams}
        except Exception as exc:
            logger.warning("Team discovery failed for provider %s: %s", provider, exc)
            return {"provider": provider, "error": str(exc)}

    discovered = await asyncio.gather(*(_discover(p) for p in prepared))
    discovered_by_provider = {item["provider"]: item for item in discovered}

    # Phase 3: persist drift in one short-lived session, sequentially. Each
    # run_drift_sync is a fast local read + write + flush.
    results: list[dict[str, Any]] = []
    async with get_postgres_session() as session:
        drift_svc = TeamDriftSyncService(session, effective_org_id)
        for provider in providers:
            if provider in skipped:
                results.append(skipped[provider])
                continue
            outcome = discovered_by_provider.get(provider)
            if outcome is None:
                continue
            if "teams" not in outcome:
                # discovery skipped or errored -- surface as-is, no DB work
                results.append(outcome)
                continue
            try:
                results.append(
                    await drift_svc.run_drift_sync(provider, outcome["teams"])
                )
            except Exception as exc:
                logger.warning(
                    "Team drift sync failed for provider %s: %s", provider, exc
                )
                results.append({"provider": provider, "error": str(exc)})

    return {"status": "success", "results": list(results)}


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="sync",
    name="dev_health_ops.workers.tasks.sync_team_drift",
)
def sync_team_drift(self, org_id: str | None = None) -> dict:
    # Fail-closed no-op (CHAOS-2600 CS5). The drift engine
    # (``_discover_and_sync_all`` -> ``TeamDriftSyncService.run_drift_sync``)
    # writes Postgres ``TeamMapping`` rows, and the bridge projecting those into
    # ClickHouse is removed in CS5 — so any output is orphaned and the admin
    # drift-review surface is disabled. The beat schedule is dropped; this
    # entrypoint no-ops so a stray queued/manual dispatch cannot write Postgres.
    # ``_discover_and_sync_all`` is retained as a function (the CHAOS-2066
    # connection-hygiene invariant test exercises it directly). The task +
    # service classes are deleted in CS6.
    return {
        "status": "deprecated",
        "reason": (
            "ClickHouse is the team system of record; the Postgres team drift "
            "engine is disabled in CHAOS-2600 CS5"
        ),
    }


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="sync",
    name="dev_health_ops.workers.tasks.reconcile_team_members",
)
def reconcile_team_members(self, org_id: str | None = None) -> dict:
    # Fail-closed no-op (CHAOS-2600 CS5). This previously read Postgres
    # ``IdentityMapping`` and REPLACED every ClickHouse team's ``members``. After
    # CS5 the admin endpoints write CH members directly and no longer write
    # Postgres ``IdentityMapping``, so running this would wipe admin-written
    # members. It does not read Postgres nor call ``insert_teams``. CS6 deletes
    # it.
    return {
        "status": "deprecated",
        "reason": (
            "ClickHouse is the team system of record; the Postgres->ClickHouse "
            "member reconcile is removed in CHAOS-2600 CS5"
        ),
    }
