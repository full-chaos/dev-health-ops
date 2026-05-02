from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import _get_db_url

logger = logging.getLogger(__name__)


def _string_list(value: object | None) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def _uuid_value(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _string_or_none(value: object | None) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


async def _discover_and_sync_all(org_id: str | None) -> dict:
    """Discover teams from every configured provider concurrently and run drift sync.

    Each provider (github, gitlab, jira) gets its own coroutine that performs the
    credential lookup, discovery call, and drift-sync call. ``asyncio.gather``
    fans them out so the wall-time equals max(per-provider-time) rather than the
    sum.
    """
    from dev_health_ops.api.services.settings import (
        IntegrationCredentialsService,
        TeamDiscoveryService,
        TeamDriftSyncService,
    )
    from dev_health_ops.db import get_postgres_session

    effective_org_id = org_id or ""

    async with get_postgres_session() as session:
        creds_svc = IntegrationCredentialsService(session, effective_org_id)
        discovery_svc = TeamDiscoveryService(session, effective_org_id)
        drift_svc = TeamDriftSyncService(session, effective_org_id)

        async def _run_one(provider: str) -> dict:
            credential = await creds_svc.get(provider, "default")
            if credential is None:
                return {"provider": provider, "skipped": "no_credential"}
            decrypted = await creds_svc.get_decrypted_credentials(provider, "default")
            if decrypted is None:
                return {"provider": provider, "skipped": "no_decrypted"}
            config: dict[str, Any] = (
                credential.config if isinstance(credential.config, dict) else {}
            )
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
                    teams = await discovery_svc.discover_gitlab(
                        token=token, group_path=group_path, url=url
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
                return await drift_svc.run_drift_sync(provider, teams)
            except Exception as exc:
                logger.warning(
                    "Team drift sync failed for provider %s: %s", provider, exc
                )
                return {"provider": provider, "error": str(exc)}

        results = await asyncio.gather(
            _run_one("github"),
            _run_one("gitlab"),
            _run_one("jira"),
        )
        await session.commit()
    return {"status": "success", "results": list(results)}


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="sync",
    name="dev_health_ops.workers.tasks.sync_team_drift",
)
def sync_team_drift(self, org_id: str | None = None) -> dict:
    try:
        return run_async(_discover_and_sync_all(org_id))
    except Exception as exc:
        logger.exception("sync_team_drift failed: %s", exc)
        raise self.retry(exc=exc, countdown=300)


@celery_app.task(
    bind=True,
    max_retries=2,
    queue="sync",
    name="dev_health_ops.workers.tasks.reconcile_team_members",
)
def reconcile_team_members(self, org_id: str | None = None) -> dict:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import IdentityMapping

    team_members: dict[str, set[str]] = {}
    with get_postgres_session_sync() as session:
        mappings = (
            session.query(IdentityMapping)
            .filter(IdentityMapping.org_id == org_id)
            .all()
        )

        for mapping in mappings:
            canonical_id = str(mapping.canonical_id)
            for team_id in _string_list(mapping.team_ids):
                if not team_id:
                    continue
                team_members.setdefault(str(team_id), set()).add(canonical_id)

    async def _run() -> dict:
        from dev_health_ops.models.teams import Team
        from dev_health_ops.storage.clickhouse import ClickHouseStore

        db_url = _get_db_url()
        if not db_url:
            raise ValueError(
                "Missing CLICKHOUSE_URI or DATABASE_URI for reconciliation"
            )

        async with ClickHouseStore(db_url) as store:
            teams = await store.get_all_teams()
            now = datetime.now(timezone.utc)
            updated_teams = [
                Team(
                    id=str(team.id),
                    team_uuid=_uuid_value(team.team_uuid),
                    name=str(team.name),
                    description=_string_or_none(team.description),
                    members=sorted(team_members.get(str(team.id), set())),
                    updated_at=now,
                )
                for team in teams
            ]
            if updated_teams:
                await store.insert_teams(updated_teams)

            return {
                "status": "success",
                "teams_scanned": len(teams),
                "teams_updated": len(updated_teams),
                "mapped_teams": len(team_members),
            }

    try:
        return run_async(_run())
    except Exception as exc:
        logger.exception("reconcile_team_members failed: %s", exc)
        raise self.retry(exc=exc, countdown=300)
