from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from sqlalchemy.orm import Session

from dev_health_ops.models.integrations import IntegrationSource
from dev_health_ops.models.settings import SyncConfiguration

PlannerConfigAuditReason = Literal[
    "missing_migrated_integration_id", "zero_tagged_enabled_sources"
]

_PLANNER_TAG_KEY = "planner_managed_sync_config_id"


@dataclass(frozen=True)
class PlannerManagedConfigAuditFinding:
    config_id: str
    org_id: str
    provider: str
    name: str
    reason: PlannerConfigAuditReason

    def to_dict(self) -> dict[str, str]:
        return {
            "config_id": self.config_id,
            "org_id": self.org_id,
            "provider": self.provider,
            "name": self.name,
            "reason": self.reason,
        }


def audit_active_planner_managed_configs(
    session: Session,
    *,
    org_id: str | None = None,
) -> list[PlannerManagedConfigAuditFinding]:
    query = session.query(SyncConfiguration).filter(
        SyncConfiguration.planner_managed.is_(True),
        SyncConfiguration.is_active.is_(True),
        SyncConfiguration.parent_id.is_(None),
    )
    if org_id:
        query = query.filter(SyncConfiguration.org_id == org_id)

    findings: list[PlannerManagedConfigAuditFinding] = []
    for config in query.order_by(
        SyncConfiguration.created_at, SyncConfiguration.id
    ).all():
        config_id = str(config.id)
        base = {
            "config_id": config_id,
            "org_id": str(config.org_id),
            "provider": str(config.provider),
            "name": str(config.name),
        }
        if config.migrated_integration_id is None:
            findings.append(
                PlannerManagedConfigAuditFinding(
                    **base,
                    reason="missing_migrated_integration_id",
                )
            )
            continue

        enabled_sources = (
            session.query(IntegrationSource)
            .filter(
                IntegrationSource.org_id == config.org_id,
                IntegrationSource.integration_id == config.migrated_integration_id,
                IntegrationSource.is_enabled.is_(True),
            )
            .all()
        )
        tagged_count = sum(
            1
            for source in enabled_sources
            if str((source.metadata_ or {}).get(_PLANNER_TAG_KEY)) == config_id
        )
        if tagged_count == 0:
            findings.append(
                PlannerManagedConfigAuditFinding(
                    **base,
                    reason="zero_tagged_enabled_sources",
                )
            )

    return findings
