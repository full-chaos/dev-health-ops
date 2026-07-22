"""Transactional repair of PagerDuty's single operational sync target."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Integration,
    IntegrationCredential,
    IntegrationDataset,
    IntegrationSource,
)
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.sync.datasets import supported_datasets


class PagerDutyOperationalTargetError(ValueError):
    """A persisted PagerDuty configuration cannot safely produce a sync plan."""


PAGERDUTY_OPERATIONAL_DATASET_KEYS = tuple(
    spec.dataset_key
    for spec in supported_datasets("pagerduty")
    if "operational" in spec.legacy_targets
)


def repair_pagerduty_operational_integration(
    session: Session,
    integration: Integration,
) -> str | None:
    """Repair legacy PagerDuty rows from the linked credential's account identity.

    A missing/unusable credential deliberately leaves a zero-unit integration
    untouched. The normal planner credential stamp remains the strict boundary
    for executable work and never falls back to environment credentials.
    """
    if str(integration.provider).lower() != "pagerduty":
        return None

    configs = _linked_pagerduty_configs(session, integration)
    if _disable_malformed_targets(configs):
        reason = (
            "PagerDuty sync target must be operational; malformed configs were disabled"
        )
        _stamp_disabled(configs, reason)
        session.flush()
        return reason

    credential = _active_linked_credential(session, integration)
    if credential is None:
        return None
    try:
        subdomain = pagerduty_provider_instance_id(credential.config)
    except PagerDutyOperationalTargetError:
        reason = "PagerDuty credential account identity is invalid"
        _stamp_disabled(configs, reason)
        session.flush()
        return reason

    canonical_source = _repair_sources(session, integration, subdomain)
    _repair_datasets(session, integration)
    canonical_source.is_enabled = True
    session.flush()
    return None


def _stamp_disabled(configs: Iterable[SyncConfiguration], reason: str) -> None:
    now = datetime.now(timezone.utc)
    for config in configs:
        config.is_active = False
        config.last_sync_at = now
        config.last_sync_success = False
        config.last_sync_error = reason
        config.last_sync_stats = {"phase": "pagerduty_repair", "error": reason}


def _linked_pagerduty_configs(
    session: Session, integration: Integration
) -> list[SyncConfiguration]:
    return (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.org_id == integration.org_id,
            SyncConfiguration.integration_id == integration.id,
            SyncConfiguration.provider == "pagerduty",
        )
        .all()
    )


def _disable_malformed_targets(configs: Iterable[SyncConfiguration]) -> bool:
    malformed = False
    for config in configs:
        if set(config.sync_targets or []) != {"operational"}:
            config.is_active = False
            malformed = True
    return malformed


def _active_linked_credential(
    session: Session, integration: Integration
) -> IntegrationCredential | None:
    credential_id = integration.credential_id
    if credential_id is None:
        return None
    credential = (
        session.query(IntegrationCredential)
        .filter(
            IntegrationCredential.id == credential_id,
            IntegrationCredential.org_id == integration.org_id,
            IntegrationCredential.provider == "pagerduty",
            IntegrationCredential.is_active.is_(True),
        )
        .one_or_none()
    )
    return credential


def pagerduty_provider_instance_id(config: Mapping[str, object] | None) -> str:
    config = config or {}
    account_id = _identity_component(config.get("account_id"))
    subdomain = _identity_component(config.get("subdomain"))
    if account_id is None and subdomain is None:
        raise PagerDutyOperationalTargetError(
            "PagerDuty credential is missing its verified account subdomain"
        )
    if account_id is None or subdomain is None:
        raise PagerDutyOperationalTargetError(
            "PagerDuty credential is missing its verified account identity"
        )
    return account_id


def _identity_component(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    component = value.strip()
    return component or None


def _repair_sources(
    session: Session, integration: Integration, subdomain: str
) -> IntegrationSource:
    sources = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.org_id == integration.org_id,
            IntegrationSource.integration_id == integration.id,
            IntegrationSource.provider == "pagerduty",
        )
        .order_by(IntegrationSource.id)
        .all()
    )
    canonical = next(
        (source for source in sources if source.external_id == subdomain),
        None,
    )
    if canonical is None:
        canonical = IntegrationSource(
            org_id=integration.org_id,
            integration_id=integration.id,
            provider="pagerduty",
            source_type="account",
            external_id=subdomain,
            name=subdomain,
            full_name=subdomain,
            metadata_={},
            is_enabled=True,
        )
        session.add(canonical)
    else:
        canonical.source_type = "account"
        canonical.external_id = subdomain
        canonical.name = subdomain
        canonical.full_name = subdomain
    for source in sources:
        if source is not canonical:
            source.is_enabled = False
    return canonical


def _repair_datasets(session: Session, integration: Integration) -> None:
    expected = set(PAGERDUTY_OPERATIONAL_DATASET_KEYS)
    rows = (
        session.query(IntegrationDataset)
        .filter(
            IntegrationDataset.org_id == integration.org_id,
            IntegrationDataset.integration_id == integration.id,
        )
        .all()
    )
    by_key = {row.dataset_key: row for row in rows}
    for dataset_key in expected:
        row = by_key.get(dataset_key)
        if row is None:
            session.add(
                IntegrationDataset(
                    org_id=integration.org_id,
                    integration_id=integration.id,
                    dataset_key=dataset_key,
                    is_enabled=True,
                    options={"legacy_targets": ["operational"]},
                )
            )
            continue
        row.is_enabled = True
        row.options = {**(row.options or {}), "legacy_targets": ["operational"]}
    for row in rows:
        if row.dataset_key not in expected:
            row.is_enabled = False
