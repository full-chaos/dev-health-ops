from __future__ import annotations

import uuid
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Final

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dev_health_ops.licensing import (
    FeatureDecisionReason,
    evaluate_org_feature_async,
    evaluate_org_feature_sync,
    is_org_feature_enabled_async,
    is_org_feature_enabled_sync,
)
from dev_health_ops.licensing.feature_decision_store import lock_feature_rows_sync
from dev_health_ops.sync.datasets import get_dataset_spec

if TYPE_CHECKING:
    from dev_health_ops.models import SyncRun

CANONICAL_INCIDENT_FEATURE_KEY: Final = "canonical_incident_ingestion"
FEATURE_DISABLED_ERROR_CATEGORY: Final = "feature_disabled"
_GATED_SYNC_TARGETS: Final = frozenset({"incidents", "operational"})


class CanonicalIncidentFeatureDisabledError(RuntimeError):
    def __init__(self, reason: FeatureDecisionReason) -> None:
        self.reason = reason
        super().__init__(
            f"{FEATURE_DISABLED_ERROR_CATEGORY}: canonical incident ingestion "
            f"is disabled ({self.reason.value})"
        )


def sync_targets_require_canonical_incident_feature(
    sync_targets: Sequence[str] | None,
) -> bool:
    if sync_targets is None:
        return False
    return any(target.lower() in _GATED_SYNC_TARGETS for target in sync_targets)


def sync_dataset_requires_canonical_incident_feature(
    provider: str,
    dataset_key: str,
) -> bool:
    spec = get_dataset_spec(provider, dataset_key)
    return spec is not None and bool(spec.legacy_targets & _GATED_SYNC_TARGETS)


def sync_datasets_require_canonical_incident_feature(
    provider: str,
    dataset_keys: Iterable[str],
) -> bool:
    return any(
        sync_dataset_requires_canonical_incident_feature(provider, dataset_key)
        for dataset_key in dataset_keys
    )


def require_canonical_incident_feature_sync(
    session: Session,
    org_id: str | uuid.UUID,
) -> None:
    try:
        parsed_org_id = uuid.UUID(str(org_id))
    except ValueError as exc:
        raise CanonicalIncidentFeatureDisabledError(
            FeatureDecisionReason.INVALID_FEATURE_STATE
        ) from exc
    decision = evaluate_org_feature_sync(
        session,
        parsed_org_id,
        CANONICAL_INCIDENT_FEATURE_KEY,
    )
    if not decision.allowed:
        raise CanonicalIncidentFeatureDisabledError(decision.reason)


def require_canonical_incident_feature_for_update_sync(
    session: Session,
    org_id: str | uuid.UUID,
) -> None:
    try:
        parsed_org_id = uuid.UUID(str(org_id))
    except ValueError as exc:
        raise CanonicalIncidentFeatureDisabledError(
            FeatureDecisionReason.INVALID_FEATURE_STATE
        ) from exc
    lock_feature_rows_sync(session, parsed_org_id, (CANONICAL_INCIDENT_FEATURE_KEY,))
    decision = evaluate_org_feature_sync(
        session,
        parsed_org_id,
        CANONICAL_INCIDENT_FEATURE_KEY,
    )
    if not decision.allowed:
        raise CanonicalIncidentFeatureDisabledError(decision.reason)


def sync_run_requires_canonical_incident_feature(
    session: Session,
    run: SyncRun,
) -> bool:
    from dev_health_ops.models import Integration, IntegrationDataset, SyncRunUnit

    unit_scopes = (
        session.query(SyncRunUnit.provider, SyncRunUnit.dataset_key)
        .filter(SyncRunUnit.sync_run_id == run.id)
        .all()
    )
    if unit_scopes:
        return any(
            sync_dataset_requires_canonical_incident_feature(
                str(provider),
                str(dataset_key),
            )
            for provider, dataset_key in unit_scopes
        )
    if run.integration_id is None:
        return False
    integration_scopes = (
        session.query(Integration.provider, IntegrationDataset.dataset_key)
        .join(
            IntegrationDataset,
            IntegrationDataset.integration_id == Integration.id,
        )
        .filter(
            Integration.id == run.integration_id,
            IntegrationDataset.is_enabled.is_(True),
        )
        .all()
    )
    return any(
        sync_dataset_requires_canonical_incident_feature(
            str(provider),
            str(dataset_key),
        )
        for provider, dataset_key in integration_scopes
    )


async def require_canonical_incident_feature_async(
    session: AsyncSession,
    org_id: str | uuid.UUID,
) -> None:
    try:
        parsed_org_id = uuid.UUID(str(org_id))
    except ValueError as exc:
        raise CanonicalIncidentFeatureDisabledError(
            FeatureDecisionReason.INVALID_FEATURE_STATE
        ) from exc
    decision = await evaluate_org_feature_async(
        session,
        parsed_org_id,
        CANONICAL_INCIDENT_FEATURE_KEY,
    )
    if not decision.allowed:
        raise CanonicalIncidentFeatureDisabledError(decision.reason)


def is_canonical_incident_feature_enabled_sync(
    session: Session,
    org_id: str | uuid.UUID,
) -> bool:
    try:
        parsed_org_id = uuid.UUID(str(org_id))
    except ValueError:
        return False
    return is_org_feature_enabled_sync(
        session,
        parsed_org_id,
        CANONICAL_INCIDENT_FEATURE_KEY,
    )


async def is_canonical_incident_feature_enabled_async(
    session: AsyncSession,
    org_id: str | uuid.UUID,
) -> bool:
    try:
        parsed_org_id = uuid.UUID(str(org_id))
    except ValueError:
        return False
    return await is_org_feature_enabled_async(
        session,
        parsed_org_id,
        CANONICAL_INCIDENT_FEATURE_KEY,
    )
