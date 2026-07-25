from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Final

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.licensing.feature_decisions import evaluate_org_features_async
from dev_health_ops.licensing.registry import CANONICAL_INCIDENT_INGESTION_FEATURE

from .errors import PermanentProcessingError

logger = logging.getLogger(__name__)

CUSTOMER_PUSH_INGEST_FEATURE: Final = "customer_push_ingest"


@dataclass(frozen=True, slots=True)
class ExternalIngestFeatureAvailability:
    customer_push_ingest: bool
    canonical_incident_ingestion: bool

    @property
    def operational_ingestion(self) -> bool:
        return self.customer_push_ingest and self.canonical_incident_ingestion


_CLOSED_AVAILABILITY = ExternalIngestFeatureAvailability(
    customer_push_ingest=False,
    canonical_incident_ingestion=False,
)


class CanonicalIncidentIngestionDisabledError(PermanentProcessingError):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(
            "feature_disabled: canonical incident ingestion is not enabled"
        )


def _org_uuid(org_id: str | uuid.UUID) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(org_id))
    except (ValueError, AttributeError, TypeError):
        return None


async def external_ingest_feature_availability(
    session: AsyncSession,
    org_id: str | uuid.UUID,
) -> ExternalIngestFeatureAvailability:
    parsed_org_id = _org_uuid(org_id)
    if parsed_org_id is None:
        return _CLOSED_AVAILABILITY
    try:
        decisions = await evaluate_org_features_async(
            session,
            parsed_org_id,
            (
                CUSTOMER_PUSH_INGEST_FEATURE,
                CANONICAL_INCIDENT_INGESTION_FEATURE,
            ),
        )
    except Exception:
        logger.exception("external_ingest.feature_gate.evaluation_failed")
        return _CLOSED_AVAILABILITY
    return ExternalIngestFeatureAvailability(
        customer_push_ingest=decisions[CUSTOMER_PUSH_INGEST_FEATURE].allowed,
        canonical_incident_ingestion=decisions[
            CANONICAL_INCIDENT_INGESTION_FEATURE
        ].allowed,
    )


async def canonical_incident_ingestion_allowed(
    session: AsyncSession,
    org_id: str | uuid.UUID,
) -> bool:
    availability = await external_ingest_feature_availability(session, org_id)
    return availability.canonical_incident_ingestion


async def external_operational_ingestion_allowed(
    session: AsyncSession,
    org_id: str | uuid.UUID,
) -> bool:
    availability = await external_ingest_feature_availability(session, org_id)
    return availability.operational_ingestion


async def external_ingest_feature_availability_for_org(
    org_id: str | uuid.UUID,
) -> ExternalIngestFeatureAvailability:
    parsed_org_id = _org_uuid(org_id)
    if parsed_org_id is None:
        return _CLOSED_AVAILABILITY
    try:
        from dev_health_ops.db import get_postgres_session

        async with get_postgres_session() as session:
            return await external_ingest_feature_availability(session, parsed_org_id)
    except Exception:
        logger.exception("external_ingest.feature_gate.session_failed")
        return _CLOSED_AVAILABILITY


async def canonical_incident_ingestion_allowed_for_org(
    org_id: str | uuid.UUID,
) -> bool:
    availability = await external_ingest_feature_availability_for_org(org_id)
    return availability.canonical_incident_ingestion


async def external_operational_ingestion_allowed_for_org(
    org_id: str | uuid.UUID,
) -> bool:
    availability = await external_ingest_feature_availability_for_org(org_id)
    return availability.operational_ingestion


__all__ = [
    "CUSTOMER_PUSH_INGEST_FEATURE",
    "CanonicalIncidentIngestionDisabledError",
    "ExternalIngestFeatureAvailability",
    "canonical_incident_ingestion_allowed",
    "canonical_incident_ingestion_allowed_for_org",
    "external_ingest_feature_availability",
    "external_ingest_feature_availability_for_org",
    "external_operational_ingestion_allowed",
    "external_operational_ingestion_allowed_for_org",
]
