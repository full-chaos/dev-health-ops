"""Canonical explicit-entitlement boundary for user-facing Ask Dev services."""

from __future__ import annotations

import uuid
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.licensing import (
    FeatureDecisionReason,
    evaluate_org_feature_async,
)
from dev_health_ops.licensing.registry import ASK_DEV_FEATURE


class AskDevEntitlementAuthorizer(Protocol):
    async def require(self, org_id: str) -> None: ...


class AskDevEntitlementDeniedError(RuntimeError):
    """Fail-closed denial from the canonical feature-decision seam."""

    def __init__(self, reason: FeatureDecisionReason) -> None:
        self.reason = reason
        super().__init__("ask_dev_not_available")


class CanonicalAskDevEntitlementAuthorizer:
    """Evaluate ``ask_dev`` without encoding plan names in the API layer."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def require(self, org_id: str) -> None:
        try:
            parsed_org_id = uuid.UUID(org_id)
        except ValueError as exc:
            raise AskDevEntitlementDeniedError(
                FeatureDecisionReason.INVALID_FEATURE_STATE
            ) from exc
        try:
            decision = await evaluate_org_feature_async(
                self._session,
                parsed_org_id,
                ASK_DEV_FEATURE,
            )
        except Exception as exc:
            raise AskDevEntitlementDeniedError(
                FeatureDecisionReason.STORAGE_ERROR
            ) from exc
        if not decision.allowed:
            raise AskDevEntitlementDeniedError(decision.reason)
