from __future__ import annotations

import uuid

import pytest

from dev_health_ops.api.dev.entitlement import (
    AskDevEntitlementDeniedError,
    CanonicalAskDevEntitlementAuthorizer,
)
from dev_health_ops.licensing import FeatureDecision, FeatureDecisionReason
from dev_health_ops.licensing.registry import ASK_DEV_FEATURE

ORG_ID = "00000000-0000-0000-0000-000000000001"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("allowed", "reason"),
    [
        (False, FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED),
        (True, FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE),
        (True, FeatureDecisionReason.ENABLED_BY_LICENSE_OVERRIDE),
    ],
)
async def test_entitlement_uses_canonical_ask_dev_feature_decision(
    monkeypatch: pytest.MonkeyPatch,
    allowed: bool,
    reason: FeatureDecisionReason,
) -> None:
    calls: list[tuple[object, uuid.UUID, str]] = []

    async def evaluate(
        session: object, org_id: uuid.UUID, feature_key: str
    ) -> FeatureDecision:
        calls.append((session, org_id, feature_key))
        return FeatureDecision(feature_key, allowed, reason)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.entitlement.evaluate_org_feature_async", evaluate
    )
    session = object()
    entitlement = CanonicalAskDevEntitlementAuthorizer(session)  # type: ignore[arg-type]

    if allowed:
        await entitlement.require(ORG_ID)
    else:
        with pytest.raises(AskDevEntitlementDeniedError) as exc_info:
            await entitlement.require(ORG_ID)
        assert exc_info.value.reason is reason

    assert calls == [(session, uuid.UUID(ORG_ID), ASK_DEV_FEATURE)]


@pytest.mark.asyncio
async def test_entitlement_storage_failure_and_invalid_org_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unavailable(*_args: object) -> FeatureDecision:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        "dev_health_ops.api.dev.entitlement.evaluate_org_feature_async", unavailable
    )
    entitlement = CanonicalAskDevEntitlementAuthorizer(object())  # type: ignore[arg-type]

    with pytest.raises(AskDevEntitlementDeniedError) as storage:
        await entitlement.require(ORG_ID)
    assert storage.value.reason is FeatureDecisionReason.STORAGE_ERROR

    with pytest.raises(AskDevEntitlementDeniedError) as invalid:
        await entitlement.require("not-an-org-id")
    assert invalid.value.reason is FeatureDecisionReason.INVALID_FEATURE_STATE
