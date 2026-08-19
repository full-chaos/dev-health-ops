from __future__ import annotations

import pathlib
import sys
from dataclasses import asdict, fields
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_SOURCE = REPO_ROOT / "src/dev_health_ops/licensing/feature_policy.py"
_EVALUATED_AT = datetime(2026, 7, 23, 12, 30, tzinfo=timezone.utc)
feature_policy = load_live_module(_SOURCE)
LicenseTier = sys.modules["dev_health_ops.licensing.types"].LicenseTier


def _reflected_fields() -> frozenset[str]:
    if pathlib.Path(feature_policy.__file__).resolve(strict=True) != _SOURCE.resolve(
        strict=True
    ):
        raise RuntimeError("Jira entitlement oracle did not import the live policy")
    return frozenset(field.name for field in fields(feature_policy.FeatureDecision))


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    raw_override = case["org_override"]
    override = None
    if raw_override is not None:
        expires_at = raw_override["expires_at"]
        override = feature_policy.FeatureOverrideSnapshot(
            is_enabled=raw_override["enabled"],
            expires_at=(
                datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                if expires_at is not None
                else None
            ),
        )
    try:
        min_tier = LicenseTier(case["min_tier"])
        storage_valid = True
    except ValueError:
        min_tier = LicenseTier.COMMUNITY
        storage_valid = False
    try:
        org_tier = LicenseTier(case["org_tier"])
    except ValueError:
        org_tier = LicenseTier.COMMUNITY
    decision = feature_policy.decide_feature(
        feature_policy.FeatureDecisionContext(
            feature_key="canonical_incident_ingestion",
            is_registered=case["registered"],
            is_storage_valid=storage_valid,
            globally_enabled=case["globally_enabled"],
            min_tier=min_tier,
            org_tier=org_tier,
            org_override=override,
            license_override=case["license_override"],
            evaluated_at=_EVALUATED_AT,
        )
    )
    return asdict(decision)


oracle_registry.register(
    oracle_registry.PairSpec(
        id="jira/incidents/entitlement",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={},
    )
)
