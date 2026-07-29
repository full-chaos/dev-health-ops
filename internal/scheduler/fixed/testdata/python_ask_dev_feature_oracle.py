"""Execute the live canonical Ask Dev feature policy for Go SQL parity cases."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / "src/dev_health_ops/licensing"


class _BaseModel:
    def __init__(self, **values: object) -> None:
        self.__dict__.update(values)


def _field(default: object = None, **_kwargs: object) -> object:
    return default


def _package(name: str) -> None:
    module = ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_package("dev_health_ops")
_package("dev_health_ops.licensing")
pydantic = ModuleType("pydantic")
pydantic.BaseModel = _BaseModel  # type: ignore[attr-defined]
pydantic.Field = _field  # type: ignore[attr-defined]
pydantic.JsonValue = object  # type: ignore[attr-defined]
sys.modules["pydantic"] = pydantic

types_module = _load("dev_health_ops.licensing.types", SOURCE / "types.py")
_load("dev_health_ops.licensing.registry", SOURCE / "registry.py")
policy = _load("dev_health_ops.licensing.feature_policy", SOURCE / "feature_policy.py")

evaluated_at = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)
results: dict[str, bool] = {}
for case in json.load(sys.stdin):
    raw_override = case.get("org_override")
    override = None
    if raw_override is not None:
        expires_at = raw_override.get("expires_at")
        override = policy.FeatureOverrideSnapshot(
            is_enabled=raw_override["is_enabled"],
            expires_at=(
                datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                if expires_at is not None
                else None
            ),
        )
    decision = policy.decide_feature(
        policy.FeatureDecisionContext(
            feature_key="ask_dev",
            is_registered=True,
            is_storage_valid=case["storage_valid"],
            globally_enabled=case["globally_enabled"],
            min_tier=types_module.LicenseTier.COMMUNITY,
            org_tier=types_module.LicenseTier.COMMUNITY,
            org_override=override,
            license_override=case.get("license_override"),
            evaluated_at=evaluated_at,
        )
    )
    results[case["id"]] = decision.allowed

json.dump(results, sys.stdout, sort_keys=True)
