from __future__ import annotations

import pathlib
from dataclasses import asdict, fields
from datetime import datetime
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/pagerduty/normalize.py"
_NORMALIZE = load_live_module(_SOURCE)


def _module() -> Any:
    if pathlib.Path(_NORMALIZE.__file__).resolve(strict=True) != _SOURCE.resolve(
        strict=True
    ):
        raise RuntimeError("PagerDuty oracle did not import the live normalizer")
    return _NORMALIZE


def _reflected_fields() -> frozenset[str]:
    return frozenset(field.name for field in fields(_module().OnCallSchedule))


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = _module()
    observed_at = datetime.fromisoformat(case["observed_at"].replace("Z", "+00:00"))
    source = module.Schedule.model_validate(case["payload"])
    normalizer = module.PagerDutyNormalizer(
        org_id=case["org_id"],
        provider_instance_id=case["provider_instance_id"],
        observed_at=observed_at,
    )
    return asdict(normalizer.schedule(source))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="pagerduty/schedules/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={},
    )
)
