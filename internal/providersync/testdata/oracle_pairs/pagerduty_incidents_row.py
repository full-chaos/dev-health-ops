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


def _normalizer(case: dict[str, Any]) -> Any:
    return _module().PagerDutyNormalizer(
        org_id=case["org_id"],
        provider_instance_id=case["provider_instance_id"],
        observed_at=datetime.fromisoformat(case["observed_at"].replace("Z", "+00:00")),
    )


def _reflected(entity: Any) -> frozenset[str]:
    return frozenset(field.name for field in fields(entity))


def _incident(case: dict[str, Any]) -> dict[str, Any]:
    module = _module()
    return asdict(
        _normalizer(case).incident(module.Incident.model_validate(case["payload"]))
    )


def _parent_id(case: dict[str, Any]) -> str:
    module = _module()
    return (
        _normalizer(case).incident(module.Incident.model_validate(case["incident"])).id
    )


def _alert(case: dict[str, Any]) -> dict[str, Any]:
    module = _module()
    return asdict(
        _normalizer(case).alert(
            module.Alert.model_validate(case["payload"]), _parent_id(case)
        )
    )


def _log_entry(case: dict[str, Any]) -> dict[str, Any]:
    module = _module()
    return asdict(
        _normalizer(case).log_entry(
            module.LogEntry.model_validate(case["payload"]), _parent_id(case)
        )
    )


def _note(case: dict[str, Any]) -> dict[str, Any]:
    module = _module()
    return asdict(
        _normalizer(case).note(
            module.Note.model_validate(case["payload"]), _parent_id(case)
        )
    )


oracle_registry.register(
    oracle_registry.PairSpec(
        id="pagerduty/incidents/row",
        build_row=_incident,
        reflected_fields=lambda: _reflected(_module().OperationalIncident),
        excluded_fields={},
    )
)
