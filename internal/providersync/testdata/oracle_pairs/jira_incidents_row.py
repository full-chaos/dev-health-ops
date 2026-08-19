from __future__ import annotations

import pathlib
from dataclasses import asdict, fields
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/jira/jsm_incidents.py"
_OBSERVED_AT = datetime(2026, 7, 23, 12, 30, tzinfo=timezone.utc)
jsm_incidents = load_live_module(_SOURCE)


def _module() -> Any:
    if pathlib.Path(jsm_incidents.__file__).resolve(strict=True) != _SOURCE.resolve(
        strict=True
    ):
        raise RuntimeError("Jira incident oracle did not import the live source")
    return jsm_incidents


def _reflected_fields() -> frozenset[str]:
    module = _module()
    return frozenset(field.name for field in fields(module.OperationalIncident))


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    module = _module()
    producer = module.JsmIncidentProducer(
        client=None,
        org_id="org-acme",
        provider_instance_id=case["cloud_id"],
        base_url=case["base_url"],
        observed_at=_OBSERVED_AT,
    )
    return asdict(producer._incident(case["raw_issue"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="jira/incidents/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={},
    )
)
