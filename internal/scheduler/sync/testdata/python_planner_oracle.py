"""Execute the live Python scheduled-plan producer for Go parity cases."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from types import ModuleType, SimpleNamespace

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / "src/dev_health_ops"


def _package(name: str) -> None:
    module = ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module


def _module(name: str, **values: object) -> ModuleType:
    module = ModuleType(name)
    for key, value in values.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _SyncRunMode(str, Enum):
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"
    FULL_RESYNC = "full_resync"


class _Placeholder:
    pass


_package("dev_health_ops")
_package("dev_health_ops.backfill")
_package("dev_health_ops.credentials")
_package("dev_health_ops.sync")
_module(
    "dev_health_ops.backfill.chunker",
    chunk_date_range=lambda **_kwargs: (),
)
_module(
    "dev_health_ops.credentials.fingerprint",
    AUTH_SOURCE_ENVIRONMENT="environment",
    AUTH_SOURCE_INTEGRATION_CREDENTIAL="integration_credential",
    credential_fingerprint=lambda *_args, **_kwargs: "unused",
)
_module(
    "dev_health_ops.models",
    Integration=_Placeholder,
    IntegrationDataset=_Placeholder,
    IntegrationSource=_Placeholder,
    SyncRun=_Placeholder,
    SyncRunMode=_SyncRunMode,
    SyncRunReferenceDiscovery=_Placeholder,
    SyncRunStatus=_Placeholder,
    SyncRunUnit=_Placeholder,
    SyncRunUnitStatus=_Placeholder,
)
_module(
    "dev_health_ops.sync.canonical_incident_gate",
    require_canonical_incident_feature_sync=lambda *_args, **_kwargs: None,
    sync_datasets_require_canonical_incident_feature=lambda *_args, **_kwargs: False,
)
datasets = _load("dev_health_ops.sync.datasets", SOURCE / "sync/datasets.py")
_module(
    "dev_health_ops.sync.dispatch_outbox",
    OUTBOX_KIND_DISCOVERY="reference_discovery",
    upsert_outbox_wakeup=lambda *_args, **_kwargs: None,
)
_module("dev_health_ops.sync.guard", _resolve_total_unit_cap=lambda *_args: 1000)
_module(
    "dev_health_ops.sync.pagerduty_repair",
    repair_pagerduty_operational_integration=lambda *_args: None,
)
_module(
    "dev_health_ops.sync.watermarks",
    get_watermark_with_overlap=lambda *_args: None,
)
planner = _load("dev_health_ops.sync.planner", SOURCE / "sync/planner.py")


def _instant(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _encode_instant(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _planned(case: dict[str, object]) -> list[dict[str, object]]:
    now = _instant(str(case["now"]))
    assert now is not None
    watermarks = {
        (str(row["source_id"]), str(row["dataset_key"])): _instant(str(row["at"]))
        for row in (case.get("watermarks") or [])
    }
    overlap = int(case.get("watermark_overlap_seconds", 0))

    def watermark(
        _session: object, _org_id: str, source_id: str, dataset_key: str
    ) -> datetime | None:
        value = watermarks.get((source_id, dataset_key))
        return value - timedelta(seconds=overlap) if value is not None else None

    planner.get_watermark_with_overlap = watermark
    planner._get_tier_backfill_days_cap = lambda *_args: case.get("tier_cap")
    integration = SimpleNamespace(
        id=case["integration_id"],
        org_id=case["org_id"],
        provider=case["provider"],
        config={"initial_sync_depth": case["integration_depth"]}
        if case.get("integration_depth") is not None
        else {},
    )
    sources = [SimpleNamespace(**source) for source in case["sources"]]
    plan_datasets = [
        SimpleNamespace(
            dataset_key=row["dataset_key"],
            options={"initial_sync_depth": row["initial_depth"]}
            if row.get("initial_depth") is not None
            else {},
        )
        for row in case["datasets"]
    ]
    request = planner.SyncPlanRequest(
        integration_id=str(case["integration_id"]),
        org_id=str(case["org_id"]),
        mode=str(case["mode"]),
        triggered_by="schedule",
        before=_instant(case.get("before")),
    )
    units = planner._build_planned_units(
        session=object(),
        request=request,
        integration=integration,
        sources=sources,
        datasets=plan_datasets,
        mode=str(case["mode"]),
        now=now,
    )
    return [
        {
            "org_id": unit.org_id,
            "integration_id": unit.integration_id,
            "source_id": unit.source_id,
            "provider": unit.provider,
            "dataset_key": unit.dataset_key,
            "cost_class": unit.cost_class,
            "mode": unit.mode,
            "window_start": _encode_instant(unit.window_start),
            "window_end": _encode_instant(unit.window_end),
            "processor_flags": dict(unit.processor_flags),
        }
        for unit in units
    ]


json.dump({case["id"]: _planned(case) for case in json.load(sys.stdin)}, sys.stdout)
