#!/usr/bin/env python3
"""Validate the Python normalized models and emit their Go sink envelopes."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, get_args

from dev_health_ops.metrics.schemas import FeatureFlagRecord
from dev_health_ops.models.operational import OperationalIncident, OperationalService
from dev_health_ops.models.work_items import WorkItem, WorkItemProvider


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _attributes(**values: object) -> dict[str, str]:
    return {
        key: str(value) for key, value in sorted(values.items()) if value is not None
    }


def _envelope(
    case: dict[str, Any],
    *,
    provider: str,
    org_id: str,
    entity_type: str,
    source_id: str,
    observed_at: str,
    attributes: dict[str, str],
) -> dict[str, Any]:
    return {
        "schema_version": "v1",
        "provider": provider,
        "org_id": org_id,
        "integration_id": case["integration_id"],
        "entity_type": entity_type,
        "source_id": source_id,
        "dedupe_key": f"{provider}:{entity_type}:{source_id}",
        "observed_at": observed_at,
        "provenance": case["provenance"],
        "attributes": attributes,
    }


def _work_item(case: dict[str, Any]) -> dict[str, Any]:
    model = case["model"]
    if model["provider"] not in get_args(WorkItemProvider):
        raise ValueError("unsupported work-item provider")
    item = WorkItem(
        work_item_id=model["work_item_id"],
        provider=model["provider"],
        title=model["title"],
        type=model["type"],
        status=model["status"],
        status_raw=model["status_raw"],
        project_key=model.get("project_key"),
        project_id=model.get("project_id"),
        native_team_key=model.get("native_team_key"),
        updated_at=_datetime(model["updated_at"]),
        org_id=model["org_id"],
    )
    return _envelope(
        case,
        provider=item.provider,
        org_id=item.org_id,
        entity_type="work_item",
        source_id=item.work_item_id,
        observed_at=model["updated_at"],
        attributes=_attributes(
            status=item.status,
            status_raw=item.status_raw,
            title=item.title,
            type=item.type,
            work_scope_id=item.work_scope_id,
        ),
    )


def _feature_flag(case: dict[str, Any]) -> dict[str, Any]:
    model = case["model"]
    item = FeatureFlagRecord(
        provider=model["provider"],
        flag_key=model["flag_key"],
        project_key=model.get("project_key"),
        repo_id=None,
        environment=model["environment"],
        flag_type=model.get("flag_type"),
        created_at=None,
        archived_at=None,
        last_synced=_datetime(model["last_synced"]),
        org_id=model["org_id"],
    )
    return _envelope(
        case,
        provider=item.provider,
        org_id=item.org_id,
        entity_type="feature_flag",
        source_id=item.flag_key,
        observed_at=model["last_synced"],
        attributes=_attributes(
            environment=item.environment,
            flag_type=item.flag_type,
            project_key=item.project_key,
        ),
    )


def _operational(case: dict[str, Any]) -> dict[str, Any]:
    model = case["model"]
    common = {
        "org_id": model["org_id"],
        "provider": model["provider"],
        "provider_instance_id": model["provider_instance_id"],
        "source_entity_type": model["source_entity_type"],
        "external_id": model["external_id"],
        "source_version_at": _datetime(model["source_version_at"]),
        "observed_at": _datetime(model["observed_at"]),
        "normalized_status": model["normalized_status"],
    }
    if case["shape"] == "operational_service":
        service = OperationalService(**common, name=model["name"])
        return _envelope(
            case,
            provider=service.provider,
            org_id=service.org_id,
            entity_type=service.entity_family,
            source_id=service.id,
            observed_at=model["observed_at"],
            attributes=_attributes(
                external_id=service.external_id,
                name=service.name,
                normalized_status=service.normalized_status,
                provider_instance_id=service.provider_instance_id,
            ),
        )
    incident = OperationalIncident(**common, title=model["title"])
    return _envelope(
        case,
        provider=incident.provider,
        org_id=incident.org_id,
        entity_type=incident.entity_family,
        source_id=incident.id,
        observed_at=model["observed_at"],
        attributes=_attributes(
            external_id=incident.external_id,
            normalized_status=incident.normalized_status,
            provider_instance_id=incident.provider_instance_id,
            title=incident.title,
        ),
    )


def _computed_envelope(case: dict[str, Any]) -> dict[str, Any]:
    match case["shape"]:
        case "work_item":
            return _work_item(case)
        case "feature_flag":
            return _feature_flag(case)
        case "operational_service" | "operational_incident":
            return _operational(case)
        case _:
            raise ValueError("unsupported normalized fixture shape")


def run(path: Path) -> dict[str, Any]:
    fixture = json.loads(path.read_text(encoding="utf-8"))
    if fixture.get("schema_version") != "v1" or not fixture.get("cases"):
        raise ValueError("invalid normalized provider fixture")
    output_cases: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for case in fixture["cases"]:
        if case["id"] in seen_ids:
            raise ValueError("duplicate normalized provider fixture id")
        seen_ids.add(case["id"])
        computed = _computed_envelope(case)
        if computed != case["envelope"]:
            raise ValueError(f"normalized envelope mismatch for {case['id']}")
        output_cases.append({"id": case["id"], "envelope": computed})
    return {"schema_version": fixture["schema_version"], "cases": output_cases}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("fixture", type=Path)
    args = parser.parse_args()
    try:
        output = run(args.fixture)
    except (KeyError, TypeError, ValueError):
        print("Python normalized provider fixture evaluation failed", file=sys.stderr)
        return 1
    json.dump(output, sys.stdout, separators=(",", ":"), ensure_ascii=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
