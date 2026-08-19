#!/usr/bin/env python3
"""Execute the live production sync-coverage payload builder for one case."""

from __future__ import annotations

import contextlib
import json
import pathlib
import sys
from datetime import datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

if TYPE_CHECKING:
    from dev_health_ops.models.integrations import IntegrationSource
    from dev_health_ops.models.settings import ScheduledJob, SyncConfiguration


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _namespace_config(case: dict[str, Any]) -> SyncConfiguration:
    raw = case["config"]
    return cast(
        "SyncConfiguration",
        SimpleNamespace(
            id=UUID(raw["id"]),
            org_id=raw["org_id"],
            provider=raw["provider"],
            is_active=raw["is_active"],
            integration_id=UUID(raw["integration_id"]),
            source_id=UUID(raw["source_id"]) if raw["source_id"] else None,
        ),
    )


def main() -> int:
    if len(sys.argv) != 2:
        return 2

    # The import is deliberately the normal production import under PYTHONPATH,
    # not a copied implementation or a fixture module. Some package initializers
    # log while importing, so route that diagnostic output away from the JSON
    # protocol on stdout.
    with contextlib.redirect_stdout(sys.stderr):
        from dev_health_ops.api.services import sync_coverage as production

    case = json.loads(pathlib.Path(sys.argv[1]).read_text())
    config = _namespace_config(case)
    scope = production.EffectiveScope(
        integration_id=UUID(case["config"]["integration_id"]),
        sources=tuple(
            cast(
                "IntegrationSource",
                SimpleNamespace(
                    id=UUID(source["id"]),
                    name=source["name"],
                    full_name=source["full_name"],
                ),
            )
            for source in case["scope"]["sources"]
        ),
        dataset_keys=tuple(case["scope"]["dataset_keys"]),
    )
    windows = tuple(
        production.UnitWindow(
            since=_datetime(window["since"]),
            before=_datetime(window["before"]),
            source_id=window["source_id"],
            dataset_key=window["dataset_key"],
            run_id=window["run_id"],
            status=window["status"],
            run_time=_datetime(window["run_time"]),
        )
        for window in case["windows"]
    )
    backfills = tuple(
        production.CoverageInterval(
            since=_datetime(interval["since"]),
            before=_datetime(interval["before"]),
            source_ids=tuple(interval["source_ids"]),
            run_ids=tuple(interval["run_ids"]),
            dataset_keys=tuple(interval["dataset_keys"]),
        )
        for interval in case["backfill_requested"]
    )
    schedule = cast(
        "ScheduledJob",
        SimpleNamespace(
            schedule_cron=case["schedule"]["schedule_cron"],
            next_run_at=_datetime(case["schedule"]["next_run_at"]),
        ),
    )
    payload = production.build_coverage_summary_payload(
        config=config,
        scope=scope,
        windows=windows,
        backfill_requested=backfills,
        active_pairs={tuple(pair) for pair in case["active_pairs"]},
        active_schedule=schedule,
        has_schedule_row=case["has_schedule_row"],
        generated_at=_datetime(case["generated_at"]),
        lookback_days=case["lookback_days"],
        latest_successful_run_at_override=_datetime(case["latest_successful_run_at"]),
        is_truncated=case["is_truncated"],
        not_enabled_dataset_keys=production._not_enabled_dataset_keys(config, scope),
    )
    encoded = production.jsonable_encoder(payload)
    json.dump(encoded, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
