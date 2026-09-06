from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from dev_health_ops import cli
from dev_health_ops.metrics import job_compounding_risk


@pytest.mark.asyncio
async def test_compounding_risk_accepts_shared_date_backfill_flags(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_run_compounding_risk_job(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(
        job_compounding_risk,
        "run_compounding_risk_job",
        fake_run_compounding_risk_job,
    )
    monkeypatch.setattr(
        job_compounding_risk,
        "resolve_sink_uri",
        lambda ns: ns.analytics_db,
    )

    parser = cli.build_parser()
    ns = parser.parse_args(
        [
            "metrics",
            "compounding-risk",
            "--analytics-db",
            "clickhouse://user:pass@localhost:8123/default",
            "--org",
            "00000000-0000-0000-0000-000000000000",
            "--before",
            "2025-02-02",
            "--backfill",
            "7",
        ]
    )
    cli._resolve_org(ns)

    assert await ns.func(ns) == 0
    assert captured == {
        "db_url": "clickhouse://user:pass@localhost:8123/default",
        "day": date(2025, 2, 1),
        "backfill_days": 7,
        "org_id": "00000000-0000-0000-0000-000000000000",
    }


def test_compounding_risk_backfill_range_is_same_size_as_other_metrics():
    assert job_compounding_risk._date_range(date(2025, 2, 1), 7) == [
        date(2025, 1, 26),
        date(2025, 1, 27),
        date(2025, 1, 28),
        date(2025, 1, 29),
        date(2025, 1, 30),
        date(2025, 1, 31),
        date(2025, 2, 1),
    ]


def test_compounding_risk_rejects_removed_day_alias():
    parser = cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["metrics", "compounding-risk", "--day", "2025-02-01"])
