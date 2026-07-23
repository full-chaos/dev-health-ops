from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from dev_health_ops.metrics.remaining_scope_contract import (
    SCOPE_MODELS,
    SCOPE_VERSION,
    canonical_scope,
)

FIXTURE = Path(__file__).parents[2] / "contracts/metrics/v1/remaining-scopes.json"


def test_python_scopes_match_go_golden() -> None:
    golden = json.loads(FIXTURE.read_text())

    assert golden["schema_version"] == SCOPE_VERSION
    assert {case["family"] for case in golden["cases"]} == set(SCOPE_MODELS)
    for case in golden["cases"]:
        assert canonical_scope(case["family"], case["input"]) == case["canonical"]


@pytest.mark.parametrize(
    ("family", "scope"),
    [
        (
            "capacity",
            {
                "version": 1,
                "all_teams": True,
                "history_days": 90,
                "simulations": 10_000,
                "command": "unsafe",
            },
        ),
        (
            "capacity",
            {
                "version": 1,
                "all_teams": False,
                "work_scope_id": "",
                "history_days": 90,
                "simulations": 10_000,
            },
        ),
        (
            "complexity",
            {"version": 1, "day": "2026-07-23", "backfill_days": 2},
        ),
        (
            "membership_backfill",
            {
                "version": 1,
                "repo_ids": [
                    "55555555-5555-4555-8555-555555555555",
                    "55555555-5555-4555-8555-555555555555",
                ],
            },
        ),
        (
            "extra_metrics",
            {
                "version": 1,
                "day": "2026-07-23",
                "backfill_days": 1,
                "sink": "postgres",
                "provider": "all",
            },
        ),
        (
            "team_metrics",
            {
                "version": 1,
                "day": "2026-07-23",
                "backfill_days": 1,
                "sink": "auto",
                "provider": "linear",
            },
        ),
    ],
)
def test_python_scopes_reject_drift(family: str, scope: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        canonical_scope(family, scope)


def test_python_scopes_reject_unknown_family() -> None:
    with pytest.raises(ValueError, match="unknown remaining metrics family"):
        canonical_scope("command", {"version": 1})
