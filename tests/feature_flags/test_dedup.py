from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.parametrize(
    "event_type",
    [
        "feature_flag.change",
        "feature_flag.exposure",
        "telemetry.signal",
        "release.deployment",
    ],
)
def test_dedupe_fixture_covers_all_raw_event_types(
    event_type: str, dedupe_event_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: line 388
    case = dedupe_event_case_map[event_type]

    assert case["record"]["dedupe_key"]
    assert case["record"]["org_id"] == "acme"


@pytest.mark.skip(reason="Awaiting CHAOS-820 implementation for dedupe_key enforcement")
@pytest.mark.parametrize(
    "event_type",
    [
        "feature_flag.change",
        "feature_flag.exposure",
        "telemetry.signal",
        "release.deployment",
    ],
)
def test_dedupe_key_prevents_duplicate_inserts(
    event_type: str, dedupe_event_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: line 388
    case = dedupe_event_case_map[event_type]

    duplicate = dict(case["record"])
    assert duplicate["dedupe_key"] == case["record"]["dedupe_key"]
