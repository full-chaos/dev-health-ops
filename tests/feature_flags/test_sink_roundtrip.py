from __future__ import annotations

from typing import Any

import pytest


def test_sink_round_trip_fixture_preserves_queryable_fields(
    sink_round_trip_case: dict[str, Any],
) -> None:
    # PRD: line 396
    record = sink_round_trip_case["record"]
    query = sink_round_trip_case["query"]

    assert query["org_id"] == record["org_id"]
    assert query["release_ref"] == record["release_ref"]
    assert query["environment"] == record["environment"]


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for release impact sink round-trip"
)
def test_sink_round_trip_writes_and_reads_back_same_fields(
    sink_round_trip_case: dict[str, Any],
) -> None:
    # PRD: line 396
    record = sink_round_trip_case["record"]

    assert record["computed_at"] is not None
