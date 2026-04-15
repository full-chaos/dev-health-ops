from __future__ import annotations

from typing import Any

import pytest


def test_smoke_fixture_covers_append_only_and_org_isolation(
    append_only_case: dict[str, Any], org_isolation_case: dict[str, Any]
) -> None:
    # PRD: lines 399-400
    assert (
        append_only_case["first"]["computed_at"]
        < append_only_case["second"]["computed_at"]
    )
    assert (
        org_isolation_case["acme_record"]["org_id"]
        != org_isolation_case["globex_record"]["org_id"]
    )


@pytest.mark.parametrize("case_name", ["trim_and_lower", "staging_casefold"])
def test_environment_normalization_fixture_covers_smoke_inputs(
    case_name: str, environment_normalization_case_map: dict[str, dict[str, str]]
) -> None:
    # PRD: line 401
    case = environment_normalization_case_map[case_name]

    assert case["expected_environment"] == case["telemetry_environment"].strip().lower()


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for append-only backfill verification"
)
def test_backfill_writes_append_only_rows_with_computed_at(
    append_only_case: dict[str, Any],
) -> None:
    # PRD: line 399
    assert (
        append_only_case["first"]["computed_at"]
        < append_only_case["second"]["computed_at"]
    )


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for org_id isolation queries"
)
def test_cross_org_queries_return_no_results(
    org_isolation_case: dict[str, Any],
) -> None:
    # PRD: line 400
    assert (
        org_isolation_case["query_org_id"]
        == org_isolation_case["acme_record"]["org_id"]
    )


@pytest.mark.skip(
    reason="Awaiting CHAOS-820 implementation for environment normalization"
)
@pytest.mark.parametrize("case_name", ["trim_and_lower", "staging_casefold"])
def test_environment_strings_match_between_deployments_and_telemetry(
    case_name: str, environment_normalization_case_map: dict[str, dict[str, str]]
) -> None:
    # PRD: line 401
    case = environment_normalization_case_map[case_name]

    assert case["deployment_environment"]
