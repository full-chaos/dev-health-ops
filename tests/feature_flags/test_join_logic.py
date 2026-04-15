from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.parametrize("provider", ["github", "gitlab", "generic"])
def test_join_logic_fixture_covers_all_provider_paths(
    provider: str, join_provider_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: line 387
    case = join_provider_case_map[provider]
    expected = case["expected"]

    assert set(join_provider_case_map) == {"github", "gitlab", "generic"}
    assert expected["release_ref"]
    assert expected["environment"]
    assert expected["confidence"] in {1.0, 0.3}


@pytest.mark.skip(reason="Awaiting CHAOS-820 implementation for provider join chain")
@pytest.mark.parametrize("provider", ["github", "gitlab", "generic"])
def test_join_logic_maps_issue_pr_deployment_to_release_ref(
    provider: str, join_provider_case_map: dict[str, dict[str, Any]]
) -> None:
    # PRD: line 387
    case = join_provider_case_map[provider]

    # Replace this with the concrete join entry point once Phase 0 lands.
    result = case["input"]

    assert result["environment"] == case["expected"]["environment"]
    assert case["expected"]["release_ref"]
