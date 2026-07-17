import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from tests.docs.user_guide_source_accuracy import (
    fixture_truth_errors,
    guide_source_accuracy_errors,
    work_graph_search_errors,
)

ROOT: Final = Path(__file__).resolve().parents[2]
GUIDES: Final = ROOT / "docs" / "user-guide"
METADATA_PATH: Final = GUIDES / "images" / "fixture-capture-metadata.json"
SEARCH_ACCEPTANCE_PATH: Final = ROOT / "docs" / "search-acceptance.json"


def _pages() -> Mapping[str, str]:
    return {
        "pr-flow.md": (GUIDES / "views" / "pr-flow.md").read_text(encoding="utf-8"),
        "reports.md": (GUIDES / "reports.md").read_text(encoding="utf-8"),
        "metrics-interpretation.md": (GUIDES / "metrics-interpretation.md").read_text(
            encoding="utf-8"
        ),
        "first-10-minutes.md": (GUIDES / "first-10-minutes.md").read_text(
            encoding="utf-8"
        ),
        "journeys/investment-view.md": (
            GUIDES / "journeys" / "investment-view.md"
        ).read_text(encoding="utf-8"),
    }


def _fixture_states() -> Mapping[str, str]:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    return {
        str(capture["route"]): str(capture["rendered_state"])
        for capture in metadata["captures"]
    }


def _work_graph_search_entry() -> Mapping[str, str]:
    acceptance = json.loads(SEARCH_ACCEPTANCE_PATH.read_text(encoding="utf-8"))
    return next(
        entry for entry in acceptance["queries"] if entry["query"] == "Work Graph"
    )


def test_guide_copy_matches_current_product_and_fixture_surfaces() -> None:
    pages = _pages()

    assert guide_source_accuracy_errors(pages) == ()
    assert fixture_truth_errors(_fixture_states(), pages) == ()
    assert work_graph_search_errors(_work_graph_search_entry()) == ()


def test_source_accuracy_contract_rejects_each_regression() -> None:
    pages = dict(_pages())
    pages["pr-flow.md"] = "PR stages report review latency and merge timing."
    pages["reports.md"] = "Custom cron schedule with timezone and provenance."
    pages["metrics-interpretation.md"] = (
        "Bus factor needs at least half of recent churn."
    )
    pages["first-10-minutes.md"] = "Cockpit fixture"
    pages["journeys/investment-view.md"] = "Investment fixture"
    fixture_states = {"/dashboard": "empty cockpit", "/investment": "allocation"}
    search_entry = {
        "query": "Work Graph View",
        "canonical_url": "/user-guide/work-graph/",
    }

    guide_errors = guide_source_accuracy_errors(pages)
    fixture_errors = fixture_truth_errors(fixture_states, pages)
    search_errors = work_graph_search_errors(search_entry)

    assert "pr-flow.md: contains unsupported claim 'review latency'" in guide_errors
    assert (
        "reports.md: contains unavailable Report Center control 'cron'" in guide_errors
    )
    assert "reports.md: overstates completed-report provenance surface" in guide_errors
    assert "metrics-interpretation.md: missing 80% bus-factor threshold" in guide_errors
    assert (
        "fixture metadata: /dashboard must record 'populated cockpit with source-connection prompt'"
        in fixture_errors
    )
    assert (
        "journeys/investment-view.md: missing fixture state disclosure 'Team-plan availability gate'"
        in fixture_errors
    )
    assert "search acceptance: Work Graph query is not exact" in search_errors
