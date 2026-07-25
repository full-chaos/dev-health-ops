import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from tests.docs.user_guide_source_accuracy import (
    PR_FLOW_UNSUPPORTED_CLAIMS,
    fixture_truth_errors,
    guide_source_accuracy_errors,
    work_graph_search_errors,
)

ROOT: Final = Path(__file__).resolve().parents[2]
# Source-accuracy characterization runs against the preserved user-guide corpus, not
# the public routes. These archived source documents are the record the migration
# preserved verbatim.
ARCHIVED_GUIDES: Final = ROOT / ".github" / "docs-legacy" / "user-guide"
ARCHIVED_METADATA_PATH: Final = (
    ARCHIVED_GUIDES / "images" / "fixture-capture-metadata.json"
)
ARCHIVED_SEARCH_ACCEPTANCE_PATH: Final = (
    ROOT / ".github" / "docs-legacy" / "search-acceptance.json"
)
# Canonical public PR Flow guide.
CANONICAL_PR_FLOW: Final = ROOT / "docs" / "use" / "delivery-flow" / "pr-flow.md"


def _archived_pages() -> Mapping[str, str]:
    return {
        "pr-flow.md": (ARCHIVED_GUIDES / "views" / "pr-flow.md").read_text(
            encoding="utf-8"
        ),
        "reports.md": (ARCHIVED_GUIDES / "reports.md").read_text(encoding="utf-8"),
        "metrics-interpretation.md": (
            ARCHIVED_GUIDES / "metrics-interpretation.md"
        ).read_text(encoding="utf-8"),
        "first-10-minutes.md": (ARCHIVED_GUIDES / "first-10-minutes.md").read_text(
            encoding="utf-8"
        ),
        "journeys/investment-view.md": (
            ARCHIVED_GUIDES / "journeys" / "investment-view.md"
        ).read_text(encoding="utf-8"),
    }


def _archived_fixture_states() -> Mapping[str, str]:
    metadata = json.loads(ARCHIVED_METADATA_PATH.read_text(encoding="utf-8"))
    return {
        str(capture["route"]): str(capture["rendered_state"])
        for capture in metadata["captures"]
    }


def _archived_work_graph_search_entry() -> Mapping[str, str]:
    acceptance = json.loads(ARCHIVED_SEARCH_ACCEPTANCE_PATH.read_text(encoding="utf-8"))
    return next(
        entry for entry in acceptance["queries"] if entry["query"] == "Work Graph"
    )


def test_archived_guide_copy_matches_source_product_and_fixture_surfaces() -> None:
    pages = _archived_pages()

    assert guide_source_accuracy_errors(pages) == ()
    assert fixture_truth_errors(_archived_fixture_states(), pages) == ()
    assert work_graph_search_errors(_archived_work_graph_search_entry()) == ()


def test_canonical_pr_flow_is_source_accurate_and_calibrated() -> None:
    guide = CANONICAL_PR_FLOW.read_text(encoding="utf-8")
    normalized = " ".join(guide.casefold().split())

    # The current Flow surface must be described accurately.
    assert "state flow" in normalized
    assert "sankey" in normalized
    assert "work-item state transitions" in normalized
    # No unsupported PR-stage / latency / merge-timing claims may leak into the page.
    for claim in PR_FLOW_UNSUPPORTED_CLAIMS:
        assert claim.casefold() not in normalized, (
            f"canonical pr-flow.md contains unsupported claim {claim!r}"
        )
    # Evidence-first, non-ranking framing must be preserved.
    assert "individual performance" in normalized


def test_source_accuracy_contract_rejects_each_regression() -> None:
    pages = dict(_archived_pages())
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
