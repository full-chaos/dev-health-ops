import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
MKDOCS_PATH = ROOT / "mkdocs.yml"
INDEX_PATH = ROOT / "docs" / "index.md"
TEMPLATE_PATH = ROOT / "docs" / "overrides" / "main.html"
TEMPLATES_DIR = ROOT / "docs" / "overrides"
SEARCH_ACCEPTANCE_PATH = ROOT / "docs" / "search-acceptance.json"
FRESHNESS_INVENTORY_PATH = ROOT / "docs" / "freshness-inventory.yml"

AUDIENCE_SECTIONS = (
    "Start here",
    "Use Dev Health",
    "Administer & integrate",
    "Deploy & operate",
    "Develop & extend",
    "Reference & history",
)

TASK_CALLS_TO_ACTION = {
    "Understand the product": "product/concepts.md",
    "Use the app": "user-guide/views-index.md",
    "Self-host Dev Health": "self-hosted-quickstart.md",
    "Integrate data": "customer-push-ingestion/overview.md",
    "Use the API": "api/graphql-overview.md",
    "Contribute": "contributing/platform-contract.md",
}

FRESHNESS_PATHS = (
    "product/concepts.md",
    "user-guide/views-index.md",
    "user-guide/views/investment-mix.md",
    "user-guide/views/investment-flows.md",
    "user-guide/views/investment-expense.md",
    "user-guide/views/code-hotspots.md",
    "user-guide/views/pr-flow.md",
    "user-guide/views/quadrants.md",
    "user-guide/views/flame-diagrams.md",
    "user-guide/views/capacity-planning.md",
    "user-guide/views/work-graph.md",
    "user-guide/views/ai-impact.md",
    "user-guide/views/ai-review-load.md",
    "user-guide/views/ai-risk.md",
    "user-guide/views/ai-attribution.md",
)


def test_navigation_has_exactly_the_six_public_audience_sections() -> None:
    config = yaml.load(MKDOCS_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    nav = config["nav"]

    assert isinstance(nav, list)
    assert tuple(item for section in nav for item in section) == AUDIENCE_SECTIONS
    assert not re.search(
        r"CHAOS-\d+|\.md", " ".join(item for section in nav for item in section)
    )


def test_landing_page_offers_each_task_call_to_action_in_its_first_viewport() -> None:
    index = INDEX_PATH.read_text(encoding="utf-8")
    first_viewport = index.split("##", maxsplit=1)[0]

    for label, destination in TASK_CALLS_TO_ACTION.items():
        assert f"[{label}]({destination})" in first_viewport


def test_page_template_exposes_metadata_and_next_step_paths() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    for field in ("audience", "canonical", "owner", "last-reviewed"):
        assert f"metadata.get('{field}')" in template
    for path in ("Next step", "Troubleshooting", "Send feedback"):
        assert path in template
    for template_name in ("guide.html", "reference.html", "troubleshooting.html"):
        assert (TEMPLATES_DIR / template_name).is_file()


def test_search_acceptance_set_has_twenty_canonical_queries_with_synonyms() -> None:
    search_set = json.loads(SEARCH_ACCEPTANCE_PATH.read_text(encoding="utf-8"))

    assert len(search_set["queries"]) == 20
    for query in search_set["queries"]:
        assert query["query"]
        assert query["canonical_url"].startswith("/")
        assert query["synonyms"]


def test_freshness_inventory_has_one_disposition_for_each_todo_five_page() -> None:
    entries = yaml.safe_load(FRESHNESS_INVENTORY_PATH.read_text(encoding="utf-8"))
    expected_paths = set(FRESHNESS_PATHS)
    actual_paths = [entry["page"] for entry in entries]

    assert set(actual_paths) == expected_paths
    assert len(actual_paths) == len(set(actual_paths))
    for entry in entries:
        assert entry["disposition"] in {"refresh", "retire"}
        assert entry["owner"]
        assert entry["last-reviewed"]
        if entry["disposition"] == "retire":
            assert entry["replacement"]
