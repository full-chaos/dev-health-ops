"""Navigation and landing-page contract for the accepted task-oriented IA.

Navigation is organized by durable reader task, not by the superseded
six-audience buckets. The root ``/`` is the task router and its first
viewport surfaces the accepted primary tasks directly, so they are
reachable without search.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
MKDOCS_PATH = ROOT / "mkdocs.yml"
DOCS_DIR = ROOT / "docs"
INDEX_PATH = DOCS_DIR / "index.md"

# The accepted top-level task domains, in navigation order, after the home
# router entry. These mirror the canonical IA task domains.
TASK_DOMAIN_SECTIONS = (
    "Get started",
    "Use Dev Health",
    "Administer Dev Health",
    "Install and operate",
    "Integrate and extend",
    "Reference",
    "Contribute",
)

# The accepted first-viewport tasks on the home router: Use Dev Health,
# Reference, Investment investigation, no-data troubleshooting, and
# weighting/aggregation.
FIRST_VIEWPORT_TASKS = {
    "Use Dev Health": "use/index.md",
    "Browse reference": "reference/index.md",
    "Investigate where effort appears to be going": (
        "use/investment/investigate-effort.md"
    ),
    "Diagnose no or incomplete data": "use/troubleshooting/no-or-incomplete-data.md",
    "Look up weighting and aggregation": (
        "reference/metrics/weighting-and-aggregation.md"
    ),
}

# Routes and buckets from the superseded architecture that must not return.
LEGACY_ROUTES = (
    "user-guide/",
    "product/concepts.md",
    "contributing/platform-contract.md",
    "customer-push-ingestion/",
    "self-hosted-quickstart.md",
    "api/graphql-overview.md",
)

LEGACY_AUDIENCE_SECTIONS = (
    "Start here",
    "Administer & integrate",
    "Deploy & operate",
    "Develop & extend",
    "Reference & history",
)


def _load_nav() -> list[dict[str, object]]:
    config = yaml.safe_load(MKDOCS_PATH.read_text(encoding="utf-8"))
    nav = config["nav"]
    assert isinstance(nav, list)
    return nav


def _top_level_labels(nav: list[dict[str, object]]) -> list[str]:
    labels: list[str] = []
    for item in nav:
        assert isinstance(item, dict) and len(item) == 1, item
        labels.append(next(iter(item)))
    return labels


def test_navigation_is_task_oriented_not_audience_bucketed() -> None:
    nav = _load_nav()
    labels = _top_level_labels(nav)

    # The home router comes first and points at the index task page.
    assert labels[0] == "Documentation home"
    assert nav[0]["Documentation home"] == "index.md"

    # The remaining top level is exactly the accepted task domains, in order.
    assert tuple(labels[1:]) == TASK_DOMAIN_SECTIONS

    # None of the superseded six-audience buckets survive.
    for legacy_section in LEGACY_AUDIENCE_SECTIONS:
        assert legacy_section not in labels


def test_navigation_and_home_do_not_reference_legacy_routes() -> None:
    nav_text = MKDOCS_PATH.read_text(encoding="utf-8")
    index_text = INDEX_PATH.read_text(encoding="utf-8")

    for legacy in LEGACY_ROUTES:
        assert legacy not in nav_text, legacy
        assert legacy not in index_text, legacy


def test_home_first_viewport_offers_each_accepted_task() -> None:
    index = INDEX_PATH.read_text(encoding="utf-8")
    # The first viewport is the hero router, before the first H2 section.
    first_viewport = index.split("\n## ", maxsplit=1)[0]

    for label, destination in FIRST_VIEWPORT_TASKS.items():
        assert f"[{label}]({destination})" in first_viewport, label


def test_first_viewport_task_destinations_exist_as_canonical_pages() -> None:
    for destination in FIRST_VIEWPORT_TASKS.values():
        assert (DOCS_DIR / destination).is_file(), destination
