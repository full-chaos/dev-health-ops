import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from tests.docs.user_guide_contracts import DIAGNOSTIC_PAGES, diagnostic_contract_errors

ROOT: Final = Path(__file__).resolve().parents[2]
DOCS: Final = ROOT / "docs"
GUIDES: Final = DOCS / "user-guide"
IMAGE_DIR: Final = GUIDES / "images"

ONBOARDING_PAGES: Final = {
    "first-10-minutes.md": (
        "Cockpit",
        "navigation",
        "chart",
        "confidence",
        "evidence",
        "help",
        "operating modes",
        "trends over absolutes",
        "signals not judgment",
    ),
    "how-to-read-dev-health.md": (
        "WorkUnit",
        "theme",
        "subcategory",
        "churn",
        "cycle time",
        "lead time",
        "throughput",
        "WIP",
        "after-hours ratio",
        "bus factor",
        "evidence quality",
    ),
    "glossary.md": (
        "WorkUnit",
        "theme",
        "subcategory",
        "cycle time",
        "lead time",
        "throughput",
        "WIP",
        "after-hours ratio",
        "bus factor",
        "evidence quality",
    ),
}
FORBIDDEN_USER_JARGON: Final = ("GraphQL", "ClickHouse", "dev-hops")
FORBIDDEN_DEFINITIVE_LANGUAGE: Final = ("detected", "determined")


def _read_guide(relative_path: str) -> str:
    return (GUIDES / relative_path).read_text(encoding="utf-8")


def _read_view(page_name: str) -> str:
    return (GUIDES / "views" / page_name).read_text(encoding="utf-8")


def _onboarding_contract_errors(pages: Mapping[str, str]) -> tuple[str, ...]:
    errors: list[str] = []
    for page_name, required_terms in ONBOARDING_PAGES.items():
        content = pages[page_name]
        for term in required_terms:
            if term.casefold() not in content.casefold():
                errors.append(f"{page_name}: missing required term {term!r}")
        for phrase in FORBIDDEN_USER_JARGON:
            if phrase.casefold() in content.casefold():
                errors.append(f"{page_name}: contains developer jargon {phrase!r}")
        for phrase in FORBIDDEN_DEFINITIVE_LANGUAGE:
            if phrase.casefold() in content.casefold():
                errors.append(
                    f"{page_name}: contains definitive AI language {phrase!r}"
                )

    reading_guide = pages["how-to-read-dev-health.md"]
    if "[glossary](glossary.md)" not in reading_guide.casefold():
        errors.append("how-to-read-dev-health.md: missing glossary link")
    return tuple(errors)


def test_product_concepts_characterization_baseline() -> None:
    concepts = (DOCS / "product" / "concepts.md").read_text(encoding="utf-8")

    assert "WorkUnit is an evidence container" in concepts
    assert "Evidence quality" in concepts
    assert "Probability distributions" in concepts


def test_onboarding_pages_meet_plain_language_contract() -> None:
    pages = {page_name: _read_guide(page_name) for page_name in ONBOARDING_PAGES}

    assert _onboarding_contract_errors(pages) == ()


def test_onboarding_negative_reports_missing_glossary_link_and_definitive_language() -> (
    None
):
    pages = {page_name: _read_guide(page_name) for page_name in ONBOARDING_PAGES}
    pages["how-to-read-dev-health.md"] = pages["how-to-read-dev-health.md"].replace(
        "[Glossary](glossary.md)",
        "glossary",
    )
    pages["first-10-minutes.md"] = (
        f"{pages['first-10-minutes.md']}\nThe system detected a pattern.\n"
    )

    errors = _onboarding_contract_errors(pages)

    assert "how-to-read-dev-health.md: missing glossary link" in errors
    assert "first-10-minutes.md: contains definitive AI language 'detected'" in errors


def test_investment_journey_has_calibrated_evidence_first_language() -> None:
    journey = _read_guide("journeys/investment-view.md")

    for term in (
        "Feature Delivery",
        "Operational / Support",
        "Maintenance / Tech Debt",
        "Quality / Reliability",
        "Risk / Security",
        "appears",
        "leans",
        "suggests",
        "evidence quality",
    ):
        assert term.casefold() in journey.casefold()
    for phrase in FORBIDDEN_DEFINITIVE_LANGUAGE:
        assert phrase not in journey.casefold()


def test_onboarding_navigation_and_sanitized_fixture_capture_are_present() -> None:
    nav = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    for page in (
        "user-guide/first-10-minutes.md",
        "user-guide/how-to-read-dev-health.md",
        "user-guide/glossary.md",
        "user-guide/journeys/investment-view.md",
    ):
        assert page in nav

    metadata = json.loads(
        (IMAGE_DIR / "fixture-capture-metadata.json").read_text(encoding="utf-8")
    )
    assert {capture["route"] for capture in metadata["captures"]} == {
        "/dashboard",
        "/investment",
    }
    for screenshot in ("cockpit-fixture.png", "investment-fixture.png"):
        assert (IMAGE_DIR / screenshot).is_file()


def test_visualization_patterns_characterization_baseline() -> None:
    patterns = (DOCS / "visualizations" / "patterns.md").read_text(encoding="utf-8")

    assert "Quadrants show raw values only" in patterns
    assert "Flames for diagnosis" in patterns
    assert "Person-to-person rankings or comparisons" in patterns


def test_quadrants_flame_and_hotspots_meet_diagnostic_contract() -> None:
    pages = {page_name: _read_view(page_name) for page_name in DIAGNOSTIC_PAGES}

    assert diagnostic_contract_errors(pages) == ()


def test_diagnostic_negative_reports_comparison_framing_and_missing_evidence_link() -> (
    None
):
    pages = {page_name: _read_view(page_name) for page_name in DIAGNOSTIC_PAGES}
    pages["quadrants.md"] = pages["quadrants.md"].replace(
        "../glossary.md", "../missing.md"
    )
    pages["flame-diagrams.md"] = f"{pages['flame-diagrams.md']}\nPercentile score.\n"

    errors = diagnostic_contract_errors(pages)

    assert "quadrants.md: missing glossary link" in errors
    assert "flame-diagrams.md: contains comparison framing 'percentile'" in errors
