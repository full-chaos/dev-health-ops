import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from tests.docs.user_guide_contracts import (
    AI_VIEW_PAGES,
    DIAGNOSTIC_PAGES,
    FLOW_PAGES,
    REPORTS_AND_METRICS_GUIDES,
    ai_view_contract_errors,
    diagnostic_contract_errors,
    flow_contract_errors,
    reports_and_metrics_contract_errors,
)

ROOT: Final = Path(__file__).resolve().parents[2]
DOCS: Final = ROOT / "docs"
# Archived user-guide corpus: the calibrated-language and evidence-first contracts are
# characterized against the preserved source documents, not the public routes.
ARCHIVED: Final = ROOT / ".github" / "docs-legacy"
GUIDES: Final = ARCHIVED / "user-guide"
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

# Canonical public guides that must exist and be navigable under the accepted IA.
CANONICAL_NAV_ROUTES: Final = (
    "get-started/index.md",
    "use/delivery-flow/pr-flow.md",
    "use/ai-workflows/impact.md",
    "use/ai-workflows/review-load.md",
    "use/ai-workflows/risk.md",
    "use/reports/index.md",
    "reference/taxonomies/investment.md",
)


def _read_archived_guide(relative_path: str) -> str:
    return (GUIDES / relative_path).read_text(encoding="utf-8")


def _read_archived_view(page_name: str) -> str:
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


# --- Archived-source characterization -------------------------------------------------


def test_archived_product_concepts_characterization_baseline() -> None:
    concepts = (ARCHIVED / "product" / "concepts.md").read_text(encoding="utf-8")

    assert "WorkUnit is an evidence container" in concepts
    assert "Evidence quality" in concepts
    assert "Probability distributions" in concepts


def test_archived_onboarding_pages_meet_plain_language_contract() -> None:
    pages = {
        page_name: _read_archived_guide(page_name) for page_name in ONBOARDING_PAGES
    }

    assert _onboarding_contract_errors(pages) == ()


def test_archived_onboarding_negative_reports_missing_glossary_link_and_definitive_language() -> (
    None
):
    pages = {
        page_name: _read_archived_guide(page_name) for page_name in ONBOARDING_PAGES
    }
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


def test_archived_investment_journey_has_calibrated_evidence_first_language() -> None:
    journey = _read_archived_guide("journeys/investment-view.md")

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


def test_archived_onboarding_sanitized_fixture_capture_is_present() -> None:
    metadata = json.loads(
        (IMAGE_DIR / "fixture-capture-metadata.json").read_text(encoding="utf-8")
    )
    assert {capture["route"] for capture in metadata["captures"]} == {
        "/dashboard",
        "/investment",
    }
    for screenshot in ("cockpit-fixture.png", "investment-fixture.png"):
        assert (IMAGE_DIR / screenshot).is_file()


def test_archived_visualization_patterns_characterization_baseline() -> None:
    patterns = (ARCHIVED / "visualizations" / "patterns.md").read_text(encoding="utf-8")

    assert "Quadrants show raw values only" in patterns
    assert "Flames for diagnosis" in patterns
    assert "Person-to-person rankings or comparisons" in patterns


def test_archived_quadrants_flame_and_hotspots_meet_diagnostic_contract() -> None:
    pages = {
        page_name: _read_archived_view(page_name) for page_name in DIAGNOSTIC_PAGES
    }

    assert diagnostic_contract_errors(pages) == ()


def test_archived_diagnostic_negative_reports_comparison_framing_and_missing_evidence_link() -> (
    None
):
    pages = {
        page_name: _read_archived_view(page_name) for page_name in DIAGNOSTIC_PAGES
    }
    pages["quadrants.md"] = pages["quadrants.md"].replace(
        "../glossary.md", "../missing.md"
    )
    pages["flame-diagrams.md"] = f"{pages['flame-diagrams.md']}\nPercentile score.\n"

    errors = diagnostic_contract_errors(pages)

    assert "quadrants.md: missing glossary link" in errors
    assert "flame-diagrams.md: contains comparison framing 'percentile'" in errors


def test_archived_product_prd_characterization_baseline_for_flow_and_work_graph() -> (
    None
):
    prd = (ARCHIVED / "product" / "prd.md").read_text(encoding="utf-8")

    assert "Flow & constraints" in prd
    assert "Work Graph" in prd


def test_archived_pr_flow_capacity_and_work_graph_meet_user_guide_contract() -> None:
    pages = {page_name: _read_archived_view(page_name) for page_name in FLOW_PAGES}

    assert flow_contract_errors(pages) == ()
    reference = _read_archived_guide("work-graph.md")
    assert "reference role" in reference.casefold()
    assert "canonical user journey" in pages["work-graph.md"].casefold()


def test_archived_flow_negative_reports_ambiguous_planning_and_comparison_framing() -> (
    None
):
    pages = {page_name: _read_archived_view(page_name) for page_name in FLOW_PAGES}
    pages["pr-flow.md"] = pages["pr-flow.md"].replace(
        "Planned behavior", "Future behavior"
    )
    pages["capacity-planning.md"] = (
        f"{pages['capacity-planning.md']}\nTop performer leaderboard.\n"
    )

    errors = flow_contract_errors(pages)

    assert "pr-flow.md: missing flow contract 'Planned behavior'" in errors
    assert "capacity-planning.md: contains comparison framing 'leaderboard'" in errors


def test_archived_ai_views_explain_current_fields_with_calibrated_language() -> None:
    pages = {page_name: _read_archived_view(page_name) for page_name in AI_VIEW_PAGES}

    assert ai_view_contract_errors(pages) == ()


def test_archived_ai_negative_reports_definitive_and_ranking_language() -> None:
    pages = {page_name: _read_archived_view(page_name) for page_name in AI_VIEW_PAGES}
    pages["ai-impact.md"] = (
        f"{pages['ai-impact.md']}\nThe model determined the result.\n"
    )
    pages["ai-review-load.md"] = (
        f"{pages['ai-review-load.md']}\nRank individual reviewers.\n"
    )

    errors = ai_view_contract_errors(pages)

    assert (
        "ai-impact.md: contains definitive or ranking language 'determined'" in errors
    )
    assert (
        "ai-review-load.md: contains definitive or ranking language 'rank individual'"
        in errors
    )


def test_archived_ai_negative_reports_invented_fields_unlabeled_estimates_and_recomputation() -> (
    None
):
    pages = {page_name: _read_archived_view(page_name) for page_name in AI_VIEW_PAGES}
    pages["ai-impact.md"] = (
        f"{pages['ai-impact.md'].replace('**estimates**', 'estimates')}\n"
        "Leverage components estimate.\n"
    )
    pages["ai-risk.md"] = f"{pages['ai-risk.md']}\nIncident counts.\n"
    pages["ai-attribution.md"] = (
        f"{pages['ai-attribution.md']}\nThe browser recomputes a verdict.\n"
    )

    errors = ai_view_contract_errors(pages)

    assert (
        "ai-impact.md: contains invented current field 'Leverage components'" in errors
    )
    assert "ai-impact.md: estimate is not explicitly labeled" in errors
    assert "ai-risk.md: contains invented current field 'incident counts'" in errors
    assert (
        "ai-attribution.md: contains definitive or ranking language 'verdict'" in errors
    )
    assert (
        "ai-attribution.md: contains definitive or ranking language 'browser recomputes'"
        in errors
    )


def test_archived_reports_and_metrics_guides_explain_current_actions_and_interpretation() -> (
    None
):
    pages = {
        page_name: _read_archived_guide(page_name)
        for page_name in REPORTS_AND_METRICS_GUIDES
    }

    assert reports_and_metrics_contract_errors(pages) == ()


def test_archived_reports_metrics_negative_reports_jargon_missing_data_and_unlabeled_ai() -> (
    None
):
    pages = {
        page_name: _read_archived_guide(page_name)
        for page_name in REPORTS_AND_METRICS_GUIDES
    }
    pages["reports.md"] = (
        pages["reports.md"].replace(
            "**AI-generated**",
            "AI-generated",
        )
        + "\nGraphQL endpoint.\n"
    )
    pages["metrics-interpretation.md"] = (
        f"{pages['metrics-interpretation.md']}\nNull means zero.\n"
    )

    errors = reports_and_metrics_contract_errors(pages)

    assert "reports.md: AI content is not explicitly labeled" in errors
    assert "reports.md: contains implementation jargon 'GraphQL'" in errors
    assert (
        "metrics-interpretation.md: claims missing data is zero 'null means zero'"
        in errors
    )


# --- Canonical public guides ----------------------------------------------------------


def test_canonical_public_guides_exist_and_are_navigable() -> None:
    nav = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    for route in CANONICAL_NAV_ROUTES:
        assert route in nav, f"canonical route not wired in mkdocs.yml: {route}"
        assert (DOCS / route).is_file(), f"missing canonical page: {route}"


def test_canonical_public_guides_do_not_reintroduce_legacy_user_guide_routes() -> None:
    nav = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "user-guide/" not in nav, (
        "mkdocs.yml must not restore the legacy user-guide/ routes"
    )


def test_canonical_delivery_flow_pr_flow_is_evidence_first() -> None:
    pr_flow = (DOCS / "use" / "delivery-flow" / "pr-flow.md").read_text(
        encoding="utf-8"
    )
    normalized = " ".join(pr_flow.casefold().split())

    assert "evidence" in normalized
    assert "not a measure of individual performance" in normalized
    for phrase in FORBIDDEN_DEFINITIVE_LANGUAGE:
        assert phrase not in normalized, (
            f"canonical pr-flow.md contains definitive AI language {phrase!r}"
        )
