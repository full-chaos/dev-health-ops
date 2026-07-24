from pathlib import Path
from typing import Final

import pytest

ROOT: Final = Path(__file__).resolve().parents[2]

# Archived AI view sources. Detailed field/semantic accuracy is characterized against
# the preserved source documents, including AI Attribution, which was intentionally
# withheld from the approved public IA and therefore has no canonical public page.
ARCHIVED_AI_VIEWS: Final = ROOT / ".github" / "docs-legacy" / "user-guide" / "views"
ARCHIVED_AI_GUIDE_PAGES: Final = (
    "ai-impact.md",
    "ai-review-load.md",
    "ai-risk.md",
    "ai-attribution.md",
)
ARCHIVED_AI_GUIDE_SOURCE_SEMANTICS: Final = (
    (
        "ai-attribution.md",
        (
            "resolver enriches the `team_id` context when the view is queried",
            "MANUAL > PR_LABEL > BOT_AUTHOR > COMMIT_TRAILER > CI_ANNOTATION > BRANCH_NAME > PR_BODY",
            "source-specific confidence from `0.0` to `1.0`",
        ),
    ),
    (
        "ai-review-load.md",
        ("changesRequestedPerPr / reviewsPerPr",),
    ),
    (
        "ai-risk.md",
        (
            "incidentsCount",
            "all incidents that started in the selected window for the same repository",
            "not a Work Graph linked-incident rollup",
        ),
    ),
)

# Canonical published AI workflow guides. AI Attribution is intentionally absent.
CANONICAL_AI_WORKFLOWS: Final = ROOT / "docs" / "use" / "ai-workflows"
CANONICAL_AI_GUIDE_PAGES: Final = (
    "impact.md",
    "review-load.md",
    "risk.md",
)
# Paths that AI Attribution must never occupy in the public tree.
WITHHELD_PUBLIC_ATTRIBUTION_PAGES: Final = (
    CANONICAL_AI_WORKFLOWS / "attribution.md",
    CANONICAL_AI_WORKFLOWS / "ai-attribution.md",
)
CANONICAL_CALIBRATED_PHRASES: Final = {
    "impact.md": (
        "an association can appear",
        "not a productivity label, forecast, or causal experiment",
    ),
    "review-load.md": (
        "does not establish that ai caused review pressure",
        "rather than a person-level judgment",
    ),
    "risk.md": (
        "not proof of policy violation",
        "derived or model-assisted risk estimates",
    ),
}
FORBIDDEN_DEFINITIVE_AI_LANGUAGE: Final = ("determined", "detected", "verdict")


@pytest.mark.parametrize(
    ("page_name", "required_semantics"),
    ARCHIVED_AI_GUIDE_SOURCE_SEMANTICS,
    ids=("resolver-team-context", "approval-friction", "repo-incident-context"),
)
def test_archived_ai_guides_preserve_source_accurate_semantics(
    page_name: str,
    required_semantics: tuple[str, ...],
) -> None:
    guide = (ARCHIVED_AI_VIEWS / page_name).read_text(encoding="utf-8")
    normalized_guide = " ".join(guide.split())

    for semantic in required_semantics:
        assert semantic.casefold() in normalized_guide.casefold()


@pytest.mark.parametrize("page_name", ARCHIVED_AI_GUIDE_PAGES)
def test_archived_ai_guides_label_ai_derived_signals_as_estimates(
    page_name: str,
) -> None:
    guide = (ARCHIVED_AI_VIEWS / page_name).read_text(encoding="utf-8")

    assert "**estimates**" in guide


def test_archived_ai_risk_does_not_claim_incidents_are_work_graph_rollups() -> None:
    guide = (ARCHIVED_AI_VIEWS / "ai-risk.md").read_text(encoding="utf-8")

    assert "Work Graph rollup" not in guide


@pytest.mark.parametrize("page_name", CANONICAL_AI_GUIDE_PAGES)
def test_canonical_ai_workflow_guides_exist(page_name: str) -> None:
    assert (CANONICAL_AI_WORKFLOWS / page_name).is_file(), (
        f"missing canonical AI workflow guide: {page_name}"
    )


@pytest.mark.parametrize("page_name", CANONICAL_AI_GUIDE_PAGES)
def test_canonical_ai_workflow_guides_use_calibrated_non_causal_language(
    page_name: str,
) -> None:
    guide = (CANONICAL_AI_WORKFLOWS / page_name).read_text(encoding="utf-8")
    normalized = " ".join(guide.casefold().split())

    for phrase in CANONICAL_CALIBRATED_PHRASES[page_name]:
        assert phrase in normalized, (
            f"{page_name}: missing calibrated, non-causal phrase {phrase!r}"
        )
    for forbidden in FORBIDDEN_DEFINITIVE_AI_LANGUAGE:
        assert forbidden not in normalized, (
            f"{page_name}: contains definitive AI language {forbidden!r}"
        )


def test_ai_attribution_has_no_public_page() -> None:
    """AI Attribution was intentionally withheld from the approved public IA.

    It must be validated only through its archived source and must never be
    republished as a public page merely to satisfy a test.
    """
    for page in WITHHELD_PUBLIC_ATTRIBUTION_PAGES:
        assert not page.exists(), (
            f"AI Attribution must not be republished as a public page: {page}"
        )
