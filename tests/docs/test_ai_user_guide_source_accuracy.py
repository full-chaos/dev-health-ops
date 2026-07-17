from pathlib import Path
from typing import Final

import pytest

ROOT: Final = Path(__file__).resolve().parents[2]
AI_GUIDE_PAGES: Final = (
    "ai-impact.md",
    "ai-review-load.md",
    "ai-risk.md",
    "ai-attribution.md",
)
AI_GUIDE_SOURCE_SEMANTICS: Final = (
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


@pytest.mark.parametrize(
    ("page_name", "required_semantics"),
    AI_GUIDE_SOURCE_SEMANTICS,
    ids=("resolver-team-context", "approval-friction", "repo-incident-context"),
)
def test_ai_guides_preserve_source_accurate_semantics(
    page_name: str,
    required_semantics: tuple[str, ...],
) -> None:
    guide = (ROOT / "docs" / "user-guide" / "views" / page_name).read_text(
        encoding="utf-8"
    )
    normalized_guide = " ".join(guide.split())

    for semantic in required_semantics:
        assert semantic.casefold() in normalized_guide.casefold()


@pytest.mark.parametrize("page_name", AI_GUIDE_PAGES)
def test_ai_guides_label_ai_derived_signals_as_estimates(page_name: str) -> None:
    guide = (ROOT / "docs" / "user-guide" / "views" / page_name).read_text(
        encoding="utf-8"
    )

    assert "**estimates**" in guide


def test_ai_risk_does_not_claim_incidents_are_work_graph_rollups() -> None:
    guide = (ROOT / "docs" / "user-guide" / "views" / "ai-risk.md").read_text(
        encoding="utf-8"
    )

    assert "Work Graph rollup" not in guide
