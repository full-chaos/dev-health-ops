"""Canonical AI-workflow guide guards.

The archived-corpus semantic/field-accuracy tests read
the legacy ``user-guide/views/ai-*.md`` corpus. Those pages were deleted, and the
live ``docs/use/ai-workflows/**`` rewrite carries none of the asserted implementation
detail (the ``MANUAL > PR_LABEL > ...`` attribution precedence chain, the
``changesRequestedPerPr / reviewsPerPr`` formula, ``incidentsCount`` semantics, or the
``**estimates**`` bold label) -- verified absent by grep across ``docs/``. Those tests
were removed rather than repointed with loosened assertions. AI Attribution still has no
public page by design, and that remains asserted below.
"""

from pathlib import Path
from typing import Final

import pytest

ROOT: Final = Path(__file__).resolve().parents[2]

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

    It must never be republished as a public page merely to satisfy a test.
    """
    for page in WITHHELD_PUBLIC_ATTRIBUTION_PAGES:
        assert not page.exists(), (
            f"AI Attribution must not be republished as a public page: {page}"
        )
