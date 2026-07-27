"""Canonical user-guide route and language guards.

The archived legacy ``user-guide/**`` corpus that the calibrated-language,
plain-language, diagnostic and reports contracts were characterized against was deleted
with the legacy tree. The live ``docs/use/**`` pages are a rewrite with a different
structure (no glossary page, no per-view "Purpose / How to read / Caveats" sections, no
formula-level field detail), so those contracts could not be repointed without weakening
every assertion; they were removed instead. What remains are the guards that already
targeted the canonical public tree directly.
"""

from pathlib import Path
from typing import Final

ROOT: Final = Path(__file__).resolve().parents[2]
DOCS: Final = ROOT / "docs"

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
