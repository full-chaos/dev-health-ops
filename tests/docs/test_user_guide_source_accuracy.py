"""Canonical PR Flow source-accuracy guard.

The archived-corpus source-accuracy characterization (and its negative twin) read
the legacy ``user-guide/**`` corpus, its ``images/fixture-capture-metadata.json``
captures, and the legacy ``search-acceptance.json`` manifest. All three were deleted with the
legacy tree, and nothing in the live ``docs/`` tree replaces them -- MkDocs uses the
built-in search plugin with no acceptance manifest, and no fixture captures ship under
``docs/``. Those tests were removed rather than repointed. The canonical PR Flow guard
below already ran against the live page and is unchanged.
"""

from pathlib import Path
from typing import Final

from tests.docs.user_guide_source_accuracy import PR_FLOW_UNSUPPORTED_CLAIMS

ROOT: Final = Path(__file__).resolve().parents[2]
# Canonical public PR Flow guide.
CANONICAL_PR_FLOW: Final = ROOT / "docs" / "use" / "delivery-flow" / "pr-flow.md"


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
