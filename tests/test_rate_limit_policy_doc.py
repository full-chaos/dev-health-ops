"""Provider-limit doc guard (CHAOS-2757).

There is no CI docs build, so the canonical public Reference page
``docs/reference/limits-and-compatibility/provider-limits.md`` is asserted to exist and
to be wired into the explicit ``mkdocs.yml`` nav here instead.

Historical note: this module also carried a differential guard
(``test_documented_route_families_match_estimators``) that ran the real per-provider
budget estimators and failed if code emitted a route family the internal rate-limit
policy catalog did not document, plus a calibrated-language pin
("Credentials are not capacity"). That catalog and that language lived only in the
deleted legacy ``providers/rate-limit-policy.md`` source and have no equivalent in
the live ``docs/`` tree -- ``provider-limits.md`` is a short authoring checklist with no
per-provider route-family tables.

The estimator drift guard has since been restored, unweakened, as a code contract
rather than as documentation: the catalog now lives at
``contracts/providers/v1/route-families.json`` and the oracle at
``tests/providers/test_route_family_contract.py``. The calibrated-language pin was not
restored -- it asserted prose, and no prose source carries that wording any more.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
# Canonical public Reference page that must stay reachable in the accepted nav.
_CANONICAL_DOC_PATH = (
    _REPO_ROOT
    / "docs"
    / "reference"
    / "limits-and-compatibility"
    / "provider-limits.md"
)
_MKDOCS_PATH = _REPO_ROOT / "mkdocs.yml"
_DOC_NAV_TARGET = "reference/limits-and-compatibility/provider-limits.md"


def test_canonical_provider_limits_page_exists_and_in_nav():
    assert _CANONICAL_DOC_PATH.is_file(), (
        f"missing canonical provider limits page: {_CANONICAL_DOC_PATH}"
    )

    nav = _MKDOCS_PATH.read_text()
    assert _DOC_NAV_TARGET in nav, (
        f"{_DOC_NAV_TARGET} is not wired into mkdocs.yml nav; the page would be "
        "unreachable (nav is fully explicit and there is no CI docs build)."
    )
