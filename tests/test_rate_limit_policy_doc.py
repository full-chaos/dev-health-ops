"""Rate-limit / provider-limit doc-drift guards (CHAOS-2757).

There is no CI docs build, so two contracts are enforced here instead:

1. The canonical public Reference page
   ``docs/reference/limits-and-compatibility/provider-limits.md`` exists and is wired into
   the accepted Reference section of ``mkdocs.yml`` nav.
2. The detailed per-provider route-family catalog is an internal/source contract preserved
   under ``.github/docs-legacy/providers/rate-limit-policy.md`` (moved out of the public tree
   during the canonical/legacy split). ``test_documented_route_families_match_estimators``
   runs the real per-provider budget estimators and fails if code emits a route family the
   preserved catalog does not document, keeping the rate-limit policy source from silently
   drifting from the ``providers/<provider>/budget.py`` estimators (per CHAOS-2742's
   docs-referenced-by-tests acceptance criterion). The calibrated-language invariant
   "Credentials are not capacity" is pinned against that same preserved source.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

from dev_health_ops.sync.budget import estimate_provider_budget
from dev_health_ops.sync.datasets import supported_datasets
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

_REPO_ROOT = Path(__file__).resolve().parents[1]
# Canonical public Reference page that must stay reachable in the accepted nav.
_CANONICAL_DOC_PATH = (
    _REPO_ROOT
    / "docs"
    / "reference"
    / "limits-and-compatibility"
    / "provider-limits.md"
)
# Preserved internal/source contract carrying the per-provider route-family catalog.
_SOURCE_DOC_PATH = (
    _REPO_ROOT / ".github" / "docs-legacy" / "providers" / "rate-limit-policy.md"
)
_MKDOCS_PATH = _REPO_ROOT / "mkdocs.yml"
_DOC_NAV_TARGET = "reference/limits-and-compatibility/provider-limits.md"

# Providers whose estimators are dispatched by ``estimate_provider_budget``.
_PROVIDERS = ("github", "gitlab", "jira", "linear", "launchdarkly")

# Enable every flag-gated route family so the estimators emit their full set
# (GitHub linked-PR expansion, Jira worklogs + AGG GraphQL enrichment, ...).
_ALL_FLAGS = {
    "sync_prs": True,
    "sync_git": True,
    "jira_fetch_worklogs": True,
    "fetch_worklogs": True,
    "atlassian_gql_enabled": True,
    "gql_enabled": True,
}

# ``<!-- route-families:<provider> -->`` markers scope the route-family tables;
# a heading line resets the active provider so only the marked table is parsed.
_MARKER_RE = re.compile(r"<!--\s*route-families:(\w+)\s*-->\s*$")
_ROW_TOKEN_RE = re.compile(r"^\|\s*`([a-z0-9_]+)`\s*\|")


def _make_context(provider: str, dataset_key: str) -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="int-1",
        source_id="src-1",
        source_external_id="ext-1",
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode="full",
        window_start=None,
        window_end=None,
        processor_flags=dict(_ALL_FLAGS),
        credential_id="cred-1",
        decrypted_credentials={"token": "x", "base_url": "https://example.test"},
        db_url="sqlite://",
    )


def _emitted_route_families(provider: str) -> set[str]:
    families: set[str] = set()
    for spec in supported_datasets(provider):
        for estimate in estimate_provider_budget(
            _make_context(provider, spec.dataset_key)
        ):
            families.add(estimate.route_family)
    return families


def _documented_route_families() -> dict[str, set[str]]:
    documented: dict[str, set[str]] = defaultdict(set)
    current: str | None = None
    for line in _SOURCE_DOC_PATH.read_text().splitlines():
        stripped = line.strip()
        marker = _MARKER_RE.match(stripped)
        if marker:
            current = marker.group(1)
            continue
        if stripped.startswith("#"):
            current = None
            continue
        if current is None:
            continue
        row = _ROW_TOKEN_RE.match(line)
        if row:
            documented[current].add(row.group(1))
    return documented


def test_canonical_provider_limits_page_exists_and_in_nav():
    assert _CANONICAL_DOC_PATH.is_file(), (
        f"missing canonical provider limits page: {_CANONICAL_DOC_PATH}"
    )

    nav = _MKDOCS_PATH.read_text()
    assert _DOC_NAV_TARGET in nav, (
        f"{_DOC_NAV_TARGET} is not wired into mkdocs.yml nav; the page would be "
        "unreachable (nav is fully explicit and there is no CI docs build)."
    )


def test_preserved_rate_limit_policy_source_keeps_calibrated_language():
    assert _SOURCE_DOC_PATH.is_file(), (
        f"missing preserved rate-limit policy source: {_SOURCE_DOC_PATH}"
    )

    body = _SOURCE_DOC_PATH.read_text()
    # The invariant and the phrase the epic requires must be present verbatim.
    assert "Credentials are not capacity" in body
    assert "credentials are not capacity" in body.lower()


def test_documented_route_families_match_estimators():
    documented = _documented_route_families()

    # The markers themselves must exist for every dispatched provider, else the
    # subset check below would vacuously pass on an empty documented set.
    for provider in _PROVIDERS:
        assert documented.get(provider), (
            f"no `<!-- route-families:{provider} -->` table found in the "
            "preserved rate-limit policy source"
        )

    for provider in _PROVIDERS:
        emitted = _emitted_route_families(provider)
        assert emitted, f"{provider} estimator emitted no route families"
        missing = emitted - documented[provider]
        assert not missing, (
            f"{provider} budget estimator emits route families that "
            f".github/docs-legacy/providers/rate-limit-policy.md does not document: "
            f"{sorted(missing)}. Add them to the "
            f"`<!-- route-families:{provider} -->` table."
        )
