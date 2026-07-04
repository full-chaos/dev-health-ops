"""Exhaustive label->family resolver tests (CHAOS-2773 CS1 [codex HIGH-1]).

``OperationResolver.resolve`` gained an explicit-prefix short-circuit: an
operation label of the form ``"<registered-family>:..."`` resolves DIRECTLY
to that family, bypassing the substring marker scan (``providers/usage.py``).
This file is the "exhaustive" test the plan requires:

1. Every operation label the EXISTING GitHub/GitLab work clients emit today
   (enumerated by grepping ``providers/github/client.py`` /
   ``providers/gitlab/client.py`` for their ``operation`` strings) resolves
   EXACTLY as it did before this change -- attribution is provably
   unshifted for unprefixed labels.
2. A representative prefixed label per REGISTERED family (both providers)
   short-circuits directly to that family, including the exact collision
   the plan calls out: GitLab's broad ``"/projects/:id"`` / ``"/projects/"``
   markers (registered under ``project``) must NOT swallow a
   ``pipelines:...`` label.
3. None of today's existing (unprefixed) work-client labels accidentally
   start with any registered ``"<family>:"`` prefix -- the false-trigger
   regression guard.

A small synthetic-registry suite additionally pins the short-circuit
mechanism in isolation, independent of the real GitHub/GitLab registries.
"""

from __future__ import annotations

import pytest

from dev_health_ops.providers.github.budget import (
    GITHUB_USAGE_RESOLVER,
    GITHUB_USAGE_ROUTE_FAMILY_KEYS,
)
from dev_health_ops.providers.gitlab.budget import (
    GITLAB_USAGE_RESOLVER,
    GITLAB_USAGE_ROUTE_FAMILY_KEYS,
)
from dev_health_ops.providers.usage import OperationResolver, UsageRouteFamily
from dev_health_ops.sync.budget_types import BudgetDimension

# ---------------------------------------------------------------------------
# Every operation label GitHubWorkClient / GitLabWorkClient emit today
# (grepped from providers/github/client.py and providers/gitlab/client.py --
# both the f-string templates, interpolated with representative values, and
# the GraphQL literal labels).
# ---------------------------------------------------------------------------

GITHUB_EXISTING_LABELS: tuple[tuple[str, str, tuple[str, BudgetDimension]], ...] = (
    ("rest", "GET /repos/acme/widgets", ("work_items", BudgetDimension.REST_CORE)),
    (
        "rest",
        "GET /repos/acme/widgets/issues",
        ("work_items", BudgetDimension.REST_CORE),
    ),
    ("rest", "GET issue events for #42", ("work_items", BudgetDimension.REST_CORE)),
    (
        "rest",
        "GET /repos/acme/widgets/pulls",
        ("work_items", BudgetDimension.REST_CORE),
    ),
    ("rest", "GET issue comments for #42", ("work_items", BudgetDimension.REST_CORE)),
    (
        "rest",
        "GET pull request review comments for #42",
        ("work_items", BudgetDimension.REST_CORE),
    ),
    (
        "rest",
        "GET /repos/acme/widgets/milestones",
        ("work_items", BudgetDimension.REST_CORE),
    ),
    (
        "graphql",
        "POST /graphql PR social data",
        ("work_item_prs", BudgetDimension.GRAPHQL_COST),
    ),
    (
        "graphql",
        "POST /graphql PR review comments",
        ("work_item_prs", BudgetDimension.GRAPHQL_COST),
    ),
    (
        "graphql",
        "POST /graphql project v2 items",
        ("work_item_prs", BudgetDimension.GRAPHQL_COST),
    ),
    (
        "graphql",
        "POST /graphql project v2 item changes",
        ("work_item_prs", BudgetDimension.GRAPHQL_COST),
    ),
)

GITLAB_EXISTING_LABELS: tuple[tuple[str, str, tuple[str, BudgetDimension]], ...] = (
    ("rest", "GET /projects/:id", ("project", BudgetDimension.REST_CORE)),
    ("rest", "GET iterator page", ("issues", BudgetDimension.REST_CORE)),
)


# ---------------------------------------------------------------------------
# 1. Existing labels resolve exactly as before this change (unshifted).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("transport,operation,expected", GITHUB_EXISTING_LABELS)
def test_github_existing_labels_resolve_unshifted(
    transport: str, operation: str, expected: tuple[str, BudgetDimension]
) -> None:
    assert (
        GITHUB_USAGE_RESOLVER.resolve(transport=transport, operation=operation)
        == expected
    )


@pytest.mark.parametrize("transport,operation,expected", GITLAB_EXISTING_LABELS)
def test_gitlab_existing_labels_resolve_unshifted(
    transport: str, operation: str, expected: tuple[str, BudgetDimension]
) -> None:
    assert (
        GITLAB_USAGE_RESOLVER.resolve(transport=transport, operation=operation)
        == expected
    )


# ---------------------------------------------------------------------------
# 1b. CHAOS-2803/CS2: the FIRST intentional re-bucketing. The PR review-batch
# enrichment (processors/github.py::_enrich_prs_with_reviews_batch) now labels
# its local GitHubWorkClient's GraphQL calls with the "pr_social:" prefix, so
# they resolve to `pr_social` instead of the unprefixed transport default
# (`work_item_prs`) -- while the SAME literal label, unprefixed (still emitted
# by providers/github/provider.py's work-items PR-as-work-item path, which
# never passes operation_family), keeps resolving to `work_item_prs` exactly
# as it always has (see GITHUB_EXISTING_LABELS above, unchanged).
# ---------------------------------------------------------------------------

GITHUB_CS2_PREFIXED_LABELS: tuple[tuple[str, str, tuple[str, BudgetDimension]], ...] = (
    (
        "graphql",
        "pr_social:POST /graphql PR social data",
        ("pr_social", BudgetDimension.GRAPHQL_COST),
    ),
    (
        "graphql",
        "pr_social:POST /graphql PR review comments",
        ("pr_social", BudgetDimension.GRAPHQL_COST),
    ),
)


@pytest.mark.parametrize("transport,operation,expected", GITHUB_CS2_PREFIXED_LABELS)
def test_github_review_batch_prefixed_labels_resolve_to_pr_social(
    transport: str, operation: str, expected: tuple[str, BudgetDimension]
) -> None:
    assert (
        GITHUB_USAGE_RESOLVER.resolve(transport=transport, operation=operation)
        == expected
    )


def test_github_review_batch_unprefixed_label_still_resolves_to_work_item_prs() -> None:
    """Sanity check mirroring the GitLab pipelines/project one below: the SAME
    literal operation, without the family prefix, is untouched -- proving the
    CS2 fix is additive (a new label, not a resolver default change)."""
    assert GITHUB_USAGE_RESOLVER.resolve(
        transport="graphql", operation="POST /graphql PR social data"
    ) == ("work_item_prs", BudgetDimension.GRAPHQL_COST)


# ---------------------------------------------------------------------------
# CHAOS-2808/CS7: files/blame GraphQL content+blame fetch is the SECOND
# intentional re-bucketing. Both families carry TWO dimension entries
# (contents_blob live, rest_core still frozen/empty); the prefix
# short-circuit only matches on route_family, so ORDERING within
# GITHUB_USAGE_ROUTE_FAMILIES (contents_blob listed first) is what resolves
# a "files:"/"blame:" label to the live dimension instead of the frozen one.
# ---------------------------------------------------------------------------

GITHUB_CS7_PREFIXED_LABELS: tuple[tuple[str, str, tuple[str, BudgetDimension]], ...] = (
    (
        "rest",
        "files:POST /graphql (get_blob_texts x3)",
        ("files", BudgetDimension.CONTENTS_BLOB),
    ),
    (
        "rest",
        "blame:POST /graphql (get_blame)",
        ("blame", BudgetDimension.CONTENTS_BLOB),
    ),
)


@pytest.mark.parametrize("transport,operation,expected", GITHUB_CS7_PREFIXED_LABELS)
def test_github_files_blame_prefixed_labels_resolve_to_contents_blob(
    transport: str, operation: str, expected: tuple[str, BudgetDimension]
) -> None:
    assert (
        GITHUB_USAGE_RESOLVER.resolve(transport=transport, operation=operation)
        == expected
    )


# ---------------------------------------------------------------------------
# 2. Representative prefixed labels per registered family short-circuit.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("family", sorted(GITHUB_USAGE_ROUTE_FAMILY_KEYS))
def test_github_prefixed_label_resolves_directly_to_its_family(family: str) -> None:
    operation = f"{family}:GET /repos/acme/widgets/{family}"
    route_family, _dimension = GITHUB_USAGE_RESOLVER.resolve(
        transport="rest", operation=operation
    )
    assert route_family == family


@pytest.mark.parametrize("family", sorted(GITLAB_USAGE_ROUTE_FAMILY_KEYS))
def test_gitlab_prefixed_label_resolves_directly_to_its_family(family: str) -> None:
    operation = f"{family}:GET /projects/1/{family}"
    route_family, _dimension = GITLAB_USAGE_RESOLVER.resolve(
        transport="rest", operation=operation
    )
    assert route_family == family


def test_gitlab_pipelines_prefix_not_swallowed_by_broad_project_marker() -> None:
    """The motivating collision from the plan: GitLab's ``project`` family
    registers broad substring markers ("/projects/:id", "/projects/") that,
    absent the short-circuit, would swallow ANY label containing that
    substring -- including a self-authored ``pipelines:`` label for the
    (also project-scoped) pipelines endpoint. The explicit prefix must win.
    """
    route_family, dimension = GITLAB_USAGE_RESOLVER.resolve(
        transport="rest", operation="pipelines:GET /projects/:id/pipelines"
    )
    assert route_family == "pipelines"
    assert dimension is BudgetDimension.REST_CORE

    # Sanity check: the SAME literal marker, unprefixed, still resolves to
    # `project` via the untouched marker scan (proves the fix is additive,
    # not a marker-scan regression).
    unprefixed_family, _ = GITLAB_USAGE_RESOLVER.resolve(
        transport="rest", operation="GET /projects/:id/pipelines"
    )
    assert unprefixed_family == "project"


@pytest.mark.parametrize(
    ("operation", "expected_family"),
    (
        (
            "merge_requests:GET /projects/:id/merge_requests",
            "merge_requests",
        ),
        (
            "notes:GET /projects/:id/merge_requests/:iid/notes",
            "notes",
        ),
    ),
)
def test_gitlab_mr_notes_prefixes_not_swallowed_by_broad_project_marker(
    operation: str, expected_family: str
) -> None:
    route_family, dimension = GITLAB_USAGE_RESOLVER.resolve(
        transport="rest", operation=operation
    )

    assert route_family == expected_family
    assert dimension is BudgetDimension.REST_CORE


# ---------------------------------------------------------------------------
# 3. False-trigger regression guard: no existing label accidentally
#    prefix-matches a registered family.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("transport,operation,_expected", GITHUB_EXISTING_LABELS)
def test_github_existing_labels_never_accidentally_prefix_match(
    transport: str, operation: str, _expected: tuple[str, BudgetDimension]
) -> None:
    lowered = operation.lower()
    for family in GITHUB_USAGE_ROUTE_FAMILY_KEYS:
        assert not lowered.startswith(f"{family}:"), (
            f"{operation!r} accidentally starts with the {family!r} prefix"
        )


@pytest.mark.parametrize("transport,operation,_expected", GITLAB_EXISTING_LABELS)
def test_gitlab_existing_labels_never_accidentally_prefix_match(
    transport: str, operation: str, _expected: tuple[str, BudgetDimension]
) -> None:
    lowered = operation.lower()
    for family in GITLAB_USAGE_ROUTE_FAMILY_KEYS:
        assert not lowered.startswith(f"{family}:"), (
            f"{operation!r} accidentally starts with the {family!r} prefix"
        )


# ---------------------------------------------------------------------------
# Synthetic-registry unit tests: pin the short-circuit mechanism in
# isolation, independent of the real GitHub/GitLab registries above.
# ---------------------------------------------------------------------------


_SYNTHETIC_RESOLVER = OperationResolver(
    families=(
        # "alpha" carries a broad marker that would otherwise swallow a
        # "beta:" labeled operation via ordinary substring scanning.
        UsageRouteFamily(
            "alpha", BudgetDimension.REST_CORE, operation_markers=("/beta/",)
        ),
        UsageRouteFamily("beta", BudgetDimension.CONTENTS_BLOB),
        UsageRouteFamily("gamma", BudgetDimension.GRAPHQL_COST, transport="graphql"),
    ),
    defaults=(("rest", "fallback", BudgetDimension.REST_CORE),),
)


class TestSyntheticShortCircuit:
    def test_prefixed_label_bypasses_marker_scan_collision(self) -> None:
        # Without the short-circuit this would resolve to "alpha" (the
        # "/beta/" marker matches and alpha is scanned first).
        route_family, dimension = _SYNTHETIC_RESOLVER.resolve(
            transport="rest", operation="beta:GET /beta/thing"
        )
        assert route_family == "beta"
        assert dimension is BudgetDimension.CONTENTS_BLOB

    def test_unprefixed_label_still_takes_marker_scan_path(self) -> None:
        route_family, _ = _SYNTHETIC_RESOLVER.resolve(
            transport="rest", operation="GET /beta/thing"
        )
        assert route_family == "alpha"

    def test_prefix_respects_family_transport_filter(self) -> None:
        # "gamma:" is registered transport="graphql" only -- a "rest"-labeled
        # operation prefixed with "gamma:" must NOT short-circuit to it.
        route_family, _ = _SYNTHETIC_RESOLVER.resolve(
            transport="rest", operation="gamma:GET /whatever"
        )
        assert route_family == "fallback"

        route_family, dimension = _SYNTHETIC_RESOLVER.resolve(
            transport="graphql", operation="gamma:POST /whatever"
        )
        assert route_family == "gamma"
        assert dimension is BudgetDimension.GRAPHQL_COST

    def test_prefix_is_case_insensitive(self) -> None:
        route_family, _ = _SYNTHETIC_RESOLVER.resolve(
            transport="rest", operation="BETA:GET /Beta/Thing"
        )
        assert route_family == "beta"

    def test_unregistered_prefix_falls_through_to_marker_scan_then_default(
        self,
    ) -> None:
        route_family, _ = _SYNTHETIC_RESOLVER.resolve(
            transport="rest", operation="not_a_family:GET /nothing"
        )
        assert route_family == "fallback"
