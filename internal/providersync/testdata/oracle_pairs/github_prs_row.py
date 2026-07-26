"""github/prs row-construction oracle pair (CHAOS-3162).

Registers the "github/prs/row" boundary: given one raw GitHub REST
`/pulls/{number}` detail payload (a plain JSON dict, the same shape
github_prs_route_test.go's fixtures use), calls the REAL, live
`_pull_from_item` (providers/github/code_client.py) -> `normalize_pr_state`
(providers/pr_state.py) -> `build_git_pull_request`
(processors/base_git.py) chain and returns the COMPLETE resulting row as a
dict -- every attribute the built `GitPullRequest` object carries, not a
chosen subset.

Importing this module is the only action needed to register the pair;
nothing in oracle_registry.py or python_generic_row_oracle.py changes.

REQUIREMENT, not a caveat: any Go test that exercises this pair (directly
or via compareRowsAgainstPythonOracle/oracleDivergences) MUST be run with
`go test -count=1`, never a bare `go test`. code_client.py/base_git.py/
pr_state.py live under src/dev_health_ops/, outside
internal/providersync/testdata/ -- `//go:embed` cannot reach across that
package-directory boundary (Go rejects a `../` pattern outright), so
oracle_compare_test.go's cache-busting fix cannot see edits to these
files. A bare `go test` can then return a stale cached PASS for a real
change to one of them. See the recipe doc's defect-class list for the
full explanation.
"""

from __future__ import annotations

import importlib.util
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dict_literal_keys
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/github/code_client.py"
_BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
_PR_STATE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/pr_state.py"


def _reflected_fields() -> frozenset[str]:
    """The complete field set build_git_pull_request (base_git.py) is
    capable of emitting, derived by statically parsing its own `values` and
    `optional_values` dict literals (codex finding #1, CHAOS-3162) -- not a
    hand-maintained list that could drift from, or start narrower than, the
    real function."""
    return dict_literal_keys(
        _BASE_GIT_SOURCE.read_text(),
        "build_git_pull_request",
        ("values", "optional_values"),
    )


def _load_pr_state() -> Any:
    """providers/pr_state.py has zero project-internal imports, so it loads
    directly -- no stub namespace required (mirrors python_registry_oracle.py)."""
    spec = importlib.util.spec_from_file_location(
        "dev_health_ops_pr_state_oracle_target", _PR_STATE_SOURCE.resolve(strict=True)
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {_PR_STATE_SOURCE}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    """case: {"raw_pr": <raw GitHub /pulls/{number} JSON payload>,
    "repo_id": <string>}.

    Loaded fresh per call (load_live_module purges and reinstalls the stub
    namespace every time) so this stays correct even if a caller loads other
    pairs' modules in the same process.
    """
    code_client = load_live_module(_CODE_CLIENT_SOURCE)
    base_git = load_live_module(_BASE_GIT_SOURCE)
    pr_state = _load_pr_state()

    gh_pr = code_client._pull_from_item(case["raw_pr"])
    state = pr_state.normalize_pr_state(gh_pr.state, gh_pr.merged_at)
    author_name = gh_pr.author_login if gh_pr.author_login else "Unknown"
    pr = base_git.build_git_pull_request(
        repo_id=case["repo_id"],
        number=gh_pr.number,
        title=gh_pr.title,
        body=gh_pr.body,
        state=state,
        author_name=author_name,
        author_email=None,
        created_at=gh_pr.created_at,
        merged_at=gh_pr.merged_at,
        closed_at=gh_pr.closed_at,
        head_branch=gh_pr.head_ref,
        base_branch=gh_pr.base_ref,
        additions=gh_pr.additions,
        deletions=gh_pr.deletions,
        changed_files=gh_pr.changed_files,
        comments_count=gh_pr.comments_count,
        # first_review_at/first_comment_at/changes_requested_count/
        # reviews_count are deliberately NOT passed: the real caller
        # (_collect_github_pr_objects) only supplies them from
        # _enrich_prs_with_reviews_batch's review fetch, which this
        # row-construction boundary does not perform (see
        # deploy/go-workers/provider-sync-porting-recipe.md's defect class
        # 9). build_git_pull_request's optional_values filtering means an
        # un-passed (None) field is OMITTED from the built row entirely, not
        # merely set to None -- so those fields are absent here exactly the
        # way they would be absent from a call site that has no review data
        # yet, and the exclusion below documents that this is a declared,
        # structural absence, not an oversight.
    )
    return {key: value for key, value in vars(pr).items() if not key.startswith("_")}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/prs/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        # codex adversarial review (CHAOS-3162, fourth round): last_synced/
        # source_id/org_id used to ALSO be declared here, redundantly with
        # oraclePullRequestGoOnlyFields in github_prs_generic_oracle_test.go.
        # That redundancy defeated checkExclusionIntegrity's own staleness
        # check for the Go-side declaration -- if someone deleted the
        # goOnlyFields entry for, say, org_id, this excluded_fields entry
        # would still suppress the comparison and the test would stay
        # green, silently masking that the Go-side declaration was no
        # longer doing anything. None of the three are part of
        # reflected_fields() (they are not keys build_git_pull_request's
        # own `values`/`optional_values` dict literals ever assign), so
        # Python-side completeness does not need them declared here at
        # all -- they belong ONLY in goOnlyFields, where they are the
        # single, real, load-bearing declaration.
        excluded_fields={
            "first_review_at": (
                "owned by github/pr-reviews' review-enrichment phase "
                "(_enrich_prs_with_reviews_batch), not this row-construction "
                "boundary -- see recipe doc defect class 9"
            ),
            "first_comment_at": (
                "same as first_review_at: review-enrichment phase field, "
                "always None/absent from this boundary regardless of caller"
            ),
            "changes_requested_count": (
                "owned by github/pr-reviews' review-enrichment phase, "
                "not this row-construction boundary"
            ),
            "reviews_count": (
                "owned by github/pr-reviews' review-enrichment phase, "
                "not this row-construction boundary"
            ),
        },
    )
)
