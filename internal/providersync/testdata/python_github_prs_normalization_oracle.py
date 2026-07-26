#!/usr/bin/env python3
"""Emit the live Python github/prs normalization output as JSON for Go parity
tests (CHAOS-3122 / codex H9 fix).

H9 found that the previous parity fixture was hand-authored, omitted the
review-enrichment phase entirely, and then asserted the resulting zero-valued
review fields as if they were verified parity -- the fixture encoded the Go
implementation's incomplete contract as the "expected" result. This oracle
closes the part of that finding that is actually `github/prs`'s to close:
`normalize_pr_state` (providers/pr_state.py) and `build_git_pull_request` /
`BaseGitProcessor.coerce_created_at` (processors/base_git.py) -- the exact
functions that own state normalization and the created_at fallback chain,
which is where the M7/H4-class defects actually lived -- are called LIVE, not
re-derived by hand, for a battery of edge cases (internal whitespace, a
trailing \r, every created_at fallback branch).

first_review_at/reviews_count/changes_requested_count are NOT covered by this
oracle and are not claimed as parity anywhere in the Go test: `github/prs`
does not populate them (see pullRequestRow's doc comment), so there is no
Python output to compare them against until `github/pr-reviews` lands. The
matrix entry stays not-route-ready for exactly this reason -- see
deploy/go-workers/provider-sync-porting-recipe.md's H1 resolution.

`_pull_from_item`'s raw-JSON-to-dataclass extraction
(providers/github/code_client.py) is intentionally NOT re-executed here: that
module imports httpx, which the stock interpreter this oracle runs under
(ci/check_go.sh) does not have. That layer is mechanical field extraction
already pinned independently by tests/providers/test_github_code_client_prs.py
in the full Python suite, and github_prs_route.go's own doc comments record
the exact field mapping this Go code mirrors. What this oracle DOES cover is
the layer that actually contained defects: state normalization and the
created_at fallback chain.

Regenerate this comparison by re-running the Go test that shells out to this
script -- there is nothing to check in beyond this file; it is the generator.
"""

from __future__ import annotations

import json
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from internal.providersync.testdata.python_oracle_loader import (  # noqa: E402
    load_live_module,
)


def _load_pr_state(source: pathlib.Path) -> Any:
    """providers/pr_state.py has zero project-internal imports, so it loads
    directly -- no stub namespace required (mirrors python_registry_oracle.py)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "dev_health_ops_pr_state_oracle_target", source.resolve(strict=True)
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {source}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode(item) for item in value]
    return value


# Every case below is a (raw_state, created_at, merged_at, closed_at) tuple
# fed through the live `normalize_pr_state` + `build_git_pull_request` +
# `coerce_created_at` chain -- covering the exact edge cases codex's H7
# and H4 findings were about: internal whitespace, a trailing \r, "opened"
# vs "open", "closed" with and without merged_at, and every created_at
# fallback branch (created_at present / merged_at fallback / closed_at
# fallback / all absent -> None, which BaseGitProcessor.coerce_created_at
# resolves to "now" -- represented here as None so the Go test can assert its
# own normalizedAt fallback separately rather than racing wall-clock values).
CASES: list[dict[str, Any]] = [
    {
        "id": "open",
        "raw_state": "open",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": None,
        "closed_at": None,
    },
    {
        "id": "opened_alias",
        "raw_state": "opened",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": None,
        "closed_at": None,
    },
    {
        "id": "closed_unmerged",
        "raw_state": "closed",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": None,
        "closed_at": "2026-07-15T00:00:00Z",
    },
    {
        "id": "closed_merged",
        "raw_state": "closed",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": "2026-07-21T15:30:00Z",
        "closed_at": "2026-07-21T15:30:00Z",
    },
    {
        "id": "merged_literal",
        "raw_state": "merged",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": "2026-07-21T15:30:00Z",
        "closed_at": "2026-07-21T15:30:00Z",
    },
    {
        "id": "internal_whitespace",
        "raw_state": "clo sed",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": None,
        "closed_at": None,
    },
    {
        "id": "trailing_carriage_return",
        "raw_state": "closed\r",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": None,
        "closed_at": "2026-07-15T00:00:00Z",
    },
    {
        "id": "leading_trailing_whitespace",
        "raw_state": "  Closed  ",
        "created_at": "2026-07-10T09:00:00Z",
        "merged_at": "2026-07-21T15:30:00Z",
        "closed_at": "2026-07-21T15:30:00Z",
    },
    {
        "id": "created_at_absent_falls_back_to_merged_at",
        "raw_state": "closed",
        "created_at": None,
        "merged_at": "2026-07-21T15:30:00Z",
        "closed_at": None,
    },
    {
        "id": "created_and_merged_absent_falls_back_to_closed_at",
        "raw_state": "closed",
        "created_at": None,
        "merged_at": None,
        "closed_at": "2026-07-22T00:00:00Z",
    },
]


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: oracle.py <pr_state.py> <base_git.py>", file=sys.stderr)
        return 2
    pr_state_source = pathlib.Path(sys.argv[1])
    base_git_source = pathlib.Path(sys.argv[2])

    pr_state = _load_pr_state(pr_state_source)
    base_git = load_live_module(base_git_source)

    results: list[dict[str, Any]] = []
    for case in CASES:
        merged_at = _parse(case["merged_at"])
        closed_at = _parse(case["closed_at"])
        created_at = _parse(case["created_at"])
        state = pr_state.normalize_pr_state(case["raw_state"], merged_at)
        resolved_created_at = base_git.BaseGitProcessor.coerce_created_at(
            created_at, merged_at, closed_at
        )
        pr = base_git.build_git_pull_request(
            repo_id="00000000-0000-0000-0000-000000000000",
            number=1,
            title="t",
            body="b",
            state=state,
            author_name="a",
            author_email=None,
            created_at=created_at,
            merged_at=merged_at,
            closed_at=closed_at,
            head_branch="h",
            base_branch="m",
        )
        results.append(
            {
                "id": case["id"],
                "state": state,
                "resolved_created_at": encode(resolved_created_at),
                "built_created_at": encode(pr.created_at),
                "built_merged_at": encode(pr.merged_at),
                "built_closed_at": encode(pr.closed_at),
            }
        )

    print(json.dumps(results, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
