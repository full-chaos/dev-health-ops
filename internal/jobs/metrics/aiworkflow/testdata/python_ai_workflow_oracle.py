"""Live-Python oracle for the ai_workflow native port (CHAOS-4280 part B /
CHAOS-4286 part B).

Runs the PRODUCTION function --
work_graph.extractors.ai_workflow.extract_ai_workflow_from_pull_requests --
over a pinned fixture and prints the result as JSON on the last stdout line.
The Go side (oracle_test.go) builds the byte-identical fixture as
[]aiworkflow.PullRequestRow, calls Compute with the SAME single provider (this
extractor takes one provider per call, exactly like Compute -- the by-provider
split lives one layer up, in job_daily.py / the native executor, and is
covered by their own tests, not this oracle), and compares every persisted
column of all three result lists, including edge_id and run_id.

Run only through ci/check_go.sh's live-python-oracles verb, which also checks
this run's proof marker. See oracle_test.go's runPythonOracle.

# What each fixture row exercises

  1. Two AI labels on one PR ("copilot", "claude-code") -- both confidence
     0.95, same Source (pr_label) -- a genuine tie. Python's max() keeps the
     FIRST maximal element, so evidence.label must be "copilot" (list order),
     not "claude-code". This is the highest-value case in the whole fixture:
     a port that used >= instead of > in its tie-break passes every
     non-tied case and fails only here.
  2. A known-AI-bot author login (copilot[bot]) with no other signal.
  3. A branch-name-only signal ("devin/..." -> agent_created kind), and a
     plain author login that must NOT itself produce a signal.
  4. A PR-body signal containing a literal NBSP between "ai" and "assisted"
     (astra finding 4) with actor=None, exercising the run.actor fallback to
     row.author_name end to end (not just at the detector unit level).
  5. issue_ids_by_pr fan-out: PR #101 (row 1's PR) is linked to TWO work
     items, producing two issue edges off the SAME run_id.
  6. A PR with no signal at all -- must be entirely absent from every output
     list (not merely absent from one).
  7. Sub-second closed_at, merged_at NULL -- observed_at/completed_at must
     fall back to closed_at with its microseconds intact (the #2229-class
     bug: a seconds-only comparator would pass this by accident).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

from dev_health_ops.work_graph.extractors.ai_workflow import (
    extract_ai_workflow_from_pull_requests,
)

ORG = UUID("8f3f7b0a-1c2d-4e3f-9a5b-6c7d8e9f0a1b")
PROVIDER = "github"

REPO_A = UUID("3a9c1e00-1111-4222-8333-944444445555")
REPO_B = UUID("3a9c1e00-2222-4333-8444-955555556666")


def dt(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    microsecond: int = 0,
) -> datetime:
    """A UTC-aware datetime, spelled out so mypy can check the call sites."""
    return datetime(
        year, month, day, hour, minute, second, microsecond, tzinfo=timezone.utc
    )


PULL_REQUESTS = [
    # 1. Tie-break case: two AI labels, same confidence AND source. Go must
    #    keep evidence.label == "copilot" (first in list order).
    {
        "repo_id": REPO_A,
        "number": 101,
        "title": "Add caching",
        "body": "",
        "head_branch": "feature/cache",
        "author_name": "dev-a",
        "labels": ["copilot", "claude-code"],
        "created_at": dt(2026, 9, 1, 9, 0, 0),
        "merged_at": dt(2026, 9, 1, 10, 0, 0),
        "closed_at": None,
        "last_synced": dt(2026, 9, 1, 23, 0, 0),
    },
    # 2. Known-AI-bot author, no other signal.
    {
        "repo_id": REPO_A,
        "number": 102,
        "title": "Automated dependency bump",
        "body": "",
        "head_branch": "",
        "author_name": "",
        "author_login": "copilot[bot]",
        "author_user_type": "Bot",
        "labels": [],
        "created_at": dt(2026, 9, 1, 11, 0, 0),
        "merged_at": None,
        "closed_at": None,
        "last_synced": dt(2026, 9, 1, 23, 0, 0),
    },
    # 3. Branch-name-only signal; author is an ordinary human login and must
    #    contribute nothing.
    {
        "repo_id": REPO_A,
        "number": 103,
        "title": "Refactor flaky test",
        "body": "",
        "head_branch": "devin/refactor-flaky-test",
        "author_name": "dev-e",
        "labels": [],
        "created_at": dt(2026, 9, 1, 12, 0, 0),
        "merged_at": dt(2026, 9, 1, 13, 0, 0),
        "closed_at": None,
        "last_synced": dt(2026, 9, 1, 23, 0, 0),
    },
    # 4. PR-body NBSP signal, actor=None -> run.actor falls back to
    #    author_name end to end.
    {
        "repo_id": REPO_B,
        "number": 201,
        "title": "Improve error messages",
        "body": "This was ai assisted work.",
        "head_branch": "",
        "author_name": "dev-c",
        "labels": [],
        "created_at": dt(2026, 9, 1, 14, 0, 0),
        "merged_at": dt(2026, 9, 1, 15, 0, 0),
        "closed_at": None,
        "last_synced": dt(2026, 9, 1, 23, 0, 0),
    },
    # 5. No signal at all -- must be entirely absent from every output list.
    {
        "repo_id": REPO_B,
        "number": 202,
        "title": "Fix typo",
        "body": "Just a typo fix, nothing AI about it.",
        "head_branch": "fix/typo",
        "author_name": "dev-d",
        "labels": [],
        "created_at": dt(2026, 9, 1, 16, 0, 0),
        "merged_at": None,
        "closed_at": None,
        "last_synced": dt(2026, 9, 1, 23, 0, 0),
    },
    # 6. Sub-second closed_at, merged_at NULL -> observed_at/completed_at
    #    fall back to closed_at WITH its microseconds intact.
    {
        "repo_id": REPO_B,
        "number": 203,
        "title": "Agent-created cleanup",
        "body": "agent-created housekeeping pass",
        "head_branch": "",
        "author_name": "dev-f",
        "labels": [],
        "created_at": dt(2026, 9, 1, 17, 0, 0),
        "merged_at": None,
        "closed_at": dt(2026, 9, 1, 17, 30, 0, 123000),
        "last_synced": dt(2026, 9, 1, 23, 0, 0),
    },
]

# PR #101 (row 1) is linked to TWO work items -> two issue edges off the same
# run_id.
ISSUE_IDS_BY_PR = {
    f"{REPO_A}:101": ["jira:ABC-1", "jira:ABC-2"],
}


def main() -> None:
    result = extract_ai_workflow_from_pull_requests(
        PULL_REQUESTS,
        org_id=ORG,
        provider=PROVIDER,
        issue_ids_by_pr=ISSUE_IDS_BY_PR,
    )
    print(
        json.dumps(
            {
                "runs": [
                    {
                        "run_id": r.run_id,
                        "org_id": str(r.org_id),
                        "provider": r.provider,
                        "run_kind": str(r.run_kind),
                        "status": str(r.status) if r.status is not None else None,
                        "tool": r.tool,
                        "actor": r.actor,
                        "repo_id": str(r.repo_id) if r.repo_id is not None else None,
                        "prompts_redacted": r.prompts_redacted,
                        "started_at": r.started_at.isoformat()
                        if r.started_at is not None
                        else None,
                        "completed_at": r.completed_at.isoformat()
                        if r.completed_at is not None
                        else None,
                        "observed_at": r.observed_at.isoformat(),
                        "metadata": json.dumps(
                            r.metadata, sort_keys=True, separators=(",", ":")
                        ),
                    }
                    for r in result.runs
                ],
                "artifact_edges": [
                    {
                        "edge_id": e.edge_id,
                        "org_id": str(e.org_id),
                        "run_id": e.run_id,
                        "artifact_type": str(e.artifact_type),
                        "artifact_id": e.artifact_id,
                        "provider": e.provider,
                        "repo_id": str(e.repo_id) if e.repo_id is not None else None,
                        "confidence": e.confidence,
                        "source": e.source,
                        "evidence": e.evidence,
                        "observed_at": e.observed_at.isoformat(),
                    }
                    for e in result.artifact_edges
                ],
                "issue_edges": [
                    {
                        "edge_id": e.edge_id,
                        "org_id": str(e.org_id),
                        "issue_id": e.issue_id,
                        "run_id": e.run_id,
                        "provider": e.provider,
                        "repo_id": str(e.repo_id) if e.repo_id is not None else None,
                        "confidence": e.confidence,
                        "source": e.source,
                        "evidence": e.evidence,
                        "observed_at": e.observed_at.isoformat(),
                    }
                    for e in result.issue_edges
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
