"""Live-Python oracle for the work_graph_edges native port (CHAOS-4286).

Runs the PRODUCTION function --
work_graph.extractors.ai_workflow.extract_review_deployment_incident_edges --
over a pinned fixture and prints the result as JSON on the last stdout line.
The Go side builds the byte-identical fixture and compares every persisted
column.

Run only through ci/check_go.sh's live-python-oracles verb, which also checks
this run's proof marker. See compute_test.go's runPythonOracle.

# What is compared

Every persisted column of all three edge lists, including edge_id.

THAT CLAIM WAS FALSE WHEN FIRST WRITTEN, and the wording is kept deliberately
so the correction is visible rather than tidied away. The comparator originally
omitted org_id, provider and repo_id from the deployment and incident lists,
and repo_id from the review list -- while this docstring and the PR body both
asserted full coverage. #2240's round-1 reviewer found it and demonstrated it:
changing the deployment edges' Provider to a literal left every asserted field
untouched (provider is not in the hash either), so the oracle passed on rows
carrying a wrong persisted provider.

The comparator now asserts all of them, and that mutation is killed. Widening
it surfaced no actual mismatches -- the kernel was right all along, which is
precisely why the gap was invisible: nothing failed, so nothing drew attention.

This oracle IS stronger than #2229's in one specific respect and it is worth
stating narrowly rather than broadly: it can compare edge_id, because Python
derives ids here (`_hash`, a sha256 over the identity tuple, ai_workflow.py:49)
whereas #2229's Python randomises event_id and has no answer to compare. That
is the whole of the advantage; it is not "stronger" in general.

computed_at is not produced by the extractor at all (the sink stamps it), so
there is nothing to exclude here.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

from dev_health_ops.work_graph.extractors.ai_workflow import (
    extract_review_deployment_incident_edges,
)

ORG = UUID("70d529e0-3c06-4597-8480-794fd02328b6")
PROVIDER = "github"

REPO_A = UUID("d4f322ad-2102-1fbf-8425-7400573194f7")
REPO_B = UUID("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")


def dt(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> datetime:
    """A UTC-aware datetime, spelled out so mypy can check the call sites.

    An earlier *args form needed a type: ignore and still failed --
    datetime()'s tzinfo is positional-or-keyword, so *args could supply it
    twice. Explicit parameters make the arity a compile-time property.
    """
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


REVIEWS = [
    # 1. Ordinary approved review: outcome set, evidence.state a string.
    {
        "repo_id": REPO_A,
        "number": 101,
        "review_id": "r-approved",
        "state": "APPROVED",
        "submitted_at": dt(2026, 9, 3, 9, 0, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 2. state is NULL -> outcome None AND evidence.state null.
    {
        "repo_id": REPO_A,
        "number": 102,
        "review_id": "r-null-state",
        "state": None,
        "submitted_at": dt(2026, 9, 3, 10, 0, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 3. THE DIVERGENCE CASE. state is the EMPTY STRING, so:
    #      outcome        = _str(row,"state") or None  -> None
    #      evidence.state = row.get("state")           -> ""
    #    One column nils and the other keeps "". A port that unified these two
    #    -- the obvious cleanup -- passes cases 1 and 2 and fails only here.
    {
        "repo_id": REPO_A,
        "number": 103,
        "review_id": "r-empty-state",
        "state": "",
        "submitted_at": dt(2026, 9, 3, 11, 0, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 4. submitted_at NULL -> _dt falls through to last_synced.
    {
        "repo_id": REPO_B,
        "number": 201,
        "review_id": "r-no-submitted",
        "state": "COMMENTED",
        "submitted_at": None,
        "last_synced": dt(2026, 9, 3, 22, 30, 0),
    },
    # 5. SUB-SECOND submitted_at. Every other row sits on an exact second,
    #    which is how #2229's round-1 P2 slipped through: a seconds-only
    #    comparator discards the fraction on both sides before comparing.
    {
        "repo_id": REPO_B,
        "number": 202,
        "review_id": "r-subsecond",
        "state": "CHANGES_REQUESTED",
        "submitted_at": datetime(2026, 9, 3, 12, 0, 0, 123000, tzinfo=timezone.utc),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 6. Empty review_id -> skipped entirely (`not review_id`).
    {
        "repo_id": REPO_A,
        "number": 104,
        "review_id": "",
        "state": "APPROVED",
        "submitted_at": dt(2026, 9, 3, 13, 0, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
]

DEPLOYMENTS = [
    # 1. Has a PR number -> produces a PR->deployment edge AND registers in
    #    the per-repo index.
    {
        "repo_id": REPO_A,
        "deployment_id": "dep-a1",
        "pull_request_number": 101,
        "started_at": dt(2026, 9, 3, 14, 0, 0),
        "finished_at": dt(2026, 9, 3, 14, 5, 0),
        "deployed_at": dt(2026, 9, 3, 14, 10, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 2. NO PR number -> NO PR->deployment edge, but it MUST still be in the
    #    per-repo index, because Python appends BEFORE the `continue`. If the
    #    port moves the append below the continue, this deployment silently
    #    stops receiving heuristic incident edges and only this fixture row
    #    notices.
    {
        "repo_id": REPO_A,
        "deployment_id": "dep-a2-no-pr",
        "pull_request_number": None,
        "started_at": dt(2026, 9, 3, 15, 0, 0),
        "finished_at": None,
        "deployed_at": None,
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 3. deployed_at and finished_at NULL -> _dt falls to started_at, pinning
    #    the coalesce ORDER rather than just "some timestamp".
    {
        "repo_id": REPO_B,
        "deployment_id": "dep-b1",
        "pull_request_number": 201,
        "started_at": dt(2026, 9, 3, 16, 0, 0),
        "finished_at": None,
        "deployed_at": None,
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
]

INCIDENTS = [
    # 1. No deployment_id -> HEURISTIC, confidence 0.3, fanned out over EVERY
    #    deployment in REPO_A in input order: dep-a1 then dep-a2-no-pr.
    #    This is the only shape the daily loader can actually produce
    #    (CHAOS-5110): active_incidents_query selects no deployment_id.
    {
        "repo_id": REPO_A,
        "incident_id": "inc-heuristic",
        "started_at": dt(2026, 9, 3, 17, 0, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 2. WITH a deployment_id -> NATIVE, confidence 1.0, single edge, and the
    #    deployment need not appear in the day's deployment list at all.
    #    Unreachable from the daily loader today; kept so the kernel stays
    #    correct if CHAOS-5110 is ever fixed.
    {
        "repo_id": REPO_B,
        "incident_id": "inc-native",
        "deployment_id": "dep-not-in-todays-list",
        "started_at": dt(2026, 9, 3, 18, 0, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
    # 3. Heuristic incident in a repo with NO deployments -> fans out to
    #    nothing, emitting zero edges rather than one edge with an empty
    #    deployment_id.
    {
        "repo_id": UUID("11111111-2222-3333-4444-555555555555"),
        "incident_id": "inc-no-deployments",
        "started_at": dt(2026, 9, 3, 19, 0, 0),
        "last_synced": dt(2026, 9, 3, 23, 0, 0),
    },
]


def main() -> None:
    result = extract_review_deployment_incident_edges(
        org_id=ORG,
        provider=PROVIDER,
        reviews=REVIEWS,
        deployments=DEPLOYMENTS,
        incidents=INCIDENTS,
    )
    print(
        json.dumps(
            {
                "review_outcome_edges": [
                    {
                        "edge_id": e.edge_id,
                        "org_id": str(e.org_id),
                        "pr_id": e.pr_id,
                        "review_outcome_id": e.review_outcome_id,
                        "outcome": e.outcome,
                        "provider": e.provider,
                        "repo_id": str(e.repo_id) if e.repo_id is not None else None,
                        "confidence": e.confidence,
                        "source": e.source,
                        "evidence": e.evidence,
                        "observed_at": e.observed_at.isoformat(),
                    }
                    for e in result.review_outcome_edges
                ],
                "pr_deployment_edges": [
                    {
                        "edge_id": e.edge_id,
                        "org_id": str(e.org_id),
                        "pr_id": e.pr_id,
                        "deployment_id": e.deployment_id,
                        "provider": e.provider,
                        "repo_id": str(e.repo_id) if e.repo_id is not None else None,
                        "confidence": e.confidence,
                        "source": e.source,
                        "evidence": e.evidence,
                        "observed_at": e.observed_at.isoformat(),
                    }
                    for e in result.pr_deployment_edges
                ],
                "deployment_incident_edges": [
                    {
                        "edge_id": e.edge_id,
                        "org_id": str(e.org_id),
                        "deployment_id": e.deployment_id,
                        "incident_id": e.incident_id,
                        "provider": e.provider,
                        "repo_id": str(e.repo_id) if e.repo_id is not None else None,
                        "confidence": e.confidence,
                        "source": e.source,
                        "evidence": e.evidence,
                        "observed_at": e.observed_at.isoformat(),
                    }
                    for e in result.deployment_incident_edges
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
