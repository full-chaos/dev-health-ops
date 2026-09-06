"""Golden generator for the review_edges family (CHAOS-4279).

Frozen output of the REAL Python compute path -- ``compute_review_edges_daily``
(src/dev_health_ops/metrics/reviews.py:22) -- over a synthetic corpus chosen to
hit every branch the native Go port has to reproduce:

  - two reviewers on one author's PR, and one reviewer reviewing twice on the
    same PR: the ordinary counting path, and the +1 accumulation.
  - a review on a PR that is ABSENT from the PR rows: the author lookup misses
    and the edge is DROPPED (reviews.py:52-54). This is the quirk produced by
    the PR loader's narrow window -- `created_at` OR `merged_at` in the day --
    so a review today on a PR neither created nor merged today vanishes. It is
    reproduced deliberately, not fixed.
  - submitted_at exactly ON the day's lower bound (kept) and exactly on the
    upper bound (excluded): the window is [start, end).
  - submitted_at before the window and after it: both excluded.
  - a PR whose author_email is empty but author_name is set: the identity falls
    through to the name.
  - a PR with both author fields empty: the identity is the literal "unknown",
    which is TRUTHY, so the edge is KEPT -- "unknown" as an author is a real
    edge, distinct from the dropped-PR case above.
  - a reviewer name that is whitespace-only, and one that is a lone U+001C:
    Python's str.strip() treats BOTH as whitespace and both fall through to
    "unknown". Go's strings.TrimSpace does NOT strip U+001C, so a naive port
    keeps "\\x1c" as its own identity and splits one contributor's edges in
    two. This is the case that pins pythonparity.Strip.
  - identities differing only by surrounding whitespace ("  ann  " vs "ann"):
    they strip to the SAME identity and their counts merge.
  - two repos, to pin the sort's leading key.

Regenerate with `python tests/fixtures/generate_daily_review_edges_python_golden.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.reviews import compute_review_edges_daily
from dev_health_ops.metrics.schemas import PullRequestReviewRow, PullRequestRow

OUTPUT = Path(__file__).with_name("daily_review_edges_python_golden.json")

DAY = date(2026, 8, 24)
COMPUTED_AT = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)

# REPO_B sorts AFTER REPO_A as a STRING, which is the comparison
# compute_review_edges_daily actually performs (str(repo_id)).
REPO_A = UUID("00000000-0000-4000-8000-00000000000a")
REPO_B = UUID("00000000-0000-4000-8000-00000000000b")


def _at(hour: int, minute: int = 0, second: int = 0, day: int = 24) -> datetime:
    return datetime(2026, 8, day, hour, minute, second, tzinfo=timezone.utc)


PULL_REQUEST_ROWS: list[PullRequestRow] = [
    # Ordinary author, both identity fields present -- email wins.
    {
        "repo_id": REPO_A,
        "number": 1,
        "author_email": "ann@example.com",
        "author_name": "Ann",
        "created_at": _at(8),
        "merged_at": None,
    },
    # Email empty -> falls through to the display name.
    {
        "repo_id": REPO_A,
        "number": 2,
        "author_email": "",
        "author_name": "Bob",
        "created_at": _at(8),
        "merged_at": None,
    },
    # Both empty -> "unknown", which is TRUTHY, so its edges are kept.
    {
        "repo_id": REPO_A,
        "number": 3,
        "author_email": "",
        "author_name": "",
        "created_at": _at(8),
        "merged_at": None,
    },
    # Whitespace-padded email: strips to the same identity as PR 1's author,
    # so edges to both PRs merge onto one row.
    {
        "repo_id": REPO_A,
        "number": 4,
        "author_email": "  ann@example.com  ",
        "author_name": "Ann",
        "created_at": _at(8),
        "merged_at": None,
    },
    # Second repo, to exercise the sort's leading key.
    {
        "repo_id": REPO_B,
        "number": 1,
        "author_email": "dee@example.com",
        "author_name": "Dee",
        "created_at": _at(8),
        "merged_at": None,
    },
]

PULL_REQUEST_REVIEW_ROWS: list[PullRequestReviewRow] = [
    # Two distinct reviewers on ann's PR.
    {
        "repo_id": REPO_A,
        "number": 1,
        "reviewer": "Bob",
        "submitted_at": _at(9),
        "state": "APPROVED",
    },
    {
        "repo_id": REPO_A,
        "number": 1,
        "reviewer": "Cal",
        "submitted_at": _at(10),
        "state": "APPROVED",
    },
    # Same reviewer twice on the same PR -> reviews_count 2 on one edge.
    {
        "repo_id": REPO_A,
        "number": 1,
        "reviewer": "Bob",
        "submitted_at": _at(11),
        "state": "COMMENTED",
    },
    # Same reviewer/author pair reached via a DIFFERENT PR whose author strips
    # to the same identity -> folds onto the same edge, count 3 for Bob->ann.
    {
        "repo_id": REPO_A,
        "number": 4,
        "reviewer": "Bob",
        "submitted_at": _at(11, 30),
        "state": "APPROVED",
    },
    # Reviewer identity padded with whitespace -> same identity as "Cal".
    {
        "repo_id": REPO_A,
        "number": 4,
        "reviewer": "  Cal  ",
        "submitted_at": _at(11, 45),
        "state": "APPROVED",
    },
    # Review of a PR that is NOT in PULL_REQUEST_ROWS -> author lookup misses,
    # edge DROPPED.
    {
        "repo_id": REPO_A,
        "number": 99,
        "reviewer": "Bob",
        "submitted_at": _at(12),
        "state": "APPROVED",
    },
    # Author "unknown" (PR 3) still produces a real edge.
    {
        "repo_id": REPO_A,
        "number": 3,
        "reviewer": "Cal",
        "submitted_at": _at(13),
        "state": "APPROVED",
    },
    # Reviewer whitespace-only -> strips to "" -> "unknown".
    {
        "repo_id": REPO_A,
        "number": 2,
        "reviewer": "   ",
        "submitted_at": _at(14),
        "state": "APPROVED",
    },
    # Reviewer is a lone U+001C. Python's str.strip() removes it (it is a
    # Python whitespace character) -> "unknown"; Go's TrimSpace would NOT.
    {
        "repo_id": REPO_A,
        "number": 2,
        "reviewer": "\x1c",
        "submitted_at": _at(14, 30),
        "state": "APPROVED",
    },
    # Exactly ON the lower bound -> INCLUDED.
    {
        "repo_id": REPO_B,
        "number": 1,
        "reviewer": "Eve",
        "submitted_at": _at(0, 0, 0),
        "state": "APPROVED",
    },
    # Exactly ON the upper bound (next midnight) -> EXCLUDED.
    {
        "repo_id": REPO_B,
        "number": 1,
        "reviewer": "Eve",
        "submitted_at": _at(0, 0, 0, day=25),
        "state": "APPROVED",
    },
    # Before the window -> EXCLUDED.
    {
        "repo_id": REPO_B,
        "number": 1,
        "reviewer": "Eve",
        "submitted_at": _at(23, 59, 59, day=23),
        "state": "APPROVED",
    },
]


def _serialize(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def main() -> str:
    records = compute_review_edges_daily(
        day=DAY,
        pull_request_rows=PULL_REQUEST_ROWS,
        pull_request_review_rows=PULL_REQUEST_REVIEW_ROWS,
        computed_at=COMPUTED_AT,
    )
    document = {
        "records": [
            {field: _serialize(value) for field, value in record.__dict__.items()}
            for record in records
        ]
    }
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    import sys

    rendered = main()
    if "--stdout" in sys.argv:
        sys.stdout.write(rendered)
    else:
        OUTPUT.write_text(rendered)
        print(f"wrote {OUTPUT}")
