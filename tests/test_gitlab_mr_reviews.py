"""Unit tests for GitLab MR-review reconstruction (CHAOS-2378).

Proves the seam that wires GitLab approvals + MR notes into
``git_pull_request_reviews``, mirroring the GitHub path so review_edges_daily /
review-latency / /ai/review-load populate for GitLab orgs. No live API or DB.
"""

import uuid
from datetime import datetime, timezone
from typing import cast
from unittest.mock import Mock

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation (mirrors tests/test_deployment_pr_inference.py).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.models.git import GitPullRequestReview
from dev_health_ops.processors.gitlab import (
    _fetch_gitlab_mr_reviews,
    _sync_gitlab_mrs_to_store,
    map_gitlab_mr_reviews,
)

REPO_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
CREATED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _approvals(*usernames_ids):
    return {
        "approved_by": [
            {"user": {"id": uid, "username": name}} for name, uid in usernames_ids
        ]
    }


def test_approval_maps_to_approved_state_and_org_scope():
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=_approvals(("alice", 7)),
        notes=[],
        fallback_at=CREATED_AT,
    )

    assert len(reviews) == 1
    row = reviews[0]
    assert isinstance(row, GitPullRequestReview)
    assert row.repo_id == REPO_ID  # org-scoped via repo_id, like GitHub
    assert row.number == 42
    assert row.reviewer == "alice"
    assert row.state == "APPROVED"
    assert row.review_id == "approval-7"
    # No per-approver timestamp -> the row uses the MR created_at fallback so
    # it is storable, but that synthetic time must NOT drive first_review_at.
    assert row.submitted_at == CREATED_AT
    assert first_review_at is None


def test_unapproval_system_note_maps_to_changes_requested():
    notes = [
        {
            "id": 100,
            "system": True,
            "body": "unapproved this merge request",
            "author": {"username": "bob"},
            "created_at": "2026-01-02T09:30:00Z",
        }
    ]
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID, number=42, approvals=None, notes=notes
    )

    assert len(reviews) == 1
    assert reviews[0].state == "CHANGES_REQUESTED"
    assert reviews[0].reviewer == "bob"
    assert reviews[0].review_id == "note-100"
    assert reviews[0].submitted_at == datetime(2026, 1, 2, 9, 30, tzinfo=timezone.utc)
    assert first_review_at == datetime(2026, 1, 2, 9, 30, tzinfo=timezone.utc)


def test_human_comment_note_maps_to_commented():
    notes = [
        {
            "id": 200,
            "system": False,
            "body": "Please rename this variable.",
            "author": {"username": "carol"},
            "created_at": "2026-01-03T08:00:00Z",
        }
    ]
    reviews, _ = map_gitlab_mr_reviews(
        repo_id=REPO_ID, number=42, approvals=None, notes=notes
    )

    assert len(reviews) == 1
    assert reviews[0].state == "COMMENTED"
    assert reviews[0].reviewer == "carol"


def test_approve_system_note_maps_to_approved():
    notes = [
        {
            "id": 300,
            "system": True,
            "body": "approved this merge request",
            "author": {"username": "dave"},
            "created_at": "2026-01-04T10:00:00Z",
        }
    ]
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID, number=42, approvals=None, notes=notes
    )

    assert len(reviews) == 1
    assert reviews[0].state == "APPROVED"
    assert first_review_at == datetime(2026, 1, 4, 10, 0, tzinfo=timezone.utc)


def test_approval_endpoint_and_matching_note_yield_one_review():
    """Regression (CHAOS-2378): an approval that appears in BOTH the approvals
    endpoint AND the approval system note must count once, with the note's real
    timestamp -- not be double-counted nor stamped at MR creation time."""
    approvals = _approvals(("alice", 7))
    notes = [
        {
            "id": 300,
            "system": True,
            "body": "approved this merge request",
            "author": {"username": "alice"},
            "created_at": "2026-01-04T10:00:00Z",
        }
    ]

    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=approvals,
        notes=notes,
        fallback_at=CREATED_AT,
    )

    # Exactly one APPROVED row for alice's single approval event.
    assert len(reviews) == 1
    row = reviews[0]
    assert row.state == "APPROVED"
    assert row.reviewer == "alice"
    assert row.review_id == "note-300"  # the timestamped note wins
    # first_review_at is the real approval time, never the MR-created fallback.
    assert first_review_at == datetime(2026, 1, 4, 10, 0, tzinfo=timezone.utc)
    assert first_review_at != CREATED_AT


def test_approvals_endpoint_backfills_only_unmatched_approvers():
    """An approver present in the endpoint but absent from the notes (e.g.
    truncated note page) is still recorded -- once -- with no real timestamp."""
    approvals = _approvals(("alice", 7), ("bob", 8))
    notes = [
        {
            "id": 300,
            "system": True,
            "body": "approved this merge request",
            "author": {"username": "alice"},
            "created_at": "2026-01-04T10:00:00Z",
        }
    ]

    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=approvals,
        notes=notes,
        fallback_at=CREATED_AT,
    )

    by_reviewer = {r.reviewer: r for r in reviews}
    assert set(by_reviewer) == {"alice", "bob"}
    assert by_reviewer["alice"].review_id == "note-300"
    assert by_reviewer["bob"].review_id == "approval-8"
    assert by_reviewer["bob"].submitted_at == CREATED_AT  # fallback for storage
    # Only the real note timestamp drives first_review_at; bob's fallback does not.
    assert first_review_at == datetime(2026, 1, 4, 10, 0, tzinfo=timezone.utc)


def test_non_review_system_notes_are_ignored():
    notes = [
        {
            "id": 400,
            "system": True,
            "body": "added 1 commit",
            "author": {"username": "eve"},
            "created_at": "2026-01-05T10:00:00Z",
        },
        {
            "id": 401,
            "system": True,
            "body": "assigned to @eve",
            "author": {"username": "eve"},
            "created_at": "2026-01-05T10:01:00Z",
        },
    ]
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID, number=42, approvals=None, notes=notes
    )

    assert reviews == []
    assert first_review_at is None


def test_combined_payload_yields_all_expected_rows():
    approvals = _approvals(("alice", 7))
    notes = [
        {
            "id": 100,
            "system": True,
            "body": "unapproved this merge request",
            "author": {"username": "bob"},
            "created_at": "2026-01-02T09:30:00Z",
        },
        {
            "id": 200,
            "system": False,
            "body": "nit: typo",
            "author": {"username": "carol"},
            "created_at": "2026-01-02T10:00:00Z",
        },
        {
            "id": 401,
            "system": True,
            "body": "changed target branch",
            "author": {"username": "eve"},
            "created_at": "2026-01-02T11:00:00Z",
        },
    ]
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=approvals,
        notes=notes,
        fallback_at=CREATED_AT,
    )

    by_state = sorted(r.state for r in reviews)
    assert by_state == ["APPROVED", "CHANGES_REQUESTED", "COMMENTED"]
    assert all(r.repo_id == REPO_ID and r.number == 42 for r in reviews)
    # Earliest real event is bob's unapproval note.
    assert first_review_at == datetime(2026, 1, 2, 9, 30, tzinfo=timezone.utc)


def test_empty_payloads_yield_no_rows():
    assert map_gitlab_mr_reviews(REPO_ID, 42, None, None) == ([], None)
    assert map_gitlab_mr_reviews(REPO_ID, 42, {"approved_by": []}, []) == ([], None)


def test_fetch_calls_rest_endpoints_and_maps():
    """The fetch helper wires the REST approvals + notes calls into the mapper."""
    connector = Mock()
    connector.rest_client.get_merge_request_approvals.return_value = _approvals(
        ("alice", 7)
    )
    connector.rest_client.get_merge_request_notes.return_value = [
        {
            "id": 200,
            "system": False,
            "body": "looks good",
            "author": {"username": "carol"},
            "created_at": "2026-01-03T08:00:00Z",
        }
    ]

    fetched = _fetch_gitlab_mr_reviews(
        connector=connector,
        project_id=99,
        mr={"iid": 42},
        repo_id=REPO_ID,
        created_at=CREATED_AT,
    )

    connector.rest_client.get_merge_request_approvals.assert_called_once_with(99, 42)
    connector.rest_client.get_merge_request_notes.assert_called_once_with(99, 42)
    assert fetched.known is True
    assert sorted(r.state for r in fetched.reviews) == ["APPROVED", "COMMENTED"]
    assert fetched.first_review_at == datetime(2026, 1, 3, 8, 0, tzinfo=timezone.utc)


def test_fetch_is_resilient_to_missing_approvals_endpoint():
    """MRs whose approvals endpoint 404s still ingest notes (skip cleanly)."""
    connector = Mock()
    connector.rest_client.get_merge_request_approvals.side_effect = Exception("404")
    connector.rest_client.get_merge_request_notes.return_value = []

    fetched = _fetch_gitlab_mr_reviews(
        connector=connector,
        project_id=99,
        mr={"iid": 42},
        repo_id=REPO_ID,
        created_at=CREATED_AT,
    )

    # Notes are authoritative and succeeded (empty) -> known, no reviews.
    assert fetched.known is True
    assert fetched.reviews == []
    assert fetched.first_review_at is None


def test_fetch_notes_failure_reports_unknown():
    """Regression (CHAOS-2378): when the authoritative notes call fails, the
    result is flagged unknown so the caller will NOT zero out review metrics."""
    connector = Mock()
    connector.rest_client.get_merge_request_approvals.return_value = _approvals(
        ("alice", 7)
    )
    connector.rest_client.get_merge_request_notes.side_effect = Exception("timeout")

    fetched = _fetch_gitlab_mr_reviews(
        connector=connector,
        project_id=99,
        mr={"iid": 42},
        repo_id=REPO_ID,
        created_at=CREATED_AT,
    )

    assert fetched.known is False
    assert fetched.reviews == []
    assert fetched.first_review_at is None
    assert fetched.changes_requested_count == 0


def test_fetch_skips_mr_without_iid():
    connector = Mock()
    fetched = _fetch_gitlab_mr_reviews(
        connector=connector,
        project_id=99,
        mr={},
        repo_id=REPO_ID,
        created_at=CREATED_AT,
    )
    assert fetched.reviews == []
    assert fetched.known is True
    connector.rest_client.get_merge_request_approvals.assert_not_called()


# --- Live-path tests: drive _sync_gitlab_mrs_to_store (the prod entrypoint) ---


class _FakeSink:
    """Captures rows written through the IngestionSink seam."""

    def __init__(self):
        self.prs = []
        self.reviews = []

    def insert_git_pull_requests(self, batch):
        self.prs.extend(batch)

        async def _noop():
            return None

        return _noop()

    def insert_git_pull_request_reviews(self, batch):
        self.reviews.extend(batch)

        async def _noop():
            return None

        return _noop()


def _make_connector(mrs_page, approvals_by_iid, notes_by_iid):
    """Build a Mock GitLab connector whose REST client returns the given data."""
    connector = Mock()
    connector.per_page = 100

    pages = {1: mrs_page}

    def _get_merge_requests(*, project_id, state, page, per_page, order_by, sort):
        return pages.get(page, [])

    connector.rest_client.get_merge_requests.side_effect = _get_merge_requests

    def _approvals_for(project_id, iid):
        val = approvals_by_iid.get(iid)
        if isinstance(val, Exception):
            raise val
        return val

    def _notes_for(project_id, iid):
        val = notes_by_iid.get(iid)
        if isinstance(val, Exception):
            raise val
        return val or []

    connector.rest_client.get_merge_request_approvals.side_effect = _approvals_for
    connector.rest_client.get_merge_request_notes.side_effect = _notes_for
    return connector


def _run_sync(connector):
    import asyncio

    sink = _FakeSink()

    async def _driver():
        loop = asyncio.get_running_loop()
        # _sync_gitlab_mrs_to_store is blocking; run it off-loop so its
        # run_coroutine_threadsafe writes can complete against this loop.
        return await loop.run_in_executor(
            None,
            lambda: _sync_gitlab_mrs_to_store(
                connector,
                99,  # project_id
                REPO_ID,
                cast(IngestionSink, sink),
                loop,
                50,  # batch_size
            ),
        )

    total = asyncio.run(_driver())
    return total, sink


def test_live_sync_approved_mr_counts_once_with_real_timestamp():
    """End-to-end through the prod entrypoint: an approved MR (endpoint + note)
    yields one review row and a real (note) first_review_at, not double counts
    nor an MR-created timestamp."""
    mr = {
        "iid": 42,
        "title": "Add feature",
        "description": "desc",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": "2026-01-04T11:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 1,
    }
    connector = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={42: _approvals(("alice", 7))},
        notes_by_iid={
            42: [
                {
                    "id": 300,
                    "system": True,
                    "body": "approved this merge request",
                    "author": {"username": "alice"},
                    "created_at": "2026-01-04T10:00:00Z",
                }
            ]
        },
    )

    total, sink = _run_sync(connector)

    assert total == 1
    assert len(sink.prs) == 1
    pr = sink.prs[0]
    assert pr.reviews_count == 1  # not double-counted
    assert pr.changes_requested_count == 0
    assert pr.first_review_at == datetime(2026, 1, 4, 10, 0, tzinfo=timezone.utc)
    assert len(sink.reviews) == 1
    assert sink.reviews[0].state == "APPROVED"


def test_live_sync_notes_failure_does_not_persist_zeroed_pr():
    """End-to-end: a notes-fetch failure must NOT persist a PR row with zeroed
    review metrics (which ReplacingMergeTree would use to clobber prior data)."""
    mr = {
        "iid": 42,
        "title": "Add feature",
        "description": "desc",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": "2026-01-04T11:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 1,
    }
    connector = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={42: _approvals(("alice", 7))},
        notes_by_iid={42: TimeoutError("notes endpoint down")},
    )

    total, sink = _run_sync(connector)

    # The MR is not counted and no PR/review row is written this cycle, so the
    # existing (previously-correct) row is preserved untouched.
    assert total == 0
    assert sink.prs == []
    assert sink.reviews == []


def test_live_sync_mr_with_no_reviews_persists_zero_counts():
    """A genuinely review-less MR (notes succeed, empty) still persists with
    explicit zero counts -- this is the legitimate 'no reviews' case, distinct
    from the failure case above."""
    mr = {
        "iid": 43,
        "title": "Chore",
        "description": "",
        "state": "opened",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": None,
        "source_branch": "chore",
        "target_branch": "main",
        "user_notes_count": 0,
    }
    connector = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={43: {"approved_by": []}},
        notes_by_iid={43: []},
    )

    total, sink = _run_sync(connector)

    assert total == 1
    assert len(sink.prs) == 1
    pr = sink.prs[0]
    assert pr.reviews_count == 0
    assert pr.changes_requested_count == 0
    assert pr.first_review_at is None
    assert sink.reviews == []
