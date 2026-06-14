"""Unit tests for GitLab MR-review reconstruction (CHAOS-2378).

Proves the seam that wires GitLab approvals + MR notes into
``git_pull_request_reviews``, mirroring the GitHub path so review_edges_daily /
review-latency / /ai/review-load populate for GitLab orgs. No live API or DB.
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import Mock

from dev_health_ops.models.git import GitPullRequestReview
from dev_health_ops.processors.gitlab import (
    _fetch_gitlab_mr_reviews,
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
    reviews = map_gitlab_mr_reviews(
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
    # No per-approver timestamp -> falls back to MR created_at
    assert row.submitted_at == CREATED_AT


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
    reviews = map_gitlab_mr_reviews(
        repo_id=REPO_ID, number=42, approvals=None, notes=notes
    )

    assert len(reviews) == 1
    assert reviews[0].state == "CHANGES_REQUESTED"
    assert reviews[0].reviewer == "bob"
    assert reviews[0].review_id == "note-100"
    assert reviews[0].submitted_at == datetime(2026, 1, 2, 9, 30, tzinfo=timezone.utc)


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
    reviews = map_gitlab_mr_reviews(
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
    reviews = map_gitlab_mr_reviews(
        repo_id=REPO_ID, number=42, approvals=None, notes=notes
    )

    assert len(reviews) == 1
    assert reviews[0].state == "APPROVED"


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
    reviews = map_gitlab_mr_reviews(
        repo_id=REPO_ID, number=42, approvals=None, notes=notes
    )

    assert reviews == []


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
    reviews = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=approvals,
        notes=notes,
        fallback_at=CREATED_AT,
    )

    by_state = sorted(r.state for r in reviews)
    assert by_state == ["APPROVED", "CHANGES_REQUESTED", "COMMENTED"]
    assert all(r.repo_id == REPO_ID and r.number == 42 for r in reviews)


def test_empty_payloads_yield_no_rows():
    assert map_gitlab_mr_reviews(REPO_ID, 42, None, None) == []
    assert map_gitlab_mr_reviews(REPO_ID, 42, {"approved_by": []}, []) == []


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

    reviews = _fetch_gitlab_mr_reviews(
        connector=connector,
        project_id=99,
        mr={"iid": 42},
        repo_id=REPO_ID,
        created_at=CREATED_AT,
    )

    connector.rest_client.get_merge_request_approvals.assert_called_once_with(99, 42)
    connector.rest_client.get_merge_request_notes.assert_called_once_with(99, 42)
    assert sorted(r.state for r in reviews) == ["APPROVED", "COMMENTED"]


def test_fetch_is_resilient_to_missing_approvals_endpoint():
    """MRs whose approvals endpoint 404s still ingest notes (skip cleanly)."""
    connector = Mock()
    connector.rest_client.get_merge_request_approvals.side_effect = Exception("404")
    connector.rest_client.get_merge_request_notes.return_value = []

    reviews = _fetch_gitlab_mr_reviews(
        connector=connector,
        project_id=99,
        mr={"iid": 42},
        repo_id=REPO_ID,
        created_at=CREATED_AT,
    )

    assert reviews == []  # no crash, empty result


def test_fetch_skips_mr_without_iid():
    connector = Mock()
    reviews = _fetch_gitlab_mr_reviews(
        connector=connector,
        project_id=99,
        mr={},
        repo_id=REPO_ID,
        created_at=CREATED_AT,
    )
    assert reviews == []
    connector.rest_client.get_merge_request_approvals.assert_not_called()
