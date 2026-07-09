"""Unit tests for GitLab MR-review reconstruction (CHAOS-2378).

Proves the seam that wires GitLab approvals + MR notes into
``git_pull_request_reviews``, mirroring the GitHub path so review_edges_daily /
review-latency / /ai/review-load populate for GitLab orgs. No live API or DB.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation (mirrors tests/test_deployment_pr_inference.py).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.models.git import GitPullRequestReview
from dev_health_ops.processors import gitlab as gitlab_processor
from dev_health_ops.processors.gitlab import (
    BatchGitLabMrSyncError,
    PartialGitLabMrSyncError,
    _fetch_all_mr_notes,
    _fetch_gitlab_mr_reviews,
    _sync_gitlab_mrs_to_store,
    map_gitlab_mr_reviews,
    process_gitlab_projects_batch,
)

REPO_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
CREATED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _approvals(*usernames_ids):
    return {
        "approved_by": [
            {"user": {"id": uid, "username": name}} for name, uid in usernames_ids
        ]
    }


class _FakeGitLabMrCodeClient:
    def __init__(self, *, mrs=None, approvals_by_iid=None, notes_by_iid=None):
        self.mrs = mrs or []
        self.approvals_by_iid = approvals_by_iid or {}
        self.notes_by_iid = notes_by_iid or {}
        self.iter_merge_requests_calls: list[dict[str, Any]] = []
        self.get_merge_requests_page_calls: list[dict[str, Any]] = []
        self.get_mr_approvals_calls: list[tuple[int, int]] = []
        self.iter_mr_notes_calls: list[tuple[int, int, int]] = []
        self.observations = [
            {"route_family": "merge_requests", "request_count": 1},
            {"route_family": "notes", "request_count": 1},
        ]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def iter_merge_requests(self, *, project_id, state, per_page):
        self.iter_merge_requests_calls.append(
            {"project_id": project_id, "state": state, "per_page": per_page}
        )
        return list(self.mrs)

    async def get_merge_requests_page(self, *, project_id, page, state, per_page):
        self.get_merge_requests_page_calls.append(
            {
                "project_id": project_id,
                "page": page,
                "state": state,
                "per_page": per_page,
            }
        )
        if page > 1:
            return []
        return list(self.mrs)

    async def get_mr_approvals(self, project_id, iid):
        self.get_mr_approvals_calls.append((project_id, iid))
        val = self.approvals_by_iid.get(iid)
        if isinstance(val, Exception):
            raise val
        return val

    async def iter_mr_notes(self, project_id, iid, *, per_page):
        self.iter_mr_notes_calls.append((project_id, iid, per_page))
        val = self.notes_by_iid.get(iid)
        if isinstance(val, Exception):
            raise val
        return list(val or [])

    def drain_usage_observations(self):
        observations = list(self.observations)
        self.observations.clear()
        return observations


class _NoSleepGitLabGate:
    def wait_sync(self) -> None:
        return

    def penalize(self, delay_seconds=None) -> float:
        return float(delay_seconds or 0)

    def reset(self) -> None:
        return


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


def test_unapproval_system_note_maps_to_dismissed_not_changes_requested():
    """Revoking an approval is NOT an explicit request-changes event. It must map
    to DISMISSED (on the timeline) and never inflate changes_requested_count /
    review-load rework signals (CHAOS-2378 round 3)."""
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
    assert reviews[0].state == "DISMISSED"
    # Crucially NOT changes-requested: rework/review-load signals only count
    # genuine CHANGES_REQUESTED, so an unapproval contributes zero.
    assert reviews[0].state != "CHANGES_REQUESTED"
    assert reviews[0].reviewer == "bob"
    assert reviews[0].review_id == "note-100"
    assert reviews[0].submitted_at == datetime(2026, 1, 2, 9, 30, tzinfo=timezone.utc)
    assert first_review_at == datetime(2026, 1, 2, 9, 30, tzinfo=timezone.utc)


def test_diff_note_by_reviewer_maps_to_commented():
    """A diff-line comment (``type == "DiffNote"``) by a non-author reviewer is
    the GitLab analogue of a GitHub review comment -> COMMENTED."""
    notes = [
        {
            "id": 200,
            "system": False,
            "type": "DiffNote",
            "body": "Please rename this variable.",
            "author": {"username": "carol"},
            "created_at": "2026-01-03T08:00:00Z",
        }
    ]
    reviews, _ = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=None,
        notes=notes,
        author_username="author",
    )

    assert len(reviews) == 1
    assert reviews[0].state == "COMMENTED"
    assert reviews[0].reviewer == "carol"


def test_generic_discussion_note_is_not_a_review():
    """Regression (CHAOS-2378): a plain MR discussion note (no diff position) is
    ordinary chatter and must NOT be counted as a COMMENTED review -- counting
    it inflated reviews_count and corrupted first_review_at."""
    notes: list[dict[str, Any]] = [
        {
            "id": 201,
            "system": False,
            "type": None,  # generic discussion note, not attached to a diff
            "body": "Thanks, will do!",
            "author": {"username": "carol"},
            "created_at": "2026-01-03T08:00:00Z",
        },
        {
            "id": 202,
            "system": False,
            "type": "DiscussionNote",  # reply in a thread, still not a diff note
            "body": "ping",
            "author": {"username": "carol"},
            "created_at": "2026-01-03T09:00:00Z",
        },
    ]
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=None,
        notes=notes,
        author_username="author",
    )

    assert reviews == []
    assert first_review_at is None


def test_author_self_diff_note_is_not_a_review():
    """Regression (CHAOS-2378): the MR author commenting on their own diff is
    not a review -- it must not mark the MR reviewed nor drive first_review_at."""
    notes = [
        {
            "id": 203,
            "system": False,
            "type": "DiffNote",
            "body": "Self-note: refactor later.",
            "author": {"username": "Author"},  # case-insensitive match
            "created_at": "2026-01-03T08:00:00Z",
        }
    ]
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=REPO_ID,
        number=42,
        approvals=None,
        notes=notes,
        author_username="author",
    )

    assert reviews == []
    assert first_review_at is None


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
            "type": "DiffNote",
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
    assert by_state == ["APPROVED", "COMMENTED", "DISMISSED"]
    # No unapproval is ever counted as a request for changes.
    assert all(r.state != "CHANGES_REQUESTED" for r in reviews)
    assert all(r.repo_id == REPO_ID and r.number == 42 for r in reviews)
    # Earliest real event is bob's unapproval (now DISMISSED) note.
    assert first_review_at == datetime(2026, 1, 2, 9, 30, tzinfo=timezone.utc)


def test_empty_payloads_yield_no_rows():
    assert map_gitlab_mr_reviews(REPO_ID, 42, None, None) == ([], None)
    assert map_gitlab_mr_reviews(REPO_ID, 42, {"approved_by": []}, []) == ([], None)


def test_fetch_calls_code_client_endpoints_and_maps():
    """The fetch helper wires the code-client approvals + notes calls into the mapper."""
    connector = Mock()
    connector.per_page = 100
    connector.rest_client.get_merge_request_approvals.side_effect = AssertionError(
        "legacy approvals path must not be called"
    )
    connector.rest_client.get_merge_request_notes.side_effect = AssertionError(
        "legacy notes path must not be called"
    )
    fake_client = _FakeGitLabMrCodeClient(
        approvals_by_iid={42: _approvals(("alice", 7))},
        notes_by_iid={
            42: [
                {
                    "id": 200,
                    "system": False,
                    "type": "DiffNote",
                    "body": "looks good",
                    "author": {"username": "carol"},
                    "created_at": "2026-01-03T08:00:00Z",
                }
            ]
        },
    )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: fake_client,
    ):
        fetched = _fetch_gitlab_mr_reviews(
            connector=connector,
            project_id=99,
            mr={"iid": 42, "author": {"username": "author"}},
            repo_id=REPO_ID,
            created_at=CREATED_AT,
        )

    assert fake_client.get_mr_approvals_calls == [(99, 42)]
    assert fake_client.iter_mr_notes_calls == [(99, 42, 100)]
    connector.rest_client.get_merge_request_approvals.assert_not_called()
    connector.rest_client.get_merge_request_notes.assert_not_called()
    assert fetched.known is True
    assert sorted(r.state for r in fetched.reviews) == ["APPROVED", "COMMENTED"]
    assert fetched.first_review_at == datetime(2026, 1, 3, 8, 0, tzinfo=timezone.utc)


def test_fetch_is_resilient_to_missing_approvals_endpoint():
    """MRs whose approvals endpoint 404s still ingest notes (skip cleanly)."""
    connector = Mock()
    connector.per_page = 100
    fake_client = _FakeGitLabMrCodeClient(
        approvals_by_iid={42: Exception("404")}, notes_by_iid={42: []}
    )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: fake_client,
    ):
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
    connector.per_page = 100
    fake_client = _FakeGitLabMrCodeClient(
        approvals_by_iid={42: _approvals(("alice", 7))},
        notes_by_iid={42: Exception("timeout")},
    )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: fake_client,
    ):
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
    connector = Mock()
    connector.per_page = 100
    connector.code_client = _FakeGitLabMrCodeClient(
        mrs=mrs_page,
        approvals_by_iid=approvals_by_iid,
        notes_by_iid=notes_by_iid,
    )
    connector.rest_client.get_merge_requests.side_effect = AssertionError(
        "legacy MR list path must not be called"
    )
    connector.rest_client.get_merge_request_approvals.side_effect = AssertionError(
        "legacy approvals path must not be called"
    )
    connector.rest_client.get_merge_request_notes.side_effect = AssertionError(
        "legacy notes path must not be called"
    )
    return connector


def _run_sync(connector, since=None):
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
                "all",  # state
                _NoSleepGitLabGate(),  # gate
                since,
            ),
        )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda connector: connector.code_client,
    ):
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


def test_live_sync_notes_failure_does_not_persist_zeroed_pr_and_fails_loud():
    """End-to-end: a notes-fetch failure must NOT persist a PR row with zeroed
    review metrics (which ReplacingMergeTree would use to clobber prior data),
    AND must NOT report success — otherwise the per-target watermark advances
    past this MR and strands it until a full resync (CHAOS-2378 round 3).

    The fix raises a retryable PartialGitLabMrSyncError so the run is marked
    FAILED, the watermark is held, and Celery retries the project.
    """
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

    with pytest.raises(PartialGitLabMrSyncError) as exc_info:
        _run_sync(connector)

    # The skip is surfaced loudly with the affected iid so it cannot be lost.
    assert exc_info.value.skipped_iids == [42]
    assert exc_info.value.project_id == 99
    # Retryable (non-terminal): a plain RuntimeError, not ValueError/Terminal.
    assert isinstance(exc_info.value, RuntimeError)
    assert not isinstance(exc_info.value, ValueError)


def test_live_sync_partial_failure_flushes_good_rows_before_raising():
    """A degraded MR in a multi-MR page must not discard the successfully
    fetched MRs: their PR/review rows are flushed before the raise (the re-run
    is idempotent via ReplacingMergeTree), so we never lose good work nor
    silently advance the watermark (CHAOS-2378 round 3)."""
    good_mr = {
        "iid": 41,
        "title": "Good MR",
        "description": "desc",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-06T12:00:00Z",
        "merged_at": "2026-01-04T11:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 1,
    }
    bad_mr = {
        "iid": 42,
        "title": "Degraded MR",
        "description": "desc",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": "2026-01-04T11:00:00Z",
        "source_branch": "feat2",
        "target_branch": "main",
        "user_notes_count": 1,
    }
    connector = _make_connector(
        # Page ordered updated_at desc: good MR (41) first, then degraded (42).
        mrs_page=[good_mr, bad_mr],
        approvals_by_iid={41: {"approved_by": []}, 42: {"approved_by": []}},
        notes_by_iid={
            41: [
                {
                    "id": 300,
                    "system": True,
                    "body": "approved this merge request",
                    "author": {"username": "alice"},
                    "created_at": "2026-01-04T10:00:00Z",
                }
            ],
            42: TimeoutError("notes endpoint down"),
        },
    )

    sink = _FakeSink()

    async def _driver():
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: _sync_gitlab_mrs_to_store(
                connector,
                99,
                REPO_ID,
                cast(IngestionSink, sink),
                loop,
                50,
                gate=_NoSleepGitLabGate(),
            ),
        )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda connector: connector.code_client,
    ):
        with pytest.raises(PartialGitLabMrSyncError) as exc_info:
            asyncio.run(_driver())

    # The good MR (41) is persisted; the degraded MR (42) is not.
    assert [pr.number for pr in sink.prs] == [41]
    assert {r.number for r in sink.reviews} == {41}
    assert exc_info.value.skipped_iids == [42]


def test_live_sync_degraded_mr_not_skipped_by_since_after_held_watermark():
    """Regression proving the stranding is closed end-to-end: because a degraded
    run does NOT advance the watermark (it raises), the next incremental run's
    ``since`` stays BELOW the degraded MR's ``updated_at`` and re-attempts it —
    it is not filtered out by the ``updated_at < since`` early-stop. Here the
    retry's notes succeed and the MR is finally persisted (CHAOS-2378 round 3)."""
    updated_at = "2026-01-05T12:00:00Z"
    mr = {
        "iid": 42,
        "title": "Add feature",
        "description": "desc",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": updated_at,
        "merged_at": "2026-01-04T11:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 1,
    }
    # The watermark that WOULD have been set had the first (degraded) run
    # wrongly reported success: just after the MR's updated_at. Since the run
    # raised, this watermark is NOT persisted, so the real ``since`` remains the
    # PRIOR (older) watermark below.
    prior_watermark = datetime(2026, 1, 4, 0, 0, tzinfo=timezone.utc)
    would_be_advanced = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc) + timedelta(
        minutes=1
    )

    # 1. First run: notes down -> raises, no watermark advance.
    connector_fail = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={42: {"approved_by": []}},
        notes_by_iid={42: TimeoutError("notes endpoint down")},
    )
    with pytest.raises(PartialGitLabMrSyncError):
        _run_sync(connector_fail, since=prior_watermark)

    # 2. Next run uses the UNADVANCED (prior) watermark as ``since``. The MR's
    #    updated_at is >= since, so it is NOT filtered out; notes now succeed.
    connector_ok = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={42: {"approved_by": []}},
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
    total, sink = _run_sync(connector_ok, since=prior_watermark)

    # Re-attempted and persisted on the retry — not stranded.
    assert total == 1
    assert [pr.number for pr in sink.prs] == [42]
    assert sink.prs[0].reviews_count == 1

    # Sanity: had the watermark wrongly advanced past the MR, the MR would have
    # been filtered by the ``updated_at < since`` early-stop and never synced.
    connector_stranded = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={42: {"approved_by": []}},
        notes_by_iid={42: []},
    )
    total_stranded, sink_stranded = _run_sync(
        connector_stranded, since=would_be_advanced
    )
    assert total_stranded == 0
    assert sink_stranded.prs == []


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


# --- Pagination + classification regressions (CHAOS-2378 round 2) ---


def test_fetch_all_mr_notes_exhausts_pages():
    """Regression (CHAOS-2378): notes are paginated; we must fetch EVERY page,
    not just page 1, or late approval/review events are silently dropped."""
    # 100 generic notes (a full first page) + a 101st note carrying the only
    # approval system note. A single-page fetch would lose it entirely.
    page_one = [
        {
            "id": i,
            "system": True,
            "body": "added 1 commit",
            "author": {"username": "ci"},
            "created_at": "2026-01-02T00:00:00Z",
        }
        for i in range(100)
    ]
    page_two = [
        {
            "id": 1000,
            "system": True,
            "body": "approved this merge request",
            "author": {"username": "alice"},
            "created_at": "2026-01-04T10:00:00Z",
        }
    ]
    all_notes = page_one + page_two

    connector = Mock()
    connector.per_page = 100
    connector.code_client = _FakeGitLabMrCodeClient(notes_by_iid={42: all_notes})
    connector.rest_client.get_merge_request_notes.side_effect = AssertionError(
        "legacy notes path must not be called"
    )

    with patch.object(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda connector: connector.code_client,
    ):
        fetched = _fetch_all_mr_notes(connector, project_id=99, iid=42)

    assert len(fetched) == 101
    # The page-2 approval note survived pagination.
    assert any(n["id"] == 1000 for n in fetched)
    assert connector.code_client.iter_mr_notes_calls == [(99, 42, 100)]
    connector.rest_client.get_merge_request_notes.assert_not_called()


def test_live_sync_approval_after_first_note_page_is_counted():
    """End-to-end regression (CHAOS-2378): an MR whose approval lands past the
    first 100 notes still yields an APPROVED review with the real timestamp --
    the sync no longer drops it by fetching only page 1."""
    notes = [
        {
            "id": i,
            "system": True,
            "body": "added 1 commit",
            "author": {"username": "ci"},
            "created_at": "2026-01-02T00:00:00Z",
        }
        for i in range(100)
    ] + [
        {
            "id": 9999,
            "system": True,
            "body": "approved this merge request",
            "author": {"username": "alice"},
            "created_at": "2026-01-04T10:00:00Z",
        }
    ]
    mr = {
        "iid": 44,
        "title": "Busy MR",
        "description": "desc",
        "state": "merged",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": "2026-01-04T11:00:00Z",
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 101,
    }
    connector = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={44: {"approved_by": []}},
        notes_by_iid={44: notes},
    )

    total, sink = _run_sync(connector)

    assert total == 1
    pr = sink.prs[0]
    assert pr.reviews_count == 1
    assert pr.first_review_at == datetime(2026, 1, 4, 10, 0, tzinfo=timezone.utc)
    assert len(sink.reviews) == 1
    assert sink.reviews[0].state == "APPROVED"


def test_live_sync_generic_notes_are_not_counted_as_reviews():
    """End-to-end regression (CHAOS-2378): generic MR discussion notes and the
    author's own replies must NOT inflate reviews_count nor set first_review_at;
    they live in comments_count only."""
    mr = {
        "iid": 45,
        "title": "Chat-heavy MR",
        "description": "desc",
        "state": "opened",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": None,
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 3,
    }
    notes = [
        {
            "id": 1,
            "system": False,
            "type": None,  # generic discussion
            "body": "When will this be ready?",
            "author": {"username": "pm"},
            "created_at": "2026-01-02T08:00:00Z",
        },
        {
            "id": 2,
            "system": False,
            "type": "DiffNote",  # but it's the AUTHOR self-commenting
            "body": "Will refactor.",
            "author": {"username": "author"},
            "created_at": "2026-01-02T09:00:00Z",
        },
        {
            "id": 3,
            "system": False,
            "type": "DiscussionNote",  # thread reply, not a diff comment
            "body": "thanks",
            "author": {"username": "pm"},
            "created_at": "2026-01-02T10:00:00Z",
        },
    ]
    connector = _make_connector(
        mrs_page=[mr],
        approvals_by_iid={45: {"approved_by": []}},
        notes_by_iid={45: notes},
    )

    total, sink = _run_sync(connector)

    assert total == 1
    pr = sink.prs[0]
    assert pr.reviews_count == 0  # no genuine review activity
    assert pr.first_review_at is None
    assert pr.comments_count == 3  # generic chatter stays here
    assert sink.reviews == []


def test_live_sync_unapproval_does_not_inflate_changes_requested_count():
    """End-to-end regression (CHAOS-2378 round 3): a reviewer revoking their
    approval (GitLab 'unapproved' system note) must NOT increment
    changes_requested_count — only a genuine request-changes event should drive
    rework/review-load pressure. The unapproval is recorded as DISMISSED on the
    timeline (reviews_count includes it) but is excluded from the rework count.
    """
    mr = {
        "iid": 46,
        "title": "Approved then unapproved",
        "description": "desc",
        "state": "opened",
        "author": {"username": "author"},
        "created_at": "2026-01-01T12:00:00Z",
        "updated_at": "2026-01-05T12:00:00Z",
        "merged_at": None,
        "source_branch": "feat",
        "target_branch": "main",
        "user_notes_count": 0,
    }
    notes = [
        {
            "id": 500,
            "system": True,
            "body": "approved this merge request",
            "author": {"username": "bob"},
            "created_at": "2026-01-02T09:00:00Z",
        },
        {
            "id": 501,
            "system": True,
            "body": "unapproved this merge request",
            "author": {"username": "bob"},
            "created_at": "2026-01-02T09:30:00Z",
        },
    ]
    connector = _make_connector(
        mrs_page=[mr],
        # Net approvals state is empty (bob revoked); the unapproval is the
        # later authoritative note.
        approvals_by_iid={46: {"approved_by": []}},
        notes_by_iid={46: notes},
    )

    total, sink = _run_sync(connector)

    assert total == 1
    pr = sink.prs[0]
    # Both the approval and the dismissal are on the review timeline...
    assert pr.reviews_count == 2
    states = sorted(r.state for r in sink.reviews)
    assert states == ["APPROVED", "DISMISSED"]
    # ...but the unapproval is NOT a request for changes: zero rework pressure.
    assert pr.changes_requested_count == 0
    assert all(r.state != "CHANGES_REQUESTED" for r in sink.reviews)


# ---------------------------------------------------------------------------
# Batch/group path: a degraded MR fetch must not look like a successful sync
# (CHAOS-2378 round 4).
# ---------------------------------------------------------------------------


class _FakeBatchSink:
    """Captures rows written by the batch store_result seam."""

    def __init__(self, store):
        self.store = store
        self.repos = []

    async def insert_repo(self, repo):
        self.repos.append(repo)


class _FakeProjectInfo:
    def __init__(self, project_id, full_name):
        self.id = project_id
        self.full_name = full_name
        self.url = f"https://gitlab.com/{full_name}"
        self.default_branch = "main"


class _FakeBatchConnector:
    """Stands in for GitLabConnector inside process_gitlab_projects_batch for
    the non-async, list-projects path."""

    def __init__(self, projects, **_kwargs):
        self._projects = projects
        self.closed = False

    def _get_projects_for_processing(self, *, group_name, pattern, max_repos):
        return list(self._projects)

    def close(self):
        self.closed = True


class _FakeBatchDiscoveryClient:
    def __init__(self, projects):
        self._projects = projects

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def list_projects(self, **_kwargs):
        return list(self._projects)

    def drain_usage_observations(self):
        return []


def _patch_batch(monkeypatch, projects, sync_behavior):
    """Wire process_gitlab_projects_batch to fakes.

    ``sync_behavior`` maps project_id -> either an int (success) or a
    PartialGitLabMrSyncError instance to raise from _sync_gitlab_mrs_to_store.
    """
    monkeypatch.setattr(
        gitlab_processor,
        "GitLabConnector",
        lambda **kw: _FakeBatchConnector(projects, **kw),
    )
    monkeypatch.setattr(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: _FakeBatchDiscoveryClient(projects),
    )
    monkeypatch.setattr(gitlab_processor, "IngestionSink", _FakeBatchSink)
    monkeypatch.setattr(gitlab_processor, "CONNECTORS_AVAILABLE", True)

    calls: list[int] = []

    def _fake_sync_mrs(
        connector,
        project_id,
        repo_id,
        sink,
        loop,
        batch_size,
        state,
        gate,
        since,
        until=None,
        usage_sink=None,
    ):
        calls.append(project_id)
        outcome = sync_behavior[project_id]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(gitlab_processor, "_sync_gitlab_mrs_to_store", _fake_sync_mrs)
    return calls


_BATCH_PRS_ONLY: dict[str, Any] = dict(
    sync_git=False,
    sync_prs=True,
    sync_cicd=False,
    sync_deployments=False,
    sync_incidents=False,
    sync_security=False,
    sync_tests=False,
    backfill_missing=False,
    use_async=False,
)


def test_batch_degraded_mr_fetch_raises_and_does_not_report_success(monkeypatch):
    """A single degraded project in a group sync must NOT let the batch return
    normally: process_gitlab_projects_batch raises BatchGitLabMrSyncError so the
    caller (sync_gitlab_target) marks the run failed and retries, instead of
    advancing the watermark past the skipped MRs (CHAOS-2378 round 4)."""
    projects = [
        _FakeProjectInfo(1, "group/healthy"),
        _FakeProjectInfo(2, "group/degraded"),
    ]
    calls = _patch_batch(
        monkeypatch,
        projects,
        sync_behavior={
            1: 5,  # 5 MRs synced fine
            2: PartialGitLabMrSyncError([42, 43], project_id=2),
        },
    )

    store = Mock()
    with pytest.raises(BatchGitLabMrSyncError) as exc_info:
        asyncio.run(
            process_gitlab_projects_batch(
                store=store,
                token="tok",
                group_name="group",
                pattern="*",
                **_BATCH_PRS_ONLY,
            )
        )

    # Both projects were attempted (the degraded one did not abort the batch).
    assert set(calls) == {1, 2}
    # The aggregate error carries the degraded project's partial failure so the
    # skipped iids are surfaced, not lost.
    assert [e.project_id for e in exc_info.value.errors] == [2]
    assert exc_info.value.errors[0].skipped_iids == [42, 43]


def test_batch_rate_limited_mr_fetch_propagates(monkeypatch):
    projects = [
        _FakeProjectInfo(1, "group/healthy"),
        _FakeProjectInfo(2, "group/limited"),
        _FakeProjectInfo(3, "group/not-reached"),
    ]
    calls = _patch_batch(
        monkeypatch,
        projects,
        sync_behavior={
            1: 5,
            2: RateLimitException("limited"),
            3: 5,
        },
    )

    store = Mock()
    with pytest.raises(RateLimitException):
        asyncio.run(
            process_gitlab_projects_batch(
                store=store,
                token="tok",
                group_name="group",
                pattern="*",
                max_concurrent=1,
                **_BATCH_PRS_ONLY,
            )
        )

    assert calls[:2] == [1, 2]


def test_batch_flushes_healthy_repos_before_raising(monkeypatch):
    """Healthy projects' repo rows are still written even when a later project
    degrades — the batch keeps flushing good work, then raises (CHAOS-2378)."""
    projects = [
        _FakeProjectInfo(1, "group/healthy-a"),
        _FakeProjectInfo(2, "group/degraded"),
        _FakeProjectInfo(3, "group/healthy-b"),
    ]
    _patch_batch(
        monkeypatch,
        projects,
        sync_behavior={
            1: 1,
            2: PartialGitLabMrSyncError([99], project_id=2),
            3: 1,
        },
    )

    captured_sinks: list[_FakeBatchSink] = []
    real_sink_cls = _FakeBatchSink

    def _capturing_sink(store):
        sink = real_sink_cls(store)
        captured_sinks.append(sink)
        return sink

    monkeypatch.setattr(gitlab_processor, "IngestionSink", _capturing_sink)

    store = Mock()
    with pytest.raises(BatchGitLabMrSyncError):
        asyncio.run(
            process_gitlab_projects_batch(
                store=store,
                token="tok",
                group_name="group",
                pattern="*",
                max_concurrent=1,  # deterministic ordering
                **_BATCH_PRS_ONLY,
            )
        )

    # All three projects' repo rows were flushed (healthy work preserved); the
    # degraded project's repo still inserts because the partial error is raised
    # AFTER the MR flush, not before the repo upsert.
    assert len(captured_sinks) == 1
    stored = {repo.repo for repo in captured_sinks[0].repos}
    assert stored == {"group/healthy-a", "group/degraded", "group/healthy-b"}


def test_batch_all_healthy_returns_success(monkeypatch):
    """Guard against over-raising: a batch where every project syncs cleanly must
    return normally (None) so the caller reports success (CHAOS-2378)."""
    projects = [
        _FakeProjectInfo(1, "group/a"),
        _FakeProjectInfo(2, "group/b"),
    ]
    _patch_batch(
        monkeypatch,
        projects,
        sync_behavior={1: 3, 2: 4},
    )

    store = Mock()
    # Must NOT raise.
    result = asyncio.run(
        process_gitlab_projects_batch(
            store=store,
            token="tok",
            group_name="group",
            pattern="*",
            **_BATCH_PRS_ONLY,
        )
    )
    assert result is None
