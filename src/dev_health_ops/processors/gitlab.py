import asyncio
import logging
import zipfile
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from dev_health_ops.analytics.complexity import (
    DEFAULT_COMPLEXITY_CONFIG_PATH,
    ComplexityScanner,
)
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.models import git as git_models
from dev_health_ops.models.git import (
    CiPipelineRun,
    Deployment,
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitPullRequest,
    GitPullRequestReview,
    Incident,
    Repo,
)
from dev_health_ops.processors.base_git import (
    BaseGitProcessor,
    backfill_file_records,
    blame_backfill_needed,
    build_ci_pipeline_run,
    build_connector_pull_request,
    build_deployment,
    build_git_pull_request,
    check_backfill_needs,
    resolve_commit_stats_limit,
    resolve_incident_labels,
    select_unblamed_paths,
)
from dev_health_ops.processors.fetch_utils import (
    AsyncBatchCollector,
    safe_parse_datetime,
)
from dev_health_ops.processors.release_ref import get_release_ref_enrichment
from dev_health_ops.processors.storage_protocol import GitSyncStore
from dev_health_ops.processors.testops_ingest import (
    MAX_ARTIFACTS_PER_RUN,
    MAX_RUNS_PER_SYNC,
    ingest_report_members,
)
from dev_health_ops.processors.testops_tests import process_gitlab_test_report
from dev_health_ops.providers.pr_state import normalize_pr_state
from dev_health_ops.utils import (
    AGGREGATE_STATS_MARKER,
    BATCH_SIZE,
    CONNECTORS_AVAILABLE,
    is_skippable,
)

if TYPE_CHECKING:
    from dev_health_ops.connectors import (
        BatchResult,
        ConnectorException,
        GitLabConnector,
    )
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate
elif CONNECTORS_AVAILABLE:
    from dev_health_ops.connectors import (
        BatchResult,
        ConnectorException,
        GitLabConnector,
    )
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate
else:
    BatchResult = None
    GitLabConnector = None
    ConnectorException = Exception
    RateLimitConfig = None
    RateLimitGate = None


# --- GitLab Sync Helpers ---


def _fetch_gitlab_project_info_sync(connector, project_id):
    """Sync helper to fetch GitLab project info."""
    gl_project = connector.gitlab.projects.get(project_id)
    # Access properties to force load if lazy
    _ = gl_project.name
    return gl_project


def _fetch_gitlab_commits_sync(
    gl_project,
    max_commits: int | None,
    repo_id,
    since: datetime | None = None,
):
    """Sync helper to fetch GitLab commits."""
    list_params: dict[str, object] = {"per_page": 100, "get_all": False}
    if since is not None:
        since_iso = since.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        list_params["since"] = since_iso

    commit_objects = []
    count = 0
    commit_hashes = []
    page = 1
    per_page = min(max_commits, 100) if max_commits else 100
    stop_due_to_since = False

    while True:
        if max_commits is not None and count >= max_commits:
            break

        page_params = dict(list_params)
        page_params["page"] = page
        page_params["per_page"] = per_page
        commits_page = gl_project.commits.list(**page_params)
        if not commits_page:
            break
        logging.debug(
            "GitLab commits page %d returned %d items",
            page,
            len(commits_page),
        )

        for commit in commits_page:
            if max_commits is not None and count >= max_commits:
                break

            committed_when = None
            if hasattr(commit, "committed_date") and commit.committed_date:
                try:
                    committed_when = safe_parse_datetime(commit.committed_date)
                except Exception:
                    committed_when = None

            if since is not None and isinstance(committed_when, datetime):
                if committed_when.astimezone(timezone.utc) < since:
                    stop_due_to_since = True
                    break

            git_commit = GitCommit(
                repo_id=repo_id,
                hash=commit.id,
                message=commit.message,
                author_name=(
                    commit.author_name if hasattr(commit, "author_name") else "Unknown"
                ),
                author_email=None,
                author_when=(
                    safe_parse_datetime(commit.authored_date)
                    if hasattr(commit, "authored_date")
                    else datetime.now(timezone.utc)
                ),
                committer_name=(
                    commit.committer_name
                    if hasattr(commit, "committer_name")
                    else "Unknown"
                ),
                committer_email=None,
                committer_when=(
                    safe_parse_datetime(commit.committed_date)
                    if hasattr(commit, "committed_date")
                    else datetime.now(timezone.utc)
                ),
                parents=len(commit.parent_ids) if hasattr(commit, "parent_ids") else 0,
            )
            commit_objects.append(git_commit)
            commit_hashes.append(commit.id)
            count += 1

        if stop_due_to_since or len(commits_page) < per_page:
            break
        page += 1

    return commit_hashes, commit_objects


def _fetch_gitlab_commit_stats_sync(
    gl_project, commit_hashes, repo_id, max_stats, gate=None
):
    """Sync helper to fetch detailed commit stats from GitLab.

    Each commit detail is one REST call, so each iteration waits on the
    rate-limit gate.
    """
    gate = BaseGitProcessor.ensure_gate(gate)
    stats_objects = []

    for commit_hash in commit_hashes[:max_stats]:
        try:
            if gate is not None:
                gate.wait_sync()
            detailed_commit = gl_project.commits.get(commit_hash)
            if hasattr(detailed_commit, "stats"):
                stat = GitCommitStat(
                    repo_id=repo_id,
                    commit_hash=commit_hash,
                    file_path=AGGREGATE_STATS_MARKER,
                    additions=detailed_commit.stats.get("additions", 0),
                    deletions=detailed_commit.stats.get("deletions", 0),
                    old_file_mode="unknown",
                    new_file_mode="unknown",
                )
                stats_objects.append(stat)
        except Exception as e:
            logging.warning(
                "Failed to get stats for commit %s: %s",
                commit_hash,
                e,
            )
    return stats_objects


def _fetch_gitlab_mrs_sync(connector, project_id, repo_id, max_mrs):
    """Sync helper to fetch GitLab Merge Requests."""
    logging.info(
        "Fetching merge requests for project %d...",
        project_id,
    )
    mrs = connector.get_merge_requests(
        project_id=project_id, state="all", max_mrs=max_mrs
    )
    pr_objects = []
    for mr in mrs:
        git_pr = build_connector_pull_request(
            mr,
            repo_id=repo_id,
            state=normalize_pr_state(mr.state, mr.merged_at),
            author_email=None,
        )
        pr_objects.append(git_pr)
    logging.info(
        "Fetched %d merge requests for project %d",
        len(pr_objects),
        project_id,
    )
    return pr_objects


# GitLab system-note bodies that signal an approval state change. GitLab does
# not expose a discrete "review" object like GitHub, so we reconstruct reviews
# from the approvals endpoint plus the MR note stream (CHAOS-2378).
_GITLAB_APPROVE_NOTE = "approved this merge request"
_GITLAB_UNAPPROVE_NOTE = "unapproved this merge request"

# A GitLab MR note attached to a diff position (``type == "DiffNote"``) is a
# code-line review comment — the GitLab analogue of a GitHub review comment.
# Plain discussion notes (``type`` null/``"DiscussionNote"``) are ordinary MR
# chatter (author replies, status banter) and are NOT review activity, so they
# must stay in ``comments_count`` only and never become COMMENTED review rows
# (CHAOS-2378: counting them inflated reviews_count and corrupted
# first_review_at / review-latency).
_GITLAB_DIFF_NOTE_TYPE = "DiffNote"


class PartialGitLabMrSyncError(RuntimeError):
    """Raised when one or more MR rows were skipped due to a degraded review
    fetch (e.g. a transient MR-notes failure).

    The MR sync must NOT report overall success when it has silently skipped MR
    rows: a successful run advances the per-target sync watermark to the run's
    start time, so on the next incremental run those MRs fall behind ``since``
    (``updated_at < since``) and are never retried until a full resync — a
    permanent, silent loss of PR facts and review metrics with no failed job to
    alert operators (CHAOS-2378).

    Raising a *retryable* (non-terminal) error instead keeps the run marked
    FAILED so the watermark is held at its prior value and the Celery task
    retries the whole project. Rows that were fetched successfully this cycle
    are flushed before the raise; the re-run is idempotent (PR/review rows are
    ReplacingMergeTree-replaced), so re-writing them is harmless.
    """

    def __init__(self, skipped_iids: list[int], project_id: int) -> None:
        self.skipped_iids = skipped_iids
        self.project_id = project_id
        super().__init__(
            f"GitLab MR sync for project {project_id} skipped "
            f"{len(skipped_iids)} MR row(s) due to degraded review fetch "
            f"(iids={skipped_iids[:20]}); not advancing watermark"
        )


class BatchGitLabMrSyncError(RuntimeError):
    """Raised by the batch/group GitLab sync when one or more projects skipped
    MR rows due to a degraded review fetch (each surfaced as a
    :class:`PartialGitLabMrSyncError` during per-project processing).

    The batch path processes many projects and must keep flushing the good rows
    from healthy projects, so it cannot abort the whole run the moment one
    project degrades. Instead it *accumulates* the per-project failures and
    raises this aggregate error after the batch summary. Raising (rather than
    returning normally) is what stops the caller — ``sync_gitlab_target`` — from
    reporting success: a successful sync advances the per-target watermark, so a
    silently-skipped MR would fall behind ``since`` on the next incremental run
    and never be retried (a permanent, silent loss of PR facts and review
    metrics with no failed job to alert operators — the exact failure mode the
    per-project :class:`PartialGitLabMrSyncError` exists to prevent). Because the
    error is retryable and PR/review rows are ReplacingMergeTree-replaced, the
    re-run is idempotent (CHAOS-2378).
    """

    def __init__(self, errors: list[PartialGitLabMrSyncError]) -> None:
        self.errors = errors
        project_ids = [e.project_id for e in errors]
        total_skipped = sum(len(e.skipped_iids) for e in errors)
        super().__init__(
            f"GitLab batch MR sync degraded for {len(errors)} project(s) "
            f"(project_ids={project_ids[:20]}); {total_skipped} MR row(s) "
            f"skipped due to degraded review fetch; not reporting success so "
            f"the run is retried"
        )


def map_gitlab_mr_reviews(
    repo_id: Any,
    number: int,
    approvals: dict[str, Any] | None,
    notes: list[dict[str, Any]] | None,
    fallback_at: datetime | None = None,
    author_username: str | None = None,
) -> tuple[list[GitPullRequestReview], datetime | None]:
    """Map GitLab approvals + MR notes to GitPullRequestReview rows.

    Mirrors the GitHub review states so downstream metrics
    (review_edges_daily, user_metrics_daily review latency,
    /ai/review-load) treat both providers identically (CHAOS-2378):

    * approval ("approved" system note / approvals.approved_by) -> APPROVED
    * "unapproved" system note -> DISMISSED (revoking an approval is not an
      explicit request for changes; it is excluded from changes_requested_count)
    * diff-line review comment by a non-author reviewer
      (``type == "DiffNote"``) -> COMMENTED

    Only diff-line notes authored by someone other than the MR author count as
    review activity. Generic MR discussion notes (``type`` null /
    ``"DiscussionNote"``) and the author's own replies are ordinary chatter:
    they stay in ``comments_count`` only and must NOT become COMMENTED rows,
    because doing so inflated ``reviews_count`` and let non-review timestamps
    drive ``first_review_at`` / review-latency (CHAOS-2378). Pass the MR author
    via ``author_username`` so self-comments are excluded.

    Approval events appear in *both* the note stream (a timestamped
    "approved this merge request" system note) and the approvals endpoint
    (a timestampless ``approved_by`` entry). To avoid double-counting one
    approval and inventing a review-at-MR-creation timestamp, system notes
    are authoritative: an approver already covered by an approval note is
    *not* re-emitted from the approvals endpoint. The approvals endpoint only
    backfills approvers with no corresponding note (e.g. notes truncated),
    and those rows carry the MR-derived fallback timestamp purely so the row
    is storable — they are excluded from ``first_review_at`` (see below).

    Returns ``(reviews, first_review_at)`` where ``first_review_at`` is the
    earliest review time derived from a *real* event timestamp only (never the
    fallback), so pickup-latency analytics are not corrupted. Rows are
    org-scoped via ``repo_id`` (the repo belongs to an org), exactly like the
    GitHub path. MRs with no approvals/notes yield ``([], None)``.
    """
    reviews: list[GitPullRequestReview] = []
    seen_ids: set[str] = set()
    # MR author: their own comments/replies are not reviews and are excluded
    # from COMMENTED rows (the author cannot review their own MR).
    author = (author_username or "").strip().lower()
    # Reviewers whose approval is already captured by a timestamped note;
    # used to dedup the timestampless approvals-endpoint entries.
    approved_reviewers: set[str] = set()
    # Earliest *real* (non-fallback) review timestamp, drives first_review_at.
    first_review_at: datetime | None = None

    def _add(
        review_id: str,
        reviewer: str,
        state: str,
        at: datetime | None,
    ) -> None:
        nonlocal first_review_at
        if not review_id or review_id in seen_ids:
            return
        seen_ids.add(review_id)
        # Only a known event time advances first_review_at; the MR-created
        # fallback must never imply a review happened at creation.
        if at is not None and (first_review_at is None or at < first_review_at):
            first_review_at = at
        reviews.append(
            GitPullRequestReview(
                repo_id=repo_id,
                number=int(number),
                review_id=review_id,
                reviewer=reviewer or "Unknown",
                state=state,
                submitted_at=at or fallback_at or datetime.now(timezone.utc),
            )
        )

    # 1. Note stream FIRST (authoritative, timestamped). System notes capture
    #    (un)approval events; diff-line notes by non-authors are review comments.
    for note in notes or []:
        if not isinstance(note, dict):
            continue
        note_author = note.get("author") if isinstance(note.get("author"), dict) else {}
        username = (note_author or {}).get("username") or "Unknown"
        created_at = safe_parse_datetime(note.get("created_at"))
        note_id = note.get("id")
        body = str(note.get("body") or "").strip().lower()

        if note.get("system"):
            if body.startswith(_GITLAB_APPROVE_NOTE):
                approved_reviewers.add(username)
                _add(f"note-{note_id}", username, "APPROVED", created_at)
            elif body.startswith(_GITLAB_UNAPPROVE_NOTE):
                # Revoking an approval is NOT an explicit "request changes" event.
                # GitLab has no native request-changes action, so an unapproval
                # is the dismissal of a prior approval, not rework pressure.
                # Map it to GitHub's canonical DISMISSED state (shared review
                # vocabulary) so it stays on the review timeline but is excluded
                # from changes_requested_count / changes_requested_given — only a
                # genuine CHANGES_REQUESTED must inflate rework/review-load
                # signals (CHAOS-2378).
                _add(f"note-{note_id}", username, "DISMISSED", created_at)
            # other system notes (label/assignee/etc.) are not reviews
            continue

        # Human note. Only a diff-line comment (``type == "DiffNote"``) by a
        # reviewer other than the MR author is review activity; generic
        # discussion notes and the author's own comments are plain chatter
        # (counted via comments_count, never as a COMMENTED review row).
        if note.get("type") != _GITLAB_DIFF_NOTE_TYPE:
            continue
        if author and username.strip().lower() == author:
            continue
        _add(f"note-{note_id}", username, "COMMENTED", created_at)

    # 2. Approvals endpoint: backfill only approvers NOT already covered by a
    #    timestamped approval note, so a single approval is counted once. These
    #    entries have no per-approver timestamp (excluded from first_review_at).
    for entry in (approvals or {}).get("approved_by") or []:
        user = entry.get("user") if isinstance(entry, dict) else None
        if not isinstance(user, dict):
            continue
        username = user.get("username") or "Unknown"
        if username in approved_reviewers:
            continue  # already emitted from the authoritative note
        approved_reviewers.add(username)
        user_id = user.get("id")
        review_id = (
            f"approval-{user_id}" if user_id is not None else f"approval-{username}"
        )
        _add(review_id, username, "APPROVED", None)

    return reviews, first_review_at


class _MrReviewFetch(NamedTuple):
    """Result of one MR's review fetch.

    ``known`` is False when the *authoritative* notes call failed, so the
    caller can avoid clobbering previously-correct PR review metrics with
    zero/null (CHAOS-2378). ``first_review_at`` is derived only from real
    event timestamps (never the MR-created fallback).
    """

    reviews: list[GitPullRequestReview]
    first_review_at: datetime | None
    changes_requested_count: int
    known: bool


def _fetch_all_mr_notes(
    connector,
    project_id: int,
    iid: int,
    gate: Any = None,
) -> list[dict[str, Any]]:
    """Fetch *every* page of an MR's notes, not just the first.

    The notes endpoint is paginated (per_page<=100); a busy MR can carry an
    approval / unapproval / review-comment event past page 1. Fetching only the
    first page would silently drop those events and undercount reviews while the
    sync still reports success (CHAOS-2378). We exhaust pages, honouring the
    shared rate-limit gate/backoff exactly like the MR-list loop.

    Raises on a non-rate-limit error so the caller can flag the MR's review
    state as ``known=False`` (an incomplete page set is as untrustworthy as a
    failed first page — both must avoid clobbering correct metrics).
    """
    per_page = int(getattr(connector, "per_page", 100) or 100)
    all_notes: list[dict[str, Any]] = []
    page = 1
    while True:
        if gate is not None:
            gate.wait_sync()
        try:
            page_notes = connector.rest_client.get_merge_request_notes(
                project_id,
                iid,
                page=page,
                per_page=per_page,
            )
            if gate is not None:
                gate.reset()
        except Exception as exc:
            retry_after = getattr(exc, "retry_after_seconds", None)
            if retry_after is not None and gate is not None:
                applied = gate.penalize(retry_after)
                logging.info(
                    "GitLab rate limited fetching notes for MR !%d; backoff %.1fs (%s)",
                    iid,
                    applied,
                    exc,
                )
                continue
            raise
        if not page_notes:
            break
        all_notes.extend(page_notes)
        # Final page when the server returns fewer than a full page.
        if len(page_notes) < per_page:
            break
        page += 1
    return all_notes


def _fetch_gitlab_mr_reviews(
    connector,
    project_id: int,
    mr: dict[str, Any],
    repo_id: Any,
    created_at: datetime | None,
    gate: Any = None,
) -> _MrReviewFetch:
    """Best-effort fetch + map of one MR's reviews (approvals + notes).

    The MR notes endpoint is the authoritative, timestamped source and is
    available on every tier; the approvals endpoint only supplements it (and
    404s on tiers without merge-request approvals). Accordingly:

    * approvals-only failure is benign (notes still give authoritative data);
    * a notes failure (or an incomplete page set) means we genuinely do not
      know the review state, so we report ``known=False`` and the caller
      preserves the existing PR row rather than overwriting its review metrics
      with zero/null.

    Never raises (CHAOS-2378).
    """
    iid = int(mr.get("iid") or 0)
    if iid <= 0:
        return _MrReviewFetch([], None, 0, known=True)

    approvals: dict[str, Any] | None = None
    try:
        approvals = connector.rest_client.get_merge_request_approvals(project_id, iid)
    except Exception as exc:  # noqa: BLE001 - best-effort, some tiers lack approvals
        logging.debug("Failed to fetch approvals for MR !%d: %s", iid, exc)

    notes: list[dict[str, Any]] = []
    notes_known = True
    try:
        notes = _fetch_all_mr_notes(connector, project_id, iid, gate=gate)
    except Exception as exc:  # noqa: BLE001 - best-effort
        notes_known = False
        logging.warning(
            "Could not fetch notes for MR !%d (project %s); preserving existing "
            "review metrics: %s",
            iid,
            project_id,
            exc,
        )

    if not notes_known:
        # Authoritative source unavailable / incomplete: signal "unknown" so the
        # caller does not persist this MR's PR row with zeroed-out review
        # metrics.
        return _MrReviewFetch([], None, 0, known=False)

    author_data = mr.get("author")
    author_username = (
        author_data.get("username") if isinstance(author_data, dict) else None
    )
    reviews, first_review_at = map_gitlab_mr_reviews(
        repo_id=repo_id,
        number=iid,
        approvals=approvals,
        notes=notes,
        fallback_at=created_at,
        author_username=author_username,
    )
    changes_requested_count = sum(1 for r in reviews if r.state == "CHANGES_REQUESTED")
    return _MrReviewFetch(reviews, first_review_at, changes_requested_count, known=True)


def _sync_gitlab_mrs_to_store(
    connector,
    project_id: int,
    repo_id,
    ingestion_sink: IngestionSink,
    loop: asyncio.AbstractEventLoop,
    batch_size: int,
    state: str = "all",
    gate: Any = None,
    since: datetime | None = None,
) -> int:
    """Fetch all MRs for a project and insert them in batches.

    Runs in a worker thread; uses run_coroutine_threadsafe to write batches.
    """
    logging.info(
        "Fetching merge requests for project %d...",
        project_id,
    )
    batch: list[GitPullRequest] = []
    review_batch: list[GitPullRequestReview] = []
    # MRs skipped this cycle because their authoritative review source (MR notes)
    # was degraded. If non-empty at the end, we MUST NOT report success (which
    # would advance the watermark past these MRs and strand them); we flush the
    # good rows then raise PartialGitLabMrSyncError so the run is retried.
    skipped_iids: list[int] = []
    total = 0
    page = 1

    gate = BaseGitProcessor.ensure_gate(gate)
    assert gate is not None

    def _flush_reviews() -> None:
        if review_batch:
            BaseGitProcessor.persist_batch_threadsafe(
                ingestion_sink.insert_git_pull_request_reviews(list(review_batch)),
                loop,
            )
            review_batch.clear()

    while True:
        try:
            gate.wait_sync()
            logging.debug(
                "GitLab MRs page %d (per_page=%d) for project %d",
                page,
                connector.per_page,
                project_id,
            )
            mrs = connector.rest_client.get_merge_requests(
                project_id=project_id,
                state=state,
                page=page,
                per_page=connector.per_page,
                order_by="updated_at",
                sort="desc",
            )
            gate.reset()
        except Exception as e:
            retry_after = getattr(e, "retry_after_seconds", None)
            if retry_after is None:
                raise
            applied = gate.penalize(retry_after)
            logging.info(
                "GitLab rate limited while fetching MRs; backoff %.1fs (%s)",
                applied,
                e,
            )
            continue
        if not mrs:
            break
        logging.debug(
            "GitLab MRs page %d returned %d items (total: %d)",
            page,
            len(mrs),
            total,
        )

        for mr in mrs:
            author_name = "Unknown"
            author_email = None
            author_data = mr.get("author")
            if author_data:
                author_name = author_data.get("username") or author_name

            merged_at = safe_parse_datetime(mr.get("merged_at"))
            closed_at = safe_parse_datetime(mr.get("closed_at"))
            updated_at = safe_parse_datetime(mr.get("updated_at"))
            created_at = safe_parse_datetime(mr.get("created_at"))

            comments_count = int(mr.get("user_notes_count") or 0)

            if (
                since is not None
                and isinstance(updated_at, datetime)
                and updated_at.astimezone(timezone.utc) < since
            ):
                mrs = []
                break

            # Reconstruct reviews (approvals + notes) so GitLab orgs populate
            # git_pull_request_reviews like GitHub does (CHAOS-2378).
            fetched = _fetch_gitlab_mr_reviews(
                connector=connector,
                project_id=project_id,
                mr=mr,
                repo_id=repo_id,
                created_at=created_at,
                gate=gate,
            )

            if not fetched.known:
                # Authoritative review source (MR notes) was unavailable for
                # this MR. The PR row is ReplacingMergeTree-replaced on write,
                # so persisting it now with zeroed review metrics would clobber
                # previously-correct first_review_at / reviews_count /
                # changes_requested_count. Skip this MR's row this cycle so the
                # prior values are preserved, and record the iid so the run
                # fails loud (no watermark advancement) and is retried — a
                # silent skip would let the watermark move past this MR and
                # strand it until a full resync (CHAOS-2378).
                skipped_iid = int(mr.get("iid") or 0)
                skipped_iids.append(skipped_iid)
                logging.warning(
                    "Skipping PR row for MR !%d (project %d): review fetch "
                    "degraded; run will not advance watermark and will retry",
                    skipped_iid,
                    project_id,
                )
                continue

            review_batch.extend(fetched.reviews)

            batch.append(
                build_git_pull_request(
                    repo_id=repo_id,
                    number=int(mr.get("iid") or 0),
                    title=mr.get("title") or None,
                    body=mr.get("description"),
                    state=normalize_pr_state(mr.get("state"), merged_at),
                    author_name=author_name,
                    author_email=author_email,
                    created_at=created_at,
                    merged_at=merged_at,
                    closed_at=closed_at,
                    head_branch=mr.get("source_branch"),
                    base_branch=mr.get("target_branch"),
                    first_review_at=fetched.first_review_at,
                    changes_requested_count=fetched.changes_requested_count,
                    reviews_count=len(fetched.reviews),
                    comments_count=comments_count,
                )
            )
            total += 1

            if len(batch) >= batch_size:
                BaseGitProcessor.persist_batch_threadsafe(
                    ingestion_sink.insert_git_pull_requests(batch),
                    loop,
                )
                logging.debug(
                    "Stored batch of %d MRs for project %d (total: %d)",
                    len(batch),
                    project_id,
                    total,
                )
                batch.clear()
                _flush_reviews()

        page += 1
        if not mrs:
            break

    if batch:
        BaseGitProcessor.persist_batch_threadsafe(
            ingestion_sink.insert_git_pull_requests(batch),
            loop,
        )
    _flush_reviews()

    logging.info(
        "Fetched %d merge requests for project %d",
        total,
        project_id,
    )

    if skipped_iids:
        # Good rows are already flushed above; raising now (after the flush)
        # preserves the work done this cycle while preventing the caller's
        # success path from advancing the per-target watermark past the skipped
        # MRs. The error is retryable (non-terminal), so the project re-syncs.
        raise PartialGitLabMrSyncError(skipped_iids, project_id)

    return total


def _fetch_gitlab_pipelines_sync(gl_project, repo_id, max_pipelines, since):
    """Sync helper to fetch GitLab CI/CD pipelines."""
    pipelines: list[CiPipelineRun] = []

    try:
        list_params = {"per_page": 100, "order_by": "updated_at", "sort": "desc"}
        if max_pipelines > 100:
            raw_pipelines = gl_project.pipelines.list(**list_params, as_list=False)
        else:
            raw_pipelines = gl_project.pipelines.list(**list_params, get_all=False)
    except Exception as exc:
        logging.debug("Failed to fetch pipelines: %s", exc)
        return pipelines

    count = 0
    for pipeline in raw_pipelines:
        if count >= max_pipelines:
            break

        created_at = safe_parse_datetime(getattr(pipeline, "created_at", None))

        if created_at is None:
            continue

        if since is not None and created_at.astimezone(timezone.utc) < since:
            break

        started_at = (
            safe_parse_datetime(getattr(pipeline, "started_at", None)) or created_at
        )

        finished_at = safe_parse_datetime(getattr(pipeline, "finished_at", None))

        # GitLab pipelines expose no clean automatic-retry counter, so default
        # to 0. (A new pipeline is created per retry rather than a run_attempt
        # being incremented, unlike GitHub Actions.)
        pipelines.append(
            build_ci_pipeline_run(
                repo_id=repo_id,
                run_id=str(getattr(pipeline, "id", "")),
                status=getattr(pipeline, "status", None),
                queued_at=created_at,
                started_at=started_at,
                finished_at=finished_at,
                retry_count=0,
            )
        )
        count += 1

    return pipelines


def _resolve_gitlab_deployment_mr(connector, project_id, sha):
    """Resolve the merged MR for a deployed commit via the commits API.

    Failure-soft: any lookup error leaves the deployment without MR
    attribution rather than failing the sync.
    """
    if not sha:
        return None, None
    try:
        mrs = connector.rest_client.get_list(
            f"projects/{project_id}/repository/commits/{sha}/merge_requests"
        )
    except Exception as exc:
        logging.debug("Failed MR lookup for deployed commit %s: %s", sha, exc)
        return None, None
    merged = [mr for mr in mrs or [] if mr.get("state") == "merged"]
    chosen = merged[0] if merged else (mrs[0] if mrs else None)
    if not chosen:
        return None, None
    merged_at = safe_parse_datetime(chosen.get("merged_at") or "")
    try:
        iid = int(chosen.get("iid"))
    except (TypeError, ValueError):
        iid = None
    return iid, merged_at


def _fetch_gitlab_deployments_sync(
    connector, project_id, repo_id, max_deployments, since
):
    """Sync helper to fetch GitLab deployments."""
    deployments: list[Deployment] = []
    release_objects = []
    try:
        release_objects = connector.rest_client.get_releases(
            project_id=project_id,
            per_page=min(max_deployments, 100),
        )
    except Exception as exc:
        logging.debug("Failed to fetch GitLab releases for release_ref: %s", exc)
    try:
        # Use REST API to fetch deployments
        raw_deployments = connector.rest_client.get_deployments(
            project_id=project_id,
            per_page=min(max_deployments, 100),
            order_by="created_at",
            sort="desc",
        )
    except Exception as exc:
        logging.debug("Failed to fetch deployments: %s", exc)
        return deployments

    for dep in raw_deployments[:max_deployments]:
        created_at_str = dep.get("created_at")
        if not created_at_str:
            continue

        created_at = safe_parse_datetime(created_at_str)
        if created_at is None:
            continue

        if since is not None and created_at.astimezone(timezone.utc) < since:
            break

        # Parse other timestamps if available
        finished_at = None
        finished_at_str = dep.get("finished_at")
        if finished_at_str:
            finished_at = safe_parse_datetime(finished_at_str)

        enrichment = get_release_ref_enrichment(
            {
                **dep,
                "deployment_id": str(dep.get("id", "")),
                "deployment_iid": dep.get("iid"),
            },
            "gitlab",
            releases=release_objects,
        )

        mr_number, mr_merged_at = _resolve_gitlab_deployment_mr(
            connector, project_id, dep.get("sha")
        )
        deployments.append(
            build_deployment(
                repo_id=repo_id,
                deployment_id=str(dep.get("id", "")),
                status=dep.get("status", None),
                environment=dep.get("environment", {}).get("name")
                if isinstance(dep.get("environment"), dict)
                else None,
                started_at=created_at,
                finished_at=finished_at,
                deployed_at=created_at,
                merged_at=mr_merged_at,
                pull_request_number=mr_number,
                release_ref=enrichment.release_ref,
                release_ref_confidence=enrichment.confidence,
            )
        )

    return deployments


def _fetch_gitlab_incidents_sync(connector, project_id, repo_id, max_issues, since):
    """Sync helper to fetch GitLab incidents (configurable incident labels)."""
    incidents: list[Incident] = []
    labels = resolve_incident_labels()
    raw_issues: list = []
    seen_issue_ids: set = set()
    for label in labels:
        try:
            label_issues = connector.rest_client.get_issues(
                project_id=project_id,
                labels=label,
                per_page=min(max_issues, 100),
                order_by="updated_at",
                sort="desc",
            )
        except Exception as exc:
            logging.debug(
                "Failed to fetch incident issues for label %r: %s", label, exc
            )
            continue
        for issue in label_issues or []:
            issue_id = issue.get("id")
            if issue_id in seen_issue_ids:
                continue
            seen_issue_ids.add(issue_id)
            raw_issues.append(issue)
    logging.info(
        "Fetched %d GitLab incident issue(s) (labels searched: %s)",
        len(raw_issues),
        ", ".join(labels),
    )
    for issue in raw_issues[:max_issues]:
        created_at_str = issue.get("created_at")
        if not created_at_str:
            continue

        created_at = safe_parse_datetime(created_at_str)
        if created_at is None:
            continue

        if since is not None and created_at.astimezone(timezone.utc) < since:
            break

        resolved_at = None
        closed_at_str = issue.get("closed_at")
        if closed_at_str:
            resolved_at = safe_parse_datetime(closed_at_str)

        incidents.append(
            Incident(
                repo_id=repo_id,
                incident_id=str(issue.get("id", "")),
                status=issue.get("state", None),
                started_at=created_at,
                resolved_at=resolved_at,
            )
        )

    return incidents


def _fetch_gitlab_security_alerts_sync(
    connector, project_id, repo_id, max_alerts, since
):
    """Sync helper to fetch GitLab security alerts (vulnerability findings, dependency scanning)."""
    security_alert_cls = getattr(git_models, "SecurityAlert")
    alerts = []
    try:
        raw_alerts = connector.get_security_alerts(
            project_id=project_id, max_alerts=max_alerts
        )
        for item in raw_alerts:
            created_at = item.created_at
            if not created_at:
                continue
            if since is not None and created_at.astimezone(timezone.utc) < since:
                continue
            alerts.append(
                security_alert_cls(
                    repo_id=repo_id,
                    alert_id=item.alert_id,
                    source=item.source,
                    severity=item.severity,
                    state=item.state,
                    package_name=item.package_name,
                    cve_id=item.cve_id,
                    url=item.url,
                    title=item.title,
                    description=item.description,
                    created_at=created_at,
                    fixed_at=item.fixed_at,
                    dismissed_at=item.dismissed_at,
                )
            )
    except Exception as exc:
        logging.debug("Failed to fetch GitLab security alerts: %s", exc)
    return alerts


def _iter_gitlab_repo_tree(
    gl_project,
    *,
    ref: str,
    per_page: int = 100,
    limit: int | None = None,
) -> Iterable[Any]:
    page = 1
    seen = 0
    # Defensive page cap. GitLab paginates until it returns an empty page, but a
    # transport/API glitch — or a test double whose ``repository_tree()`` is a
    # truthy Mock that never yields a falsy page — would otherwise spin forever
    # (an unbounded REST crawl that hangs the sync; this hung the unit suite to
    # the CI job timeout). 10k pages at per_page=100 covers ~1M tree entries,
    # far beyond any real repository.
    max_pages = 10_000

    while page <= max_pages:
        try:
            page_items = gl_project.repository_tree(
                ref=ref,
                recursive=True,
                per_page=per_page,
                page=page,
                get_all=False,
            )
        except TypeError:
            page_items = gl_project.repository_tree(
                ref=ref,
                recursive=True,
                per_page=per_page,
                page=page,
                all=False,
            )
        # An empty page marks the end. Check the length explicitly: a real
        # GitLab response is a list (``[]`` is falsy), but a truthy-but-empty
        # container — e.g. a test Mock with no configured return — is not caught
        # by ``not page_items`` alone and would loop until ``max_pages``.
        try:
            page_len = len(page_items)
        except TypeError:
            page_len = 0
        if not page_items or page_len == 0:
            break
        seen += page_len
        logging.debug(
            "GitLab repo tree page %d returned %d items (total: %d)",
            page,
            page_len,
            seen,
        )
        yield from page_items
        if limit is not None and seen >= limit:
            return
        page += 1


def _fetch_gitlab_blame_sync(gl_project, connector, project_id, repo_id, limit=50):
    """Sync helper to fetch GitLab blame data."""
    blame_batch = []
    try:
        # Get files from repository tree (paged to avoid huge responses)
        files_to_process = []
        for item in _iter_gitlab_repo_tree(
            gl_project,
            ref=gl_project.default_branch,
            per_page=100,
            limit=None,
        ):
            if item["type"] == "blob" and not is_skippable(item["path"]):
                files_to_process.append(item["path"])
                if len(files_to_process) >= limit:
                    break

        # Limit files
        files_to_process = files_to_process[:limit]
        logging.debug(
            "GitLab blame: processing %d files (limit %d)",
            len(files_to_process),
            limit,
        )

        for idx, file_path in enumerate(files_to_process, start=1):
            try:
                logging.debug(
                    "GitLab blame fetch %d/%d: %s",
                    idx,
                    len(files_to_process),
                    file_path,
                )
                blame = connector.get_file_blame(
                    project_id=project_id,
                    file_path=file_path,
                    ref=gl_project.default_branch,
                )
                if blame and blame.ranges:
                    for r in blame.ranges:
                        blame_obj = GitBlame(
                            repo_id=repo_id,
                            path=file_path,
                            line_no=r.starting_line,
                            author_name=r.author,
                            author_email=r.author_email,
                            commit_hash=r.commit_sha,
                            line="<remote>",
                            author_when=datetime.now(timezone.utc),
                        )
                        blame_batch.append(blame_obj)
            except Exception as e:
                logging.warning(f"Failed to get blame for {file_path}: {e}")

    except Exception as e:
        logging.error(f"Error fetching files for blame: {e}")

    return blame_batch


# Bounds for API-based file-content backfill (parity with the GitHub
# processor): skip blobs the complexity scanner would reject, oversized
# blobs (enforced via GraphQL rawSize), and runaway repos.
CONTENT_FETCH_MAX_BYTES = 1_000_000
CONTENT_FETCH_MAX_FILES = 2_000

# Cap on per-file blame fetches during onboarding backfill (parity with the
# GitHub processor). Blame costs one REST call per file; without a cap a large
# project would turn a normal sync into thousands of calls (rate-limit
# failures / timeouts). The repo-level has_any_git_blame gate makes this a
# bounded one-time cost on first onboarding; full coverage remains available
# via the dedicated blame sync target (CHAOS-2376).
BLAME_BACKFILL_MAX_FILES = 500


async def _fetch_scannable_contents(
    connector: Any,
    project_full_name: str,
    ref: str,
    file_paths: list[str],
) -> dict[str, str]:
    """Fetch text for scanner-eligible files via batched GraphQL blob queries.

    Only paths matching the complexity scanner's include/exclude globs are
    fetched, keeping API volume proportional to what the metrics jobs can
    actually use. Errors degrade to a paths-only backfill (contents stay
    NULL) rather than failing the sync.
    """
    scanner = ComplexityScanner(config_path=DEFAULT_COMPLEXITY_CONFIG_PATH)
    scannable: list[str] = []
    for path in file_paths:
        if not scanner.should_process(path):
            continue
        scannable.append(path)
        if len(scannable) >= CONTENT_FETCH_MAX_FILES:
            logging.warning(
                "Capping content fetch at %d files for %s",
                CONTENT_FETCH_MAX_FILES,
                project_full_name,
            )
            break

    if not scannable:
        return {}

    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(
            None,
            lambda: connector.get_file_contents(
                project_full_name,
                scannable,
                ref=ref,
                max_bytes=CONTENT_FETCH_MAX_BYTES,
            ),
        )
    except Exception as e:
        logging.warning(
            "Failed to fetch file contents for %s: %s", project_full_name, e
        )
        return {}


async def _backfill_gitlab_missing_data(
    store: Any,
    ingestion_sink: IngestionSink,
    connector: Any,
    db_repo: Repo,
    project_full_name: str,
    default_branch: str,
    max_commits: int | None,
    blame_only: bool = False,
    include_blame: bool = True,
    include_commit_stats: bool = True,
) -> None:
    # check_backfill_needs's blame_only flag doubles as "skip commit stats".
    needs = await check_backfill_needs(
        store, db_repo.id, blame_only=blame_only or not include_commit_stats
    )

    # Repos synced before content fetching existed have paths-only rows
    # (contents NULL), which has_any_git_files treats as "done". Upgrade
    # them by re-running the files backfill when no contents exist yet;
    # ReplacingMergeTree(last_synced) supersedes the stale rows.
    needs_files = needs.files
    if not needs_files and hasattr(store, "has_any_git_file_contents"):
        needs_files = not await store.has_any_git_file_contents(db_repo.id)

    # Blame backfill is coverage-aware (CHAOS-2376 round-3). The crawl is capped
    # at BLAME_BACKFILL_MAX_FILES per sync, so an any-row gate (needs.blame)
    # would mark the project "done" after the first capped batch and strand
    # every file past the cap without blame. Keep the blame branch alive while
    # any tracked file still lacks blame, so successive syncs advance coverage.
    needs_blame = await blame_backfill_needed(
        store,
        db_repo.id,
        include_blame=include_blame,
        any_row_needs_blame=needs.blame,
    )
    if not (needs_files or needs_blame or needs.commit_stats):
        return

    try:
        project = connector.gitlab.projects.get(project_full_name)
    except Exception as e:
        logging.warning(f"Failed to load GitLab project {project_full_name}: {e}")
        return

    file_paths: list[str] = []
    blame_paths: list[str] = []
    if needs_files or needs_blame:
        try:
            items = _iter_gitlab_repo_tree(
                project,
                ref=default_branch,
                per_page=100,
                limit=None,
            )
        except Exception as e:
            logging.warning(f"Failed to list GitLab files for {project_full_name}: {e}")
            items = []

        for item in items or []:
            if item.get("type") != "blob":
                continue
            path = item.get("path")
            if not path:
                continue
            file_paths.append(path)

        if needs_files and file_paths:
            contents_by_path = await _fetch_scannable_contents(
                connector,
                project_full_name,
                default_branch,
                file_paths,
            )
            await backfill_file_records(
                ingestion_sink,
                db_repo.id,
                file_paths,
                project_full_name,
                contents_by_path=contents_by_path,
            )

    if needs.commit_stats:
        try:
            commits = project.commits.list(
                ref_name=default_branch, per_page=100, get_all=True
            )
        except TypeError:
            commits = project.commits.list(
                ref_name=default_branch, per_page=100, all=True
            )

        async with AsyncBatchCollector(
            ingestion_sink.insert_git_commit_stats
        ) as stats_collector:
            commit_count = 0
            for commit in commits or []:
                if max_commits and commit_count >= max_commits:
                    break
                commit_count += 1
                sha = getattr(commit, "id", None) or getattr(commit, "sha", None)
                if not sha:
                    continue
                try:
                    stats = connector.get_commit_stats_by_project(
                        sha=sha,
                        project_name=project_full_name,
                    )
                    stats_collector.add(
                        GitCommitStat(
                            repo_id=db_repo.id,
                            commit_hash=sha,
                            file_path=AGGREGATE_STATS_MARKER,
                            additions=getattr(stats, "additions", 0),
                            deletions=getattr(stats, "deletions", 0),
                            old_file_mode="unknown",
                            new_file_mode="unknown",
                        )
                    )
                    await stats_collector.maybe_flush()
                except Exception as e:
                    logging.debug(
                        f"Failed commit stat fetch for {project_full_name}@{sha}: {e}"
                    )

    if needs_blame and file_paths:
        # Bound the blame crawl: one REST call per file, so cap the number of
        # files we blame on a single sync to avoid rate-limit failures /
        # timeouts on large projects (CHAOS-2376). Select the *next* unblamed
        # batch (diffing the live tree against already-blamed paths) so each
        # rerun advances coverage instead of reblaming the same capped prefix;
        # the capped prefix is a fallback when the store lacks per-path coverage.
        blame_paths = await select_unblamed_paths(
            store, db_repo.id, file_paths, BLAME_BACKFILL_MAX_FILES
        )
        if not blame_paths:
            logging.info(
                "Blame coverage already complete for %s; skipping crawl",
                project_full_name,
            )
    if needs_blame and blame_paths:
        logging.info(
            "Backfilling blame for %d unblamed files in %s (cap %d)...",
            len(blame_paths),
            project_full_name,
            BLAME_BACKFILL_MAX_FILES,
        )
        async with AsyncBatchCollector(
            ingestion_sink.insert_blame_data
        ) as blame_collector:
            for path in blame_paths:
                try:
                    blame_items = connector.rest_client.get_file_blame(
                        project.id,
                        path,
                        default_branch,
                    )
                except Exception as e:
                    logging.debug(
                        f"Failed blame fetch for {project_full_name}:{path}: {e}"
                    )
                    continue

                line_no = 1
                for item in blame_items or []:
                    commit = item.get("commit", {})
                    for line in item.get("lines", []) or []:
                        blame_collector.add(
                            GitBlame(
                                repo_id=db_repo.id,
                                path=path,
                                line_no=line_no,
                                author_email=commit.get("author_email"),
                                author_name=commit.get("author_name"),
                                author_when=None,
                                commit_hash=commit.get("id"),
                                line=line.rstrip("\n")
                                if isinstance(line, str)
                                else None,
                            )
                        )
                        line_no += 1
                        await blame_collector.maybe_flush()


def _is_report_name(name: str) -> bool:
    """ZIP member filter: JUnit/coverage report files we know how to parse."""
    lowered = name.lower()
    return lowered.endswith(".xml") or lowered.endswith(".info")


def _fetch_gitlab_test_reports_sync(
    connector: Any,
    gl_project: Any,
    project_id: int,
    since: datetime | None,
    default_branch: str | None,
    max_pipelines: int,
) -> tuple[
    list[tuple[str, dict[str, Any], datetime | None, datetime | None]],
    list[tuple[str, list[tuple[str, bytes]]]],
]:
    """Blocking: collect native test reports + coverage artifact members.

    Returns ``(test_reports, coverage_members)`` where ``test_reports`` is
    ``[(run_id, test_report_json, started_at, finished_at), ...]`` (GitLab's
    parsed JUnit JSON + the pipeline timestamps used to date the suites) and
    ``coverage_members`` is ``[(run_id, [(filename, bytes), ...]), ...]`` from
    job artifact ZIPs. ``run_id`` is ``str(pipeline.id)`` so rows join to the
    pipeline. Bounded to the default branch and ``max_pipelines`` pipelines.
    """
    from dev_health_ops.connectors.utils.safe_archive import iter_zip_members

    test_reports: list[
        tuple[str, dict[str, Any], datetime | None, datetime | None]
    ] = []
    coverage_members: list[tuple[str, list[tuple[str, bytes]]]] = []
    since_aware = since
    if since_aware is not None and since_aware.tzinfo is None:
        since_aware = since_aware.replace(tzinfo=timezone.utc)

    # Filter by update time SERVER-SIDE. We must NOT early-`break` on created_at:
    # the list is ordered by updated_at, and a recently-updated pipeline can be
    # old by created_at, so breaking would silently skip valid in-window
    # pipelines (Codex review). The max_pipelines cap bounds the scan instead.
    list_params: dict[str, Any] = {
        "per_page": 100,
        "order_by": "updated_at",
        "sort": "desc",
    }
    if since_aware is not None:
        list_params["updated_after"] = since_aware.isoformat()
    try:
        if max_pipelines > 100:
            raw_pipelines = gl_project.pipelines.list(**list_params, as_list=False)
        else:
            raw_pipelines = gl_project.pipelines.list(**list_params, get_all=False)
    except Exception as exc:
        logging.warning(
            "Could not list pipelines for GitLab project %s: %s", project_id, exc
        )
        return test_reports, coverage_members

    count = 0
    for pipeline in raw_pipelines:
        if count >= max_pipelines:
            break
        ref = getattr(pipeline, "ref", None)
        if default_branch and ref and ref != default_branch:
            continue
        pipeline_id = getattr(pipeline, "id", None)
        if pipeline_id is None:
            continue
        count += 1
        run_id = str(pipeline_id)
        created_at = safe_parse_datetime(getattr(pipeline, "created_at", None))
        started_at = (
            safe_parse_datetime(getattr(pipeline, "started_at", None)) or created_at
        )
        finished_at = safe_parse_datetime(getattr(pipeline, "finished_at", None))

        # Native parsed test report (pass/fail/duration) — preferred over XML.
        try:
            report = connector.rest_client.get_pipeline_test_report(
                project_id, pipeline_id
            )
            if report and report.get("test_suites"):
                test_reports.append((run_id, report, started_at, finished_at))
        except Exception as exc:
            logging.debug("test_report failed for pipeline %s: %s", pipeline_id, exc)

        # Coverage from job artifacts (best-effort, bounded).
        try:
            jobs = connector.rest_client.get_list(
                f"projects/{project_id}/pipelines/{pipeline_id}/jobs",
                params={"per_page": 100},
            )
        except Exception:
            jobs = []
        members: list[tuple[str, bytes]] = []
        artifact_jobs = 0
        for job in jobs:
            if artifact_jobs >= MAX_ARTIFACTS_PER_RUN:
                break
            if not (job.get("artifacts_file") or job.get("artifacts")):
                continue
            job_id = job.get("id")
            if job_id is None:
                continue
            artifact_jobs += 1
            try:
                data = connector.rest_client.download_job_artifacts(project_id, job_id)
            except Exception as exc:
                logging.debug("artifact download failed for job %s: %s", job_id, exc)
                continue
            if not data:
                continue
            try:
                members.extend(iter_zip_members(data, name_filter=_is_report_name))
            except zipfile.BadZipFile:
                continue
        if members:
            coverage_members.append((run_id, members))

    return test_reports, coverage_members


async def _sync_gitlab_test_reports(
    *,
    connector: Any,
    gl_project: Any,
    project_id: int,
    token: str,
    repo_id: Any,
    org_id: str,
    ingestion_sink: IngestionSink,
    loop: asyncio.AbstractEventLoop,
    since: datetime | None,
) -> None:
    """Ingest TestOps data for one GitLab project (CHAOS-2370).

    (1) Extended pipeline + job rows via the async adapter (closes the
    ci_job_runs gap). (2) Test suites/cases from GitLab's native test_report
    JSON. (3) Coverage from job artifacts (test cases from artifacts are
    discarded — the native report is authoritative — so they aren't
    double-counted). Each stage is independently fault-tolerant.
    """
    from dev_health_ops.processors.testops_pipeline import TestOpsPipelineProcessor
    from dev_health_ops.providers.gitlab.testops_pipeline import GitLabCIAdapter

    # (1) Extended pipelines + jobs. Explicit token (Codex review item G).
    try:
        adapter = GitLabCIAdapter(base_url=connector.rest_client.base_url, token=token)
        processor = TestOpsPipelineProcessor(ingestion_sink)
        async with adapter:
            result = await processor.fetch_and_store(
                adapter,
                since_date=since,
                project_id=project_id,
                repo_id=repo_id,
                org_id=org_id,
            )
        logging.info(
            "TestOps GitLab project %s: %d pipelines, %d jobs",
            project_id,
            result.pipeline_runs,
            result.job_runs,
        )
    except Exception as exc:
        logging.warning(
            "TestOps pipeline/job ingestion failed for GitLab project %s: %s",
            project_id,
            exc,
        )

    default_branch = getattr(gl_project, "default_branch", None)
    test_reports, coverage_members = await loop.run_in_executor(
        None,
        _fetch_gitlab_test_reports_sync,
        connector,
        gl_project,
        project_id,
        since,
        default_branch,
        MAX_RUNS_PER_SYNC,
    )

    suite_rows: list[Any] = []
    case_rows: list[Any] = []
    coverage_rows: list[Any] = []
    for run_id, report, started_at, finished_at in test_reports:
        suites, cases = await process_gitlab_test_report(
            repo_id=repo_id,
            run_id=run_id,
            report=report,
            org_id=org_id,
            started_at=started_at,
            finished_at=finished_at,
        )
        suite_rows.extend(suites)
        case_rows.extend(cases)
    for run_id, members in coverage_members:
        # Keep only coverage rows; suites/cases come from the native report.
        _, _, coverage = await ingest_report_members(
            members, repo_id=repo_id, run_id=run_id, org_id=org_id
        )
        coverage_rows.extend(coverage)

    if suite_rows:
        await ingestion_sink.insert_test_suite_results(suite_rows)
    if case_rows:
        await ingestion_sink.insert_test_case_results(case_rows)
    if coverage_rows:
        await ingestion_sink.insert_coverage_snapshots(coverage_rows)
    logging.info(
        "TestOps GitLab project %s: %d suites, %d cases, %d coverage",
        project_id,
        len(suite_rows),
        len(case_rows),
        len(coverage_rows),
    )


async def process_gitlab_project(
    store: GitSyncStore | Any,
    project_id: int,
    token: str,
    gitlab_url: str,
    fetch_blame: bool = False,
    blame_only: bool = False,
    max_commits: int | None = None,
    sync_git: bool = True,
    sync_prs: bool = True,
    sync_cicd: bool = True,
    sync_deployments: bool = True,
    sync_incidents: bool = True,
    sync_security: bool = True,
    sync_tests: bool = False,
    backfill_missing: bool = True,
    since: datetime | None = None,
) -> None:
    """
    Process a GitLab project using the GitLab connector.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info(f"Processing GitLab project: {project_id}")
    loop = asyncio.get_running_loop()
    ingestion_sink = IngestionSink(store)

    connector_cls = cast(Any, GitLabConnector)
    connector = connector_cls(url=gitlab_url, private_token=token)
    try:
        # 1. Fetch Project Info
        logging.info("Fetching project information...")
        gl_project = await loop.run_in_executor(
            None, _fetch_gitlab_project_info_sync, connector, project_id
        )

        logging.info(f"Found project: {gl_project.name}")

        # Create/Insert Repo
        full_name = (
            gl_project.path_with_namespace
            if hasattr(gl_project, "path_with_namespace")
            else gl_project.name
        )

        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=full_name,
            provider="gitlab",
            settings={
                "source": "gitlab",
                "project_id": gl_project.id,
                "url": gl_project.web_url if hasattr(gl_project, "web_url") else None,
                "default_branch": (
                    gl_project.default_branch
                    if hasattr(gl_project, "default_branch")
                    else "main"
                ),
            },
            tags=["gitlab"],
        )

        await ingestion_sink.insert_repo(db_repo)
        logging.info(f"Project stored: {db_repo.repo} ({db_repo.id})")

        if blame_only:
            await _backfill_gitlab_missing_data(
                store=store,
                ingestion_sink=ingestion_sink,
                connector=connector,
                db_repo=db_repo,
                project_full_name=full_name,
                default_branch=db_repo.settings.get("default_branch", "main"),
                max_commits=max_commits,
                blame_only=True,
            )
            logging.info("Completed blame-only sync for GitLab project: %s", project_id)
            return

        if sync_git:
            # 2. Fetch Commits
            if max_commits is None:
                logging.info("Fetching all commits from GitLab...")
            else:
                logging.info(f"Fetching up to {max_commits} commits from GitLab...")
            commit_hashes, commit_objects = await loop.run_in_executor(
                None,
                _fetch_gitlab_commits_sync,
                gl_project,
                max_commits,
                db_repo.id,
                since,
            )

            if commit_objects:
                await ingestion_sink.insert_git_commit_data(commit_objects)
                logging.info(f"Stored {len(commit_objects)} commits from GitLab")

            # 3. Fetch Stats
            logging.info("Fetching commit stats from GitLab...")
            stats_limit = resolve_commit_stats_limit(
                len(commit_hashes), max_commits, since
            )
            stats_objects = await loop.run_in_executor(
                None,
                _fetch_gitlab_commit_stats_sync,
                gl_project,
                commit_hashes,
                db_repo.id,
                stats_limit,
            )

            if stats_objects:
                await ingestion_sink.insert_git_commit_stats(stats_objects)
                logging.info(f"Stored {len(stats_objects)} commit stats from GitLab")

        if sync_prs:
            # 4. Fetch Merge Requests
            logging.info("Fetching merge requests from GitLab...")
            mr_total = await loop.run_in_executor(
                None,
                _sync_gitlab_mrs_to_store,
                connector,
                project_id,
                db_repo.id,
                ingestion_sink,
                loop,
                BATCH_SIZE,
                "all",
                None,
                since,
            )
            logging.info(f"Stored {mr_total} merge requests from GitLab")

        if sync_cicd:
            logging.info("Fetching CI/CD pipelines from GitLab...")
            pipeline_runs = await loop.run_in_executor(
                None,
                _fetch_gitlab_pipelines_sync,
                gl_project,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if pipeline_runs:
                await ingestion_sink.insert_ci_pipeline_runs(pipeline_runs)
                logging.info(f"Stored {len(pipeline_runs)} pipeline runs from GitLab")

        if sync_tests:
            await _sync_gitlab_test_reports(
                connector=connector,
                gl_project=gl_project,
                project_id=project_id,
                token=token,
                repo_id=db_repo.id,
                org_id=getattr(store, "org_id", "") or "",
                ingestion_sink=ingestion_sink,
                loop=loop,
                since=since,
            )

        if sync_deployments:
            logging.info("Fetching deployments from GitLab...")
            deployments = await loop.run_in_executor(
                None,
                _fetch_gitlab_deployments_sync,
                connector,
                project_id,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if deployments:
                await ingestion_sink.insert_deployments(deployments)
                logging.info(f"Stored {len(deployments)} deployments from GitLab")

        if sync_incidents:
            logging.info("Fetching incidents from GitLab...")
            incidents = await loop.run_in_executor(
                None,
                _fetch_gitlab_incidents_sync,
                connector,
                project_id,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if incidents:
                await ingestion_sink.insert_incidents(incidents)
                logging.info(f"Stored {len(incidents)} incidents from GitLab")

        if sync_security:
            logging.info("Fetching security alerts from GitLab...")
            security_alerts = await loop.run_in_executor(
                None,
                _fetch_gitlab_security_alerts_sync,
                connector,
                project_id,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if security_alerts:
                insert_security_alerts = getattr(
                    ingestion_sink, "insert_security_alerts"
                )
                await insert_security_alerts(security_alerts)
                logging.info(
                    "Stored %d security alerts from GitLab", len(security_alerts)
                )

        # 5. Fetch Blame (Optional)
        if fetch_blame:
            logging.info("Fetching blame data from GitLab (this may take a while)...")
            blame_batch = await loop.run_in_executor(
                None,
                _fetch_gitlab_blame_sync,
                gl_project,
                connector,
                project_id,
                db_repo.id,
                50,
            )

            if blame_batch:
                await ingestion_sink.insert_blame_data(blame_batch)
                logging.info(f"Stored {len(blame_batch)} blame records from GitLab")

        # 6. Backfill file records + contents so DB-based metrics (e.g.
        # complexity, hotspots, ownership-risk) can run without a local
        # checkout. Gated on sync_git so non-git targets (prs, cicd, ...)
        # stay lean. Blame is included so the /complexity Ownership-risk tab
        # is populated on normal onboarding. The has_any_git_blame gate is
        # repo-level, so blame is fetched once per project on first onboarding
        # (skipped once any blame exists) and capped at BLAME_BACKFILL_MAX_FILES
        # files per sync so a large project cannot turn onboarding into an
        # unbounded REST crawl (CHAOS-2376).
        if backfill_missing and sync_git:
            try:
                await _backfill_gitlab_missing_data(
                    store=store,
                    ingestion_sink=ingestion_sink,
                    connector=connector,
                    db_repo=db_repo,
                    project_full_name=full_name,
                    default_branch=db_repo.settings.get("default_branch", "main"),
                    max_commits=max_commits,
                    include_blame=True,
                    include_commit_stats=False,
                )
            except Exception as e:
                logging.warning(
                    "Backfill failed for GitLab project %s: %s", full_name, e
                )

        logging.info(f"Successfully processed GitLab project: {project_id}")

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing GitLab project: {e}")
        raise
    finally:
        connector.close()


async def process_gitlab_projects_batch(
    store: Any,
    token: str,
    gitlab_url: str = "https://gitlab.com",
    group_name: str | None = None,
    pattern: str | None = None,
    batch_size: int = 10,
    max_concurrent: int = 4,
    rate_limit_delay: float = 1.0,
    max_commits_per_project: int | None = None,
    max_projects: int | None = None,
    use_async: bool = False,
    sync_git: bool = True,
    sync_prs: bool = True,
    sync_cicd: bool = True,
    sync_deployments: bool = True,
    sync_incidents: bool = True,
    sync_security: bool = True,
    sync_tests: bool = False,
    blame_only: bool = False,
    backfill_missing: bool = True,
    since: datetime | None = None,
) -> None:
    """
    Process multiple GitLab projects using batch processing with pattern matching.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info("=== GitLab Batch Project Processing ===")
    connector_cls = cast(Any, GitLabConnector)
    connector = connector_cls(url=gitlab_url, private_token=token)
    loop = asyncio.get_running_loop()
    ingestion_sink = IngestionSink(store)

    mr_gate = None
    mr_semaphore = None
    if sync_prs:
        rate_limit_gate_cls = cast(Any, RateLimitGate)
        rate_limit_config_cls = cast(Any, RateLimitConfig)
        mr_gate = rate_limit_gate_cls(
            rate_limit_config_cls(initial_backoff_seconds=max(1.0, rate_limit_delay))
        )
        mr_semaphore = asyncio.Semaphore(max(1, max_concurrent))

    all_results: list[Any] = []
    stored_count = 0
    # Per-project degraded MR fetches accumulated across the batch. The batch
    # keeps flushing healthy projects' rows, then raises after the summary so
    # the overall run is marked failed/retried instead of silently succeeding
    # while MR rows were skipped (CHAOS-2378).
    degraded_mr_errors: list[PartialGitLabMrSyncError] = []

    results_queue: asyncio.Queue | None = None
    _queue_sentinel = object()

    async def store_result(result: Any) -> None:
        """Store a single result in the database (upsert)."""
        nonlocal stored_count
        if not result.success:
            return

        project_info = result.repository
        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=project_info.full_name,
            provider="gitlab",
            settings={
                "source": "gitlab",
                "project_id": project_info.id,
                "url": project_info.url,
                "default_branch": project_info.default_branch,
                "batch_processed": True,
            },
            tags=["gitlab"],
        )

        await ingestion_sink.insert_repo(db_repo)
        stored_count += 1
        logging.debug(f"Stored project ({stored_count}): {db_repo.repo}")

        if blame_only:
            try:
                await _backfill_gitlab_missing_data(
                    store=store,
                    ingestion_sink=ingestion_sink,
                    connector=connector,
                    db_repo=db_repo,
                    project_full_name=project_info.full_name,
                    default_branch=project_info.default_branch,
                    max_commits=max_commits_per_project,
                    blame_only=True,
                )
            except Exception as e:
                logging.debug(
                    "Blame-only backfill failed for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )
            return

        gl_project = None
        if sync_git:
            # Fetch commits and stats to populate git_commits/git_commit_stats.
            if max_commits_per_project is None and since is None:
                commit_limit = 100
            else:
                commit_limit = max_commits_per_project or 100
            try:
                if gl_project is None:
                    gl_project = await loop.run_in_executor(
                        None, connector.gitlab.projects.get, project_info.id
                    )
                commit_hashes, commit_objects = await loop.run_in_executor(
                    None,
                    _fetch_gitlab_commits_sync,
                    gl_project,
                    commit_limit,
                    db_repo.id,
                    since,
                )
                if commit_objects:
                    await ingestion_sink.insert_git_commit_data(commit_objects)

                stats_objects = await loop.run_in_executor(
                    None,
                    _fetch_gitlab_commit_stats_sync,
                    gl_project,
                    commit_hashes,
                    db_repo.id,
                    50 if commit_limit is None else min(commit_limit, 50),
                )
                if stats_objects:
                    await ingestion_sink.insert_git_commit_stats(stats_objects)
            except Exception as e:
                logging.warning(
                    "Failed to fetch commits for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if sync_prs:
            # Fetch ALL merge requests for batch-processed projects, storing in batches.
            try:
                assert mr_semaphore is not None
                async with mr_semaphore:
                    await loop.run_in_executor(
                        None,
                        _sync_gitlab_mrs_to_store,
                        connector,
                        project_info.id,
                        db_repo.id,
                        ingestion_sink,
                        loop,
                        BATCH_SIZE,
                        "all",
                        mr_gate,
                        since,
                    )
            except PartialGitLabMrSyncError as e:
                # Degraded review fetch skipped MR rows for this project. Keep
                # processing the rest of the batch (so healthy projects'
                # commits/MRs/CI still flush), but ACCUMULATE the failure: the
                # batch raises a BatchGitLabMrSyncError after the summary so the
                # whole run is marked failed/retried. Returning normally here
                # would let sync_gitlab_target report success and advance the
                # per-target watermark past the skipped MRs, stranding them
                # behind ``since`` on the next incremental run — a permanent,
                # silent loss with no failed job to alert operators (CHAOS-2378).
                logging.warning(
                    "Degraded MR review fetch for GitLab project %s: %s "
                    "(batch continues; run will be marked failed for retry)",
                    project_info.full_name,
                    e,
                )
                degraded_mr_errors.append(e)
            except Exception as e:
                logging.warning(
                    "Failed to fetch/store MRs for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if sync_cicd:
            try:
                if gl_project is None:
                    gl_project = await loop.run_in_executor(
                        None, connector.gitlab.projects.get, project_info.id
                    )
                pipeline_runs = await loop.run_in_executor(
                    None,
                    _fetch_gitlab_pipelines_sync,
                    gl_project,
                    db_repo.id,
                    BATCH_SIZE,
                    since,
                )
                if pipeline_runs:
                    await ingestion_sink.insert_ci_pipeline_runs(pipeline_runs)
            except Exception as e:
                logging.warning(
                    "Failed to fetch CI/CD runs for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if sync_tests:
            try:
                if gl_project is None:
                    gl_project = await loop.run_in_executor(
                        None, connector.gitlab.projects.get, project_info.id
                    )
                await _sync_gitlab_test_reports(
                    connector=connector,
                    gl_project=gl_project,
                    project_id=project_info.id,
                    token=token,
                    repo_id=db_repo.id,
                    org_id=getattr(store, "org_id", "") or "",
                    ingestion_sink=ingestion_sink,
                    loop=loop,
                    since=since,
                )
            except Exception as e:
                logging.warning(
                    "Failed to sync test reports for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if sync_deployments:
            try:
                deployments = await loop.run_in_executor(
                    None,
                    _fetch_gitlab_deployments_sync,
                    connector,
                    project_info.id,
                    db_repo.id,
                    BATCH_SIZE,
                    since,
                )
                if deployments:
                    await ingestion_sink.insert_deployments(deployments)
            except Exception as e:
                logging.warning(
                    "Failed to fetch deployments for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if sync_incidents:
            try:
                incidents = await loop.run_in_executor(
                    None,
                    _fetch_gitlab_incidents_sync,
                    connector,
                    project_info.id,
                    db_repo.id,
                    BATCH_SIZE,
                    since,
                )
                if incidents:
                    await ingestion_sink.insert_incidents(incidents)
            except Exception as e:
                logging.warning(
                    "Failed to fetch incidents for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if sync_security:
            try:
                security_alerts = await loop.run_in_executor(
                    None,
                    _fetch_gitlab_security_alerts_sync,
                    connector,
                    project_info.id,
                    db_repo.id,
                    BATCH_SIZE,
                    since,
                )
                if security_alerts:
                    insert_security_alerts = getattr(
                        ingestion_sink, "insert_security_alerts"
                    )
                    await insert_security_alerts(security_alerts)
            except Exception as e:
                logging.warning(
                    "Failed to fetch security alerts for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if result.stats and sync_git:
            stat = GitCommitStat(
                repo_id=db_repo.id,
                commit_hash=AGGREGATE_STATS_MARKER,
                file_path=AGGREGATE_STATS_MARKER,
                additions=result.stats.additions,
                deletions=result.stats.deletions,
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
            await ingestion_sink.insert_git_commit_stats([stat])

        if backfill_missing and sync_git:
            try:
                # Blame is included so the /complexity Ownership-risk tab is
                # populated on normal onboarding. The has_any_git_blame gate is
                # repo-level (fetched once per project on first onboarding) and
                # the per-sync crawl is capped at BLAME_BACKFILL_MAX_FILES files
                # so a large project cannot exhaust API quota (CHAOS-2376).
                await _backfill_gitlab_missing_data(
                    store=store,
                    ingestion_sink=ingestion_sink,
                    connector=connector,
                    db_repo=db_repo,
                    project_full_name=project_info.full_name,
                    default_branch=project_info.default_branch,
                    max_commits=max_commits_per_project,
                    include_blame=True,
                    include_commit_stats=False,
                )
            except Exception as e:
                logging.debug(
                    "Backfill failed for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

    def on_project_complete(result: Any) -> None:
        all_results.append(result)
        if result.success:
            stats_info = ""
            if result.stats:
                stats_info = f" ({result.stats.total_commits} commits)"
                logging.info(
                    f"  ✓ Processed: {result.repository.full_name}{stats_info}"
                )
        else:
            logging.warning(
                f"  ✗ Failed: {result.repository.full_name}: {result.error}"
            )

        if results_queue is not None:
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None

            def _enqueue() -> None:
                assert results_queue is not None
                try:
                    results_queue.put_nowait(result)
                except asyncio.QueueFull:
                    asyncio.create_task(results_queue.put(result))

            if running_loop is loop:
                _enqueue()
            else:
                loop.call_soon_threadsafe(_enqueue)

    try:
        if sync_git:
            results_queue = asyncio.Queue(maxsize=max(1, max_concurrent * 2))

            async def _consume_results() -> None:
                assert results_queue is not None
                while True:
                    item = await results_queue.get()
                    try:
                        if item is _queue_sentinel:
                            return
                        await store_result(item)
                    finally:
                        results_queue.task_done()

            consumer_task = asyncio.create_task(_consume_results())

            if use_async:
                await connector.get_projects_with_stats_async(
                    group_name=group_name,
                    pattern=pattern,
                    batch_size=batch_size,
                    max_concurrent=max_concurrent,
                    rate_limit_delay=rate_limit_delay,
                    max_commits_per_repo=max_commits_per_project,
                    max_repos=max_projects,
                    on_project_complete=on_project_complete,
                )
            else:
                await loop.run_in_executor(
                    None,
                    lambda: connector.get_projects_with_stats(
                        group_name=group_name,
                        pattern=pattern,
                        batch_size=batch_size,
                        max_concurrent=max_concurrent,
                        rate_limit_delay=rate_limit_delay,
                        max_commits_per_repo=max_commits_per_project,
                        max_repos=max_projects,
                        on_project_complete=on_project_complete,
                    ),
                )

            await results_queue.join()
            await results_queue.put(_queue_sentinel)
            await consumer_task
        else:
            logging.info(
                "Listing GitLab projects for PR sync (group=%s, pattern=%s, max=%s)",
                group_name,
                pattern,
                max_projects,
            )
            projects = await loop.run_in_executor(
                None,
                lambda: connector._get_projects_for_processing(
                    group_name=group_name,
                    pattern=pattern,
                    max_repos=max_projects,
                ),
            )
            logging.info("Discovered %d GitLab projects for PR sync", len(projects))
            semaphore = asyncio.Semaphore(max(1, max_concurrent))

            async def _process_project(project_info) -> None:
                async with semaphore:
                    batch_result_cls = cast(Any, BatchResult)
                    result = batch_result_cls(
                        repository=project_info,
                        stats=None,
                        success=True,
                    )
                    try:
                        await store_result(result)
                    except Exception as e:
                        result = batch_result_cls(
                            repository=project_info,
                            stats=None,
                            error=str(e),
                            success=False,
                        )
                    on_project_complete(result)

            tasks = [asyncio.create_task(_process_project(p)) for p in projects]
            if tasks:
                await asyncio.gather(*tasks)

        # Summary
        successful = sum(1 for r in all_results if r.success)
        failed = sum(1 for r in all_results if not r.success)
        logging.info("=== Batch Processing Complete ===")
        logging.info(f"  Successful: {successful}")
        logging.info(f"  Failed: {failed}")
        logging.info(f"  Total: {len(all_results)}")
        logging.info(f"  Stored: {stored_count}")
        if degraded_mr_errors:
            logging.warning(
                "  Degraded MR sync: %d project(s)", len(degraded_mr_errors)
            )

        # Good rows from healthy projects are fully flushed at this point.
        # Raise so the caller marks the run failed/retried rather than reporting
        # success while some MR rows were silently skipped (CHAOS-2378).
        if degraded_mr_errors:
            raise BatchGitLabMrSyncError(degraded_mr_errors)

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except BatchGitLabMrSyncError:
        # Intentional control-flow signal: healthy rows are flushed, but the run
        # must fail so the watermark is held and the batch retries (CHAOS-2378).
        raise
    except Exception as e:
        logging.error(f"Error in batch processing: {e}")
        raise
    finally:
        connector.close()
