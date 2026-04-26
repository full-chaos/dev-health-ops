import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.credentials.types import GitHubCredentials
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
    check_backfill_needs,
)
from dev_health_ops.processors.fetch_utils import (
    AsyncBatchCollector,
    SyncBatchCollector,
    extract_retry_after,
)
from dev_health_ops.processors.fetch_utils import (
    safe_parse_datetime as _coerce_datetime,
)
from dev_health_ops.processors.release_ref import get_release_ref_enrichment
from dev_health_ops.processors.storage_protocol import GitSyncStore
from dev_health_ops.providers.pr_state import normalize_pr_state
from dev_health_ops.utils import (
    AGGREGATE_STATS_MARKER,
    BATCH_SIZE,
    CONNECTORS_AVAILABLE,
    is_skippable,
)

_unused_BATCH_COLLECTOR_TYPES = (SyncBatchCollector, AsyncBatchCollector)

if CONNECTORS_AVAILABLE:
    from github import RateLimitExceededException

    from dev_health_ops.connectors import (
        BatchResult,
        ConnectorException,
        GitHubConnector,
    )
    from dev_health_ops.connectors.models import Repository
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate
else:
    BatchResult = None  # type: ignore
    GitHubConnector = None  # type: ignore
    ConnectorException = Exception
    Repository = None  # type: ignore
    RateLimitConfig = None  # type: ignore
    RateLimitGate = None  # type: ignore
    RateLimitExceededException = Exception


# --- GitHub Sync Helpers ---


def _fetch_github_repo_info_sync(connector, owner, repo_name):
    """Sync helper to fetch GitHub repository info."""
    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")
    _ = gh_repo.id
    return gh_repo


def _fetch_github_commits_sync(
    gh_repo,
    max_commits: int | None,
    repo_id,
    since: datetime | None = None,
):
    """Sync helper to fetch and parse GitHub commits."""
    raw_commits = []
    if since is not None:
        commits_iter = gh_repo.get_commits(since=since)
    else:
        commits_iter = gh_repo.get_commits()

    for commit in commits_iter:
        raw_commits.append(commit)
        if max_commits is not None and len(raw_commits) >= max_commits:
            break

    commit_objects = []
    for commit in raw_commits:
        if since is not None:
            commit_when = None
            if getattr(commit, "commit", None) and getattr(
                commit.commit, "committer", None
            ):
                commit_when = getattr(commit.commit.committer, "date", None)
            if (
                commit_when is None
                and getattr(commit, "commit", None)
                and getattr(commit.commit, "author", None)
            ):
                commit_when = getattr(commit.commit.author, "date", None)

            if (
                isinstance(commit_when, datetime)
                and commit_when.astimezone(timezone.utc) < since
            ):
                continue

        # Prefer GitHub user `login` when available; do not store emails.
        author_login = getattr(commit, "author", None)
        committer_login = getattr(commit, "committer", None)

        author_name = getattr(author_login, "login", None) or (
            commit.commit.author.name if commit.commit.author else "Unknown"
        )
        committer_name = getattr(committer_login, "login", None) or (
            commit.commit.committer.name if commit.commit.committer else "Unknown"
        )

        # Safely obtain emails: prefer commit metadata (no extra API calls),
        # fallback to user.email but guard against API-triggered exceptions (e.g., 404).
        def _safe_user_email(user):
            try:
                return getattr(user, "email", None)
            except Exception:
                return None

        author_email = None
        if getattr(commit, "commit", None) and getattr(commit.commit, "author", None):
            author_email = getattr(commit.commit.author, "email", None)
        if not author_email:
            author_email = _safe_user_email(author_login)

        committer_email = None
        if getattr(commit, "commit", None) and getattr(
            commit.commit, "committer", None
        ):
            committer_email = getattr(commit.commit.committer, "email", None)
        if not committer_email:
            committer_email = _safe_user_email(committer_login)

        git_commit = GitCommit(
            repo_id=repo_id,
            hash=commit.sha,
            message=commit.commit.message,
            author_name=author_name,
            author_email=author_email,
            author_when=(
                commit.commit.author.date
                if commit.commit.author
                else datetime.now(timezone.utc)
            ),
            committer_name=committer_name,
            committer_email=committer_email,
            committer_when=(
                commit.commit.committer.date
                if commit.commit.committer
                else datetime.now(timezone.utc)
            ),
            parents=len(commit.parents),
        )
        commit_objects.append(git_commit)
    return raw_commits, commit_objects


def _fetch_github_commit_stats_sync(
    raw_commits,
    repo_id,
    max_stats,
    since: datetime | None = None,
):
    """Sync helper to fetch detailed commit stats (files)."""
    stats_objects = []
    for commit in raw_commits[:max_stats]:
        if since is not None:
            commit_when = None
            if getattr(commit, "commit", None) and getattr(
                commit.commit, "committer", None
            ):
                commit_when = getattr(commit.commit.committer, "date", None)
            if (
                commit_when is None
                and getattr(commit, "commit", None)
                and getattr(commit.commit, "author", None)
            ):
                commit_when = getattr(commit.commit.author, "date", None)

            if (
                isinstance(commit_when, datetime)
                and commit_when.astimezone(timezone.utc) < since
            ):
                continue

        try:
            files = commit.files
            if files is None:
                continue

            for file in files:
                stat = GitCommitStat(
                    repo_id=repo_id,
                    commit_hash=commit.sha,
                    file_path=file.filename,
                    additions=file.additions,
                    deletions=file.deletions,
                    old_file_mode="unknown",
                    new_file_mode="unknown",
                )
                stats_objects.append(stat)
        except Exception as e:
            logging.warning(
                "Failed to get stats for commit %s: %s",
                commit.sha,
                e,
            )
    return stats_objects


def _fetch_github_prs_sync(connector, owner, repo_name, repo_id, max_prs):
    """Sync helper to fetch Pull Requests."""
    prs = connector.get_pull_requests(
        owner,
        repo_name,
        state="all",
        max_prs=max_prs,
    )
    pr_objects = []
    for pr in prs:
        git_pr = GitPullRequest(
            repo_id=repo_id,
            number=pr.number,
            title=pr.title,
            body=pr.body,
            state=pr.state,
            author_name=pr.author.username if pr.author else "Unknown",
            author_email=pr.author.email if pr.author else None,
            created_at=BaseGitProcessor.coerce_created_at(
                pr.created_at, pr.merged_at, pr.closed_at
            ),
            merged_at=pr.merged_at,
            closed_at=pr.closed_at,
            head_branch=pr.head_branch,
            base_branch=pr.base_branch,
        )
        pr_objects.append(git_pr)
    return pr_objects


def _fetch_github_workflow_runs_sync(gh_repo, repo_id, max_runs, since):
    runs = []
    if not hasattr(gh_repo, "get_workflow_runs"):
        if not hasattr(gh_repo, "get_workflows"):
            return runs

    try:
        if hasattr(gh_repo, "get_workflow_runs"):
            raw_runs = []
            for run in gh_repo.get_workflow_runs():
                raw_runs.append(run)
                if len(raw_runs) >= max_runs:
                    break
        else:
            raw_runs = []
            for workflow in gh_repo.get_workflows():
                for run in workflow.get_runs():
                    raw_runs.append(run)
                    if len(raw_runs) >= max_runs:
                        break
                if len(raw_runs) >= max_runs:
                    break
    except Exception as exc:
        logging.debug("Failed to fetch workflow runs: %s", exc)
        return runs

    for run in raw_runs:
        queued_at = _coerce_datetime(getattr(run, "created_at", None))
        started_at = _coerce_datetime(getattr(run, "run_started_at", None)) or queued_at
        if started_at is None:
            continue
        if since is not None and started_at.astimezone(timezone.utc) < since:
            continue
        finished_at = _coerce_datetime(getattr(run, "updated_at", None))
        runs.append(
            CiPipelineRun(
                repo_id=repo_id,
                run_id=str(getattr(run, "id", "")),
                status=getattr(run, "conclusion", None) or getattr(run, "status", None),
                queued_at=queued_at,
                started_at=started_at,
                finished_at=finished_at,
            )
        )
    return runs


def _fetch_github_deployments_sync(gh_repo, repo_id, max_deployments, since):
    deployments = []
    if not hasattr(gh_repo, "get_deployments"):
        return deployments
    release_objects = []
    if hasattr(gh_repo, "get_releases"):
        try:
            release_objects = list(gh_repo.get_releases()[:max_deployments])
        except Exception as exc:
            logging.debug("Failed to fetch GitHub releases for release_ref: %s", exc)
    try:
        raw_deployments = list(gh_repo.get_deployments()[:max_deployments])
    except Exception as exc:
        logging.debug("Failed to fetch deployments: %s", exc)
        return deployments

    for dep in raw_deployments:
        created_at = getattr(dep, "created_at", None)
        if not isinstance(created_at, datetime):
            continue
        if since is not None and created_at.astimezone(timezone.utc) < since:
            continue
        enrichment = get_release_ref_enrichment(
            dep,
            "github",
            releases=release_objects,
        )
        deployments.append(
            Deployment(
                repo_id=repo_id,
                deployment_id=str(getattr(dep, "id", "")),
                status=getattr(dep, "state", None),
                environment=getattr(dep, "environment", None),
                started_at=created_at,
                finished_at=None,
                deployed_at=created_at,
                merged_at=None,
                pull_request_number=None,
                release_ref=enrichment.release_ref,
                release_ref_confidence=enrichment.confidence,
            )
        )
    return deployments


def _fetch_github_incidents_sync(gh_repo, repo_id, max_issues, since):
    incidents = []
    if not hasattr(gh_repo, "get_issues"):
        return incidents
    try:
        raw_issues = list(
            gh_repo.get_issues(state="all", labels=["incident"])[:max_issues]
        )
    except Exception as exc:
        logging.debug("Failed to fetch incident issues: %s", exc)
        return incidents

    for issue in raw_issues:
        created_at = getattr(issue, "created_at", None)
        if not isinstance(created_at, datetime):
            continue
        if since is not None and created_at.astimezone(timezone.utc) < since:
            continue
        incidents.append(
            Incident(
                repo_id=repo_id,
                incident_id=str(getattr(issue, "id", "")),
                status=getattr(issue, "state", None),
                started_at=created_at,
                resolved_at=getattr(issue, "closed_at", None),
            )
        )
    return incidents


def _fetch_github_security_alerts_sync(
    connector, owner, repo_name, repo_id, max_alerts, since
):
    """Sync helper to fetch GitHub security alerts (Dependabot, code scanning, advisories)."""
    security_alert_cls = getattr(git_models, "SecurityAlert")
    alerts = []
    for fetch_fn in [
        connector.get_dependabot_alerts,
        connector.get_code_scanning_alerts,
        connector.get_security_advisories,
    ]:
        try:
            raw_alerts = fetch_fn(owner, repo_name, max_alerts=max_alerts)
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
            logging.debug("Failed to fetch %s: %s", fetch_fn.__name__, exc)
    return alerts


def _collect_github_pr_objects(
    connector,
    owner: str,
    repo_name: str,
    repo_id,
    gh_repo,
    gate: "RateLimitGate",
    state: str = "all",
    since: datetime | None = None,
) -> tuple[list[GitPullRequest], list[Any]]:
    """Collect raw PR objects and build GitPullRequest records without per-PR review/comment API calls.

    Reviews and comments are fetched in a subsequent batch pass to avoid N+1 API calls.
    Returns (pr_objects, raw_gh_prs) for downstream review/comment batching.
    """
    pr_objects: list[GitPullRequest] = []
    raw_gh_prs: list[Any] = []

    sorted_by_updated = True
    try:
        pr_iter = iter(gh_repo.get_pulls(state=state, sort="updated", direction="desc"))
    except TypeError:
        sorted_by_updated = False
        pr_iter = iter(gh_repo.get_pulls(state=state))

    while True:
        try:
            gate.wait_sync()
            gh_pr = next(pr_iter)
            gate.reset()
        except StopIteration:
            break
        except RateLimitExceededException as e:
            retry_after = extract_retry_after(e, connector)
            applied = gate.penalize(retry_after)
            logging.info(
                "GitHub rate limited fetching PRs; backoff %.1fs (%s)",
                applied,
                e,
            )
            continue
        except Exception as e:
            retry_after = extract_retry_after(e, connector=None)
            if retry_after is not None:
                applied = gate.penalize(retry_after)
                logging.info(
                    "GitHub rate limited fetching PRs; backoff %.1fs (%s)",
                    applied,
                    e,
                )
                continue
            raise

        if since is not None and sorted_by_updated:
            updated_at = getattr(gh_pr, "updated_at", None)
            if (
                isinstance(updated_at, datetime)
                and updated_at.astimezone(timezone.utc) < since
            ):
                break

        author_name = "Unknown"
        author_email = None
        if getattr(gh_pr, "user", None):
            author_name = getattr(gh_pr.user, "login", None) or author_name

        merged_at = getattr(gh_pr, "merged_at", None)
        closed_at = getattr(gh_pr, "closed_at", None)
        created_at = BaseGitProcessor.coerce_created_at(
            getattr(gh_pr, "created_at", None), merged_at, closed_at
        )

        additions = getattr(gh_pr, "additions", 0)
        deletions = getattr(gh_pr, "deletions", 0)
        changed_files = getattr(gh_pr, "changed_files", 0)
        comments_count = getattr(gh_pr, "comments", 0)

        pr_objects.append(
            GitPullRequest(
                repo_id=repo_id,
                number=int(getattr(gh_pr, "number", 0) or 0),
                title=getattr(gh_pr, "title", None),
                body=getattr(gh_pr, "body", None),
                state=normalize_pr_state(getattr(gh_pr, "state", None), merged_at),
                author_name=author_name,
                author_email=author_email,
                created_at=created_at,
                merged_at=merged_at,
                closed_at=closed_at,
                head_branch=getattr(getattr(gh_pr, "head", None), "ref", None),
                base_branch=getattr(getattr(gh_pr, "base", None), "ref", None),
                additions=additions,
                deletions=deletions,
                changed_files=changed_files,
                first_review_at=None,
                first_comment_at=None,
                changes_requested_count=0,
                reviews_count=0,
                comments_count=comments_count,
            )
        )
        raw_gh_prs.append(gh_pr)

    return pr_objects, raw_gh_prs


def _enrich_prs_with_reviews_batch(
    connector,
    owner: str,
    repo_name: str,
    repo_id,
    pr_objects: list[GitPullRequest],
    raw_gh_prs: list[Any],
    ingestion_sink: IngestionSink,
    loop: asyncio.AbstractEventLoop,
) -> list[GitPullRequestReview]:
    """Batch-fetch reviews for all PRs and enrich pr_objects in place.

    Collects all review objects and returns them for a single bulk insert,
    replacing the N+1 pattern (one review API call per PR).
    """
    all_review_objects: list[GitPullRequestReview] = []

    for pr_obj, gh_pr in zip(pr_objects, raw_gh_prs):
        try:
            reviews = connector.get_pull_request_reviews(owner, repo_name, gh_pr.number)
        except Exception as e:
            logging.debug("Failed to fetch reviews for PR #%d: %s", gh_pr.number, e)
            continue

        if not reviews:
            continue

        first_review_at = None
        reviews_count = 0
        changes_requested_count = 0

        for r in reviews:
            review_at = r.submitted_at or pr_obj.created_at
            if first_review_at is None or review_at < first_review_at:
                first_review_at = review_at
            if r.state == "CHANGES_REQUESTED":
                changes_requested_count += 1
            reviews_count += 1
            all_review_objects.append(
                GitPullRequestReview(
                    repo_id=repo_id,
                    number=gh_pr.number,
                    review_id=r.id,
                    reviewer=r.reviewer,
                    state=r.state,
                    submitted_at=review_at,
                )
            )

        # Mutate the pr_obj fields that require review data
        object.__setattr__(pr_obj, "first_review_at", first_review_at) if hasattr(
            type(pr_obj), "__dataclass_fields__"
        ) else None
        try:
            pr_obj.first_review_at = first_review_at  # type: ignore[misc]
            pr_obj.reviews_count = reviews_count  # type: ignore[misc]
            pr_obj.changes_requested_count = changes_requested_count  # type: ignore[misc]
        except AttributeError:
            pass  # frozen dataclass; fields stay at default

    return all_review_objects


def _sync_github_prs_to_store(
    connector,
    owner: str,
    repo_name: str,
    repo_id,
    ingestion_sink: IngestionSink,
    loop: asyncio.AbstractEventLoop,
    batch_size: int,
    state: str = "all",
    gate: RateLimitGate | None = None,
    since: datetime | None = None,
) -> int:
    """Fetch all PRs for a repo and insert them in batches.

    Runs in a worker thread; uses run_coroutine_threadsafe to write batches.
    Reviews are fetched in a single batch pass after all PRs are collected,
    avoiding N+1 API calls.
    """
    logging.info(
        "Fetching PRs for %s/%s...",
        owner,
        repo_name,
    )
    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")

    gate = BaseGitProcessor.ensure_gate(gate)

    # Phase 1: collect all PR objects without per-PR review/comment API calls
    pr_objects, raw_gh_prs = _collect_github_pr_objects(
        connector=connector,
        owner=owner,
        repo_name=repo_name,
        repo_id=repo_id,
        gh_repo=gh_repo,
        gate=gate,
        state=state,
        since=since,
    )
    total = len(pr_objects)

    # Phase 2: batch-fetch reviews for all collected PRs (single pass, no N+1)
    review_objects = _enrich_prs_with_reviews_batch(
        connector=connector,
        owner=owner,
        repo_name=repo_name,
        repo_id=repo_id,
        pr_objects=pr_objects,
        raw_gh_prs=raw_gh_prs,
        ingestion_sink=ingestion_sink,
        loop=loop,
    )

    # Phase 3: persist reviews in one bulk insert
    if review_objects:
        BaseGitProcessor.persist_batch_threadsafe(
            ingestion_sink.insert_git_pull_request_reviews(review_objects),
            loop,
        )
        logging.debug(
            "Stored %d reviews for %s/%s",
            len(review_objects),
            owner,
            repo_name,
        )

    # Phase 4: persist PRs in batches
    for i in range(0, len(pr_objects), batch_size):
        batch = pr_objects[i : i + batch_size]
        BaseGitProcessor.persist_batch_threadsafe(
            ingestion_sink.insert_git_pull_requests(batch),
            loop,
        )
        logging.debug(
            "Stored batch of %d PRs for %s/%s (total so far: %d)",
            len(batch),
            owner,
            repo_name,
            min(i + batch_size, total),
        )

    logging.info(
        "Fetched %d PRs for %s/%s",
        total,
        owner,
        repo_name,
    )

    return total


def _fetch_github_blame_sync(gh_repo, repo_id, limit=50):
    """Sync helper to fetch (simulated) blame by listing files."""
    files_to_process = []
    try:
        contents = gh_repo.get_contents("", ref=gh_repo.default_branch)
        while contents:
            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(
                    gh_repo.get_contents(
                        file_content.path,
                        ref=gh_repo.default_branch,
                    )
                )
            else:
                if not is_skippable(file_content.path):
                    files_to_process.append(file_content.path)

            if len(files_to_process) >= limit:
                break
    except Exception as e:
        logging.error(f"Error listing files: {e}")
    return []


def _split_full_name(full_name: str) -> tuple[str, str]:
    parts = (full_name or "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid repo/project full name: {full_name}")
    return parts[0], parts[1]


async def _backfill_github_missing_data(
    store: Any,
    ingestion_sink: IngestionSink,
    connector: GitHubConnector,
    db_repo: Repo,
    repo_full_name: str,
    default_branch: str,
    max_commits: int | None,
    blame_only: bool = False,
) -> None:
    # Logic matches the CLI sync orchestration.
    logging.info(
        "Backfilling data for %s...",
        repo_full_name,
    )
    owner, repo_name = _split_full_name(repo_full_name)

    needs = await check_backfill_needs(store, db_repo.id, blame_only=blame_only)
    if not needs.any:
        return

    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")

    file_paths: list[str] = []
    if needs.files or needs.blame:
        try:
            branch = gh_repo.get_branch(default_branch)
            tree = gh_repo.get_git_tree(branch.commit.sha, recursive=True)
            for entry in getattr(tree, "tree", []) or []:
                if getattr(entry, "type", None) != "blob":
                    continue
                path = getattr(entry, "path", None)
                if not path:
                    continue
                file_paths.append(path)

            if needs.files and file_paths:
                await backfill_file_records(
                    ingestion_sink, db_repo.id, file_paths, repo_full_name
                )
        except Exception as e:
            logging.warning(
                f"Failed to backfill GitHub files for {repo_full_name}: {e}"
            )

    if needs.commit_stats:
        try:
            logging.info(
                "Backfilling commit stats for %s...",
                repo_full_name,
            )
            commits_iter = gh_repo.get_commits()
            async with AsyncBatchCollector(
                ingestion_sink.insert_git_commit_stats
            ) as stats_collector:
                commit_count = 0
                for commit in commits_iter:
                    if max_commits and commit_count >= max_commits:
                        break
                    commit_count += 1
                    try:
                        detailed = gh_repo.get_commit(commit.sha)
                        for file in getattr(detailed, "files", []) or []:
                            stats_collector.add(
                                GitCommitStat(
                                    repo_id=db_repo.id,
                                    commit_hash=commit.sha,
                                    file_path=getattr(
                                        file, "filename", AGGREGATE_STATS_MARKER
                                    ),
                                    additions=getattr(file, "additions", 0),
                                    deletions=getattr(file, "deletions", 0),
                                    old_file_mode="unknown",
                                    new_file_mode="unknown",
                                )
                            )
                            await stats_collector.maybe_flush()
                    except Exception as e:
                        logging.debug(
                            "Failed commit stat fetch for %s@%s: %s",
                            repo_full_name,
                            commit.sha,
                            e,
                        )
            logging.info(
                "Backfilled commit stats for %d commits in %s",
                commit_count,
                repo_full_name,
            )
        except Exception as e:
            logging.warning(
                "Failed to backfill GitHub commit stats for %s: %s",
                repo_full_name,
                e,
            )

    if needs.blame and file_paths:
        try:
            logging.info(
                "Backfilling blame for %d files in %s...",
                len(file_paths),
                repo_full_name,
            )
            async with AsyncBatchCollector(
                ingestion_sink.insert_blame_data
            ) as blame_collector:
                processed_files = 0
                for path in file_paths:
                    try:
                        blame = connector.get_file_blame(
                            owner=owner,
                            repo=repo_name,
                            path=path,
                            ref=default_branch,
                        )
                        processed_files += 1
                    except Exception as e:
                        logging.debug(
                            f"Failed blame fetch for {repo_full_name}:{path}: {e}"
                        )
                        continue

                    for rng in blame.ranges:
                        for line_no in range(
                            rng.starting_line,
                            rng.ending_line + 1,
                        ):
                            blame_collector.add(
                                GitBlame(
                                    repo_id=db_repo.id,
                                    path=path,
                                    line_no=line_no,
                                    author_email=rng.author_email,
                                    author_name=rng.author,
                                    author_when=None,
                                    commit_hash=rng.commit_sha,
                                    line=None,
                                )
                            )
                            await blame_collector.maybe_flush()
            logging.info(
                "Backfilled blame for %d files in %s",
                processed_files,
                repo_full_name,
            )
        except Exception as e:
            logging.warning(
                f"Failed to backfill GitHub blame for {repo_full_name}: {e}"
            )


async def process_github_repo(
    store: GitSyncStore | Any,
    owner: str,
    repo_name: str,
    token: str | GitHubCredentials,
    fetch_blame: bool = False,
    blame_only: bool = False,
    max_commits: int | None = None,
    sync_git: bool = True,
    sync_prs: bool = True,
    sync_cicd: bool = True,
    sync_deployments: bool = True,
    sync_incidents: bool = True,
    sync_security: bool = True,
    since: datetime | None = None,
) -> None:
    """
    Process a GitHub repository using the GitHub connector.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError("Connectors unavailable. Install required dependencies.")

    logging.info(f"Processing GitHub repository: {owner}/{repo_name}")
    loop = asyncio.get_running_loop()
    ingestion_sink = IngestionSink(store)

    connector = (
        GitHubConnector(credentials=token)
        if isinstance(token, GitHubCredentials)
        else GitHubConnector(token=token)
    )
    try:
        # 1. Fetch Repo Info
        logging.info("Fetching repository information...")
        gh_repo = await loop.run_in_executor(
            None, _fetch_github_repo_info_sync, connector, owner, repo_name
        )

        # Create/Insert Repo
        repo_info = Repository(
            id=gh_repo.id,
            name=gh_repo.name,
            full_name=gh_repo.full_name,
            default_branch=gh_repo.default_branch,
            description=gh_repo.description,
            url=gh_repo.html_url,
            created_at=gh_repo.created_at,
            updated_at=gh_repo.updated_at,
            language=gh_repo.language,
            stars=gh_repo.stargazers_count,
            forks=gh_repo.forks_count,
        )

        db_repo = Repo(
            repo_path=None,
            repo=repo_info.full_name,
            provider="github",
            settings={
                "source": "github",
                "repo_id": repo_info.id,
                "url": repo_info.url,
                "default_branch": repo_info.default_branch,
            },
            tags=[
                "github",
                repo_info.language,
            ]
            if repo_info.language
            else ["github"],
        )

        await ingestion_sink.insert_repo(db_repo)
        logging.info(f"Repository stored: {db_repo.repo} ({db_repo.id})")

        if blame_only:
            await _backfill_github_missing_data(
                store=store,
                ingestion_sink=ingestion_sink,
                connector=connector,
                db_repo=db_repo,
                repo_full_name=db_repo.repo,
                default_branch=repo_info.default_branch,
                max_commits=max_commits,
                blame_only=True,
            )
            logging.info(
                "Completed blame-only sync for GitHub repository: %s/%s",
                owner,
                repo_name,
            )
            return

        if sync_git:
            # 2. Fetch Commits
            if max_commits is None:
                logging.info("Fetching all commits from GitHub...")
            else:
                logging.info(f"Fetching up to {max_commits} commits from GitHub...")
            raw_commits, commit_objects = await loop.run_in_executor(
                None,
                _fetch_github_commits_sync,
                gh_repo,
                max_commits,
                db_repo.id,
                since,
            )

            if commit_objects:
                await ingestion_sink.insert_git_commit_data(commit_objects)
                logging.info(f"Stored {len(commit_objects)} commits from GitHub")

            # 3. Fetch Stats
            logging.info("Fetching commit stats from GitHub...")
            stats_limit = 50 if max_commits is None else min(max_commits, 50)
            stats_objects = await loop.run_in_executor(
                None,
                _fetch_github_commit_stats_sync,
                raw_commits,
                db_repo.id,
                stats_limit,
                since,
            )

            if stats_objects:
                await ingestion_sink.insert_git_commit_stats(stats_objects)
                logging.info(
                    "Stored %d commit stats from GitHub",
                    len(stats_objects),
                )

        if sync_prs:
            # 4. Fetch PRs
            logging.info("Fetching pull requests from GitHub...")
            pr_total = await loop.run_in_executor(
                None,
                _sync_github_prs_to_store,
                connector,
                owner,
                repo_name,
                db_repo.id,
                ingestion_sink,
                loop,
                BATCH_SIZE,
                "all",
                None,
                since,
            )
            logging.info(f"Stored {pr_total} pull requests from GitHub")

        if sync_cicd:
            logging.info("Fetching CI/CD workflow runs from GitHub...")
            pipeline_runs = await loop.run_in_executor(
                None,
                _fetch_github_workflow_runs_sync,
                gh_repo,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if pipeline_runs:
                await ingestion_sink.insert_ci_pipeline_runs(pipeline_runs)
                logging.info("Stored %d workflow runs", len(pipeline_runs))

        if sync_deployments:
            logging.info("Fetching deployments from GitHub...")
            deployments = await loop.run_in_executor(
                None,
                _fetch_github_deployments_sync,
                gh_repo,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if deployments:
                await ingestion_sink.insert_deployments(deployments)
                logging.info("Stored %d deployments", len(deployments))

        if sync_incidents:
            logging.info("Fetching incident issues from GitHub...")
            incidents = await loop.run_in_executor(
                None,
                _fetch_github_incidents_sync,
                gh_repo,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if incidents:
                await ingestion_sink.insert_incidents(incidents)
                logging.info("Stored %d incidents", len(incidents))

        if sync_security:
            logging.info("Fetching security alerts from GitHub...")
            security_alerts = await loop.run_in_executor(
                None,
                _fetch_github_security_alerts_sync,
                connector,
                owner,
                repo_name,
                db_repo.id,
                BATCH_SIZE,
                since,
            )
            if security_alerts:
                insert_security_alerts = getattr(
                    ingestion_sink, "insert_security_alerts"
                )
                await insert_security_alerts(security_alerts)
                logging.info("Stored %d security alerts", len(security_alerts))

        # 5. Fetch Blame (Optional & Stubbed)
        if fetch_blame:
            logging.info("Fetching blame data (file list) from GitHub...")
            await loop.run_in_executor(
                None, _fetch_github_blame_sync, gh_repo, db_repo.id
            )

        logging.info(
            "Successfully processed GitHub repository: %s/%s",
            owner,
            repo_name,
        )

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing GitHub repository: {e}")
        raise
    finally:
        connector.close()


async def process_github_repos_batch(
    store: Any,
    token: str | GitHubCredentials,
    org_name: str = None,
    user_name: str = None,
    pattern: str = None,
    batch_size: int = 10,
    max_concurrent: int = 4,
    rate_limit_delay: float = 1.0,
    max_commits_per_repo: int = None,
    max_repos: int = None,
    use_async: bool = False,
    sync_git: bool = True,
    sync_prs: bool = True,
    sync_cicd: bool = True,
    sync_deployments: bool = True,
    sync_incidents: bool = True,
    sync_security: bool = True,
    blame_only: bool = False,
    backfill_missing: bool = True,
    since: datetime | None = None,
) -> None:
    """
    Process multiple GitHub repositories using batch processing with
    pattern matching.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError("Connectors unavailable. Install required dependencies.")

    logging.info("=== GitHub Batch Repository Processing ===")
    connector = (
        GitHubConnector(credentials=token)
        if isinstance(token, GitHubCredentials)
        else GitHubConnector(token=token)
    )
    loop = asyncio.get_running_loop()
    ingestion_sink = IngestionSink(store)

    pr_gate = None
    pr_semaphore = None
    if sync_prs:
        pr_gate = RateLimitGate(
            RateLimitConfig(initial_backoff_seconds=max(1.0, rate_limit_delay))
        )
        pr_semaphore = asyncio.Semaphore(max(1, max_concurrent))

    # Track results for summary and incremental storage
    all_results: list[BatchResult] = []
    stored_count = 0

    results_queue: asyncio.Queue | None = None
    _queue_sentinel = object()

    async def store_result(result: BatchResult) -> None:
        """Store a single result in the database (upsert)."""
        nonlocal stored_count
        if not result.success:
            return

        repo_info = result.repository
        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=repo_info.full_name,
            provider="github",
            settings={
                "source": "github",
                "repo_id": repo_info.id,
                "url": repo_info.url,
                "default_branch": repo_info.default_branch,
                "batch_processed": True,
            },
            tags=[
                "github",
                repo_info.language,
            ]
            if repo_info.language
            else ["github"],
        )

        await ingestion_sink.insert_repo(db_repo)
        stored_count += 1
        logging.debug(f"Stored repository ({stored_count}): {db_repo.repo}")

        if blame_only:
            try:
                await _backfill_github_missing_data(
                    store=store,
                    ingestion_sink=ingestion_sink,
                    connector=connector,
                    db_repo=db_repo,
                    repo_full_name=repo_info.full_name,
                    default_branch=repo_info.default_branch,
                    max_commits=max_commits_per_repo,
                    blame_only=True,
                )
            except Exception as e:
                logging.debug(
                    "Blame-only backfill failed for GitHub repo %s: %s",
                    repo_info.full_name,
                    e,
                )
            return

        gh_repo = None
        if sync_git:
            # Fetch commits and stats to populate git_commits/git_commit_stats.
            if max_commits_per_repo is None and since is None:
                commit_limit = 100
            else:
                commit_limit = max_commits_per_repo
            try:
                if gh_repo is None:
                    gh_repo = connector.github.get_repo(repo_info.full_name)
                raw_commits, commit_objects = await loop.run_in_executor(
                    None,
                    _fetch_github_commits_sync,
                    gh_repo,
                    commit_limit,
                    db_repo.id,
                    since,
                )
                if commit_objects:
                    await ingestion_sink.insert_git_commit_data(commit_objects)

                stats_objects = await loop.run_in_executor(
                    None,
                    _fetch_github_commit_stats_sync,
                    raw_commits,
                    db_repo.id,
                    50 if commit_limit is None else min(commit_limit, 50),
                    since,
                )
                if stats_objects:
                    await ingestion_sink.insert_git_commit_stats(stats_objects)
            except Exception as e:
                logging.warning(
                    "Failed to fetch commits for GitHub repo %s: %s",
                    repo_info.full_name,
                    e,
                    exc_info=True,
                )

        if sync_prs:
            # Fetch ALL PRs for batch-processed repos, storing in batches.
            try:
                owner, repo_name = _split_full_name(repo_info.full_name)
                async with pr_semaphore:
                    await loop.run_in_executor(
                        None,
                        _sync_github_prs_to_store,
                        connector,
                        owner,
                        repo_name,
                        db_repo.id,
                        ingestion_sink,
                        loop,
                        BATCH_SIZE,
                        "all",
                        pr_gate,
                        since,
                    )
            except Exception as e:
                logging.error(
                    "Failed to fetch/store PRs for GitHub repo %s: %s",
                    repo_info.full_name,
                    e,
                )
                raise

        if sync_cicd:
            try:
                if gh_repo is None:
                    gh_repo = connector.github.get_repo(repo_info.full_name)
                pipeline_runs = await loop.run_in_executor(
                    None,
                    _fetch_github_workflow_runs_sync,
                    gh_repo,
                    db_repo.id,
                    BATCH_SIZE,
                    since,
                )
                if pipeline_runs:
                    await ingestion_sink.insert_ci_pipeline_runs(pipeline_runs)
            except Exception as e:
                logging.warning(
                    "Failed to fetch CI/CD runs for GitHub repo %s: %s",
                    repo_info.full_name,
                    e,
                )

        if sync_deployments:
            try:
                if gh_repo is None:
                    gh_repo = connector.github.get_repo(repo_info.full_name)
                deployments = await loop.run_in_executor(
                    None,
                    _fetch_github_deployments_sync,
                    gh_repo,
                    db_repo.id,
                    BATCH_SIZE,
                    since,
                )
                if deployments:
                    await ingestion_sink.insert_deployments(deployments)
            except Exception as e:
                logging.warning(
                    "Failed to fetch deployments for GitHub repo %s: %s",
                    repo_info.full_name,
                    e,
                )

        if sync_incidents:
            try:
                if gh_repo is None:
                    gh_repo = connector.github.get_repo(repo_info.full_name)
                incidents = await loop.run_in_executor(
                    None,
                    _fetch_github_incidents_sync,
                    gh_repo,
                    db_repo.id,
                    BATCH_SIZE,
                    since,
                )
                if incidents:
                    await ingestion_sink.insert_incidents(incidents)
            except Exception as e:
                logging.warning(
                    "Failed to fetch incidents for GitHub repo %s: %s",
                    repo_info.full_name,
                    e,
                )

        if sync_security:
            try:
                owner, repo_name = _split_full_name(repo_info.full_name)
                security_alerts = await loop.run_in_executor(
                    None,
                    _fetch_github_security_alerts_sync,
                    connector,
                    owner,
                    repo_name,
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
                    "Failed to fetch security alerts for GitHub repo %s: %s",
                    repo_info.full_name,
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

        if backfill_missing:
            try:
                await _backfill_github_missing_data(
                    store=store,
                    ingestion_sink=ingestion_sink,
                    connector=connector,
                    db_repo=db_repo,
                    repo_full_name=repo_info.full_name,
                    default_branch=repo_info.default_branch,
                    max_commits=max_commits_per_repo,
                )
            except Exception as e:
                logging.debug(
                    "Backfill failed for GitHub repo %s: %s",
                    repo_info.full_name,
                    e,
                )

    def on_repo_complete(result: BatchResult) -> None:
        all_results.append(result)
        if result.success:
            stats_info = ""
            if result.stats:
                stats_info = f" ({result.stats.total_commits} commits)"
            logging.info(
                "  \u2713 Processed: %s%s",
                result.repository.full_name,
                stats_info,
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
                await connector.get_repos_with_stats_async(
                    org_name=org_name,
                    user_name=user_name,
                    pattern=pattern,
                    batch_size=batch_size,
                    max_concurrent=max_concurrent,
                    rate_limit_delay=rate_limit_delay,
                    max_commits_per_repo=max_commits_per_repo,
                    max_repos=max_repos,
                    on_repo_complete=on_repo_complete,
                )
            else:
                await loop.run_in_executor(
                    None,
                    lambda: connector.get_repos_with_stats(
                        org_name=org_name,
                        user_name=user_name,
                        pattern=pattern,
                        batch_size=batch_size,
                        max_concurrent=max_concurrent,
                        rate_limit_delay=rate_limit_delay,
                        max_commits_per_repo=max_commits_per_repo,
                        max_repos=max_repos,
                        on_repo_complete=on_repo_complete,
                    ),
                )

            await results_queue.join()
            await results_queue.put(_queue_sentinel)
            await consumer_task
        else:
            repos = await loop.run_in_executor(
                None,
                lambda: connector.list_repositories(
                    org_name=org_name,
                    user_name=user_name,
                    pattern=pattern,
                    max_repos=max_repos,
                ),
            )
            semaphore = asyncio.Semaphore(max(1, max_concurrent))

            async def _process_repo(repo_info) -> None:
                async with semaphore:
                    result = BatchResult(
                        repository=repo_info,
                        stats=None,
                        success=True,
                    )
                    try:
                        await store_result(result)
                    except Exception as e:
                        result = BatchResult(
                            repository=repo_info,
                            stats=None,
                            error=str(e),
                            success=False,
                        )
                    on_repo_complete(result)

            tasks = [asyncio.create_task(_process_repo(repo)) for repo in repos]
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

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in batch processing: {e}")
        raise
    finally:
        connector.close()
