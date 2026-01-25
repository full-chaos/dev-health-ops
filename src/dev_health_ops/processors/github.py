import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Tuple, List, Optional

from dev_health_ops.models.git import (
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
    GitPullRequest,
    GitPullRequestReview,
    CiPipelineRun,
    Deployment,
    Incident,
    Repo,
)
from dev_health_ops.utils import (
    AGGREGATE_STATS_MARKER,
    BATCH_SIZE,
    CONNECTORS_AVAILABLE,
    is_skippable,
)

if CONNECTORS_AVAILABLE:
    from dev_health_ops.connectors import (
        BatchResult,
        GitHubConnector,
        ConnectorException,
    )
    from dev_health_ops.connectors.models import Repository
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate
    from github import RateLimitExceededException
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
    max_commits: Optional[int],
    repo_id,
    since: Optional[datetime] = None,
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
    since: Optional[datetime] = None,
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
        created_at = (
            pr.created_at or pr.merged_at or pr.closed_at or datetime.now(timezone.utc)
        )
        git_pr = GitPullRequest(
            repo_id=repo_id,
            number=pr.number,
            title=pr.title,
            body=pr.body,
            state=pr.state,
            author_name=pr.author.username if pr.author else "Unknown",
            author_email=pr.author.email if pr.author else None,
            created_at=created_at,
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

    def _coerce_datetime(value):
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return None
        return None

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


def _sync_github_prs_to_store(
    connector,
    owner: str,
    repo_name: str,
    repo_id,
    store,
    loop: asyncio.AbstractEventLoop,
    batch_size: int,
    state: str = "all",
    gate: Optional[RateLimitGate] = None,
    since: Optional[datetime] = None,
) -> int:
    """Fetch all PRs for a repo and insert them in batches.

    Runs in a worker thread; uses run_coroutine_threadsafe to write batches.
    """
    logging.info(
        "Fetching PRs for %s/%s...",
        owner,
        repo_name,
    )
    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")
    batch: List[GitPullRequest] = []
    total = 0

    if gate is None:
        gate = RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

    # per_page is set at Github client level during connector initialization
    # (some test doubles or older PyGithub versions may not accept sort/direction).
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
            retry_after = None
            if hasattr(connector, "_rate_limit_reset_delay_seconds"):
                try:
                    retry_after = connector._rate_limit_reset_delay_seconds()
                except Exception:
                    retry_after = None
            if retry_after is None:
                headers = getattr(e, "headers", None)
                if isinstance(headers, dict):
                    headers_ci = {str(k).lower(): v for k, v in headers.items()}
                    ra = headers_ci.get("retry-after")
                    if ra is not None:
                        try:
                            retry_after = float(ra)
                        except ValueError:
                            retry_after = None
            applied = gate.penalize(retry_after)
            logging.info(
                "GitHub rate limited fetching PRs; backoff %.1fs (%s)",
                applied,
                e,
            )
            continue
        except Exception as e:
            headers = getattr(e, "headers", None)
            retry_after = None
            if isinstance(headers, dict):
                # PyGithub header casing varies; treat keys case-insensitively.
                headers_ci = {str(k).lower(): v for k, v in headers.items()}
                ra = headers_ci.get("retry-after")
                if ra:
                    try:
                        retry_after = float(ra)
                    except ValueError:
                        retry_after = None
                if retry_after is None:
                    reset = headers_ci.get("x-ratelimit-reset")
                    if reset:
                        try:
                            import time

                            retry_after = max(0.0, float(reset) - time.time())
                        except ValueError:
                            retry_after = None

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

        created_at = (
            getattr(gh_pr, "created_at", None)
            or getattr(gh_pr, "merged_at", None)
            or getattr(gh_pr, "closed_at", None)
            or datetime.now(timezone.utc)
        )

        merged_at = getattr(gh_pr, "merged_at", None)
        closed_at = getattr(gh_pr, "closed_at", None)

        # Lazy load full PR for stats (additions/deletions)
        additions = getattr(gh_pr, "additions", 0)
        deletions = getattr(gh_pr, "deletions", 0)
        changed_files = getattr(gh_pr, "changed_files", 0)

        # Fetch and store reviews for this PR
        first_review_at = None
        reviews_count = 0
        changes_requested_count = 0
        try:
            reviews = connector.get_pull_request_reviews(owner, repo_name, gh_pr.number)
            reviews_count = len(reviews)
            if reviews:
                review_objects = []
                for r in reviews:
                    review_at = r.submitted_at or created_at
                    if first_review_at is None or review_at < first_review_at:
                        first_review_at = review_at
                    if r.state == "CHANGES_REQUESTED":
                        changes_requested_count += 1

                    review_objects.append(
                        GitPullRequestReview(
                            repo_id=repo_id,
                            number=gh_pr.number,
                            review_id=r.id,
                            reviewer=r.reviewer,
                            state=r.state,
                            submitted_at=review_at,
                        )
                    )
                asyncio.run_coroutine_threadsafe(
                    store.insert_git_pull_request_reviews(review_objects),
                    loop,
                ).result()
        except Exception as e:
            logging.debug(f"Failed to fetch reviews for PR #{gh_pr.number}: {e}")

        # Fetch first comment for "Pickup Time"
        first_comment_at = None
        comments_count = 0
        try:
            # Issue comments include top-level PR comments
            issue_comments = gh_pr.get_issue_comments()
            # This is a paginated list, we only need the first one's date but we want total count
            # To avoid fetching all comments if there are many, we can just get the first page for first_comment_at
            # and maybe the count is available on the issue object.
            # However, for simplicity and since we are already Doing one call per PR, let's just get the first one.
            comments_count = gh_pr.comments  # issue comments count
            if comments_count > 0:
                for c in issue_comments:
                    if first_comment_at is None or c.created_at < first_comment_at:
                        first_comment_at = c.created_at
                    # if we only want the first one, we can break if they are sorted,
                    # but they might not be.
        except Exception as e:
            logging.debug(f"Failed to fetch comments for PR #{gh_pr.number}: {e}")

        batch.append(
            GitPullRequest(
                repo_id=repo_id,
                number=int(getattr(gh_pr, "number", 0) or 0),
                title=getattr(gh_pr, "title", None),
                body=getattr(gh_pr, "body", None),
                state=getattr(gh_pr, "state", None),
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
                first_review_at=first_review_at,
                first_comment_at=first_comment_at,
                changes_requested_count=changes_requested_count,
                reviews_count=reviews_count,
                comments_count=comments_count,
            )
        )
        total += 1

        if len(batch) >= batch_size:
            asyncio.run_coroutine_threadsafe(
                store.insert_git_pull_requests(batch),
                loop,
            ).result()
            logging.debug(
                "Stored batch of %d PRs for %s/%s (total: %d)",
                len(batch),
                owner,
                repo_name,
                total,
            )
            batch.clear()

    if batch:
        asyncio.run_coroutine_threadsafe(
            store.insert_git_pull_requests(batch),
            loop,
        ).result()

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


def _split_full_name(full_name: str) -> Tuple[str, str]:
    parts = (full_name or "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid repo/project full name: {full_name}")
    return parts[0], parts[1]


async def _backfill_github_missing_data(
    store: Any,
    connector: GitHubConnector,
    db_repo: Repo,
    repo_full_name: str,
    default_branch: str,
    max_commits: Optional[int],
    blame_only: bool = False,
) -> None:
    # Logic matches the CLI sync orchestration.
    logging.info(
        "Backfilling data for %s...",
        repo_full_name,
    )
    owner, repo_name = _split_full_name(repo_full_name)

    if not (
        hasattr(store, "has_any_git_files")
        and hasattr(store, "has_any_git_blame")
        and hasattr(store, "has_any_git_commit_stats")
    ):
        return

    needs_files = not await store.has_any_git_files(db_repo.id)
    needs_commit_stats = (
        False if blame_only else not await store.has_any_git_commit_stats(db_repo.id)
    )
    needs_blame = not await store.has_any_git_blame(db_repo.id)

    if not (needs_files or needs_commit_stats or needs_blame):
        return

    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")

    file_paths: List[str] = []
    if needs_files or needs_blame:
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

            if needs_files and file_paths:
                batch: List[GitFile] = []
                for path in file_paths:
                    batch.append(
                        GitFile(
                            repo_id=db_repo.id,
                            path=path,
                            executable=False,
                            contents=None,
                        )
                    )
                    if len(batch) >= BATCH_SIZE:
                        await store.insert_git_file_data(batch)
                        batch.clear()
                if batch:
                    await store.insert_git_file_data(batch)
                logging.info(
                    "Backfilled %d files for %s",
                    len(file_paths),
                    repo_full_name,
                )
        except Exception as e:
            logging.warning(
                f"Failed to backfill GitHub files for {repo_full_name}: {e}"
            )

    if needs_commit_stats:
        try:
            logging.info(
                "Backfilling commit stats for %s...",
                repo_full_name,
            )
            commits_iter = gh_repo.get_commits()
            commit_stats_batch: List[GitCommitStat] = []
            commit_count = 0
            for commit in commits_iter:
                if max_commits and commit_count >= max_commits:
                    break
                commit_count += 1
                try:
                    detailed = gh_repo.get_commit(commit.sha)
                    for file in getattr(detailed, "files", []) or []:
                        commit_stats_batch.append(
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
                        if len(commit_stats_batch) >= BATCH_SIZE:
                            await store.insert_git_commit_stats(commit_stats_batch)
                            logging.debug(
                                "Stored batch of %d commit stats for %s",
                                len(commit_stats_batch),
                                repo_full_name,
                            )
                            commit_stats_batch.clear()
                except Exception as e:
                    logging.debug(
                        "Failed commit stat fetch for %s@%s: %s",
                        repo_full_name,
                        commit.sha,
                        e,
                    )

            if commit_stats_batch:
                await store.insert_git_commit_stats(commit_stats_batch)
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

    if needs_blame and file_paths:
        try:
            logging.info(
                "Backfilling blame for %d files in %s...",
                len(file_paths),
                repo_full_name,
            )
            blame_batch: List[GitBlame] = []
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
                        blame_batch.append(
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
                        if len(blame_batch) >= BATCH_SIZE:
                            await store.insert_blame_data(blame_batch)
                            logging.debug(
                                "Stored batch of %d blame entries for %s",
                                len(blame_batch),
                                repo_full_name,
                            )
                            blame_batch.clear()

            if blame_batch:
                await store.insert_blame_data(blame_batch)
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
    store: Any,
    owner: str,
    repo_name: str,
    token: str,
    fetch_blame: bool = False,
    blame_only: bool = False,
    max_commits: Optional[int] = None,
    sync_git: bool = True,
    sync_prs: bool = True,
    sync_cicd: bool = True,
    sync_deployments: bool = True,
    sync_incidents: bool = True,
    since: Optional[datetime] = None,
) -> None:
    """
    Process a GitHub repository using the GitHub connector.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError("Connectors unavailable. Install required dependencies.")

    logging.info(f"Processing GitHub repository: {owner}/{repo_name}")
    loop = asyncio.get_running_loop()

    connector = GitHubConnector(token=token)
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

        await store.insert_repo(db_repo)
        logging.info(f"Repository stored: {db_repo.repo} ({db_repo.id})")

        if blame_only:
            await _backfill_github_missing_data(
                store=store,
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
                await store.insert_git_commit_data(commit_objects)
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
                await store.insert_git_commit_stats(stats_objects)
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
                store,
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
                await store.insert_ci_pipeline_runs(pipeline_runs)
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
                await store.insert_deployments(deployments)
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
                await store.insert_incidents(incidents)
                logging.info("Stored %d incidents", len(incidents))

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
    token: str,
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
    blame_only: bool = False,
    backfill_missing: bool = True,
    since: Optional[datetime] = None,
) -> None:
    """
    Process multiple GitHub repositories using batch processing with
    pattern matching.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError("Connectors unavailable. Install required dependencies.")

    logging.info("=== GitHub Batch Repository Processing ===")
    connector = GitHubConnector(token=token)
    loop = asyncio.get_running_loop()

    pr_gate = None
    pr_semaphore = None
    if sync_prs:
        pr_gate = RateLimitGate(
            RateLimitConfig(initial_backoff_seconds=max(1.0, rate_limit_delay))
        )
        pr_semaphore = asyncio.Semaphore(max(1, max_concurrent))

    # Track results for summary and incremental storage
    all_results: List[BatchResult] = []
    stored_count = 0

    results_queue: Optional[asyncio.Queue] = None
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

        await store.insert_repo(db_repo)
        stored_count += 1
        logging.debug(f"Stored repository ({stored_count}): {db_repo.repo}")

        if blame_only:
            try:
                await _backfill_github_missing_data(
                    store=store,
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
                    await store.insert_git_commit_data(commit_objects)

                stats_objects = await loop.run_in_executor(
                    None,
                    _fetch_github_commit_stats_sync,
                    raw_commits,
                    db_repo.id,
                    50 if commit_limit is None else min(commit_limit, 50),
                    since,
                )
                if stats_objects:
                    await store.insert_git_commit_stats(stats_objects)
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
                        store,
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
                    await store.insert_ci_pipeline_runs(pipeline_runs)
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
                    await store.insert_deployments(deployments)
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
                    await store.insert_incidents(incidents)
            except Exception as e:
                logging.warning(
                    "Failed to fetch incidents for GitHub repo %s: %s",
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
            await store.insert_git_commit_stats([stat])

        if backfill_missing:
            try:
                await _backfill_github_missing_data(
                    store=store,
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
