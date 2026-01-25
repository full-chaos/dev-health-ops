import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional

from dev_health_ops.models.git import (
    GitCommit,
    GitCommitStat,
    Repo,
    GitPullRequest,
    GitBlame,
    GitFile,
    CiPipelineRun,
    Deployment,
    Incident,
)
from dev_health_ops.utils import (
    AGGREGATE_STATS_MARKER,
    is_skippable,
    CONNECTORS_AVAILABLE,
    BATCH_SIZE,
)

if CONNECTORS_AVAILABLE:
    from dev_health_ops.connectors import (
        BatchResult,
        GitLabConnector,
        ConnectorException,
    )
    from dev_health_ops.connectors.models import Repository
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate
else:
    BatchResult = None  # type: ignore
    GitLabConnector = None  # type: ignore
    ConnectorException = Exception
    Repository = None  # type: ignore
    RateLimitConfig = None  # type: ignore
    RateLimitGate = None  # type: ignore


# --- GitLab Sync Helpers ---


def _fetch_gitlab_project_info_sync(connector, project_id):
    """Sync helper to fetch GitLab project info."""
    gl_project = connector.gitlab.projects.get(project_id)
    # Access properties to force load if lazy
    _ = gl_project.name
    return gl_project


def _fetch_gitlab_commits_sync(
    gl_project,
    max_commits: Optional[int],
    repo_id,
    since: Optional[datetime] = None,
):
    """Sync helper to fetch GitLab commits."""
    list_params = {"per_page": 100, "get_all": False}
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
                    committed_when = datetime.fromisoformat(
                        commit.committed_date.replace("Z", "+00:00")
                    )
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
                    datetime.fromisoformat(commit.authored_date.replace("Z", "+00:00"))
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
                    datetime.fromisoformat(commit.committed_date.replace("Z", "+00:00"))
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


def _fetch_gitlab_commit_stats_sync(gl_project, commit_hashes, repo_id, max_stats):
    """Sync helper to fetch detailed commit stats from GitLab."""
    stats_objects = []

    for commit_hash in commit_hashes[:max_stats]:
        try:
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
        created_at = (
            mr.created_at or mr.merged_at or mr.closed_at or datetime.now(timezone.utc)
        )
        git_pr = GitPullRequest(
            repo_id=repo_id,
            number=mr.number,
            title=mr.title,
            body=mr.body,
            state=mr.state,
            author_name=mr.author.username if mr.author else "Unknown",
            author_email=None,
            created_at=created_at,
            merged_at=mr.merged_at,
            closed_at=mr.closed_at,
            head_branch=mr.head_branch,
            base_branch=mr.base_branch,
        )
        pr_objects.append(git_pr)
    logging.info(
        "Fetched %d merge requests for project %d",
        len(pr_objects),
        project_id,
    )
    return pr_objects


def _sync_gitlab_mrs_to_store(
    connector,
    project_id: int,
    repo_id,
    store,
    loop: asyncio.AbstractEventLoop,
    batch_size: int,
    state: str = "all",
    gate: Optional[RateLimitGate] = None,
    since: Optional[datetime] = None,
) -> int:
    """Fetch all MRs for a project and insert them in batches.

    Runs in a worker thread; uses run_coroutine_threadsafe to write batches.
    """
    logging.info(
        "Fetching merge requests for project %d...",
        project_id,
    )
    batch: List[GitPullRequest] = []
    total = 0
    page = 1

    if gate is None:
        gate = RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

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

            def _parse_dt(value):
                if not value:
                    return None
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except Exception:
                    return None

            created_at = _parse_dt(mr.get("created_at"))
            updated_at = _parse_dt(mr.get("updated_at"))
            merged_at = _parse_dt(mr.get("merged_at"))
            closed_at = _parse_dt(mr.get("closed_at"))
            created_at = (
                created_at or merged_at or closed_at or datetime.now(timezone.utc)
            )

            comments_count = int(mr.get("user_notes_count") or 0)

            if (
                since is not None
                and isinstance(updated_at, datetime)
                and updated_at.astimezone(timezone.utc) < since
            ):
                mrs = []
                break

            batch.append(
                GitPullRequest(
                    repo_id=repo_id,
                    number=int(mr.get("iid") or 0),
                    title=mr.get("title") or None,
                    body=mr.get("description"),
                    state=mr.get("state") or None,
                    author_name=author_name,
                    author_email=author_email,
                    created_at=created_at,
                    merged_at=merged_at,
                    closed_at=closed_at,
                    head_branch=mr.get("source_branch"),
                    base_branch=mr.get("target_branch"),
                    changes_requested_count=0,
                    reviews_count=0,
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
                    "Stored batch of %d MRs for project %d (total: %d)",
                    len(batch),
                    project_id,
                    total,
                )
                batch.clear()

        page += 1
        if not mrs:
            break

    if batch:
        asyncio.run_coroutine_threadsafe(
            store.insert_git_pull_requests(batch),
            loop,
        ).result()

    logging.info(
        "Fetched %d merge requests for project %d",
        total,
        project_id,
    )
    return total


def _fetch_gitlab_pipelines_sync(gl_project, repo_id, max_pipelines, since):
    """Sync helper to fetch GitLab CI/CD pipelines."""
    pipelines = []

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

        created_at = _coerce_datetime(getattr(pipeline, "created_at", None))

        if created_at is None:
            continue

        if since is not None and created_at.astimezone(timezone.utc) < since:
            break

        started_at = created_at
        started_at = (
            _coerce_datetime(getattr(pipeline, "started_at", None)) or created_at
        )

        finished_at = _coerce_datetime(getattr(pipeline, "finished_at", None))

        pipelines.append(
            CiPipelineRun(
                repo_id=repo_id,
                run_id=str(getattr(pipeline, "id", "")),
                status=getattr(pipeline, "status", None),
                queued_at=created_at,
                started_at=started_at,
                finished_at=finished_at,
            )
        )
        count += 1

    return pipelines


def _fetch_gitlab_deployments_sync(
    connector, project_id, repo_id, max_deployments, since
):
    """Sync helper to fetch GitLab deployments."""
    deployments = []
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

        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except Exception:
            continue

        if since is not None and created_at.astimezone(timezone.utc) < since:
            break

        # Parse other timestamps if available
        finished_at = None
        finished_at_str = dep.get("finished_at")
        if finished_at_str:
            try:
                finished_at = datetime.fromisoformat(
                    finished_at_str.replace("Z", "+00:00")
                )
            except Exception:
                pass

        deployments.append(
            Deployment(
                repo_id=repo_id,
                deployment_id=str(dep.get("id", "")),
                status=dep.get("status", None),
                environment=dep.get("environment", {}).get("name")
                if isinstance(dep.get("environment"), dict)
                else None,
                started_at=created_at,
                finished_at=finished_at,
                deployed_at=created_at,
                merged_at=None,
                pull_request_number=None,
            )
        )

    return deployments


def _fetch_gitlab_incidents_sync(connector, project_id, repo_id, max_issues, since):
    """Sync helper to fetch GitLab incidents (issues labeled 'incident')."""
    incidents = []
    try:
        raw_issues = connector.rest_client.get_issues(
            project_id=project_id,
            labels="incident",
            per_page=min(max_issues, 100),
            order_by="updated_at",
            sort="desc",
        )
    except Exception as exc:
        logging.debug("Failed to fetch incident issues: %s", exc)
        return incidents

    for issue in raw_issues[:max_issues]:
        created_at_str = issue.get("created_at")
        if not created_at_str:
            continue

        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except Exception:
            continue

        if since is not None and created_at.astimezone(timezone.utc) < since:
            break

        resolved_at = None
        closed_at_str = issue.get("closed_at")
        if closed_at_str:
            try:
                resolved_at = datetime.fromisoformat(
                    closed_at_str.replace("Z", "+00:00")
                )
            except Exception:
                pass

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


def _iter_gitlab_repo_tree(
    gl_project,
    *,
    ref: str,
    per_page: int = 100,
    limit: Optional[int] = None,
) -> Iterable[Any]:
    page = 1
    seen = 0

    while True:
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
        if not page_items:
            break
        seen += len(page_items)
        logging.debug(
            "GitLab repo tree page %d returned %d items (total: %d)",
            page,
            len(page_items),
            seen,
        )
        for item in page_items:
            yield item
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


async def _backfill_gitlab_missing_data(
    store: Any,
    connector: GitLabConnector,
    db_repo: Repo,
    project_full_name: str,
    default_branch: str,
    max_commits: Optional[int],
    blame_only: bool = False,
) -> None:
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

    try:
        project = connector.gitlab.projects.get(project_full_name)
    except Exception as e:
        logging.warning(f"Failed to load GitLab project {project_full_name}: {e}")
        return

    file_paths: List[str] = []
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

    if needs_commit_stats:
        try:
            commits = project.commits.list(
                ref_name=default_branch, per_page=100, get_all=True
            )
        except TypeError:
            commits = project.commits.list(
                ref_name=default_branch, per_page=100, all=True
            )

        commit_stats_batch: List[GitCommitStat] = []
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
                commit_stats_batch.append(
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
                if len(commit_stats_batch) >= BATCH_SIZE:
                    await store.insert_git_commit_stats(commit_stats_batch)
                    commit_stats_batch.clear()
            except Exception as e:
                logging.debug(
                    f"Failed commit stat fetch for {project_full_name}@{sha}: {e}"
                )

        if commit_stats_batch:
            await store.insert_git_commit_stats(commit_stats_batch)

    if needs_blame and file_paths:
        blame_batch: List[GitBlame] = []
        for path in file_paths:
            try:
                blame_items = connector.rest_client.get_file_blame(
                    project.id,
                    path,
                    default_branch,
                )
            except Exception as e:
                logging.debug(f"Failed blame fetch for {project_full_name}:{path}: {e}")
                continue

            line_no = 1
            for item in blame_items or []:
                commit = item.get("commit", {})
                for line in item.get("lines", []) or []:
                    blame_batch.append(
                        GitBlame(
                            repo_id=db_repo.id,
                            path=path,
                            line_no=line_no,
                            author_email=commit.get("author_email"),
                            author_name=commit.get("author_name"),
                            author_when=None,
                            commit_hash=commit.get("id"),
                            line=line.rstrip("\n") if isinstance(line, str) else None,
                        )
                    )
                    line_no += 1
                    if len(blame_batch) >= BATCH_SIZE:
                        await store.insert_blame_data(blame_batch)
                        blame_batch.clear()

        if blame_batch:
            await store.insert_blame_data(blame_batch)


async def process_gitlab_project(
    store: Any,
    project_id: int,
    token: str,
    gitlab_url: str,
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
    Process a GitLab project using the GitLab connector.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info(f"Processing GitLab project: {project_id}")
    loop = asyncio.get_running_loop()

    connector = GitLabConnector(url=gitlab_url, private_token=token)
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

        await store.insert_repo(db_repo)
        logging.info(f"Project stored: {db_repo.repo} ({db_repo.id})")

        if blame_only:
            await _backfill_gitlab_missing_data(
                store=store,
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
                await store.insert_git_commit_data(commit_objects)
                logging.info(f"Stored {len(commit_objects)} commits from GitLab")

            # 3. Fetch Stats
            logging.info("Fetching commit stats from GitLab...")
            stats_limit = 50 if max_commits is None else min(max_commits, 50)
            stats_objects = await loop.run_in_executor(
                None,
                _fetch_gitlab_commit_stats_sync,
                gl_project,
                commit_hashes,
                db_repo.id,
                stats_limit,
            )

            if stats_objects:
                await store.insert_git_commit_stats(stats_objects)
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
                store,
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
                await store.insert_ci_pipeline_runs(pipeline_runs)
                logging.info(f"Stored {len(pipeline_runs)} pipeline runs from GitLab")

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
                await store.insert_deployments(deployments)
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
                await store.insert_incidents(incidents)
                logging.info(f"Stored {len(incidents)} incidents from GitLab")

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
                await store.insert_blame_data(blame_batch)
                logging.info(f"Stored {len(blame_batch)} blame records from GitLab")

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
    group_name: str = None,
    pattern: str = None,
    batch_size: int = 10,
    max_concurrent: int = 4,
    rate_limit_delay: float = 1.0,
    max_commits_per_project: int = None,
    max_projects: int = None,
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
    Process multiple GitLab projects using batch processing with pattern matching.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info("=== GitLab Batch Project Processing ===")
    connector = GitLabConnector(url=gitlab_url, private_token=token)
    loop = asyncio.get_running_loop()

    mr_gate = None
    mr_semaphore = None
    if sync_prs:
        mr_gate = RateLimitGate(
            RateLimitConfig(initial_backoff_seconds=max(1.0, rate_limit_delay))
        )
        mr_semaphore = asyncio.Semaphore(max(1, max_concurrent))

    all_results: List[BatchResult] = []
    stored_count = 0

    results_queue: Optional[asyncio.Queue] = None
    _queue_sentinel = object()

    async def store_result(result: BatchResult) -> None:
        """Store a single result in the database (upsert)."""
        nonlocal stored_count
        if not result.success:
            return

        project_info = result.repository
        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=project_info.full_name,
            settings={
                "source": "gitlab",
                "project_id": project_info.id,
                "url": project_info.url,
                "default_branch": project_info.default_branch,
                "batch_processed": True,
            },
            tags=["gitlab"],
        )

        await store.insert_repo(db_repo)
        stored_count += 1
        logging.debug(f"Stored project ({stored_count}): {db_repo.repo}")

        if blame_only:
            try:
                await _backfill_gitlab_missing_data(
                    store=store,
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
                commit_limit = max_commits_per_project
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
                    await store.insert_git_commit_data(commit_objects)

                stats_objects = await loop.run_in_executor(
                    None,
                    _fetch_gitlab_commit_stats_sync,
                    gl_project,
                    commit_hashes,
                    db_repo.id,
                    50 if commit_limit is None else min(commit_limit, 50),
                )
                if stats_objects:
                    await store.insert_git_commit_stats(stats_objects)
            except Exception as e:
                logging.warning(
                    "Failed to fetch commits for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

        if sync_prs:
            # Fetch ALL merge requests for batch-processed projects, storing in batches.
            try:
                async with mr_semaphore:
                    await loop.run_in_executor(
                        None,
                        _sync_gitlab_mrs_to_store,
                        connector,
                        project_info.id,
                        db_repo.id,
                        store,
                        loop,
                        BATCH_SIZE,
                        "all",
                        mr_gate,
                        since,
                    )
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
                    await store.insert_ci_pipeline_runs(pipeline_runs)
            except Exception as e:
                logging.warning(
                    "Failed to fetch CI/CD runs for GitLab project %s: %s",
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
                    await store.insert_deployments(deployments)
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
                    await store.insert_incidents(incidents)
            except Exception as e:
                logging.warning(
                    "Failed to fetch incidents for GitLab project %s: %s",
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
            await store.insert_git_commit_stats([stat])

        if backfill_missing:
            try:
                await _backfill_gitlab_missing_data(
                    store=store,
                    connector=connector,
                    db_repo=db_repo,
                    project_full_name=project_info.full_name,
                    default_branch=project_info.default_branch,
                    max_commits=max_commits_per_project,
                )
            except Exception as e:
                logging.debug(
                    "Backfill failed for GitLab project %s: %s",
                    project_info.full_name,
                    e,
                )

    def on_project_complete(result: BatchResult) -> None:
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
                    result = BatchResult(
                        repository=project_info,
                        stats=None,
                        success=True,
                    )
                    try:
                        await store_result(result)
                    except Exception as e:
                        result = BatchResult(
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

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in batch processing: {e}")
        raise
    finally:
        connector.close()
