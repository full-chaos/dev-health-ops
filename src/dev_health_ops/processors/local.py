import asyncio
import logging
import os
import re
import uuid
from typing import List, Optional, Tuple, Set, Dict, Any
from pathlib import Path
from datetime import datetime, timezone

from dev_health_ops.models.git import (
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
    GitPullRequest,
    Repo,
)
from dev_health_ops.utils import (
    BATCH_SIZE,
    MAX_WORKERS,
    collect_changed_files,
    iter_commits_since,
    _normalize_datetime,
)


_GITHUB_MERGE_PR_RE = re.compile(
    r"^Merge pull request #(?P<number>\d+)\b",
    re.MULTILINE,
)
_GITLAB_MERGE_MR_RE = re.compile(r"\bSee merge request\b.*!(?P<number>\d+)\b")


def _first_meaningful_title_line(message: str) -> Optional[str]:
    if not message:
        return None
    lines = [ln.strip() for ln in message.splitlines()]
    # Prefer the first non-empty line that is not a generic merge header.
    for ln in lines:
        if not ln:
            continue
        if ln.startswith("Merge pull request #"):
            continue
        if ln.startswith("Merge branch "):
            continue
        if ln.startswith("See merge request"):
            continue
        return ln
    return None


def infer_merged_pull_requests_from_commits(
    commits: List[Any],
    repo_id: uuid.UUID,
    since: Optional[datetime] = None,
) -> List[GitPullRequest]:
    """Infer merged PRs/MRs from merge commit messages.

    Supports common GitHub merge-commit messages ("Merge pull request #123") and
    common GitLab merge-commit trailers ("See merge request group/project!123").
    """
    inferred: Dict[int, GitPullRequest] = {}

    for commit in commits or []:
        try:
            commit_dt = _normalize_datetime(getattr(commit, "committed_datetime"))
            if since and commit_dt and commit_dt < since:
                continue

            message = getattr(commit, "message", "") or ""
            match = _GITHUB_MERGE_PR_RE.search(message)
            if not match:
                match = _GITLAB_MERGE_MR_RE.search(message)
            if not match:
                continue

            number = int(match.group("number"))
            title = _first_meaningful_title_line(message)

            author = getattr(commit, "author", None)
            author_name = getattr(author, "name", None)
            author_email = getattr(author, "email", None)

            # Use the merge commit timestamp as a safe, non-null created_at.
            created_at = commit_dt or datetime.now(timezone.utc)
            merged_at = commit_dt or datetime.now(timezone.utc)

            inferred[number] = GitPullRequest(
                repo_id=repo_id,
                number=number,
                title=title,
                state="merged",
                author_name=author_name,
                author_email=author_email,
                created_at=created_at,
                merged_at=merged_at,
                closed_at=None,
                head_branch=None,
                base_branch=None,
            )
        except Exception:
            continue

    return list(inferred.values())


def infer_open_pull_requests_from_refs(
    repo_obj: Any,
    repo_id: uuid.UUID,
) -> List[GitPullRequest]:
    """Infer open PRs/MRs from local refs, if present.

    Requires that the local clone has fetched provider-specific refs:
    - GitHub: refs/pull/<id>/head
    - GitLab: refs/merge-requests/<id>/head
    """
    inferred: Dict[int, GitPullRequest] = {}

    refs = getattr(repo_obj, "refs", None) or []
    for ref in refs:
        ref_path = getattr(ref, "path", None) or ""
        m = re.match(r"^refs/pull/(?P<number>\d+)/head$", ref_path)
        if not m:
            m = re.match(r"^refs/merge-requests/(?P<number>\d+)/head$", ref_path)
        if not m:
            continue

        try:
            number = int(m.group("number"))
            tip_commit = getattr(ref, "commit", None)
            if tip_commit is None and hasattr(repo_obj, "commit"):
                tip_commit = repo_obj.commit(ref)

            commit_dt = None
            if tip_commit is not None:
                commit_dt = _normalize_datetime(
                    getattr(tip_commit, "committed_datetime", None)
                )

            inferred[number] = GitPullRequest(
                repo_id=repo_id,
                number=number,
                title=None,
                state="open",
                author_name=None,
                author_email=None,
                created_at=commit_dt or datetime.now(timezone.utc),
                merged_at=None,
                closed_at=None,
                head_branch=ref_path,
                base_branch=None,
            )
        except Exception:
            continue

    return list(inferred.values())


async def process_local_pull_requests(
    repo: Repo,
    store: Any,
    repo_obj: Any,
    commits: Optional[List[Any]] = None,
    since: Optional[datetime] = None,
) -> None:
    """Collect PR/MR-like records for local repos and store them.

    - Always tries merge-commit inference (merged PRs/MRs)
    - Also tries ref-based inference if refs are present
    """
    logging.info("Processing local pull/merge requests...")

    commit_list = commits or []
    merged = infer_merged_pull_requests_from_commits(
        commit_list,
        repo.id,
        since=since,
    )
    open_refs = infer_open_pull_requests_from_refs(repo_obj, repo.id)

    pr_objects: List[GitPullRequest] = []
    pr_objects.extend(open_refs)
    pr_objects.extend(merged)

    if pr_objects:
        await store.insert_git_pull_requests(pr_objects)
        logging.info(f"Inserted/updated {len(pr_objects)} local PR/MR records")


# We need access to the store, but we pass it as an argument usually.
# The 'Repo' type hint comes from dev_health_ops.models.git
# We need to import DataStore type but avoiding circular deps might be tricky if we enforce strict types.
# For now, we will use 'Any' or specific imports if available.


def _extract_commit_info(
    commit: Any, repo_id: uuid.UUID
) -> Tuple[Optional[GitCommit], Optional[str]]:
    """Helper to safely extract commit info in a thread."""
    try:
        git_commit = GitCommit(
            repo_id=repo_id,
            hash=commit.hexsha,
            message=commit.message,
            author_name=commit.author.name,
            author_email=commit.author.email,
            author_when=_normalize_datetime(commit.committed_datetime),
            committer_name=commit.committer.name,
            committer_email=commit.committer.email,
            committer_when=_normalize_datetime(commit.committed_datetime),
            parents=len(commit.parents),
        )
        return git_commit, None
    except Exception as e:
        return None, str(e)


def _compute_commit_stats_sync(commit: Any, repo_id: uuid.UUID) -> List[GitCommitStat]:
    """Helper to compute commit stats (git diff) in a thread."""
    stats: List[GitCommitStat] = []
    commit_stats: Dict[str, Dict[str, int]] = {}

    try:
        stats_attr = getattr(commit, "stats", None)
        if stats_attr and hasattr(stats_attr, "files"):
            commit_stats = stats_attr.files or {}
    except Exception:
        commit_stats = {}

    try:
        if commit.parents:
            diffs = commit.parents[0].diff(commit, create_patch=False)
        else:
            diffs = commit.diff(None, create_patch=False)
    except Exception:
        # Diff failed (e.g. bad object)
        return []

    for diff in diffs:
        file_path = diff.b_path if diff.b_path else diff.a_path
        if not file_path:
            continue

        file_stat = commit_stats.get(file_path, {})
        old_mode = str(diff.a_mode) if diff.a_mode else "000000"
        new_mode = str(diff.b_mode) if diff.b_mode else "000000"
        additions = file_stat.get("insertions", 0) if isinstance(file_stat, dict) else 0
        deletions = file_stat.get("deletions", 0) if isinstance(file_stat, dict) else 0

        stats.append(
            GitCommitStat(
                repo_id=repo_id,
                commit_hash=commit.hexsha,
                file_path=file_path,
                additions=additions,
                deletions=deletions,
                old_file_mode=old_mode,
                new_file_mode=new_mode,
            )
        )
    return stats


async def process_git_commits(
    repo: Repo,
    store: Any,  # DataStore
    commits: Optional[Any] = None,  # Iterable
    since: Optional[datetime] = None,
) -> None:
    """
    Process and insert GitCommit data into the database.
    """
    logging.info("Processing git commits...")
    commit_batch: List[GitCommit] = []
    loop = asyncio.get_running_loop()

    # If commits is None, we need to manually iterate using iter_commits
    # But iter_commits_since is also in shared utils now? No, it takes 'repo' object.
    # We'll expect the caller to pass the iterable or handle initialization.
    # For now, if commits is None, we rely on the caller or fail?
    # In main(), it's passed.

    if commits is None:
        # We need to recreate the iterator logic if not provided
        # This requires the gitpython Repo object, not our model Repo.
        # The 'repo' arg here is our model Repo.
        # This signature is slightly confusing in the original code.
        # The original 'process_git_commits' took (repo: Repo, ...).
        # But inside it called `commit.hexsha`.
        # So 'commits' MUST be the iterator of GitPython objects.
        logging.warning("No commits iterable provided to process_git_commits")
        return

    try:
        commit_count = 0
        for commit in commits:
            commit_dt = _normalize_datetime(commit.committed_datetime)
            if since and commit_dt < since:
                continue

            # Run extraction in thread
            git_commit, error = await loop.run_in_executor(
                None, _extract_commit_info, commit, repo.id
            )

            if error:
                logging.warning(f"Skipping commit {commit.hexsha}: {error}")
                continue

            if git_commit:
                commit_batch.append(git_commit)
                commit_count += 1

            if len(commit_batch) >= BATCH_SIZE:
                await store.insert_git_commit_data(commit_batch)
                logging.info(f"Inserted {len(commit_batch)} commits")
                commit_batch.clear()

        # Insert remaining
        if commit_batch:
            await store.insert_git_commit_data(commit_batch)
            logging.info(f"Inserted final {len(commit_batch)} commits")

    except Exception as e:
        logging.error(f"Error processing commits: {e}")


async def process_git_commit_stats(
    repo: Repo,
    store: Any,
    commits: Optional[Any] = None,
    since: Optional[datetime] = None,
) -> None:
    """
    Process and insert GitCommitStat data into the database.
    """
    logging.info("Processing git commit stats...")
    commit_stats_batch: List[GitCommitStat] = []
    loop = asyncio.get_running_loop()

    if commits is None:
        logging.warning("No commits iterable provided to process_git_commit_stats")
        return

    try:
        commit_count = 0
        for commit in commits:
            commit_dt = _normalize_datetime(commit.committed_datetime)
            if since and commit_dt < since:
                continue

            # Run blocking git diff in executor
            stats = await loop.run_in_executor(
                None, _compute_commit_stats_sync, commit, repo.id
            )

            if stats:
                commit_stats_batch.extend(stats)

            commit_count += 1

            if len(commit_stats_batch) >= BATCH_SIZE:
                await store.insert_git_commit_stats(commit_stats_batch)
                logging.info(
                    f"Inserted {len(commit_stats_batch)} commit stats ({commit_count} commits)"
                )
                commit_stats_batch.clear()

        # Insert remaining stats
        if commit_stats_batch:
            await store.insert_git_commit_stats(commit_stats_batch)
            logging.info(
                f"Inserted final {len(commit_stats_batch)} commit stats ({commit_count} commits)"
            )
    except Exception as e:
        logging.error(f"Error processing commit stats: {e}")


def _process_file_and_blame_sync(
    filepath: Path, repo_id: uuid.UUID, repo_root: str, do_blame: bool
) -> Tuple[Optional[GitFile], List[GitBlame], Optional[str]]:
    """
    Helper to process a single file for both content and blame safely in a thread.
    """
    try:
        rel_path = os.path.relpath(filepath, repo_root)

        # 1. Process File Content
        is_executable = os.access(filepath, os.X_OK)
        contents = None
        try:
            if os.path.getsize(filepath) < 1_000_000:  # Skip files > 1MB
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    contents = f.read()
        except Exception:
            pass  # Content read failed, but we still proceed

        git_file = GitFile(
            repo_id=repo_id,
            path=rel_path,
            executable=is_executable,
            contents=contents,
        )

        # 2. Process Blame (if requested)
        blame_results = []
        if do_blame:
            # Optimization: We already have repo_root.
            # GitBlame.fetch_blame is static and handles logic
            blame_data = GitBlame.fetch_blame(repo_root, filepath, repo_id, repo=None)
            blame_results = [
                GitBlame(
                    repo_id=row[0],
                    author_email=row[1],
                    author_name=row[2],
                    author_when=row[3],
                    commit_hash=row[4],
                    line_no=row[5],
                    line=row[6],
                    path=row[7],
                )
                for row in blame_data
            ]

        return git_file, blame_results, None

    except Exception as e:
        return None, [], str(e)


async def process_files_and_blame(
    repo: Repo,
    all_files: List[Path],
    files_for_blame: Set[Path],
    store: Any,
    repo_root_path: str,  # Passed explicitly now
) -> None:
    """
    Process files for both content and blame in a single pass.
    """
    logging.info(
        f"Processing {len(all_files)} git files (blame for {len(files_for_blame)})..."
    )

    file_batch: List[GitFile] = []
    blame_batch: List[GitBlame] = []
    failed_files: List[Tuple[Path, str]] = []

    loop = asyncio.get_running_loop()
    concurrency = MAX_WORKERS
    semaphore = asyncio.Semaphore(concurrency)

    async def _worker(filepath: Path):
        async with semaphore:
            do_blame = filepath in files_for_blame
            return await loop.run_in_executor(
                None,
                _process_file_and_blame_sync,
                filepath,
                repo.id,
                repo_root_path,
                do_blame,
            )

    # Process results in chunks
    chunk_size = BATCH_SIZE * 2

    # Import tqdm for progress bar
    try:
        from tqdm import tqdm
        import sys

        use_tqdm = True
    except ImportError:
        use_tqdm = False

    blame_count = len(files_for_blame)

    # Create progress bar if tqdm is available - use stderr and force refresh
    if use_tqdm and blame_count > 0:
        pbar = tqdm(
            total=len(all_files),
            desc=f"Processing files ({blame_count} with blame)",
            unit="file",
            file=sys.stderr,
            mininterval=0.1,
            dynamic_ncols=True,
        )
    elif use_tqdm:
        pbar = tqdm(
            total=len(all_files),
            desc="Processing files",
            unit="file",
            file=sys.stderr,
            mininterval=0.1,
            dynamic_ncols=True,
        )
    else:
        pbar = None

    for i in range(0, len(all_files), chunk_size):
        chunk_files = all_files[i : i + chunk_size]
        chunk_tasks = [_worker(fp) for fp in chunk_files]

        results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

        for idx, result in enumerate(results):
            original_file = chunk_files[idx]

            if isinstance(result, Exception):
                logging.debug(f"Task failed for {original_file}: {result}")
                failed_files.append((original_file, str(result)))
                continue

            git_file, blame_rows, error = result

            if error:
                logging.debug(f"Error processing {original_file}: {error}")
                failed_files.append((original_file, error))
                continue

            if git_file:
                file_batch.append(git_file)

            if blame_rows:
                blame_batch.extend(blame_rows)

        # Update progress bar
        if pbar:
            pbar.update(len(chunk_files))

        # Flush batches
        if len(file_batch) >= BATCH_SIZE:
            await store.insert_git_file_data(file_batch)
            file_batch.clear()

        if len(blame_batch) >= BATCH_SIZE:
            await store.insert_blame_data(blame_batch)
            blame_batch.clear()

    # Close progress bar
    if pbar:
        pbar.close()

    # Flush remaining
    if file_batch:
        await store.insert_git_file_data(file_batch)
        logging.info(f"Inserted final {len(file_batch)} git files")

    if blame_batch:
        await store.insert_blame_data(blame_batch)
        logging.info(f"Inserted final {len(blame_batch)} git blame lines")

    if failed_files:
        logging.warning(f"Failed to process {len(failed_files)} files")


async def process_local_repo(
    store: Any,
    repo_path: str,
    since: Optional[datetime] = None,
    fetch_blame: bool = False,
    sync_git: bool = True,
    sync_prs: bool = True,
    sync_blame: bool = True,
) -> None:
    """
    Orchestrate the local repository sync pipeline.

    Use the sync_* flags to control which stages run.
    """
    repo_root = Path(repo_path).resolve()
    logging.info("Processing local repository at %s", repo_root)
    if not (repo_root / ".git").exists():
        logging.error("No git repository found at %s", repo_root)
        return

    repo_name = repo_root.name
    repo = Repo(repo_path=str(repo_root), repo=repo_name)
    await store.insert_repo(repo)
    logging.info("Repository stored: %s (%s)", repo.repo, repo.id)

    from git import Repo as GitPythonRepo

    repo_obj = GitPythonRepo(str(repo_root))
    commits_iter = list(iter_commits_since(repo_obj, since))

    if sync_git:
        await process_git_commits(repo, store, commits_iter, since)

    if sync_prs:
        await process_local_pull_requests(
            repo=repo,
            store=store,
            repo_obj=repo_obj,
            commits=commits_iter,
            since=since,
        )

    if sync_git:
        commits_for_stats = list(iter_commits_since(repo_obj, since))
        await process_git_commit_stats(repo, store, commits_for_stats, since)

    if sync_blame or fetch_blame:
        files_for_blame = set()
        if fetch_blame:
            files_for_blame = set(collect_changed_files(repo_root, commits_iter))

        all_files_path = []
        for root, _, files in os.walk(str(repo_root)):
            root_path = Path(root)
            if ".git" in root_path.parts:
                continue
            for file in files:
                file_path = (root_path / file).resolve()
                all_files_path.append(file_path)

        files_for_blame_path = {Path(p).resolve() for p in files_for_blame}

        await process_files_and_blame(
            repo,
            all_files_path,
            files_for_blame_path,
            store,
            str(repo_root),
        )

    logging.info("Local repository processing complete.")


async def process_local_blame(
    store: Any,
    repo_path: str,
    since: Optional[datetime] = None,
) -> None:
    """
    Backfill git blame data without syncing commits or PRs.
    """
    repo_root = Path(repo_path).resolve()
    logging.info("Processing local blame at %s", repo_root)
    if not (repo_root / ".git").exists():
        logging.error("No git repository found at %s", repo_root)
        return

    repo_name = repo_root.name
    repo = Repo(repo_path=str(repo_root), repo=repo_name)
    await store.insert_repo(repo)
    logging.info("Repository stored: %s (%s)", repo.repo, repo.id)

    from git import Repo as GitPythonRepo

    repo_obj = GitPythonRepo(str(repo_root))
    commits_iter = list(iter_commits_since(repo_obj, since))
    files_for_blame = (
        set(collect_changed_files(repo_root, commits_iter)) if commits_iter else set()
    )

    all_files_path = []
    for root, _, files in os.walk(str(repo_root)):
        root_path = Path(root)
        if ".git" in root_path.parts:
            continue
        for file in files:
            file_path = (root_path / file).resolve()
            all_files_path.append(file_path)

    if not files_for_blame:
        files_for_blame_path = set(all_files_path)
    else:
        files_for_blame_path = {Path(p).resolve() for p in files_for_blame}

    await process_files_and_blame(
        repo,
        all_files_path,
        files_for_blame_path,
        store,
        str(repo_root),
    )

    logging.info("Local blame sync complete.")
