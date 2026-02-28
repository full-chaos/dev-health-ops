"""BaseGitProcessor — shared orchestration logic for GitHub and GitLab processors.

Both processors follow the same backfill-needs-check pattern:

    1. Ask the store whether file/stat/blame data already exists.
    2. If any gap is found, fetch the missing data from the provider.
    3. Write results via AsyncBatchCollector.

This module extracts the common pieces into a reusable base so that github.py
and gitlab.py only contain provider-specific fetch logic.
"""

from __future__ import annotations

import logging
from typing import Any, List

from dev_health_ops.models.git import GitBlame, GitCommitStat, GitFile
from dev_health_ops.processors.fetch_utils import AsyncBatchCollector

logger = logging.getLogger(__name__)


class BackfillNeeds:
    """Result of the backfill gap-check against a store."""

    __slots__ = ("files", "commit_stats", "blame")

    def __init__(
        self,
        *,
        files: bool,
        commit_stats: bool,
        blame: bool,
    ) -> None:
        self.files = files
        self.commit_stats = commit_stats
        self.blame = blame

    @property
    def any(self) -> bool:
        return self.files or self.commit_stats or self.blame

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"BackfillNeeds(files={self.files}, "
            f"commit_stats={self.commit_stats}, blame={self.blame})"
        )


async def check_backfill_needs(
    store: Any,
    repo_id: Any,
    blame_only: bool = False,
) -> BackfillNeeds:
    """Query the store to determine which backfill gaps exist.

    Args:
        store: A GitSyncStore-compatible instance.
        repo_id: The repository identifier.
        blame_only: When True, skip commit stats (blame-only sync).

    Returns:
        BackfillNeeds describing which data is missing.
    """
    # Guard: stores that do not implement the check methods skip backfill.
    if not (
        hasattr(store, "has_any_git_files")
        and hasattr(store, "has_any_git_blame")
        and hasattr(store, "has_any_git_commit_stats")
    ):
        return BackfillNeeds(files=False, commit_stats=False, blame=False)

    needs_files = not await store.has_any_git_files(repo_id)
    needs_commit_stats = (
        False if blame_only else not await store.has_any_git_commit_stats(repo_id)
    )
    needs_blame = not await store.has_any_git_blame(repo_id)

    return BackfillNeeds(
        files=needs_files,
        commit_stats=needs_commit_stats,
        blame=needs_blame,
    )


async def backfill_file_records(
    store: Any,
    repo_id: Any,
    file_paths: List[str],
    repo_full_name: str,
) -> None:
    """Persist file path records from a pre-fetched list.

    Shared between GitHub and GitLab backfill paths. Provider-specific file
    listing happens upstream; this function only handles the write path.

    Args:
        store: A GitSyncStore-compatible instance.
        repo_id: The repository identifier.
        file_paths: List of repository-relative file paths.
        repo_full_name: Human-readable name used in log messages.
    """
    if not file_paths:
        return

    async with AsyncBatchCollector(store.insert_git_file_data) as collector:
        for path in file_paths:
            collector.add(
                GitFile(
                    repo_id=repo_id,
                    path=path,
                    executable=False,
                    contents=None,
                )
            )
            await collector.maybe_flush()

    logger.info("Backfilled %d file records for %s", len(file_paths), repo_full_name)


async def backfill_commit_stat_records(
    store: Any,
    repo_id: Any,
    stat_rows: List[GitCommitStat],
    repo_full_name: str,
) -> None:
    """Persist pre-built GitCommitStat rows via AsyncBatchCollector.

    Args:
        store: A GitSyncStore-compatible instance.
        repo_id: The repository identifier (used only for logging).
        stat_rows: Pre-constructed GitCommitStat records.
        repo_full_name: Human-readable name used in log messages.
    """
    if not stat_rows:
        return

    async with AsyncBatchCollector(store.insert_git_commit_stats) as collector:
        for stat in stat_rows:
            collector.add(stat)
            await collector.maybe_flush()

    logger.info(
        "Backfilled %d commit stat records for %s",
        len(stat_rows),
        repo_full_name,
    )


async def backfill_blame_records(
    store: Any,
    repo_id: Any,
    blame_rows: List[GitBlame],
    repo_full_name: str,
) -> None:
    """Persist pre-built GitBlame rows via AsyncBatchCollector.

    Args:
        store: A GitSyncStore-compatible instance.
        repo_id: The repository identifier (used only for logging).
        blame_rows: Pre-constructed GitBlame records.
        repo_full_name: Human-readable name used in log messages.
    """
    if not blame_rows:
        return

    async with AsyncBatchCollector(store.insert_blame_data) as collector:
        for blame in blame_rows:
            collector.add(blame)
            await collector.maybe_flush()

    logger.info(
        "Backfilled %d blame records for %s",
        len(blame_rows),
        repo_full_name,
    )
