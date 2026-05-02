"""BaseGitProcessor — shared orchestration logic for GitHub and GitLab processors.

Both processors follow the same backfill-needs-check pattern:

    1. Ask the store whether file/stat/blame data already exists.
    2. If any gap is found, fetch the missing data from the provider.
    3. Write results via AsyncBatchCollector.

This module extracts the common pieces into a reusable base so that github.py
and gitlab.py only contain provider-specific fetch logic.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dev_health_ops.models.git import GitBlame, GitCommitStat, GitFile
from dev_health_ops.processors.fetch_utils import AsyncBatchCollector
from dev_health_ops.utils import CONNECTORS_AVAILABLE

if TYPE_CHECKING:
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate
elif CONNECTORS_AVAILABLE:
    from dev_health_ops.connectors.utils import RateLimitConfig, RateLimitGate
else:
    RateLimitConfig = None
    RateLimitGate = None

logger = logging.getLogger(__name__)


class BaseGitProcessor:
    """Shared helper methods for GitHub and GitLab processors.

    Encapsulates patterns that appear in both processors so they can be
    maintained in one place:

    - Default rate-limit gate creation.
    - Thread-safe batch persistence from synchronous worker threads.
    - PR/MR ``created_at`` coercion (first non-None of created/merged/closed).
    """

    # Default initial backoff used when no gate is provided.
    DEFAULT_INITIAL_BACKOFF: float = 1.0

    @staticmethod
    def make_default_gate() -> Any:
        """Create a RateLimitGate with the standard default configuration.

        Both GitHub and GitLab processors share the same default:
        ``RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))``.

        Returns:
            A configured RateLimitGate instance, or None when connectors
            are not available (test environments).
        """
        if RateLimitGate is None or RateLimitConfig is None:
            return None
        return RateLimitGate(
            RateLimitConfig(
                initial_backoff_seconds=BaseGitProcessor.DEFAULT_INITIAL_BACKOFF
            )
        )

    @staticmethod
    def ensure_gate(gate: Any) -> Any:
        """Return *gate* unchanged, or a fresh default gate if None.

        Convenience wrapper replacing the boilerplate::

            if gate is None:
                gate = RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

        Args:
            gate: An existing gate instance or None.

        Returns:
            The provided gate, or a newly created default gate.
        """
        if gate is None:
            return BaseGitProcessor.make_default_gate()
        return gate

    @staticmethod
    def coerce_created_at(
        created_at: datetime | None,
        merged_at: datetime | None = None,
        closed_at: datetime | None = None,
    ) -> datetime:
        """Return the first non-None timestamp from created/merged/closed, or now.

        Both GitHub and GitLab processors use::

            created_at = x.created_at or x.merged_at or x.closed_at
                         or datetime.now(timezone.utc)

        This method centralises that pattern.

        Args:
            created_at: Primary creation timestamp.
            merged_at: Merge timestamp (fallback).
            closed_at: Close timestamp (second fallback).

        Returns:
            A non-None ``datetime`` (UTC).
        """
        return created_at or merged_at or closed_at or datetime.now(timezone.utc)

    @staticmethod
    def persist_batch_threadsafe(
        coro: Coroutine[Any, Any, Any],
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        """Run an async persistence coroutine from a synchronous worker thread.

        Both processors use ``asyncio.run_coroutine_threadsafe(...).result()``
        to write batches to the async store while running in a sync thread.

        Args:
            coro: The coroutine to schedule (e.g. ``ingestion_sink.insert_git_pull_requests(batch)``).
            loop: The running event loop to schedule the coroutine on.

        Returns:
            The result of the coroutine.
        """
        return asyncio.run_coroutine_threadsafe(coro, loop).result()


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
    ingestion_sink: Any,
    repo_id: Any,
    file_paths: list[str],
    repo_full_name: str,
) -> None:
    """Persist file path records from a pre-fetched list.

    Shared between GitHub and GitLab backfill paths. Provider-specific file
    listing happens upstream; this function only handles the write path.

    Args:
        ingestion_sink: A GitSyncStore-compatible sink adapter.
        repo_id: The repository identifier.
        file_paths: List of repository-relative file paths.
        repo_full_name: Human-readable name used in log messages.
    """
    if not file_paths:
        return

    async with AsyncBatchCollector(ingestion_sink.insert_git_file_data) as collector:
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
    ingestion_sink: Any,
    repo_id: Any,
    stat_rows: list[GitCommitStat],
    repo_full_name: str,
) -> None:
    """Persist pre-built GitCommitStat rows via AsyncBatchCollector.

    Args:
        ingestion_sink: A GitSyncStore-compatible sink adapter.
        repo_id: The repository identifier (used only for logging).
        stat_rows: Pre-constructed GitCommitStat records.
        repo_full_name: Human-readable name used in log messages.
    """
    if not stat_rows:
        return

    async with AsyncBatchCollector(ingestion_sink.insert_git_commit_stats) as collector:
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
    blame_rows: list[GitBlame],
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
