"""
Base connector class for Git repository connectors.

This module provides an abstract base class that defines the common interface
for all Git connectors (GitHub, GitLab, local, etc.).
"""

import asyncio
import hashlib
import json
import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from queue import Empty as QueueEmpty
from queue import Queue
from typing import Callable, List, Optional, Any
import redis

from dev_health_ops.connectors.models import (
    Author,
    CommitStats,
    FileBlame,
    Organization,
    PullRequest,
    Repository,
    RepoStats,
)
from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)

logger = logging.getLogger(__name__)


class RateLimitException(Exception):
    """Exception raised when API rate limit is reached."""

    def __init__(self, message: str, retry_after_seconds: Optional[float] = None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


@dataclass
class BatchResult:
    """Result of a batch repository processing operation."""

    repository: Repository
    stats: Optional[RepoStats] = None
    error: Optional[str] = None
    success: bool = True


class GitConnector(ABC):
    """
    Abstract base class for Git repository connectors.

    This class defines the common interface that all Git connectors
    (GitHub, GitLab, local) must implement.
    """

    def __init__(
        self,
        per_page: int = 100,
        max_workers: int = 4,
        cache: Optional[redis.Redis] = None,
        cache_prefix: str = "git:",
        cache_ttl: int = 3600,
    ):
        """
        Initialize the base connector.

        :param per_page: Number of items per page for pagination.
        :param max_workers: Maximum concurrent workers for operations.
        :param cache: Optional Redis client for caching.
        :param cache_prefix: Prefix for Redis keys.
        :param cache_ttl: TTL for cached items in seconds.
        """
        self.per_page = per_page
        self.max_workers = max_workers
        self.cache = cache
        self.cache_prefix = cache_prefix
        self.cache_ttl = cache_ttl

    @abstractmethod
    def list_organizations(
        self,
        max_orgs: Optional[int] = None,
    ) -> List[Organization]:
        """
        List organizations/groups accessible to the authenticated user.

        :param max_orgs: Maximum number of organizations to retrieve.
        :return: List of Organization objects.
        """
        pass

    @abstractmethod
    def list_repositories(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        search: Optional[str] = None,
        pattern: Optional[str] = None,
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """
        List repositories for an organization, user, or search query.

        :param org_name: Optional organization name.
        :param user_name: Optional user name.
        :param search: Optional search query.
        :param pattern: Optional fnmatch-style pattern to filter repositories.
        :param max_repos: Maximum number of repositories to retrieve.
        :return: List of Repository objects.
        """
        pass

    @abstractmethod
    def get_contributors(
        self,
        owner: str,
        repo: str,
        max_contributors: Optional[int] = None,
    ) -> List[Author]:
        """
        Get contributors for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param max_contributors: Maximum number of contributors to retrieve.
        :return: List of Author objects.
        """
        pass

    @abstractmethod
    def get_commit_stats(
        self,
        owner: str,
        repo: str,
        sha: str,
    ) -> CommitStats:
        """
        Get statistics for a specific commit.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param sha: Commit SHA.
        :return: CommitStats object.
        """
        pass

    @abstractmethod
    def get_repo_stats(
        self,
        owner: str,
        repo: str,
        max_commits: Optional[int] = None,
    ) -> RepoStats:
        """
        Get aggregated statistics for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param max_commits: Maximum number of commits to analyze.
        :return: RepoStats object.
        """
        pass

    @abstractmethod
    def get_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        max_prs: Optional[int] = None,
    ) -> List[PullRequest]:
        """
        Get pull requests/merge requests for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param state: State filter ('open', 'closed', 'all').
        :param max_prs: Maximum number of pull requests to retrieve.
        :return: List of PullRequest objects.
        """
        pass

    @abstractmethod
    def get_file_blame(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str = "HEAD",
    ) -> FileBlame:
        """
        Get blame information for a file.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param path: File path within the repository.
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: FileBlame object.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connector and cleanup resources."""
        pass

    # --- Caching Support ---

    def _get_cache_key(self, method: str, **kwargs) -> str:
        """Generate a stable cache key for a method and its arguments."""
        # Sort kwargs to ensure stability
        sorted_args = sorted(kwargs.items())
        args_str = json.dumps(sorted_args, default=str)
        args_hash = hashlib.md5(args_str.encode()).hexdigest()
        return f"{self.cache_prefix}{method}:{args_hash}"

    def _get_cached_item(self, key: str, model_class: Any) -> Optional[Any]:
        """Retrieve and deserialize an item from cache."""
        if not self.cache:
            return None

        try:
            data = self.cache.get(key)
            if data:
                logger.debug(f"Cache hit: {key}")
                raw_data = json.loads(data)
                if isinstance(raw_data, list):
                    return [model_class(**item) for item in raw_data]
                return model_class(**raw_data)
        except Exception as e:
            logger.warning(f"Cache retrieval error for {key}: {e}")
        return None

    def _set_cached_item(self, key: str, item: Any) -> None:
        """Serialize and store an item in cache."""
        if not self.cache:
            return

        try:
            if isinstance(item, list):
                data = json.dumps(
                    [i.__dict__ if hasattr(i, "__dict__") else i for i in item],
                    default=str,
                )
            elif hasattr(item, "__dict__"):
                data = json.dumps(item.__dict__, default=str)
            else:
                data = json.dumps(item, default=str)

            self.cache.setex(key, self.cache_ttl, data)
            logger.debug(f"Cache store: {key}")
        except Exception as e:
            logger.warning(f"Cache storage error for {key}: {e}")

    def _get_repositories_for_processing(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """Standard implementation of repository discovery for batch processing."""
        effective_org = org_name
        effective_user = user_name

        if not org_name and not user_name and pattern:
            if "/" in pattern:
                parts = pattern.split("/", 1)
                owner_part = parts[0]
                if owner_part and "*" not in owner_part and "?" not in owner_part:
                    effective_user = owner_part
                    logger.info(
                        f"Extracted owner '{owner_part}' from pattern '{pattern}'"
                    )

        return self.list_repositories(
            org_name=effective_org,
            user_name=effective_user,
            pattern=pattern,
            max_repos=max_repos,
        )

    def _process_single_repo_stats(
        self,
        repo: Repository,
        max_commits: Optional[int] = None,
    ) -> BatchResult:
        """Standard implementation of single repository processing."""
        try:
            parts = repo.full_name.split("/")
            if len(parts) != 2:
                return BatchResult(
                    repository=repo,
                    error=f"Invalid repository name: {repo.full_name}",
                    success=False,
                )

            owner, repo_name = parts
            stats = self.get_repo_stats(owner, repo_name, max_commits=max_commits)

            return BatchResult(
                repository=repo,
                stats=stats,
                success=True,
            )
        except RateLimitException:
            raise
        except Exception as e:
            logger.warning(f"Failed to get stats for {repo.full_name}: {e}")
            return BatchResult(
                repository=repo,
                error=str(e),
                success=False,
            )

    def get_repos_with_stats(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        batch_size: int = 10,
        max_concurrent: int = 4,
        rate_limit_delay: float = 1.0,
        max_commits_per_repo: Optional[int] = None,
        max_repos: Optional[int] = None,
        on_repo_complete: Optional[Callable[[BatchResult], None]] = None,
    ) -> List[BatchResult]:
        """Standard implementation of batch repository processing."""
        repos = self._get_repositories_for_processing(
            org_name=org_name,
            user_name=user_name,
            pattern=pattern,
            max_repos=max_repos,
        )

        logger.info(
            f"Processing {len(repos)} repositories with batch_size={batch_size}"
        )
        results: List[BatchResult] = []

        for batch_start in range(0, len(repos), batch_size):
            batch_end = min(batch_start + batch_size, len(repos))
            batch = repos[batch_start:batch_end]

            work_q: Queue[Repository] = Queue()
            for repo in batch:
                work_q.put(repo)

            gate = RateLimitGate(
                RateLimitConfig(initial_backoff_seconds=max(1.0, rate_limit_delay))
            )
            results_lock = threading.Lock()

            def worker():
                while True:
                    try:
                        repo = work_q.get_nowait()
                    except QueueEmpty:
                        return

                    attempts = 0
                    while attempts < 10:
                        gate.wait_sync()
                        try:
                            result = self._process_single_repo_stats(
                                repo, max_commits=max_commits_per_repo
                            )
                            gate.reset()
                            with results_lock:
                                results.append(result)
                            if on_repo_complete:
                                on_repo_complete(result)
                            break
                        except RateLimitException as e:
                            attempts += 1
                            applied = gate.penalize(e.retry_after_seconds)
                            logger.info(f"Rate limited; backoff {applied:.1f}s ({e})")
                        except Exception as e:
                            with results_lock:
                                results.append(
                                    BatchResult(
                                        repository=repo, error=str(e), success=False
                                    )
                                )
                            break

            threads = [
                threading.Thread(target=worker)
                for _ in range(min(max_concurrent, len(batch)))
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        return results

    async def get_repos_with_stats_async(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        batch_size: int = 10,
        max_concurrent: int = 4,
        rate_limit_delay: float = 1.0,
        max_commits_per_repo: Optional[int] = None,
        max_repos: Optional[int] = None,
        on_repo_complete: Optional[Callable[[BatchResult], None]] = None,
    ) -> List[BatchResult]:
        """Async implementation of batch repository processing."""
        # For base implementation, we'll wrap the sync one but in a real provider
        # this might use aiohttp or similar.
        return await asyncio.to_thread(
            self.get_repos_with_stats,
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

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
