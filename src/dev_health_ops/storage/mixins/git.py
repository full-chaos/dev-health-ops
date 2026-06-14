from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select

from dev_health_ops.models.git import (
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
)
from dev_health_ops.storage.mixins.base import SQLAlchemyStoreMixinProtocol


class GitDataMixin(SQLAlchemyStoreMixinProtocol):
    async def has_any_git_files(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count()).select_from(GitFile).where(GitFile.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def has_any_git_file_contents(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count())
            .select_from(GitFile)
            .where(
                GitFile.repo_id == repo_id,
                GitFile.contents.is_not(None),
                GitFile.contents != "",
            )
        )
        return (result.scalar() or 0) > 0

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count())
            .select_from(GitCommitStat)
            .where(GitCommitStat.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def has_any_git_blame(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count())
            .select_from(GitBlame)
            .where(GitBlame.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def get_blamed_paths(self, repo_id) -> set[str]:
        """Return the set of file paths that already have blame for this repo.

        Drives the coverage-aware onboarding blame backfill (CHAOS-2376
        round-3): the crawl is capped per sync, so the backfill must diff the
        live tree against already-blamed paths and process the next unblamed
        batch instead of stopping once any blame row exists.
        """
        assert self.session is not None
        result = await self.session.execute(
            select(GitBlame.path).distinct().where(GitBlame.repo_id == repo_id)
        )
        return {str(path) for (path,) in result.all() if path is not None}

    async def has_unblamed_files(self, repo_id) -> bool:
        """Whether any tracked file still lacks blame for this repo.

        Keeps the capped onboarding blame crawl alive across reruns without
        re-walking the provider tree once coverage is complete (CHAOS-2376
        round-3). Returns False when no files are tracked yet so first
        onboarding starts via the ``has_any_git_blame`` any-row gate.
        """
        assert self.session is not None
        blamed_subq = select(GitBlame.path).where(GitBlame.repo_id == repo_id)
        result = await self.session.execute(
            select(GitFile.path)
            .where(
                GitFile.repo_id == repo_id,
                GitFile.path.not_in(blamed_subq),
            )
            .limit(1)
        )
        return result.first() is not None

    async def insert_git_file_data(self, file_data: list[GitFile]) -> None:
        if not file_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in file_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "path": item.get("path"),
                    "executable": item.get("executable"),
                    "contents": item.get("contents"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": item.repo_id,
                    "path": item.path,
                    "executable": item.executable,
                    "contents": item.contents,
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitFile,
            rows,
            conflict_columns=["repo_id", "path"],
            update_columns=["executable", "contents", "last_synced"],
        )

    async def insert_git_commit_data(self, commit_data: list[GitCommit]) -> None:
        if not commit_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in commit_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "hash": item.get("hash"),
                    "message": item.get("message"),
                    "author_name": item.get("author_name"),
                    "author_email": item.get("author_email"),
                    "author_when": item.get("author_when"),
                    "committer_name": item.get("committer_name"),
                    "committer_email": item.get("committer_email"),
                    "committer_when": item.get("committer_when"),
                    "parents": item.get("parents"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": item.repo_id,
                    "hash": item.hash,
                    "message": item.message,
                    "author_name": item.author_name,
                    "author_email": item.author_email,
                    "author_when": item.author_when,
                    "committer_name": item.committer_name,
                    "committer_email": item.committer_email,
                    "committer_when": item.committer_when,
                    "parents": item.parents,
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitCommit,
            rows,
            conflict_columns=["repo_id", "hash"],
            update_columns=[
                "message",
                "author_name",
                "author_email",
                "author_when",
                "committer_name",
                "committer_email",
                "committer_when",
                "parents",
                "last_synced",
            ],
        )

    async def insert_git_commit_stats(self, commit_stats: list[GitCommitStat]) -> None:
        if not commit_stats:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in commit_stats:
            if isinstance(item, dict):
                old_mode = item.get("old_file_mode") or "unknown"
                new_mode = item.get("new_file_mode") or "unknown"
                row = {
                    "repo_id": item.get("repo_id"),
                    "commit_hash": item.get("commit_hash"),
                    "file_path": item.get("file_path"),
                    "additions": item.get("additions"),
                    "deletions": item.get("deletions"),
                    "old_file_mode": old_mode,
                    "new_file_mode": new_mode,
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                old_mode = getattr(item, "old_file_mode", None) or "unknown"
                new_mode = getattr(item, "new_file_mode", None) or "unknown"
                row = {
                    "repo_id": item.repo_id,
                    "commit_hash": item.commit_hash,
                    "file_path": item.file_path,
                    "additions": item.additions,
                    "deletions": item.deletions,
                    "old_file_mode": old_mode,
                    "new_file_mode": new_mode,
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitCommitStat,
            rows,
            conflict_columns=["repo_id", "commit_hash", "file_path"],
            update_columns=[
                "additions",
                "deletions",
                "old_file_mode",
                "new_file_mode",
                "last_synced",
            ],
        )

    async def insert_blame_data(self, data_batch: list[GitBlame]) -> None:
        if not data_batch:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in data_batch:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "path": item.get("path"),
                    "line_no": item.get("line_no"),
                    "author_email": item.get("author_email"),
                    "author_name": item.get("author_name"),
                    "author_when": item.get("author_when"),
                    "commit_hash": item.get("commit_hash"),
                    "line": item.get("line"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": item.repo_id,
                    "path": item.path,
                    "line_no": item.line_no,
                    "author_email": item.author_email,
                    "author_name": item.author_name,
                    "author_when": item.author_when,
                    "commit_hash": item.commit_hash,
                    "line": item.line,
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitBlame,
            rows,
            conflict_columns=["repo_id", "path", "line_no"],
            update_columns=[
                "author_email",
                "author_name",
                "author_when",
                "commit_hash",
                "line",
                "last_synced",
            ],
        )
