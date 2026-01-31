from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import func, select

from dev_health_ops.models.git import (
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
)


class GitDataMixin:
    async def has_any_git_files(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count()).select_from(GitFile).where(GitFile.repo_id == repo_id)
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

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        if not file_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
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
                    "repo_id": getattr(item, "repo_id"),
                    "path": getattr(item, "path"),
                    "executable": getattr(item, "executable"),
                    "contents": getattr(item, "contents"),
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

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        if not commit_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
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
                    "repo_id": getattr(item, "repo_id"),
                    "hash": getattr(item, "hash"),
                    "message": getattr(item, "message"),
                    "author_name": getattr(item, "author_name"),
                    "author_email": getattr(item, "author_email"),
                    "author_when": getattr(item, "author_when"),
                    "committer_name": getattr(item, "committer_name"),
                    "committer_email": getattr(item, "committer_email"),
                    "committer_when": getattr(item, "committer_when"),
                    "parents": getattr(item, "parents"),
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

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        if not commit_stats:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
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
                    "repo_id": getattr(item, "repo_id"),
                    "commit_hash": getattr(item, "commit_hash"),
                    "file_path": getattr(item, "file_path"),
                    "additions": getattr(item, "additions"),
                    "deletions": getattr(item, "deletions"),
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

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        if not data_batch:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
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
                    "repo_id": getattr(item, "repo_id"),
                    "path": getattr(item, "path"),
                    "line_no": getattr(item, "line_no"),
                    "author_email": getattr(item, "author_email"),
                    "author_name": getattr(item, "author_name"),
                    "author_when": getattr(item, "author_when"),
                    "commit_hash": getattr(item, "commit_hash"),
                    "line": getattr(item, "line"),
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
