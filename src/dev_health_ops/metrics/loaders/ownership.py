from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.metrics.query_builder import OrgScopedQuery
from dev_health_ops.metrics.schemas import CommitStatRow


@dataclass(frozen=True)
class OwnershipWindow:
    stats: list[CommitStatRow]
    repo_names: dict[uuid.UUID, str]


class OwnershipClickHouseLoader:
    def __init__(self, client: Any, org_id: str = "") -> None:
        self.client = client
        self.org_id = org_id
        self._scope = OrgScopedQuery(org_id)

    async def _repo_ids_for_team(self, team_id: str) -> list[str]:
        params = self._scope.inject({"team_id": team_id})
        rows = await query_dicts(
            self.client,
            f"""
            SELECT DISTINCT repo_id AS repo_id
            FROM user_metrics_daily
            WHERE team_id = {{team_id:String}}
              {self._scope.filter()}
            """,
            params,
        )
        return [str(row["repo_id"]) for row in rows if row.get("repo_id")]

    async def load_commit_ownership_stats(
        self,
        *,
        repo_id: uuid.UUID | None = None,
        team_id: str | None = None,
    ) -> OwnershipWindow:
        params: dict[str, Any] = {}
        filters: list[str] = []

        repo_ids: list[str] | None = None
        if team_id:
            repo_ids = await self._repo_ids_for_team(team_id)
            if repo_id is not None:
                repo_ids = [str(repo_id)] if str(repo_id) in set(repo_ids) else []
            if not repo_ids:
                return OwnershipWindow(stats=[], repo_names={})

        if repo_ids is not None:
            params["repo_ids"] = repo_ids
            filters.append("gc.repo_id IN {repo_ids:Array(UUID)}")
        elif repo_id is not None:
            params["repo_id"] = str(repo_id)
            filters.append("gc.repo_id = {repo_id:UUID}")

        params = self._scope.inject(params)
        where_clause = ""
        if filters:
            where_clause = "AND " + " AND ".join(filters)

        rows = await query_dicts(
            self.client,
            f"""
            SELECT
                gc.repo_id AS repo_id,
                coalesce(r.repo, toString(gc.repo_id)) AS repo_name,
                gcs.commit_hash AS commit_hash,
                gc.author_email AS author_email,
                gc.author_name AS author_name,
                gc.committer_when AS committer_when,
                gcs.file_path AS file_path,
                gcs.additions AS additions,
                gcs.deletions AS deletions,
                gcs.old_file_mode AS old_file_mode,
                gcs.new_file_mode AS new_file_mode
            FROM git_commit_stats AS gcs
            INNER JOIN git_commits AS gc
                ON gc.repo_id = gcs.repo_id
               AND gc.hash = gcs.commit_hash
               AND gc.org_id = gcs.org_id
            LEFT JOIN repos AS r
                ON r.id = gc.repo_id
               AND r.org_id = gc.org_id
            WHERE 1 = 1
              {self._scope.filter(alias="gc")}
              {self._scope.filter(alias="gcs")}
              {where_clause}
            """,
            params,
        )

        stats: list[CommitStatRow] = []
        repo_names: dict[uuid.UUID, str] = {}
        for row in rows:
            row_repo_id = row.get("repo_id")
            if row_repo_id is None:
                continue
            parsed_repo_id = uuid.UUID(str(row_repo_id))
            repo_names[parsed_repo_id] = str(row.get("repo_name") or parsed_repo_id)
            stats.append(
                {
                    "repo_id": parsed_repo_id,
                    "commit_hash": str(row.get("commit_hash") or ""),
                    "author_email": row.get("author_email"),
                    "author_name": row.get("author_name"),
                    "committer_when": row["committer_when"],
                    "file_path": row.get("file_path"),
                    "additions": int(row.get("additions") or 0),
                    "deletions": int(row.get("deletions") or 0),
                    "old_file_mode": row.get("old_file_mode"),
                    "new_file_mode": row.get("new_file_mode"),
                }
            )

        return OwnershipWindow(stats=stats, repo_names=repo_names)
