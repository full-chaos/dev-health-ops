from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List


class WorkItemMixin:
    async def insert_work_item_dependencies(self, dependencies: List[Any]) -> None:
        if not dependencies:
            return
        rows: List[Dict[str, Any]] = []
        synced_at_default = datetime.now(timezone.utc)
        for item in dependencies:
            if isinstance(item, dict):
                rows.append(
                    {
                        "source_work_item_id": item.get("source_work_item_id"),
                        "target_work_item_id": item.get("target_work_item_id"),
                        "relationship_type": item.get("relationship_type"),
                        "relationship_type_raw": item.get("relationship_type_raw"),
                        "last_synced": item.get("last_synced") or synced_at_default,
                    }
                )
            else:
                rows.append(
                    {
                        "source_work_item_id": getattr(item, "source_work_item_id"),
                        "target_work_item_id": getattr(item, "target_work_item_id"),
                        "relationship_type": getattr(item, "relationship_type"),
                        "relationship_type_raw": getattr(item, "relationship_type_raw"),
                        "last_synced": getattr(item, "last_synced", None)
                        or synced_at_default,
                    }
                )

        await self._upsert_many(
            self._work_item_dependencies_table,
            rows,
            conflict_columns=[
                "source_work_item_id",
                "target_work_item_id",
                "relationship_type",
            ],
            update_columns=["relationship_type_raw", "last_synced"],
        )

    async def insert_work_graph_issue_pr(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        synced_at_default = datetime.now(timezone.utc)
        payload = []
        for r in records:
            payload.append(
                {
                    **r,
                    "repo_id": str(r["repo_id"]),
                    "last_synced": r.get("last_synced") or synced_at_default,
                }
            )
        await self._upsert_many(
            self._work_graph_issue_pr_table,
            payload,
            conflict_columns=["repo_id", "work_item_id", "pr_number"],
            update_columns=["confidence", "provenance", "evidence", "last_synced"],
        )

    async def insert_work_graph_pr_commit(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        synced_at_default = datetime.now(timezone.utc)
        payload = []
        for r in records:
            payload.append(
                {
                    **r,
                    "repo_id": str(r["repo_id"]),
                    "last_synced": r.get("last_synced") or synced_at_default,
                }
            )
        await self._upsert_many(
            self._work_graph_pr_commit_table,
            payload,
            conflict_columns=["repo_id", "pr_number", "commit_hash"],
            update_columns=["confidence", "provenance", "evidence", "last_synced"],
        )

    async def insert_work_items(self, work_items: List[Any]) -> None:
        if not work_items:
            return

        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in work_items:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda k, default=None: getattr(item, k, default)
            )
            repo_id_val = get("repo_id")
            if repo_id_val:
                repo_id_val = str(repo_id_val)

            rows.append(
                {
                    "work_item_id": str(get("work_item_id")),
                    "repo_id": repo_id_val,
                    "provider": str(get("provider") or ""),
                    "title": str(get("title") or ""),
                    "description": get("description"),
                    "type": str(get("type") or ""),
                    "status": str(get("status") or ""),
                    "status_raw": str(get("status_raw") or ""),
                    "project_key": str(get("project_key") or ""),
                    "project_id": str(get("project_id") or ""),
                    "assignees": get("assignees") or [],
                    "reporter": str(get("reporter") or ""),
                    "created_at": get("created_at"),
                    "updated_at": get("updated_at"),
                    "started_at": get("started_at"),
                    "completed_at": get("completed_at"),
                    "closed_at": get("closed_at"),
                    "labels": get("labels") or [],
                    "story_points": float(get("story_points"))  # type: ignore[arg-type]
                    if get("story_points") is not None
                    else None,
                    "sprint_id": str(get("sprint_id") or ""),
                    "sprint_name": str(get("sprint_name") or ""),
                    "parent_id": str(get("parent_id") or ""),
                    "epic_id": str(get("epic_id") or ""),
                    "url": str(get("url") or ""),
                    "priority_raw": str(get("priority_raw") or ""),
                    "service_class": str(get("service_class") or ""),
                    "due_at": get("due_at"),
                    "last_synced": get("last_synced") or synced_at_default,
                }
            )

        await self._upsert_many(
            self._work_items_table,
            rows,
            conflict_columns=["work_item_id"],
            update_columns=[
                "repo_id",
                "provider",
                "title",
                "description",
                "type",
                "status",
                "status_raw",
                "project_key",
                "project_id",
                "assignees",
                "reporter",
                "created_at",
                "updated_at",
                "started_at",
                "completed_at",
                "closed_at",
                "labels",
                "story_points",
                "sprint_id",
                "sprint_name",
                "parent_id",
                "epic_id",
                "url",
                "priority_raw",
                "service_class",
                "due_at",
                "last_synced",
            ],
        )

    async def insert_work_item_transitions(self, transitions: List[Any]) -> None:
        if not transitions:
            return

        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in transitions:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda k, default=None: getattr(item, k, default)
            )
            repo_id_val = get("repo_id")
            if repo_id_val:
                repo_id_val = str(repo_id_val)

            rows.append(
                {
                    "work_item_id": str(get("work_item_id")),
                    "occurred_at": get("occurred_at"),
                    "repo_id": repo_id_val,
                    "provider": str(get("provider") or ""),
                    "from_status": str(get("from_status") or ""),
                    "to_status": str(get("to_status") or ""),
                    "from_status_raw": str(get("from_status_raw") or ""),
                    "to_status_raw": str(get("to_status_raw") or ""),
                    "actor": str(get("actor") or ""),
                    "last_synced": get("last_synced") or synced_at_default,
                }
            )

        await self._upsert_many(
            self._work_item_transitions_table,
            rows,
            conflict_columns=["work_item_id", "occurred_at"],
            update_columns=[
                "repo_id",
                "provider",
                "from_status",
                "to_status",
                "from_status_raw",
                "to_status_raw",
                "actor",
                "last_synced",
            ],
        )
