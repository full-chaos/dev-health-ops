"""Jira activity inference service.

Walks a Jira project's recent issues to infer who is actively involved
(assignee, reporter, commenter) and ranks them by activity, so the team
can confirm or skip each suggestion when forming an internal team.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession

from ._helpers import _get_jira_activity_schema_classes

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import (
        InferredMember,
    )


class JiraActivityInferenceService:
    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    def _parse_jira_datetime(self, value: Any) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _confidence_for_count(self, count: int) -> str:
        if count >= 5:
            return "core"
        if count >= 2:
            return "active"
        return "peripheral"

    async def infer_members(
        self,
        email: str,
        api_token: str,
        jira_url: str,
        project_key: str,
        window_days: int = 90,
    ) -> list[InferredMember]:
        InferredMember, _ = _get_jira_activity_schema_classes()

        jql = f"project = '{project_key}' AND updated >= '-{int(window_days)}d'"
        from dev_health_ops.providers.jira.client import JiraAuth, JiraClient

        client = JiraClient(
            auth=JiraAuth(base_url=jira_url, email=email, api_token=api_token),
            org_id=self.org_id,
        )

        def _fetch_issues() -> list[dict[str, Any]]:
            try:
                return list(
                    client.iter_issues(
                        jql=jql,
                        fields=["assignee", "reporter", "creator", "comment"],
                        expand_changelog=False,
                        limit=500,
                    )
                )
            finally:
                client.close()

        issues = await asyncio.to_thread(_fetch_issues)

        activity_map: dict[str, dict[str, Any]] = {}

        def _touch(
            actor: Any,
            role: str,
            issue_updated_at: datetime | None,
        ) -> None:
            if not isinstance(actor, dict):
                return
            account_id = actor.get("accountId")
            if not account_id:
                return

            current = activity_map.get(account_id)
            if current is None:
                current = {
                    "account_id": account_id,
                    "display_name": actor.get("displayName"),
                    "email": actor.get("emailAddress"),
                    "activity_count": 0,
                    "roles": set(),
                    "last_active": None,
                }
                activity_map[account_id] = current

            current["activity_count"] += 1
            current["roles"].add(role)
            if not current.get("display_name") and actor.get("displayName"):
                current["display_name"] = actor.get("displayName")
            if not current.get("email") and actor.get("emailAddress"):
                current["email"] = actor.get("emailAddress")

            existing_last_active = current.get("last_active")
            if issue_updated_at and (
                existing_last_active is None or issue_updated_at > existing_last_active
            ):
                current["last_active"] = issue_updated_at

        for issue in issues:
            fields = issue.get("fields") or {}
            issue_updated_at = self._parse_jira_datetime(fields.get("updated"))
            _touch(fields.get("assignee"), "assignee", issue_updated_at)
            _touch(fields.get("reporter"), "reporter", issue_updated_at)
            _touch(fields.get("creator"), "commenter", issue_updated_at)

        inferred_members = [
            InferredMember(
                account_id=str(data["account_id"]),
                display_name=data.get("display_name"),
                email=data.get("email"),
                activity_count=int(data.get("activity_count", 0)),
                confidence=self._confidence_for_count(
                    int(data.get("activity_count", 0))
                ),
                roles=sorted(list(data.get("roles", set()))),
                last_active=data.get("last_active"),
            )
            for data in activity_map.values()
        ]

        return sorted(
            inferred_members,
            key=lambda member: (-member.activity_count, member.account_id),
        )
