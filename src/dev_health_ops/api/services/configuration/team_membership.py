"""Team membership service.

Discovers team members from external providers (GitHub, GitLab, Jira),
matches them against existing identity mappings, and confirms or creates
new identity links to teams.
"""

from __future__ import annotations

import asyncio
import difflib
from typing import TYPE_CHECKING, Any

import requests
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.settings import IdentityMapping

from ._helpers import (
    _get_discovered_member_cls,
    _get_identity_mapping_response_cls,
    _get_member_match_result_cls,
)
from .identity_mapping import IdentityMappingService

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import (
        ConfirmMemberLink,
        DiscoveredMember,
    )


class TeamMembershipService:
    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def discover_members_github(
        self,
        token: str,
        org_name: str,
        team_slug: str,
    ) -> list[DiscoveredMember]:
        def _discover() -> list[DiscoveredMember]:
            from github import Auth, Github

            DiscoveredMember = _get_discovered_member_cls()
            auth = Auth.Token(token)
            gh = Github(auth=auth, per_page=100)
            try:
                org = gh.get_organization(org_name)
                team = org.get_team_by_slug(team_slug)
                members: list[Any] = []
                for member in team.get_members():
                    members.append(
                        DiscoveredMember(
                            provider_type="github",
                            provider_identity=member.login,
                            display_name=getattr(member, "name", None),
                            email=getattr(member, "email", None),
                            role=None,
                        )
                    )
                return members
            finally:
                gh.close()

        return await asyncio.to_thread(_discover)

    async def discover_members_gitlab(
        self,
        token: str,
        group_path: str,
        url: str,
    ) -> list[DiscoveredMember]:
        def _discover() -> list[DiscoveredMember]:
            import gitlab as gl_lib

            DiscoveredMember = _get_discovered_member_cls()
            gl = gl_lib.Gitlab(url=url, private_token=token)
            group = gl.groups.get(group_path)
            members: list[Any] = []
            for member in group.members.list(per_page=100, get_all=True):
                members.append(
                    DiscoveredMember(
                        provider_type="gitlab",
                        provider_identity=str(getattr(member, "username", "")),
                        display_name=getattr(member, "name", None),
                        email=getattr(member, "email", None),
                        role=str(getattr(member, "access_level", "")) or None,
                    )
                )
            return [m for m in members if m.provider_identity]

        return await asyncio.to_thread(_discover)

    async def discover_members_linear(
        self,
        api_key: str,
        team_key: str,
    ) -> list[DiscoveredMember]:
        """Discover members of a Linear team by its key (e.g. "ENG").

        Linear assignees normalize to emails, so the member email doubles as
        the provider identity (falling back to the user id when absent).
        """

        def _discover() -> list[DiscoveredMember]:
            from dev_health_ops.providers.linear.client import LinearAuth, LinearClient

            DiscoveredMember = _get_discovered_member_cls()
            normalized_key = team_key.removeprefix("linear:")
            with LinearClient(
                auth=LinearAuth(api_key=api_key), org_id=self.org_id
            ) as client:
                for team in client.iter_teams():
                    if team.get("key") != normalized_key:
                        continue
                    members_page = team.get("members") or {}
                    nodes = list(members_page.get("nodes") or [])
                    if (members_page.get("pageInfo") or {}).get("hasNextPage"):
                        nodes = client.get_team_members(str(team.get("id") or ""))
                    members: list[Any] = []
                    for node in nodes:
                        if node.get("active") is False:
                            continue
                        member_email = node.get("email")
                        provider_identity = member_email or node.get("id")
                        if not provider_identity:
                            continue
                        members.append(
                            DiscoveredMember(
                                provider_type="linear",
                                provider_identity=str(provider_identity),
                                display_name=node.get("name"),
                                email=member_email,
                                role=None,
                            )
                        )
                    return members
            return []

        return await asyncio.to_thread(_discover)

    async def discover_members_jira(
        self,
        email: str,
        api_token: str,
        url: str,
        project_key: str,
    ) -> list[DiscoveredMember]:
        def _discover() -> list[DiscoveredMember]:
            DiscoveredMember = _get_discovered_member_cls()
            response = requests.get(
                f"{url.rstrip('/')}/rest/api/3/project/{project_key}",
                auth=(email, api_token),
                headers={"Accept": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            lead = payload.get("lead") or {}
            provider_identity = (
                lead.get("accountId")
                or lead.get("emailAddress")
                or lead.get("displayName")
                or ""
            )
            if not provider_identity:
                return []
            return [
                DiscoveredMember(
                    provider_type="jira",
                    provider_identity=provider_identity,
                    display_name=lead.get("displayName"),
                    email=lead.get("emailAddress"),
                    role="lead",
                )
            ]

        return await asyncio.to_thread(_discover)

    async def discover_members_jira_bulk(
        self,
        *,
        email: str,
        api_token: str,
        url: str,
        project_keys: list[str],
        concurrency: int = 5,
    ) -> list[Any]:
        """Fan out Jira project member lookups concurrently.

        Uses an asyncio.Semaphore to cap simultaneous HTTPS requests so we
        don't trip Jira's per-IP rate limits.
        """
        sem = asyncio.Semaphore(max(1, concurrency))

        async def _one(project_key: str) -> list[Any]:
            async with sem:
                return await self.discover_members_jira(
                    email=email,
                    api_token=api_token,
                    url=url,
                    project_key=project_key,
                )

        results = await asyncio.gather(*(_one(k) for k in project_keys))
        flat: list[Any] = []
        for group in results:
            flat.extend(group)
        return flat

    async def match_members(
        self,
        members: list[DiscoveredMember],
        identity_store: Any | None = None,
    ) -> list[Any]:
        # CHAOS-2600 CS5: when a ClickHouse identity store is provided, match
        # against the ClickHouse-native identity catalog (the admin system of
        # record) instead of Postgres ``IdentityMapping``.
        if identity_store is not None:
            return await self._match_members_clickhouse(members, identity_store)

        IdentityMappingResponse = _get_identity_mapping_response_cls()
        member_match_result_cls = _get_member_match_result_cls()
        identity_svc = IdentityMappingService(self.session, self.org_id)
        matched: list[Any] = []

        for member in members:
            mapping = await identity_svc.find_by_provider_identity(
                member.provider_type,
                member.provider_identity,
            )
            if mapping is not None:
                matched.append(
                    member_match_result_cls(
                        discovered=member,
                        match_status="matched",
                        matched_identity=IdentityMappingResponse.model_validate(
                            mapping
                        ),
                        confidence=1.0,
                    )
                )
                continue

            if member.email:
                stmt = select(IdentityMapping).where(
                    IdentityMapping.org_id == self.org_id,
                    IdentityMapping.email == member.email,
                    IdentityMapping.is_active == True,  # noqa: E712
                )
                email_result = await self.session.execute(stmt)
                email_match = email_result.scalar_one_or_none()
                if email_match is not None:
                    matched.append(
                        member_match_result_cls(
                            discovered=member,
                            match_status="suggested",
                            matched_identity=IdentityMappingResponse.model_validate(
                                email_match
                            ),
                            confidence=0.95,
                            suggestion_reason="email_match",
                        )
                    )
                    continue

            if member.display_name:
                name_stmt = select(IdentityMapping).where(
                    IdentityMapping.org_id == self.org_id,
                    IdentityMapping.display_name.isnot(None),
                    IdentityMapping.is_active == True,  # noqa: E712
                )
                name_result = await self.session.execute(name_stmt)
                best_match: IdentityMapping | None = None
                best_score = 0.0
                for candidate in name_result.scalars().all():
                    if not candidate.display_name:
                        continue
                    score = difflib.SequenceMatcher(
                        a=member.display_name.lower(),
                        b=candidate.display_name.lower(),
                    ).ratio()
                    if score > best_score:
                        best_score = score
                        best_match = candidate

                if best_match is not None and best_score >= 0.8:
                    matched.append(
                        member_match_result_cls(
                            discovered=member,
                            match_status="suggested",
                            matched_identity=IdentityMappingResponse.model_validate(
                                best_match
                            ),
                            confidence=round(best_score, 2),
                            suggestion_reason="display_name_similarity",
                        )
                    )
                    continue

            matched.append(
                member_match_result_cls(
                    discovered=member,
                    match_status="unmatched",
                    matched_identity=None,
                    confidence=None,
                    suggestion_reason=None,
                )
            )

        return matched

    async def _match_members_clickhouse(
        self,
        members: list[DiscoveredMember],
        identity_store: Any,
    ) -> list[Any]:
        """ClickHouse-backed variant of ``match_members`` (CHAOS-2600 CS5).

        Same three strategies as the Postgres path — provider-identity exact,
        email exact, then display-name similarity — but candidates come from
        the ClickHouse-native identity catalog. Loads the org's active
        identities once and matches in memory.
        """
        IdentityMappingResponse = _get_identity_mapping_response_cls()
        member_match_result_cls = _get_member_match_result_cls()
        candidates = await identity_store.list_all(active_only=True)
        matched: list[Any] = []

        for member in members:
            provider_match = next(
                (
                    c
                    for c in candidates
                    if member.provider_identity
                    in c.provider_identities.get(member.provider_type, [])
                ),
                None,
            )
            if provider_match is not None:
                matched.append(
                    member_match_result_cls(
                        discovered=member,
                        match_status="matched",
                        matched_identity=IdentityMappingResponse.model_validate(
                            provider_match
                        ),
                        confidence=1.0,
                    )
                )
                continue

            if member.email:
                email_match = next(
                    (c for c in candidates if c.email == member.email), None
                )
                if email_match is not None:
                    matched.append(
                        member_match_result_cls(
                            discovered=member,
                            match_status="suggested",
                            matched_identity=IdentityMappingResponse.model_validate(
                                email_match
                            ),
                            confidence=0.95,
                            suggestion_reason="email_match",
                        )
                    )
                    continue

            if member.display_name:
                best_match = None
                best_score = 0.0
                for candidate in candidates:
                    if not candidate.display_name:
                        continue
                    score = difflib.SequenceMatcher(
                        a=member.display_name.lower(),
                        b=candidate.display_name.lower(),
                    ).ratio()
                    if score > best_score:
                        best_score = score
                        best_match = candidate
                if best_match is not None and best_score >= 0.8:
                    matched.append(
                        member_match_result_cls(
                            discovered=member,
                            match_status="suggested",
                            matched_identity=IdentityMappingResponse.model_validate(
                                best_match
                            ),
                            confidence=round(best_score, 2),
                            suggestion_reason="display_name_similarity",
                        )
                    )
                    continue

            matched.append(
                member_match_result_cls(
                    discovered=member,
                    match_status="unmatched",
                    matched_identity=None,
                    confidence=None,
                    suggestion_reason=None,
                )
            )

        return matched

    async def confirm_links(
        self,
        team_id: str,
        links: list[ConfirmMemberLink],
    ) -> dict[str, int]:
        identity_svc = IdentityMappingService(self.session, self.org_id)
        linked = 0
        created = 0
        skipped = 0

        for link in links:
            if link.action == "skip":
                skipped += 1
                continue

            if link.action == "link":
                mapping: Any | None = await identity_svc.get(link.canonical_id)
                if mapping is None:
                    skipped += 1
                    continue

                team_ids = list(mapping.team_ids or [])
                if team_id not in team_ids:
                    team_ids.append(team_id)
                    setattr(mapping, "team_ids", team_ids)
                await identity_svc.add_provider_identity(
                    canonical_id=link.canonical_id,
                    provider=link.provider,
                    identity=link.provider_identity,
                )
                linked += 1
                continue

            if link.action == "create":
                await identity_svc.create_or_update(
                    canonical_id=link.canonical_id,
                    provider_identities={link.provider: [link.provider_identity]},
                    team_ids=[team_id],
                )
                created += 1
                continue

            skipped += 1

        await self.session.flush()
        return {
            "linked": linked,
            "created": created,
            "skipped": skipped,
        }
