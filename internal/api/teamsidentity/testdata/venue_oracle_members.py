"""Venue-oracle CLI for CHAOS-6313's member discovery (GitHub and Linear).

Runs the REAL TeamMembershipService.discover_members_* and match_members
against a caller-supplied stub, then renders the route's own
TeamMembersDiscoverResponse the way FastAPI does. GitHub and Linear reach
fixed hosts in production, so (like the team-discovery oracle) the client
class / URL constant is redirected here, never edited in the source tree.

Usage:
  venue_oracle_members.py github <stub_url> <org> <team_slug> <token> <identities.json>
  venue_oracle_members.py linear <stub_url> <api_key> <team_key> <identities.json>
"""

import asyncio
import json
import sys
import uuid
from datetime import datetime
from typing import Any


class _Store:
    def __init__(self, identities: list[Any]) -> None:
        self._identities = identities

    async def list_all(self, active_only: bool = True) -> list[Any]:
        return [i for i in self._identities if i.is_active or not active_only]


def _identities(path: str) -> list[Any]:
    from dev_health_ops.api.services.configuration.clickhouse_identity_admin import (
        ClickHouseIdentity,
    )

    out = []
    for row in json.load(open(path)):
        out.append(
            ClickHouseIdentity(
                canonical_id=row["canonical_id"],
                identity_uuid=uuid.UUID(row["identity_uuid"]),
                display_name=row["display_name"],
                email=row["email"],
                provider_identities=row["provider_identities"],
                team_ids=row["team_ids"],
                is_active=row["is_active"],
                updated_at=datetime.fromisoformat(row["updated_at"]),
                org_id=row["org_id"],
            )
        )
    return out


async def _run(mode: str, args: list[str]) -> str:
    from dev_health_ops.api.admin.schemas import TeamMembersDiscoverResponse
    from dev_health_ops.api.services.configuration import TeamMembershipService

    svc = TeamMembershipService(None, "venue-oracle-org")  # type: ignore[arg-type]
    if mode == "github":
        stub_url, org, slug, token, identities_path = args
        import github

        class _BaseURLInjectedGithub(github.Github):
            def __init__(self, *a: Any, **kw: Any) -> None:
                kw.setdefault("base_url", stub_url)
                super().__init__(*a, **kw)

        setattr(github, "Github", _BaseURLInjectedGithub)
        members = await svc.discover_members_github(
            token=token, org_name=org, team_slug=slug
        )
        team_id, provider = "gh:" + slug, "github"
    else:
        stub_url, api_key, team_key, identities_path = args
        from dev_health_ops.providers.linear import client as linear_client

        setattr(linear_client, "LINEAR_API_URL", stub_url + "/graphql")
        members = await svc.discover_members_linear(api_key=api_key, team_key=team_key)
        team_id, provider = team_key, "linear"
    matched = await svc.match_members(members, identity_store=_Store(_identities(identities_path)))
    response = TeamMembersDiscoverResponse(
        team_id=team_id, provider=provider, members=matched, total=len(matched)
    )
    return json.dumps(
        response.model_dump(mode="json"),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


if __name__ == "__main__":
    sys.stdout.write(asyncio.run(_run(sys.argv[1], sys.argv[2:])))
