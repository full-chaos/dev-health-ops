"""Venue-oracle CLI for CHAOS-6374's Jira team discovery.

Runs the REAL production TeamDiscoveryService.discover_jira against a
caller-supplied base_url (a Go httptest.Server serving a real, captured
fixture -- see venue_oracle_jira_test.go), so the Go port and the Python
original are proven against the identical stub response, not two separately
-imagined ones. discover_jira already accepts a url parameter directly, no
monkeypatch needed.

Usage: venue_oracle_discover_jira.py <base_url> <email> <api_token>
Prints the route's full TeamDiscoverResponse body (compact JSON, as FastAPI
renders it) to stdout.
"""

import asyncio
import json
import sys


def main() -> None:
    base_url, email, api_token = sys.argv[1], sys.argv[2], sys.argv[3]

    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )

    async def run() -> str:
        from dev_health_ops.api.admin.schemas import TeamDiscoverResponse

        svc = TeamDiscoveryService(None, "venue-oracle-org")
        teams = await svc.discover_jira(email=email, api_token=api_token, url=base_url)
        # The route's own response envelope (teams.py:312-324): jira never
        # sets truncated/warnings away from their defaults.
        response = TeamDiscoverResponse(
            provider="jira",
            teams=teams,
            total=len(teams),
            truncated=False,
            warnings=[],
        )
        return json.dumps(
            response.model_dump(mode="json"),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )

    sys.stdout.write(asyncio.run(run()))


if __name__ == "__main__":
    main()
