"""Venue-oracle CLI for CHAOS-6374's GitLab team discovery.

Runs the REAL production TeamDiscoveryService.discover_gitlab against a
caller-supplied base_url (a Go httptest.Server serving real, captured
fixtures -- see venue_oracle_gitlab_test.go), so the Go port and the Python
original are proven against the identical stub responses, not two separately
-imagined ones. discover_gitlab already accepts a url parameter (unlike
GitHub's PyGithub SDK, no monkeypatch is needed here).

Usage: venue_oracle_discover_gitlab.py <base_url> <group_path> <token>
Prints the route's full TeamDiscoverResponse body (compact JSON, as FastAPI
renders it) to stdout.
"""

import asyncio
import json
import sys


def main() -> None:
    base_url, group_path, token = sys.argv[1], sys.argv[2], sys.argv[3]

    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )

    async def run() -> str:
        from dev_health_ops.api.admin.schemas import TeamDiscoverResponse

        svc = TeamDiscoveryService(None, "venue-oracle-org")
        result = await svc.discover_gitlab(
            token=token, group_path=group_path, url=base_url
        )
        # The route's own response envelope (teams.py:285-293,318-324):
        # exactly one discover_gitlab call, then TeamDiscoverResponse from
        # its teams/truncated/warnings, serialized the way FastAPI does
        # (model_dump(mode="json") then compact json.dumps).
        response = TeamDiscoverResponse(
            provider="gitlab",
            teams=result.teams,
            total=len(result.teams),
            truncated=result.truncated,
            warnings=result.warnings,
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
