"""Venue-oracle CLI for CHAOS-6374's Linear team discovery.

Runs the REAL production TeamDiscoveryService.discover_linear against a
caller-supplied graphql_url (a Go httptest.Server serving a real, captured
fixture -- see venue_oracle_linear_test.go), so the Go port and the Python
original are proven against the identical stub response, not two separately
-imagined ones.

LinearClient posts to a fixed module-level LINEAR_API_URL constant
(client.py:28) -- discover_linear itself takes no url parameter. Rather than
adding one (a Python fix, which this repo does not make -- port to Go, never
patch Python), this script monkeypatches that module attribute before
discover_linear's own `from dev_health_ops.providers.linear.client import
LinearAuth, LinearClient` (a call-time-scoped import) resolves it: Python
looks up the bare name LINEAR_API_URL inside client.py's own module globals
at CALL time, so patching the module attribute takes effect regardless of
import order, the same class of technique the GitHub oracle uses for
PyGithub's own fixed Github() construction.

Usage: venue_oracle_discover_linear.py <graphql_url> <api_key>
Prints the route's full TeamDiscoverResponse body (compact JSON, as FastAPI
renders it) to stdout.
"""

import asyncio
import json
import sys


def main() -> None:
    graphql_url, api_key = sys.argv[1], sys.argv[2]

    import dev_health_ops.providers.linear.client as linear_client

    linear_client.LINEAR_API_URL = graphql_url

    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )

    async def run() -> str:
        from dev_health_ops.api.admin.schemas import TeamDiscoverResponse

        svc = TeamDiscoveryService(None, "venue-oracle-org")
        teams = await svc.discover_linear(api_key=api_key)
        # The route's own response envelope (teams.py:294-301,318-324):
        # linear never sets truncated/warnings away from their defaults.
        response = TeamDiscoverResponse(
            provider="linear",
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
