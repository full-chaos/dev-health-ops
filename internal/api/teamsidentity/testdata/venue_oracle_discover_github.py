"""Venue-oracle CLI for CHAOS-6311's GitHub team discovery (ruling 25).

Runs the REAL production TeamDiscoveryService.discover_github against a
caller-supplied base_url (a Go httptest.Server serving real, captured
fixtures -- see venue_oracle_github_test.go), so the Go port and the
Python original are proven against the identical stub responses, not two
separately-imagined ones.

PyGithub's discover_github call site (team_discovery.py:127-158) does not
itself expose a base_url parameter -- it always constructs
`Github(auth=auth, per_page=100)`. Rather than adding one (a Python fix,
which this repo does not make -- port to Go, never patch Python), this
script monkeypatches the `github.Github` class with `functools.partial`,
injecting base_url the same way a GitHub Enterprise deployment would
before team_discovery's own `from github import Auth, Github` (a
call-time-scoped import) resolves it.

Usage: venue_oracle_discover_github.py <base_url> <org_name> <token>
Prints the DiscoveredTeam list as JSON (list of dicts, TeamDiscoveryService's
own field names) to stdout.
"""

import asyncio
import functools
import json
import sys


def main() -> None:
    base_url, org_name, token = sys.argv[1], sys.argv[2], sys.argv[3]

    import github

    github.Github = functools.partial(github.Github, base_url=base_url)

    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )

    async def run():
        svc = TeamDiscoveryService(None, "venue-oracle-org")
        teams = await svc.discover_github(token=token, org_name=org_name)
        return [
            {
                "provider_type": t.provider_type,
                "provider_team_id": t.provider_team_id,
                "name": t.name,
                "description": t.description,
                "member_count": t.member_count,
                "associations": t.associations,
            }
            for t in teams
        ]

    result = asyncio.run(run())
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()
