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
script monkeypatches the `github.Github` NAME with a real subclass that
injects base_url, the same way a GitHub Enterprise deployment would,
before team_discovery's own `from github import Auth, Github` (a
call-time-scoped import) resolves it. A subclass is used rather than
`functools.partial` specifically so the assignment stays type-correct: a
`type[_BaseURLInjectedGithub]` is a valid `type[Github]` by ordinary
subtype covariance, unlike a `functools.partial` object, which is not a
`type[Github]` at all and needs a suppression to assign.

Usage: venue_oracle_discover_github.py <base_url> <org_name> <token>
Prints the route's full TeamDiscoverResponse body (compact JSON, as FastAPI
renders it) to stdout.
"""

import asyncio
import json
import sys
from typing import Any


def main() -> None:
    base_url, org_name, token = sys.argv[1], sys.argv[2], sys.argv[3]

    import github

    class _BaseURLInjectedGithub(github.Github):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs.setdefault("base_url", base_url)
            super().__init__(*args, **kwargs)

    # setattr, not `github.Github = ...`: mypy statically treats a module-
    # level class NAME as a type binding and refuses ANY direct
    # reassignment ("Cannot assign to a type") regardless of subtype
    # compatibility. Going through setattr mutates the same module
    # attribute at runtime -- team_discovery.py's own later
    # `from github import Github` still resolves to this subclass -- but
    # isn't a static assignment mypy's type-binding rule applies to.
    setattr(github, "Github", _BaseURLInjectedGithub)

    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )

    async def run() -> str:
        from dev_health_ops.api.admin.schemas import TeamDiscoverResponse

        svc = TeamDiscoveryService(None, "venue-oracle-org")
        teams = await svc.discover_github(token=token, org_name=org_name)
        # The route's own response envelope, serialized the way FastAPI does
        # (model_dump(mode="json") then compact json.dumps), so the Go side
        # can compare the WHOLE body byte for byte, not selected fields.
        response = TeamDiscoverResponse(
            provider="github",
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
