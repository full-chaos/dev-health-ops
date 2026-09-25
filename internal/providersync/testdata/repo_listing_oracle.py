"""Live-Python oracle for the --search batch repository listing.

Reads a JSON list of cases from stdin. Each case is
{"provider": "github"|"gitlab", "listing": {...}, "script": [...]} where a
script entry is {"uri": "<path>?<sorted query>", "status": int, "link": str,
"headers": {...}, "body": <json>}. The REAL Python listing runs against an
httpx.MockTransport that answers from the script (an unscripted URI answers
404) and records every request. One JSON line comes back: a list of
{"results": [[name, full_name, id], ...], "requests": [uri, ...],
"error": <exception class name or null>}.
"""

import asyncio
import json
import sys
from urllib.parse import parse_qsl

import httpx

from dev_health_ops.processors import github as github_processor
from dev_health_ops.processors import gitlab as gitlab_processor
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient
from dev_health_ops.providers.gitlab.code_client import GitLabCodeClient


def canonical(request: httpx.Request) -> str:
    query = sorted(parse_qsl(request.url.query.decode(), keep_blank_values=True))
    text = "&".join(f"{key}={value}" for key, value in query)
    path = request.url.raw_path.decode().split("?", 1)[0]
    return f"{path}?{text}" if text else path


def transport_for(script, requests):
    by_uri = {entry["uri"]: entry for entry in script}

    def handler(request: httpx.Request) -> httpx.Response:
        uri = canonical(request)
        requests.append(uri)
        entry = by_uri.get(uri)
        if entry is None:
            return httpx.Response(404, json={"message": "not scripted"})
        headers = dict(entry.get("headers") or {})
        if entry.get("link"):
            headers["Link"] = entry["link"]
        return httpx.Response(
            entry.get("status", 200), json=entry["body"], headers=headers
        )

    return httpx.MockTransport(handler)


def run_github(case, requests):
    listing = case["listing"]
    transport = transport_for(case["script"], requests)

    def client_from_connector(_connector):
        return GitHubCodeClient(
            auth=GitHubAuth(
                token="t",
                base_url="https://api.github.com" + case.get("base_path", ""),
            ),
            transport=transport,
        )

    github_processor._github_code_client_from_connector = client_from_connector
    repos = asyncio.run(
        github_processor._list_github_repositories_for_batch(
            object(),
            org_name=listing.get("org") or None,
            user_name=listing.get("user") or None,
            pattern=listing.get("pattern") or None,
            max_repos=listing.get("max"),
            usage_sink=None,
        )
    )
    return [[r.name, r.full_name, 0] for r in repos]


async def gitlab_list(case, transport):
    listing = case["listing"]
    group = gitlab_processor._gitlab_effective_group(
        listing.get("group") or None, listing.get("pattern") or None
    )
    client = GitLabCodeClient(
        private_token="t",
        base_url="https://gitlab.com" + case.get("base_path", ""),
        transport=transport,
    )
    try:
        repos = await client.list_projects(
            group_name=group,
            pattern=listing.get("pattern") or None,
            max_projects=listing.get("max"),
        )
    finally:
        await client.close()
    return [[r.name, r.full_name, r.id] for r in repos]


def run_gitlab(case, requests):
    return asyncio.run(gitlab_list(case, transport_for(case["script"], requests)))


def main():
    cases = json.loads(sys.stdin.read())
    out = []
    for case in cases:
        requests: list[str] = []
        error = None
        results: list = []
        try:
            runner = run_github if case["provider"] == "github" else run_gitlab
            results = runner(case, requests)
        except Exception as exc:  # an error is a result, never a skip
            error = type(exc).__name__
        out.append({"results": results, "requests": requests, "error": error})
    sys.stdout.write(json.dumps(out))


main()
