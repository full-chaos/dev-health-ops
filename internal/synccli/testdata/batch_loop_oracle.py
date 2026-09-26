"""Live-Python oracle for the batch LOOP of `dho sync --search` (CHAOS-6684).

A line protocol on stdin/stdout, one JSON object per line:

  {"provider": "github"|"gitlab", "repos": [{"name": "o/r", "fail": "", "delay_ms": 0}],
   "batch_size": "10", "max_concurrent": "4"}  ->  {"events": [...], "stage": ..., ...}

It runs the REAL process_github_repos_batch / process_gitlab_projects_batch with the
network and the store replaced: the repository listing returns the scripted
repositories (the listing itself has its own oracle), and the store's insert_repo, the
one call a batch makes for every repository it processes, records "start", waits, records
"end" and, when the script says so, raises. Every sync_* flag is off, so insert_repo is
all a repository costs. The events, in the order they happened, are what the Go loop is
compared on: which repositories ran, in what groups, how many at once, and how a failure
ended the run.
"""

import asyncio
import json
import sys
from typing import Any

from dev_health_ops.connectors.models import Repository
from dev_health_ops.processors import github as gh
from dev_health_ops.processors import gitlab as gl

EVENTS: list[list[str]] = []
SCRIPT: dict[str, dict] = {}


class DummyConnector:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass


class DummyIngestionSink:
    def __init__(self, store):
        pass

    async def insert_repo(self, repo):
        name = repo.repo
        EVENTS.append(["start", name])
        script = SCRIPT.get(name, {})
        await asyncio.sleep(script.get("delay_ms", 20) / 1000)
        EVENTS.append(["end", name])
        fail = script.get("fail", "")
        if fail == "error":
            raise RuntimeError(f"scripted failure of {name}")
        if fail == "ratelimit":
            raise gh.RateLimitException(f"scripted rate limit of {name}")


class DummyClient:
    def __init__(self, repos):
        self.repos = repos

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def list_projects(self, **kwargs):
        return self.repos

    def drain_usage_observations(self):
        return []


def repository(index: int, name: str) -> Repository:
    return Repository(
        id=index + 1,
        name=name.split("/")[-1],
        full_name=name,
        default_branch="main",
        url=f"https://example.invalid/{name}",
    )


def tag(value):
    return {"t": "str", "v": str(value)}


async def run_batch(request):
    global SCRIPT
    EVENTS.clear()
    SCRIPT = {r["name"]: r for r in request["repos"]}
    repos = [repository(i, r["name"]) for i, r in enumerate(request["repos"])]
    flags: dict[str, Any] = dict(
        sync_git=False,
        sync_prs=False,
        sync_cicd=False,
        sync_deployments=False,
        sync_incidents=False,
        sync_security=False,
        sync_tests=False,
        blame_only=False,
        backfill_missing=False,
    )
    batch_size = int(request["batch_size"])
    max_concurrent = int(request["max_concurrent"])
    if request["provider"] == "github":

        async def listing(connector, **kwargs):
            return repos

        setattr(gh, "GitHubConnector", DummyConnector)
        setattr(gh, "_list_github_repositories_for_batch", listing)
        setattr(gh, "IngestionSink", DummyIngestionSink)
        await gh.process_github_repos_batch(
            None,
            "token",
            org_name="acme",
            pattern="acme/*",
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            **flags,
        )
    else:
        setattr(gl, "GitLabConnector", DummyConnector)
        setattr(gl, "IngestionSink", DummyIngestionSink)
        setattr(gl, "_gitlab_code_client_from_connector", lambda c: DummyClient(repos))
        flags.pop("sync_incidents")
        await gl.process_gitlab_projects_batch(
            None,
            "token",
            group_name="acme",
            pattern="acme/*",
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            sync_incidents=False,
            **flags,
        )


def run(request):
    try:
        asyncio.run(run_batch(request))
    except (Exception, SystemExit) as exc:  # an uncaught traceback: exit 1
        return {
            "events": EVENTS,
            "stage": tag("error"),
            "type": tag(type(exc).__name__),
        }
    return {"events": EVENTS, "stage": tag("ok"), "type": tag("")}


def main():
    import logging

    logging.disable(logging.CRITICAL)
    for line in sys.stdin:
        print(json.dumps(run(json.loads(line))), flush=True)


main()
