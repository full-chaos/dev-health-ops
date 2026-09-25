"""Live-Python oracle for `dho sync <target>` argument handling.

Reads a JSON list of cases from stdin: {"args": [...], "env": {...}}. For each
case it runs the REAL Python producer end to end -- build_parser().parse_args,
run_preflight_checks, run_sync_target -- with only the I/O seams replaced (the
store connection and the provider processors record what they would have been
called with; the Postgres credential lookup reports a miss). It prints one
JSON line: a list of typed results, one per case.

Every leaf value is tagged {"t": type, "v": string} so no bare JSON number or
boolean reaches the comparison.
"""

import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
from datetime import date

from dev_health_ops import cli as devhops_cli
from dev_health_ops.credentials import CredentialSource, GitHubCredentials
from dev_health_ops.processors import sync as sync_mod
from dev_health_ops.utils import cli as utils_cli

# The real functions, captured once: each case installs spies over them.
REAL_REPO_NAME = sync_mod._resolve_synthetic_repo_name
REAL_DATE_RANGE = sync_mod.resolve_date_range

TODAY = date(2026, 9, 25)
utils_cli.utc_today = lambda: TODAY

MANAGED_ENV = [
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "DATABASE_URL",
    "ORG_ID",
    "GITHUB_TOKEN",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY_PATH",
    "GITHUB_APP_INSTALLATION_ID",
    "GITHUB_URL",
    "GITHUB_BASE_URL",
    "GITLAB_TOKEN",
    "GITLAB_URL",
    "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN",
]


def tag(value):
    if value is None:
        return {"t": "null", "v": ""}
    if isinstance(value, bool):
        return {"t": "bool", "v": "true" if value else "false"}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    if isinstance(value, float):
        return {"t": "float", "v": repr(value)}
    return {"t": "str", "v": str(value)}


_PARSERS: dict[tuple[str | None, ...], argparse.ArgumentParser] = {}


def fresh_parser():
    # --gitlab-url's default is read from GITLAB_URL when the parser is built,
    # so one parser per distinct value.
    # The root parser's --db / --analytics-db defaults are read from the
    # environment at build time too, so the key is every managed variable.
    key = tuple(os.environ.get(name) for name in MANAGED_ENV)
    if key not in _PARSERS:
        _PARSERS[key] = devhops_cli.build_parser()
    return _PARSERS[key]


def run_case(case, keyfile):
    for name in MANAGED_ENV:
        os.environ.pop(name, None)
    for name, value in case.get("env", {}).items():
        os.environ[name] = value.replace("{KEYFILE}", keyfile)
    args = [a.replace("{KEYFILE}", keyfile) for a in case["args"]]
    # GITLAB_URL is read when the parser is built (default of --gitlab-url).
    parser = fresh_parser()

    err = io.StringIO()
    out = io.StringIO()
    try:
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(out):
            ns = parser.parse_args(["sync", *args])
    except SystemExit as exc:
        return {
            "stage": tag("argparse"),
            "code": tag(exc.code if exc.code is not None else 0),
        }
    # main(): _resolve_org, then the first-org lookup when no org resolved,
    # then the preflight. The lookup is the Postgres read this oracle replaces.
    devhops_cli._resolve_org(ns)
    org_source = (
        "flag" if ns.org_explicit else ("env" if ns.org is not None else "none")
    )
    if devhops_cli._should_resolve_org(ns):
        # The first-organization lookup reads Postgres; the oracle answers it.
        ns.org = "FIRST-ORG"
        org_source = "db-first"
    try:
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(out):
            devhops_cli.run_preflight_checks(parser, ns)
    except SystemExit as exc:
        return {"stage": tag("preflight"), "code": tag(exc.code)}

    recorded = {}

    synthetic = ns.provider == "synthetic"

    async def fake_run_with_store(db_uri, db_type, handler, org_id=None):
        recorded["sink_uri"] = db_uri
        recorded["store_org"] = org_id
        if synthetic:
            # The synthetic handler generates and writes rows through a real
            # store; everything it needs to decide is resolved before this
            # call, and what it does afterwards is recorded below.
            recorded["call"] = "synthetic"
            return
        await handler(object())

    def recorder(name, positional=()):
        async def record(*a, **kw):
            recorded["call"] = name
            merged = dict(kw)
            for key, value in zip(positional, a):
                merged[key] = value
            recorded["kwargs"] = merged

        return record

    def creds_miss(*a, **kw):
        # The Postgres credential lookup: report a hit so the plan proceeds.
        recorded["db_lookup"] = True
        return GitHubCredentials(
            token="db-token", source=CredentialSource.DATABASE, credential_name="db"
        )

    def repo_name_spy(namespace):
        name = REAL_REPO_NAME(namespace)
        recorded["repo_name"] = name
        return name

    def date_range_spy(namespace):
        end_day, days = REAL_DATE_RANGE(namespace)
        recorded["end_day"] = end_day
        recorded["days"] = days
        return end_day, days

    def finalize_spy(**kwargs):
        recorded["finalizes"] = True
        recorded["finalize_org"] = kwargs["org_id"]
        recorded["finalize_repo"] = kwargs["repo_full_name"]

    sync_mod.run_with_store = fake_run_with_store
    sync_mod.resolve_credentials_sync = creds_miss
    sync_mod.process_github_repo = recorder(
        "github_single", ("store", "owner", "repo", "credentials")
    )
    sync_mod.process_github_repos_batch = recorder("github_batch")
    sync_mod.process_gitlab_project = recorder(
        "gitlab_single", ("store", "project_id", "token", "gitlab_url")
    )
    sync_mod.process_gitlab_projects_batch = recorder("gitlab_batch")
    sync_mod.process_local_repo = recorder("local_repo")
    sync_mod.process_local_blame = recorder("local_blame")
    sync_mod._resolve_synthetic_repo_name = repo_name_spy
    sync_mod.resolve_date_range = date_range_spy
    sync_mod._complete_synthetic_sync_run = finalize_spy

    try:
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(out):
            rc = sync_mod.run_sync_target(ns)
    except SystemExit as exc:
        return {"stage": tag("exit"), "message": tag(str(exc.code))}
    except (ValueError, OverflowError) as exc:
        # detect_db_type on a URI that passed the scheme preflight: an
        # uncaught ValueError, a traceback and exit 1.
        return {"stage": tag("error"), "type": tag(type(exc).__name__)}
    except Exception as exc:  # a crash is a result, never a skip
        return {"stage": tag("crash"), "message": tag(type(exc).__name__)}

    return {
        "stage": tag("ok"),
        "rc": tag(rc),
        "run": describe(recorded, ns, org_source),
    }


def creds_view(credentials, db_lookup=False):
    if credentials is None:
        return None
    if db_lookup:
        return {"mode": "db", "name": "", "base_url": None}
    if getattr(credentials, "app_id", None):
        mode = "app"
    else:
        mode = "pat"
    return {
        "mode": mode,
        "name": getattr(credentials, "credential_name", ""),
        "base_url": getattr(credentials, "base_url", None),
    }


def describe(recorded, ns, org_source):
    kw = recorded.get("kwargs", {})
    call = recorded.get("call")
    if call == "synthetic":
        return {
            "call": tag(call),
            "sink_uri": tag(recorded.get("sink_uri")),
            "store_org": tag(recorded.get("store_org")),
            "org_source": tag(org_source),
            "db": tag(getattr(ns, "db", None)),
            "repo_name": tag(recorded.get("repo_name")),
            "days": tag(recorded.get("days")),
            "end_day": tag(recorded["end_day"].isoformat()),
            "defer_finalize": tag(bool(getattr(ns, "defer_finalize", False))),
            "finalizes": tag(bool(recorded.get("finalizes", False))),
            "finalize_org": tag(recorded.get("finalize_org")),
            "finalize_repo": tag(recorded.get("finalize_repo")),
        }
    since = kw.get("since")
    result = {
        "call": tag(call),
        "sink_uri": tag(recorded.get("sink_uri")),
        "store_org": tag(recorded.get("store_org")),
        "org_source": tag(org_source),
        "db": tag(getattr(ns, "db", None)),
        "db_lookup": tag(bool(recorded.get("db_lookup"))),
        "since": tag(since.isoformat() if since is not None else None),
    }
    flags = {}
    for key in (
        "sync_git",
        "sync_prs",
        "sync_cicd",
        "sync_deployments",
        "sync_incidents",
        "sync_security",
        "sync_tests",
        "blame_only",
    ):
        flags[key] = tag(kw.get(key))
    if call in ("local_repo", "local_blame"):
        result["repo_path"] = tag(kw.get("repo_path"))
        # local_blame carries no flags; local_repo carries sync_git/sync_prs only.
        flags = (
            {k: tag(kw.get(k)) for k in ("sync_git", "sync_prs")}
            if call == "local_repo"
            else {}
        )
    result["flags"] = flags
    if call == "github_single":
        result["owner"] = tag(kw.get("owner"))
        result["repo"] = tag(kw.get("repo"))
        result["max_commits"] = tag(kw.get("max_commits"))
        cred = creds_view(kw.get("credentials"), recorded.get("db_lookup", False))
    elif call == "github_batch":
        for key in (
            "org_name",
            "user_name",
            "pattern",
            "batch_size",
            "max_concurrent",
            "rate_limit_delay",
            "max_repos",
            "use_async",
            "max_commits_per_repo",
            "backfill_missing",
        ):
            result[key] = tag(kw.get(key))
        cred = creds_view(kw.get("token"), recorded.get("db_lookup", False))
    elif call == "gitlab_single":
        result["project_id"] = tag(kw.get("project_id"))
        result["gitlab_url"] = tag(kw.get("gitlab_url"))
        result["max_commits"] = tag(kw.get("max_commits"))
        result["token"] = tag(kw.get("token"))
        cred = None
    elif call == "gitlab_batch":
        for key in (
            "gitlab_url",
            "group_name",
            "pattern",
            "batch_size",
            "max_concurrent",
            "rate_limit_delay",
            "max_projects",
            "use_async",
            "max_commits_per_project",
            "backfill_missing",
        ):
            result[key] = tag(kw.get(key))
        result["token"] = tag(kw.get("token"))
        cred = None
    else:
        cred = None
    if cred is not None:
        result["credential_mode"] = tag(cred["mode"])
        result["credential_name"] = tag(cred["name"])
        result["credential_base_url"] = tag(cred["base_url"])
    return result


def main():
    cases = json.loads(sys.stdin.read())
    with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as handle:
        handle.write("-----BEGIN KEY-----\n")
        keyfile = handle.name
    results = [run_case(case, keyfile) for case in cases]
    os.unlink(keyfile)
    sys.stdout.write(json.dumps(results))


main()
