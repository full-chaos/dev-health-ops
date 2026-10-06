"""Record what the frozen Python sent as the work-item fetch window.

The Python sync job is deleted (job_work_items.py went in 643edf960), so this
program cannot import it. It reads each file with `git show <commit>:<path>`,
takes the exact statements and functions out of the source by name, and
EXECUTES them. No expected value is written by hand: a statement that is not
found, or found twice, stops the program.

Run from the repository root (full history is needed):

    python3 internal/providersync/testdata/work_item_fetch_window_recorder.py \
        0dcecf34a^ > internal/providersync/testdata/work_item_fetch_window.frozen-python.golden.json
    go run ./internal/testsupport/recordedfiles/manifest -kind python-recorded \
        internal/providersync/testdata/work_item_fetch_window.frozen-python.golden.json

0dcecf34a^ is the last tree before the Go provider runtime (0dcecf34a, #1738).
Every leaf is written as {"t": type, "v": text} so that no bare JSON number
or date reaches a decoder.
"""

from __future__ import annotations
import __future__ as future

import ast
import json
import subprocess
import sys
from datetime import date, datetime, time, timedelta, timezone
from types import SimpleNamespace
from typing import Any

SRC = "src/dev_health_ops/"
ADAPTERS = SRC + "processors/dataset_adapters.py"
JOB = SRC + "metrics/job_work_items.py"
COMMON = SRC + "utils/datetime.py"
JIRA_PROVIDER = SRC + "providers/jira/provider.py"
JIRA_CLIENT = SRC + "providers/jira/client.py"
GITHUB_PROVIDER = SRC + "providers/github/provider.py"
GITHUB_CLIENT = SRC + "providers/github/client.py"
GITLAB_PROVIDER = SRC + "providers/gitlab/provider.py"
LINEAR_PROVIDER = SRC + "providers/linear/provider.py"
LINEAR_CLIENT = SRC + "providers/linear/client.py"

# How the job handed since_dt / until_dt to each provider. The program checks
# that each text is in the job exactly `count` times; it does not run them
# (they are calls into the provider clients).
JOB_CALL_SITES = [
    ("jira", "since=since_dt,\n                until=until_dt,", 1),
    (
        "github",
        "updated_since=since_dt,\n                        active_until=until_dt,",
        1,
    ),
    (
        "gitlab",
        "repos=discovered_repos,\n                since=since_dt,\n                status_mapping=",
        1,
    ),
    (
        "linear",
        "window=IngestionWindow(updated_since=since_dt, active_until=until_dt)",
        1,
    ),
]

# (name, unit since_at, unit before_at). The instants are what the worker read
# from the timestamptz columns, which Postgres hands over in UTC.
CASES = [
    (
        "hourly_window_inside_a_day",
        "2026-06-17T12:00:00+00:00",
        "2026-06-17T13:00:00+00:00",
    ),
    (
        "window_crosses_midnight_utc",
        "2026-06-17T23:30:00+00:00",
        "2026-06-18T00:30:00+00:00",
    ),
    (
        "multi_day_catch_up",
        "2026-06-14T09:15:27.123456+00:00",
        "2026-06-17T13:00:00+00:00",
    ),
    ("first_sync_of_90_days", "2026-03-19T13:00:00+00:00", "2026-06-17T13:00:00+00:00"),
    (
        "window_ends_exactly_at_midnight",
        "2026-06-17T23:00:00+00:00",
        "2026-06-18T00:00:00+00:00",
    ),
    (
        "instant_with_an_offset",
        "2026-06-17T01:30:00+02:00",
        "2026-06-17T02:30:00+02:00",
    ),
]


def source(commit: str, path: str) -> str:
    return subprocess.run(
        ["git", "show", f"{commit}:{path}"], check=True, capture_output=True, text=True
    ).stdout


def run(nodes: list[ast.stmt], namespace: dict[str, Any]) -> None:
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    code = compile(module, "<frozen>", "exec", flags=future.annotations.compiler_flag)
    exec(code, namespace)


def one(found: list[Any], what: str) -> Any:
    if len(found) != 1:
        raise SystemExit(f"{what}: found {len(found)}, want exactly 1")
    return found[0]


def function(tree: ast.AST, name: str) -> ast.FunctionDef:
    """The one def of `name` that is not a typing @overload stub."""
    return one(
        [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef)
            and n.name == name
            and not any(ast.unparse(d) == "overload" for d in n.decorator_list)
        ],
        f"def {name}",
    )


def assignment(tree: ast.AST, target: str, value_has: str) -> ast.stmt:
    """The one assignment to `target` whose source holds `value_has`."""
    found = []
    for node in ast.walk(tree):
        names: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            names = node.targets
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            continue
        if any(isinstance(n, ast.Name) and n.id == target for n in names):
            if value_has in ast.unparse(node):
                found.append(node)
    return one(found, f"{target} = ...{value_has}...")


def if_on(tree: ast.AST, name: str) -> ast.If:
    return one(
        [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.If)
            and isinstance(n.test, ast.Name)
            and n.test.id == name
        ],
        f"if {name}:",
    )


def leaf(value: Any) -> dict[str, str] | None:
    if value is None:
        # Bare null, as internal/testsupport/oraclecompare encodes a nil.
        return None
    if isinstance(value, bool):
        return {"t": "bool", "v": "true" if value else "false"}
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise SystemExit(f"naive datetime {value!r}")
        utc = value.astimezone(timezone.utc)
        return {"t": "datetime", "v": utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
    if isinstance(value, date):
        return {"t": "date", "v": value.isoformat()}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    if isinstance(value, str):
        return {"t": "str", "v": value}
    raise SystemExit(f"no leaf for {type(value).__name__}")


def main() -> None:
    commit = sys.argv[1]
    base = {
        "datetime": datetime,
        "date": date,
        "time": time,
        "timedelta": timedelta,
        "timezone": timezone,
        "Any": Any,
    }

    adapters = ast.parse(source(commit, ADAPTERS))
    job_text = source(commit, JOB)
    job = ast.parse(job_text)
    for provider, text, count in JOB_CALL_SITES:
        if job_text.count(text) != count:
            raise SystemExit(
                f"job call site for {provider}: {job_text.count(text)} != {count}"
            )
    sync_job = function(job, "run_work_items_sync_job")
    common = ast.parse(source(commit, COMMON))
    jira_provider = ast.parse(source(commit, JIRA_PROVIDER))
    # The Jira provider reduces the window to dates in two methods with the
    # same statements; the REST/JQL listing path is this one.
    jira_legacy = function(jira_provider, "_ingest_via_legacy_client")
    jira_client = ast.parse(source(commit, JIRA_CLIENT))
    github_provider = ast.parse(source(commit, GITHUB_PROVIDER))
    github_client = ast.parse(source(commit, GITHUB_CLIENT))
    # The provider gives iter_issues and iter_pull_requests the same window.
    github_text = source(commit, GITHUB_PROVIDER)
    for call in ("client.iter_issues(", "client.iter_pull_requests("):
        at = github_text.index(call)
        if (
            github_text.count(call) != 1
            or "since=since," not in github_text[at : at + 400]
        ):
            raise SystemExit(f"github provider: {call} does not pass since=since")
    pulls_at = github_text.index("client.iter_pull_requests(")
    if "until=until," not in github_text[pulls_at : pulls_at + 400]:
        raise SystemExit(
            "github provider: iter_pull_requests does not pass until=until"
        )
    gitlab_provider = ast.parse(source(commit, GITLAB_PROVIDER))
    linear_provider = ast.parse(source(commit, LINEAR_PROVIDER))
    linear_client = ast.parse(source(commit, LINEAR_CLIENT))

    to_utc_ns = dict(base)
    run([function(common, "to_utc")], to_utc_ns)
    to_utc = to_utc_ns["to_utc"]

    cases = []
    for name, since_text, before_text in CASES:
        since_at = datetime.fromisoformat(since_text)
        before_at = datetime.fromisoformat(before_text)
        # The worker held the instants as the database returned them: UTC.
        context = SimpleNamespace(
            window_start=since_at.astimezone(timezone.utc),
            window_end=before_at.astimezone(timezone.utc),
        )

        ns = dict(base)
        run(
            [
                function(adapters, "_window_backfill_days"),
                function(adapters, "_window_day"),
            ],
            ns,
        )
        ns["day"] = ns["_window_day"](context)
        ns["backfill_days"] = ns["_window_backfill_days"](context)
        run(
            [
                function(job, "_date_range"),
                assignment(sync_job, "days", "_date_range(day, backfill_days)"),
                assignment(sync_job, "since_dt", "min(days)"),
                assignment(sync_job, "until_dt", "max(days)"),
            ],
            ns,
        )
        since_dt, until_dt = ns["since_dt"], ns["until_dt"]
        ctx = SimpleNamespace(
            window=SimpleNamespace(updated_since=since_dt, active_until=until_dt)
        )

        jira = dict(base, _to_utc=to_utc, ctx=ctx)
        run(
            [
                assignment(jira_legacy, "updated_since", "ctx.window.updated_since"),
                assignment(jira_legacy, "active_until", "ctx.window.active_until"),
                function(jira_client, "build_jira_jql"),
            ],
            jira,
        )
        jql = jira["build_jira_jql"](
            project_key="OPS",
            updated_since=jira["updated_since"],
            active_until=jira["active_until"],
        )

        github = dict(base, _to_utc=to_utc, ctx=ctx)
        run(
            [
                assignment(github_provider, "since", "ctx.window.updated_since"),
                assignment(github_provider, "until", "ctx.window.active_until"),
                function(github_provider, "within_active_window"),
            ],
            github,
        )
        end_day = until_dt.date()
        probes = [
            (
                "one_second_after_the_unit_end",
                context.window_end + timedelta(seconds=1),
            ),
            (
                "last_second_of_the_last_day",
                datetime.combine(end_day, time(23, 59, 59), tzinfo=timezone.utc),
            ),
            (
                "first_second_of_the_next_day",
                datetime.combine(
                    end_day + timedelta(days=1), time.min, tzinfo=timezone.utc
                ),
            ),
        ]
        kept = {
            label: github["within_active_window"](SimpleNamespace(updated_at=at))
            for label, at in probes
        }

        # The pull-request list (client.iter_pull_requests): it is sorted by
        # update time, newest first; iteration STOPS at the first pull request
        # updated before `since` and SKIPS one updated after `until`.
        pulls = dict(base)
        run(
            [
                function(github_client, "_parse_github_datetime"),
                function(github_client, "_item_updated_before"),
                function(github_client, "_item_updated_after"),
            ],
            pulls,
        )
        cutoff = pulls["_parse_github_datetime"](github["since"])
        until_cutoff = pulls["_parse_github_datetime"](github["until"])
        first_day = since_dt.date()
        pull_probes = [
            (
                "a_first_second_of_the_next_day",
                datetime.combine(
                    end_day + timedelta(days=1), time.min, tzinfo=timezone.utc
                ),
            ),
            (
                "b_last_second_of_the_last_day",
                datetime.combine(end_day, time(23, 59, 59), tzinfo=timezone.utc),
            ),
            (
                "c_one_second_after_the_unit_end",
                context.window_end + timedelta(seconds=1),
            ),
            (
                "d_one_second_before_the_unit_start",
                context.window_start - timedelta(seconds=1),
            ),
            (
                "e_start_of_the_first_day",
                datetime.combine(first_day, time.min, tzinfo=timezone.utc),
            ),
            (
                "f_last_second_before_the_first_day",
                datetime.combine(first_day, time.min, tzinfo=timezone.utc)
                - timedelta(seconds=1),
            ),
        ]
        pulls_kept = {}
        stopped = False
        for label, at in pull_probes:
            item = SimpleNamespace(updated_at=at)
            if not stopped and pulls["_item_updated_before"](item, cutoff):
                stopped = True
            pulls_kept[label] = not stopped and not pulls["_item_updated_after"](
                item, until_cutoff
            )

        gitlab = dict(base, _to_utc=to_utc, ctx=ctx)
        run(
            [assignment(gitlab_provider, "updated_after", "ctx.window.updated_since")],
            gitlab,
        )

        linear = dict(base, _to_utc=to_utc, ctx=ctx, updated_at_filter={})
        run(
            [
                assignment(
                    linear_provider, "updated_after", "ctx.window.updated_since"
                ),
                assignment(
                    linear_provider, "updated_before", "ctx.window.active_until"
                ),
                if_on(function(linear_client, "iter_issues_pages"), "updated_after"),
                if_on(function(linear_client, "iter_issues_pages"), "updated_before"),
            ],
            linear,
        )
        linear_filter = linear["updated_at_filter"]

        cases.append(
            {
                "name": name,
                "unit": {"since_at": leaf(since_at), "before_at": leaf(before_at)},
                # Each dict below is compared WHOLE with what the Go route
                # sends; nothing in it is informational.
                "python": {
                    "window": {
                        "day": leaf(ns["day"]),
                        "backfill_days": leaf(ns["backfill_days"]),
                        "first_day": leaf(min(ns["days"])),
                        "last_day": leaf(max(ns["days"])),
                        "day_count": leaf(len(ns["days"])),
                        "since_dt": leaf(since_dt),
                        "until_dt": leaf(until_dt),
                    },
                    "github": {
                        "since": leaf(github["since"]),
                        # `until` is a client-side filter: what it keeps.
                        "kept": {
                            label: {"updated_at": leaf(at), "kept": leaf(kept[label])}
                            for label, at in probes
                        },
                        "pull_requests_kept": {
                            label: {
                                "updated_at": leaf(at),
                                "kept": leaf(pulls_kept[label]),
                            }
                            for label, at in pull_probes
                        },
                    },
                    "gitlab": {
                        "updated_after": leaf(gitlab["updated_after"]),
                        # The job passes `since=since_dt` only to GitLab.
                        "updated_before": leaf(None),
                    },
                    "linear": {
                        "gte": leaf(datetime.fromisoformat(linear_filter["gte"])),
                        "lte": leaf(datetime.fromisoformat(linear_filter["lte"])),
                    },
                    "jira": {"jql": leaf(jql)},
                },
                # What Python gives for the instant with its OWN offset (not
                # converted to UTC first). Go converts to UTC; the two differ
                # only when the offset moves the instant to another day.
                "python_with_own_offset": {
                    "first_day": leaf(since_at.date()),
                    "last_day": leaf(before_at.date()),
                },
            }
        )

    resolved = subprocess.run(
        ["git", "rev-parse", commit], check=True, capture_output=True, text=True
    ).stdout.strip()
    json.dump(
        {
            "recorded_from": {"commit": resolved, "argument": commit},
            "program": "internal/providersync/testdata/work_item_fetch_window_recorder.py",
            "cases": cases,
        },
        sys.stdout,
        indent=2,
        sort_keys=True,
    )
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
