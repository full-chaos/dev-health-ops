"""``dev-hops metrics daily``/``rebuild``/``complexity``/``dora``/
``release-impact``/``capacity`` -- dispatch to the Go worker (CHAOS-5055).

CHAOS-5055: these two verbs used to call ``run_daily_metrics_job``
(``job_daily.py``) directly, recomputing -- and rewriting -- every native
daily family (``file_hotspots`` included, a SUM-aggregated table with no
dedup on replay) on top of whatever the Go worker's own compatibility-bridge
call had already written for the same (org, day, repo) scope. The bare CLI
path never passed ``skip_families`` the way ``worker_metrics.py``'s HTTP
bridge does (see that module's ``skip_families`` handling), so any day the
worker had already touched got recomputed a second time in Python underneath
it.

Both verbs now enqueue through ``dev-health-workerctl metrics daily-start``
instead, which dispatches through the SAME ``StartRunTx`` coordinator
transaction the post-sync and fixed-schedule fanout paths use
(``internal/jobs/metrics/daily/manual_run.go``) -- the worker decides the
native/bridge split either way, so there is no second, unguarded write path.
This module intentionally imports nothing from ``job_daily`` (or any other
compute module): it only builds an argv and shells out.

Consequence of dispatching instead of computing in-process: per-run
overrides that only ever made sense for a direct Python call --
``--repo-name`` (resolve it to ``--repo-id`` yourself), ``--no-commits``,
``--sink``, ``--provider`` -- have no equivalent on the worker's dispatch
path and are no longer accepted here. ``rebuild`` is kept as a separate verb
for operator muscle memory, but both now build the identical request.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

from dev_health_ops.utils.cli import add_date_range_args, resolve_date_range

logger = logging.getLogger(__name__)

WORKERCTL_BIN_ENV = "DEV_HEALTH_WORKERCTL_BIN"
_WORKERCTL_BIN_NAME = "dev-health-workerctl"


def resolve_workerctl_binary() -> str | None:
    """Locate the ``dev-health-workerctl`` binary.

    ``DEV_HEALTH_WORKERCTL_BIN`` overrides PATH lookup (e.g. a build output
    directory that isn't itself on PATH); otherwise falls back to
    ``shutil.which``. Returns ``None`` if neither resolves.
    """
    override = os.getenv(WORKERCTL_BIN_ENV)
    if override:
        return override
    return shutil.which(_WORKERCTL_BIN_NAME)


def run_workerctl(args: list[str]) -> int:
    """Run ``dev-health-workerctl`` with ``args``, inheriting stdio.

    The subprocess's own stdout/stderr (its JSON result or error payload)
    reaches the operator directly; this only translates the exit code.
    """
    binary = resolve_workerctl_binary()
    if not binary:
        logger.error(
            "%s not found on PATH (set %s to its path if it isn't). Build it "
            "from cmd/dev-health-workerctl -- this command dispatches to the "
            "Go worker rather than computing metrics directly (CHAOS-5055).",
            _WORKERCTL_BIN_NAME,
            WORKERCTL_BIN_ENV,
        )
        return 1
    try:
        result = subprocess.run([binary, *args], stdout=sys.stdout, stderr=sys.stderr)
    except OSError as exc:
        logger.error("failed to execute %s: %s", binary, exc)
        return 1
    return result.returncode


def _daily_start_args(ns: argparse.Namespace) -> list[str]:
    end_day, backfill_days = resolve_date_range(ns)
    from_day = end_day - timedelta(days=backfill_days - 1)
    org = getattr(ns, "org", None) or ""
    if not org:
        raise SystemExit(
            "--org is required for metrics daily/rebuild (CHAOS-5055: the "
            "worker dispatch path is always org-scoped)."
        )
    args = [
        "metrics",
        "daily-start",
        "--org",
        org,
        "--day",
        from_day.isoformat(),
        "--to",
        end_day.isoformat(),
    ]
    for repo_id in getattr(ns, "repo_ids", None) or []:
        args += ["--repo-id", str(repo_id)]
    return args


def _cmd_metrics_daily(ns: argparse.Namespace) -> int:
    return run_workerctl(_daily_start_args(ns))


def _cmd_metrics_rebuild(ns: argparse.Namespace) -> int:
    return run_workerctl(_daily_start_args(ns))


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    daily = subparsers.add_parser(
        "daily",
        help=(
            "Dispatch daily metrics for one or more days. CHAOS-5055: enqueues "
            "through `dev-health-workerctl metrics daily-start` (the worker's "
            "own native/bridge split) instead of computing in Python directly."
        ),
    )
    add_date_range_args(daily)
    daily.add_argument(
        "--repo-id",
        action="append",
        dest="repo_ids",
        default=[],
        help="Repository UUID to scope this run to; repeatable. Omit for "
        "every org repository (deferred discovery, resolved by the worker).",
    )
    daily.set_defaults(func=_cmd_metrics_daily)

    rebuild = subparsers.add_parser(
        "rebuild",
        help=(
            "Alias for `daily` (CHAOS-5055): both dispatch the identical "
            "`dev-health-workerctl metrics daily-start` request. Kept as a "
            "separate verb for operator muscle memory."
        ),
    )
    add_date_range_args(rebuild)
    rebuild.add_argument(
        "--repo-id",
        action="append",
        dest="repo_ids",
        default=[],
        help="Repository UUID to rebuild; repeatable. Omit to rebuild all repos.",
    )
    rebuild.set_defaults(func=_cmd_metrics_rebuild)


# ---------------------------------------------------------------------------
# `dev-hops metrics complexity`/`dora`/`release-impact` -- dispatch to
# `dev-health-workerctl metrics remaining trigger-backstop` (CHAOS-5055).
# ---------------------------------------------------------------------------
#
# These three used to call their own compute function directly
# (job_complexity_db.run_complexity_db_job / job_dora / job_release_impact),
# always bypassing the worker entirely -- even where the worker's own kind is
# native (dora). They now dispatch through the SAME generalized
# `trigger-backstop` verb CHAOS-5016 built for work_item_attribution
# (internal/jobs/metrics/remaining/manual_backfill.go), reusing its
# dedup/generation semantics instead of a second, unguarded write path.
#
# CORRECTNESS NOTE (team-lead ruling, informed by lane-5016's review):
# dora/release_impact are "append_latest_generation" families in
# families.json -- a second live generation for the same (org, day)
# genuinely appends a duplicate row, with NO dedup on replay (CHAOS-4242).
# The verb itself enforces the policy this implies (day defaults to
# yesterday, `--today` must be explicit, `--review-evidence` is required)
# by family write-mode; this module does NOT re-implement that policy -- it
# always forwards `--today` when the requested day is today and always
# forwards `--review-evidence` when given, and lets the verb's own refusal
# (surfaced verbatim via inherited stderr) tell the operator when evidence
# is actually required. Do not "helpfully" invent evidence text here.
def _trigger_backstop_argv_for_day(
    *,
    family: str,
    org: str,
    day: date,
    today_utc: date,
    review_evidence: str | None,
) -> list[str]:
    argv = [
        "metrics",
        "remaining",
        "trigger-backstop",
        "--family",
        family,
        "--org",
        org,
        "--day",
        day.isoformat(),
    ]
    if day == today_utc:
        argv.append("--today")
    if review_evidence:
        argv += ["--review-evidence", review_evidence]
    return argv


def _dispatch_trigger_backstop_range(ns: argparse.Namespace, family: str) -> int:
    end_day, backfill_days = resolve_date_range(ns)
    from_day = end_day - timedelta(days=backfill_days - 1)
    org = getattr(ns, "org", None) or ""
    if not org:
        raise SystemExit(
            f"--org is required for metrics {family.replace('_', '-')} "
            "(CHAOS-5055: the worker dispatch path is always org-scoped)."
        )
    review_evidence = getattr(ns, "review_evidence", None)
    today_utc = datetime.now(timezone.utc).date()
    for offset in range(backfill_days):
        day = from_day + timedelta(days=offset)
        rc = run_workerctl(
            _trigger_backstop_argv_for_day(
                family=family,
                org=org,
                day=day,
                today_utc=today_utc,
                review_evidence=review_evidence,
            )
        )
        if rc != 0:
            return rc
    return 0


def _cmd_metrics_complexity(ns: argparse.Namespace) -> int:
    return _dispatch_trigger_backstop_range(ns, "complexity")


def _cmd_metrics_dora(ns: argparse.Namespace) -> int:
    return _dispatch_trigger_backstop_range(ns, "dora")


def _cmd_metrics_release_impact(ns: argparse.Namespace) -> int:
    return _dispatch_trigger_backstop_range(ns, "release_impact")


def _add_review_evidence_arg(parser: argparse.ArgumentParser) -> None:
    # Required uniformly for every family (team-lead ruling, CHAOS-5055):
    # the verb applies ONE flag policy regardless of a family's families.json
    # `replay` mode, rather than forking by mode -- so this is a real
    # argparse-level requirement here, not merely forwarded when given.
    parser.add_argument(
        "--review-evidence",
        dest="review_evidence",
        required=True,
        help="REQUIRED: justification text for this manual trigger (e.g. "
        '"CHAOS-1234 -- confirmed no automatic run has covered this day '
        'yet"). Forwarded verbatim to the verb, which is the actual '
        "enforcement point.",
    )


def register_trigger_backstop_commands(subparsers: argparse._SubParsersAction) -> None:
    complexity = subparsers.add_parser(
        "complexity",
        help=(
            "Dispatch file complexity metrics for one or more days. "
            "CHAOS-5055: enqueues through `dev-health-workerctl metrics "
            "remaining trigger-backstop --family complexity` instead of "
            "computing in Python directly."
        ),
    )
    add_date_range_args(complexity)
    _add_review_evidence_arg(complexity)
    complexity.set_defaults(func=_cmd_metrics_complexity)

    dora = subparsers.add_parser(
        "dora",
        help=(
            "Dispatch DORA metrics for one or more days. CHAOS-5055: "
            "enqueues through `dev-health-workerctl metrics remaining "
            "trigger-backstop --family dora` instead of computing in "
            "Python directly."
        ),
    )
    add_date_range_args(dora)
    _add_review_evidence_arg(dora)
    dora.set_defaults(func=_cmd_metrics_dora)

    release_impact = subparsers.add_parser(
        "release-impact",
        help=(
            "Dispatch release-impact metrics for one or more days. "
            "CHAOS-5055: enqueues through `dev-health-workerctl metrics "
            "remaining trigger-backstop --family release_impact` instead of "
            "computing in Python directly."
        ),
    )
    add_date_range_args(release_impact)
    _add_review_evidence_arg(release_impact)
    release_impact.set_defaults(func=_cmd_metrics_release_impact)


# ---------------------------------------------------------------------------
# `dev-hops metrics capacity` -- dispatch to `dev-health-workerctl metrics
# remaining trigger-backstop --family capacity` (CHAOS-5055).
# ---------------------------------------------------------------------------
#
# Unlike complexity/dora/release-impact, capacity scopes by team (not a
# calendar range) and needs a Monte Carlo GenerationSeed the worker derives
# itself -- there is no per-day loop here, one dispatch per invocation.
# --db/--history-days/--simulations/--target-items/--target-date/
# --work-scope-id/--dry-run had no equivalent on the dispatch path (the
# worker always uses the fixed-schedule fanout's own history_days=90/
# simulations=10000, and there is no synchronous preview -- every dispatch is
# a real, durable, async run) and are no longer accepted.
def _cmd_metrics_capacity(ns: argparse.Namespace) -> int:
    org = getattr(ns, "org", None) or ""
    if not org:
        raise SystemExit(
            "--org is required for metrics capacity (CHAOS-5055: the worker "
            "dispatch path is always org-scoped)."
        )
    team_id = getattr(ns, "team_id", None)
    all_teams = bool(getattr(ns, "all_teams", False))
    if bool(team_id) == all_teams:
        raise SystemExit(
            "exactly one of --team-id or --all-teams is required for metrics capacity."
        )
    today_utc = datetime.now(timezone.utc).date()
    day = getattr(ns, "day", None) or today_utc.isoformat()
    argv = [
        "metrics",
        "remaining",
        "trigger-backstop",
        "--family",
        "capacity",
        "--org",
        org,
        "--day",
        day,
        "--review-evidence",
        ns.review_evidence,
    ]
    if team_id:
        argv += ["--team", team_id]
    if all_teams:
        argv.append("--all-teams")
    if day == today_utc.isoformat():
        argv.append("--today")
    return run_workerctl(argv)


def register_capacity_trigger_command(subparsers: argparse._SubParsersAction) -> None:
    capacity = subparsers.add_parser(
        "capacity",
        help=(
            "Dispatch a capacity forecast. CHAOS-5055: enqueues through "
            "`dev-health-workerctl metrics remaining trigger-backstop "
            "--family capacity` instead of computing in Python directly."
        ),
    )
    capacity.add_argument(
        "--team-id",
        dest="team_id",
        default=None,
        help="Team id (uuid) to scope this forecast to; exactly one of "
        "--team-id/--all-teams is required.",
    )
    capacity.add_argument(
        "--all-teams",
        dest="all_teams",
        action="store_true",
        help="Forecast every team in the organization.",
    )
    capacity.add_argument(
        "--day",
        default=None,
        help="Dedup day for the run this trigger becomes, NOT a compute "
        "window (YYYY-MM-DD, UTC). Defaults to today UTC.",
    )
    _add_review_evidence_arg(capacity)
    capacity.set_defaults(func=_cmd_metrics_capacity)
