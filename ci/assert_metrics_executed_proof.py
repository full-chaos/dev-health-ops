#!/usr/bin/env python3
"""Executed-proof readback for the metrics.daily family (CHAOS-4266).

CHAOS-4263/CHAOS-4264 ran undetected on prod and local for a week because
every existing check asserted that a job *ran* ("trigger fired", "zero rows
logged") rather than that it *produced rows for the org it was supposed to
compute for*. This script is the one readback assertion shared by the CI
live-e2e gate and `ci/local_validate.sh`'s `metrics_readback` stage, so both
callers judge "did the pipeline work" by the same oracle: rows land in
ClickHouse for the seeded org, with `computed_at` at or after the run start,
and every `repo_id` those rows carry is a repo ClickHouse actually knows about
for that org.

CHAOS-4775: `family_readback`'s row query is bound to `org_id = {org}` (not
interpolated), not just the org's repo_id set -- a repo_id set alone lets a
foreign/blank-org row for the target org's own repo satisfy the readback.

Exit codes: 0 = every requested family produced valid rows; 1 = at least one
family is zero-rows-with-source-data or produced a repo_id ClickHouse repos
does not have (the exact CHAOS-4263 defect shape: dead/mismatched repo ids).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime
from typing import TypedDict

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink


class FamilyRowCount(TypedDict):
    rows: int
    latest_computed_at: str


# One representative table per family, keyed (repo_id, day) with a
# `computed_at` column (src/dev_health_ops/migrations/clickhouse/
# 004_quality_delivery_metrics.sql and friends). families.json
# (internal/jobs/metrics/daily/families.json) lists every table each compute
# call writes; checking the first is checking the call executed, since every
# table in one family's `writes` list is written from the same in-process
# call in job_daily.py.
REPO_DAY_FAMILIES: dict[str, str] = {
    "cicd": "cicd_metrics_daily",
    "deploy": "deploy_metrics_daily",
    "incident": "incident_metrics_daily",
    "testops_pipeline": "testops_pipeline_metrics_daily",
    "testops_test": "testops_test_metrics_daily",
    # CHAOS-4284: added when testops_coverage went native alongside
    # testops_pipeline/testops_test (which were already here). Same
    # (repo_id, day) shape as its two siblings -- one row per repo that
    # had a coverage snapshot attached to a pipeline run that day.
    "testops_coverage": "testops_coverage_metrics_daily",
    "repo_user_commit": "repo_metrics_daily",
    "dora": "dora_metrics_daily",
    # "complexity" is the family internal/jobs/metrics/daily/families.json
    # calls "file_hotspots" (native FileHotspotsExecutor;
    # CHAOS-5234/CHAOS-3092 deleted the old Python compute_file_hotspots
    # path, src/dev_health_ops/metrics/hotspots.py, whole-file) -- its table
    # is file_metrics_daily. The actual `metrics complexity` CLI command (a
    # separate job, job_complexity.py/job_complexity_db.py, not in
    # families.json at all) writes repo_complexity_daily/
    # file_complexity_snapshots instead. This dict originally pointed
    # "complexity" at file_hotspots' table -- caught during CHAOS-3092 Wave 1
    # scoping (lane-3092-scope) -- fixed so each family name here checks the
    # table its own compute call actually writes.
    "complexity": "repo_complexity_daily",
    "file_hotspots": "file_metrics_daily",
    # CHAOS-4277: file_risk_hotspots is file_hotspots' sibling family
    # (same deleted source file, formerly hotspots.py:113
    # compute_file_risk_hotspots -- now native, FileRiskHotspotsExecutor)
    # writing file_hotspot_daily. Unlike every other REPO_DAY_FAMILIES entry it has
    # no same-day activity gate (see FileRiskHotspotsExecutor's doc comment,
    # internal/jobs/metrics/daily/file_hotspots_native_executor.go): it
    # unions churned files with complexity-only files, so family_readback's
    # generic (repo_id, count, max(computed_at)) grouping needs no file_path
    # awareness -- a repo with either churn OR a complexity snapshot in the
    # window produces rows here.
    "file_risk_hotspots": "file_hotspot_daily",
    # CHAOS-4279: review_edges_daily is (repo_id, day)-shaped with a
    # computed_at column, so it needs no new readback shape -- it slots into
    # the repo-keyed family above unchanged.
    "review_edges": "review_edges_daily",
    # CHAOS-4290, r2 finding #4: ic_finalize does NOT belong here -- its
    # first write target, user_metrics_daily, needs the extra author_email
    # identity column REPO_DAY_FAMILIES' generic (repo_id, day, computed_at)
    # shape cannot express (it is a per-author, not a per-repo-day, table).
    # It is correctly classified below, in SYNTHESIZED_REPO_ID_FAMILIES,
    # only. An entry here as well (this branch's own stale leftover from
    # before that reclassification landed) would shadow/duplicate the
    # --families choice -- test_ic_finalize_is_not_double_registered exists
    # specifically to catch this class of drift.
}

# uuid5 namespace + payload MUST match
# internal/jobs/metrics/daily/icfinalize/executor.go's synthesizedRepoNamespace
# and SynthesizedRepoID exactly -- see synthesized_repo_id() below.
_SYNTHESIZED_REPO_NAMESPACE = uuid.UUID("1b4e28ba-2fa1-11d2-883f-b9a761bde3fb")


def synthesized_repo_id(org_id: str, identity_id: str) -> str:
    """Python mirror of SynthesizedRepoID (CHAOS-4290).

    Go computes `uuid.NewSHA1(synthesizedRepoNamespace, []byte(orgID+"\\x1f"+identityID))`
    -- RFC4122 UUIDv5 (SHA1) over the namespace bytes plus a UTF-8 payload.
    Python's `uuid.uuid5` is the same construction over the same namespace and
    payload bytes, so this reproduces the identical UUID for the identical
    (org_id, identity_id) pair without needing to shell out to Go.
    """
    return str(uuid.uuid5(_SYNTHESIZED_REPO_NAMESPACE, f"{org_id}\x1f{identity_id}"))


# CHAOS-4290, r2 finding #4: ic_finalize's first write target,
# user_metrics_daily, mixes TWO id spaces in the same repo_id column -- a
# git-backed identity keeps its real repo_id, but a work-item-only identity
# (no git record at all) gets a deterministic per-identity SynthesizedRepoID
# instead (executor.go's writeUserMetrics). REPO_DAY_FAMILIES' generic
# stray-check assumes every repo_id is a live repo; applied to this table it
# flags every synthesized row as a CHAOS-4263 dead id, and family_readback's
# repo_ids-only filter can undercount total_rows to zero for an org whose
# identities are all work-item-only. (table, identity_column) per family.
SYNTHESIZED_REPO_ID_FAMILIES: dict[str, tuple[str, str]] = {
    # CHAOS-4290: ic_finalize is FINALIZE-scoped (families.json's phase_note --
    # it runs once per run, after every partition, not once per partition).
    # The second write, ic_landscape_rolling_30d, is not checked separately
    # for the same reason no other family here checks every table in its own
    # `writes` list: one table in a single in-process compute call proves the
    # call executed. author_email is the identity column: writeUserMetrics
    # writes the SAME identity string to both author_email and identity_id.
    "ic_finalize": ("user_metrics_daily", "author_email"),
}


def synthesized_repo_readback(
    client,
    table: str,
    identity_column: str,
    org_id: str,
    repo_ids: set[str],
    run_start: datetime,
) -> tuple[int, set[str]]:
    """Like family_readback, but a repo_id is valid if it is EITHER a live
    repo OR the deterministic synthesized id for that row's own identity --
    the two id spaces this table's rows deliberately mix (CHAOS-4290).

    Returns (total_rows, stray_repo_ids) rather than the per-repo dict
    family_readback returns: a synthesized id is per-identity, not
    org-wide-live, so a "repos with rows" summary would just print every
    identity's own private synthesized UUID with nothing for a reader to act
    on.
    """
    result = client.query(
        f"""
        SELECT repo_id, {identity_column}, count() AS n
        FROM {table}
        WHERE org_id = {{org_id:String}}
          AND computed_at >= {{run_start:DateTime64(6)}}
        GROUP BY repo_id, {identity_column}
        """,
        parameters={"org_id": org_id, "run_start": run_start},
    )
    total_rows = 0
    stray: set[str] = set()
    for repo_id_value, identity_id, count_value in result.result_rows:
        repo_id = str(repo_id_value)
        total_rows += int(count_value)
        if repo_id in repo_ids:
            continue
        if repo_id == synthesized_repo_id(org_id, identity_id):
            continue
        stray.add(repo_id)
    return total_rows, stray


# Team-keyed families (CHAOS-4276): unlike REPO_DAY_FAMILIES, these tables are
# NOT (repo_id, day)-shaped -- team_metrics_daily is keyed (team_id, day) and
# scoped only by org_id. A commit whose repo/author resolve to no team lands
# under the synthetic "unassigned" team_id (compute_team_wellbeing_metrics_daily's
# unknown_team_id default), which is a VALID row, not a dead id -- so this
# family gets its own readback (team_readback) rather than reusing
# family_readback/unscoped_repo_ids, which would incorrectly demand every
# team_id be a live repo_id.
TEAM_DAY_FAMILIES: dict[str, str] = {
    "team_wellbeing": "team_metrics_daily",
    # CHAOS-4365 item 2 (4347-C): team_cognitive_load_daily is (org_id,
    # team_id, day)-shaped like team_metrics_daily -- team_id is resolved
    # from OWNERSHIP only (team_repo_ownership / teams.repo_patterns), never
    # from a repo/author with no owning team, so unlike team_wellbeing there
    # is no synthetic "unassigned" bucket here: an org with zero owned repos
    # for any team on a given day legitimately writes zero rows for it.
    "team_cognitive_load": "team_cognitive_load_daily",
    # CHAOS-5051: team_complexity_daily is (org_id, team_id, day)-shaped
    # identically to team_cognitive_load_daily above, and the SAME ownership-
    # only resolution contract applies -- team_id comes from
    # team_repo_ownership/teams.repo_patterns exclusively, never a synthetic
    # "unassigned" fallback, so an org with zero owned repos for a team on a
    # given day legitimately writes zero rows for it (see
    # buildTeamComplexityRows' own "unowned repo contributes to no team"
    # contract, proven by team_complexity_test.go).
    "team_complexity": "team_complexity_daily",
    # CHAOS-4278: work_item_state_durations_daily has no repo_id column at
    # all (day, provider, work_scope_id, team_id, status) -- it is neither
    # REPO_DAY_FAMILIES- nor a repo-scoped TEAM_DAY_FAMILIES-shaped table,
    # but it IS (org_id, team_id, computed_at)-shaped, which is all
    # team_readback actually requires. "unassigned" is a legitimate team_id
    # here too (an item whose primary attribution row is missing or has a
    # NULL team_id normalizes to "unassigned" -- see
    # WorkItemStateExecutor/resolveWorkItemPrimaryTeam), so this reuses
    # team_readback rather than inventing a fourth shape.
    "work_item_state": "work_item_state_durations_daily",
    # CHAOS-3092: recommendations_daily is keyed (org_id, team_id, rule_id,
    # window_end), also team-shaped and no repo_id column at all, like
    # work_item_state above. team_readback filters on `computed_at`, not
    # `day`/`window_end` -- that column is the RMT VERSION column here, freshly
    # stamped on every write (see recommendations_native_clickhouse.go's
    # writeRecommendations doc comment), so it works unchanged. Zero rows here
    # is a real proof failure, not a false positive from the readiness gate's
    # fail-open: an org with no daily_metrics_runs row for the day PROCEEDS
    # (DailyMetricsReady's absent-row case), and this org has one only once
    # metrics.daily has actually run for it -- which every other family in
    # this script already requires.
    "recommendations": "recommendations_daily",
}

# Scope-keyed families (CHAOS-4287): a THIRD shape, needed because
# compounding_risk_daily is keyed (org_id, scope, scope_id, day, computed_at)
# and has no repo_id column at all. Its repo-scope rows carry the repo id in
# `scope_id`, but as a String rather than a UUID, alongside team-scope rows
# under the same table -- so neither family_readback (typed repo_id, no scope
# discriminator) nor team_readback (would silently count team rows as proof
# the repo path ran) is correct here.
#
# The value is the table; the readback below pins scope='repo' and cross-checks
# scope_id against live_repo_ids, so this shape keeps the SAME dead-id oracle
# family_readback has (CHAOS-4263) rather than trading it away for a looser
# org-only check. Team-scope rows are deliberately NOT proven here: they are
# written by a DIFFERENT family in a DIFFERENT scope (compounding_risk_team,
# run-scoped, from the finalize handler), so counting them would let this
# gate pass with the repo-scope partition path dead. The original reason was
# that those rows came from Python's run_daily_metrics_finalize; CHAOS-5084
# ported that writer to a native finalize executor and CHAOS-3092 (PR-A)
# deleted the bridge entirely, but the scope pin stays for the reason above
# -- proving compounding_risk_team is a separate gate, not this one.
SCOPE_ID_REPO_FAMILIES: dict[str, str] = {
    "compounding_risk": "compounding_risk_daily",
}

# Scope-KEY families (CHAOS-4288): a FOURTH shape. The benchmarking tables are
# keyed (metric_name, scope_type, scope_key, period_end) -- no repo_id, no
# team_id, and no `day` column at all. Their repo-scope rows carry the repo id
# in `scope_key` (a String), so the same dead-id oracle applies, but the column
# NAMES differ from compounding_risk's scope/scope_id pair, which is why this
# cannot reuse SCOPE_ID_REPO_FAMILIES' query.
#
# One representative table per family, as everywhere else here:
# testops_metric_baselines is written from the same in-process call as the
# other five, so checking it is checking the call executed.
SCOPE_KEY_FAMILIES: dict[str, str] = {
    "benchmarking": "testops_metric_baselines",
}


def live_repo_ids(client, org_id: str) -> set[str]:
    """The org's real ClickHouse repo ids.

    Byte-for-byte the same query as
    internal/jobs/metrics/daily/clickhouse.go
    ClickHouseRepositoryDiscoverer.RepositoryIDs. That query IS the oracle
    CHAOS-4263 found daily_metrics_partitions bypassing (it was persisting
    Postgres integration_sources.id instead) -- validating readback against
    a hand-rolled re-derivation of "the real repos" here would just move the
    same id-space bug into the test.
    """
    result = client.query(
        """
        SELECT id FROM (
          SELECT id, argMax(tuple(repo, settings, provider), last_synced) AS latest
          FROM repos
          WHERE org_id = {org_id:String}
          GROUP BY org_id, id
        )
        ORDER BY id
        """,
        parameters={"org_id": org_id},
    )
    return {str(row[0]) for row in result.result_rows}


def family_readback(
    client, table: str, org_id: str, repo_ids: set[str], run_start: datetime
) -> dict[str, FamilyRowCount]:
    """Rows this table holds for THIS org's repos (CHAOS-4775: org-scoped).

    Before CHAOS-4775 this query filtered only by `repo_id IN {repo_ids}` --
    `repo_ids` itself already came from `live_repo_ids(client, org_id)`, so a
    row with the TARGET org's real repo_id but a foreign/blank org_id (e.g. a
    regression that drops org_id on write, or a cross-tenant repo_id
    collision) satisfied the readback even though the target org's own
    GraphQL reader -- which filters by org_id directly -- would see zero
    rows. Codex proved this by executed repro against a simulated fresh row
    with org_id=ORG-B, repo_id=<ORG-A's real repo>: exit_code 0. Binding
    org_id here closes it; the predicate only narrows what already-correct,
    already-org-scoped data satisfies.
    """
    if not repo_ids:
        return {}
    result = client.query(
        f"""
        SELECT repo_id, count() AS n, max(computed_at) AS latest
        FROM {table}
        WHERE org_id = {{org_id:String}}
          AND repo_id IN {{repo_ids:Array(UUID)}}
          AND computed_at >= {{run_start:DateTime64(6)}}
        GROUP BY repo_id
        """,
        parameters={
            "org_id": org_id,
            "repo_ids": sorted(repo_ids),
            "run_start": run_start,
        },
    )
    return {
        str(row[0]): {"rows": int(row[1]), "latest_computed_at": str(row[2])}
        for row in result.result_rows
    }


def unscoped_repo_ids(client, table: str, run_start: datetime) -> set[str]:
    """repo_ids the table carries for THIS run, regardless of org.

    Used only to detect the CHAOS-4263 failure mode directly: a family that
    wrote rows with a repo_id ClickHouse repos does not have at all (dead
    id), as opposed to simply writing zero rows.
    """
    result = client.query(
        f"SELECT DISTINCT repo_id FROM {table} WHERE computed_at >= {{run_start:DateTime64(6)}}",
        parameters={"run_start": run_start},
    )
    return {str(row[0]) for row in result.result_rows}


def team_readback(
    client, table: str, org_id: str, run_start: datetime
) -> dict[str, FamilyRowCount]:
    """Team-keyed counterpart of family_readback (CHAOS-4276).

    Scoped by org_id directly rather than an enumerated id set: team_id has
    no separate "live id" oracle the way repo_id does (repos table) --
    "unassigned" is a legitimate team_id with no corresponding row anywhere
    else, so there is nothing to cross-check it against.
    """
    result = client.query(
        f"""
        SELECT team_id, count() AS n, max(computed_at) AS latest
        FROM {table}
        WHERE org_id = {{org_id:String}}
          AND computed_at >= {{run_start:DateTime64(6)}}
        GROUP BY team_id
        """,
        parameters={"org_id": org_id, "run_start": run_start},
    )
    return {
        str(row[0]): {"rows": int(row[1]), "latest_computed_at": str(row[2])}
        for row in result.result_rows
    }


def scope_key_repo_readback(
    client, table: str, org_id: str, repo_ids: set[str], run_start: datetime
) -> dict[str, FamilyRowCount]:
    """scope_id_repo_readback's counterpart for scope_type/scope_key tables.

    Same two properties: `scope_type = 'repo'` is pinned so team- and
    global-scope rows cannot stand in as proof the repo path ran, and
    `scope_key` is bound as Array(String) because it is a String column.
    """
    if not repo_ids:
        return {}
    result = client.query(
        f"""
        SELECT scope_key, count() AS n, max(computed_at) AS latest
        FROM {table}
        WHERE org_id = {{org_id:String}}
          AND scope_type = 'repo'
          AND scope_key IN {{repo_ids:Array(String)}}
          AND computed_at >= {{run_start:DateTime64(6)}}
        GROUP BY scope_key
        """,
        parameters={
            "org_id": org_id,
            "repo_ids": sorted(repo_ids),
            "run_start": run_start,
        },
    )
    return {
        str(row[0]): {"rows": int(row[1]), "latest_computed_at": str(row[2])}
        for row in result.result_rows
    }


def scope_id_repo_readback(
    client, table: str, org_id: str, repo_ids: set[str], run_start: datetime
) -> dict[str, FamilyRowCount]:
    """family_readback's counterpart for scope/scope_id-keyed tables (CHAOS-4287).

    Two differences from family_readback, both load-bearing:

    * ``scope = 'repo'`` is pinned. The table holds team-scope rows too,
      written by the run-scoped ``compounding_risk_team`` finalize family
      (native since CHAOS-5084) rather than by this partition-scope family --
      counting them would let this gate pass with the native repo path dead.
    * ``scope_id`` is a String, not a UUID, so the repo-id set is bound as
      ``Array(String)`` rather than ``Array(UUID)``.

    The cross-check against ``live_repo_ids`` is otherwise identical, which is
    what keeps the CHAOS-4263 dead-id oracle intact for this shape.
    """
    if not repo_ids:
        return {}
    result = client.query(
        f"""
        SELECT scope_id, count() AS n, max(computed_at) AS latest
        FROM {table}
        WHERE org_id = {{org_id:String}}
          AND scope = 'repo'
          AND scope_id IN {{repo_ids:Array(String)}}
          AND computed_at >= {{run_start:DateTime64(6)}}
        GROUP BY scope_id
        """,
        parameters={
            "org_id": org_id,
            "repo_ids": sorted(repo_ids),
            "run_start": run_start,
        },
    )
    return {
        str(row[0]): {"rows": int(row[1]), "latest_computed_at": str(row[2])}
        for row in result.result_rows
    }


def unscoped_scope_ids(client, table: str, run_start: datetime) -> set[str]:
    """unscoped_repo_ids' counterpart for scope/scope_id-keyed tables."""
    result = client.query(
        f"SELECT DISTINCT scope_id FROM {table} "
        f"WHERE scope = 'repo' AND computed_at >= {{run_start:DateTime64(6)}}",
        parameters={"run_start": run_start},
    )
    return {str(row[0]) for row in result.result_rows}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # Defaults from the environment so a caller never has to put a
    # credential-bearing DSN into argv, where any process listing can read it
    # (CHAOS-4457). Mirrors `fixtures generate --sink`, which already resolves
    # CLICKHOUSE_URI the same way. Still overridable on the command line for
    # ad-hoc use against a throwaway store.
    parser.add_argument(
        "--clickhouse-uri",
        default=os.getenv("CLICKHOUSE_URI"),
        help="ClickHouse DSN. Env: CLICKHOUSE_URI (preferred — keeps the credential out of argv).",
    )
    parser.add_argument("--org-id", required=True)
    parser.add_argument(
        "--run-start",
        required=True,
        help="ISO8601 UTC timestamp captured BEFORE the pipeline ran. Rows "
        "with computed_at before this are stale evidence, not proof this run "
        "computed anything.",
    )
    all_families = (
        sorted(REPO_DAY_FAMILIES)
        + sorted(TEAM_DAY_FAMILIES)
        + sorted(SCOPE_ID_REPO_FAMILIES)
        + sorted(SCOPE_KEY_FAMILIES)
        + sorted(SYNTHESIZED_REPO_ID_FAMILIES)
    )
    parser.add_argument(
        "--families",
        nargs="+",
        default=all_families,
        choices=all_families,
        help=(
            "Subset of families to check (default: all -- repo-keyed, "
            "team-keyed, and scope-keyed)."
        ),
    )
    parser.add_argument(
        "--summary-json",
        help="Optional path to write a machine-readable per-family "
        "rows-written summary (CI wires this into $GITHUB_OUTPUT / the job "
        "step summary).",
    )
    args = parser.parse_args()
    # Defaulting from the environment must not become "silently None": with

    # neither the flag nor CLICKHOUSE_URI set, this used to proceed and fail

    # deep inside the sink with an unrelated-looking error.

    if not args.clickhouse_uri:
        parser.error("--clickhouse-uri or CLICKHOUSE_URI is required")

    # clickhouse_connect binds DateTime64(6) parameters from a real datetime
    # object, not a raw ISO8601 string -- a string with a "+00:00" offset
    # (what Python's datetime.isoformat() produces, and what both callers of
    # this script pass) fails ClickHouse's DateTime64 parser with
    # "isn't parsed completely", which reads as this script being broken,
    # not as the CHAOS-4263 defect it exists to catch.
    run_start = datetime.fromisoformat(args.run_start)

    sink = ClickHouseMetricsSink(args.clickhouse_uri)
    client = sink.client
    try:
        repo_ids = live_repo_ids(client, args.org_id)
        if not repo_ids:
            print(
                f"FAILED: org {args.org_id} has no rows in ClickHouse repos. "
                "Nothing was actually synced for this org -- the readback "
                "below would be checking against an empty oracle, which is "
                "the same false-clean shape as CHAOS-4263 (zero-repo "
                "partitions reporting succeeded).",
                file=sys.stderr,
            )
            return 1

        summary: dict[str, dict[str, object]] = {}
        failures: list[str] = []
        for family in args.families:
            if family in SYNTHESIZED_REPO_ID_FAMILIES:
                table, identity_column = SYNTHESIZED_REPO_ID_FAMILIES[family]
                total_rows, stray = synthesized_repo_readback(
                    client, table, identity_column, args.org_id, repo_ids, run_start
                )
                summary[family] = {
                    "table": table,
                    "org_id": args.org_id,
                    "rows_written": total_rows,
                    "repo_ids_outside_org": sorted(stray),
                }
                if total_rows == 0:
                    failures.append(
                        f"{family} ({table}): zero_rows_with_source_data -- no "
                        f"row with computed_at >= {args.run_start} for "
                        f"org {args.org_id}."
                    )
                if stray:
                    failures.append(
                        f"{family} ({table}): wrote repo_id(s) {sorted(stray)} that "
                        "are neither a live repo for this org nor the expected "
                        "synthesized id for their own identity -- the exact "
                        "CHAOS-4263 dead-id shape."
                    )
                continue

            if family in TEAM_DAY_FAMILIES:
                table = TEAM_DAY_FAMILIES[family]
                team_rows = team_readback(client, table, args.org_id, run_start)
                total_rows = sum(int(v["rows"]) for v in team_rows.values())
                summary[family] = {
                    "table": table,
                    "org_id": args.org_id,
                    "rows_written": total_rows,
                    "teams_with_rows": sorted(team_rows),
                }
                if total_rows == 0:
                    failures.append(
                        f"{family} ({table}): zero_rows_with_source_data -- no "
                        f"row with computed_at >= {args.run_start} for "
                        f"org {args.org_id}."
                    )
                continue

            if family in SCOPE_KEY_FAMILIES:
                table = SCOPE_KEY_FAMILIES[family]
                key_rows = scope_key_repo_readback(
                    client, table, args.org_id, repo_ids, run_start
                )
                total_rows = sum(int(v["rows"]) for v in key_rows.values())
                summary[family] = {
                    "table": table,
                    "org_id": args.org_id,
                    "rows_written": total_rows,
                    "repos_with_rows": sorted(key_rows),
                }
                if total_rows == 0:
                    failures.append(
                        f"{family} ({table}): zero_rows_with_source_data -- no "
                        f"scope_type='repo' row with computed_at >= "
                        f"{args.run_start} for any of org {args.org_id}'s "
                        f"{len(repo_ids)} live repo(s)."
                    )
                continue

            if family in SCOPE_ID_REPO_FAMILIES:
                table = SCOPE_ID_REPO_FAMILIES[family]
                scope_rows = scope_id_repo_readback(
                    client, table, args.org_id, repo_ids, run_start
                )
                total_rows = sum(int(v["rows"]) for v in scope_rows.values())
                stray = unscoped_scope_ids(client, table, run_start) - repo_ids
                summary[family] = {
                    "table": table,
                    "org_id": args.org_id,
                    "rows_written": total_rows,
                    "repos_with_rows": sorted(scope_rows),
                    "repo_ids_outside_org": sorted(stray),
                }
                if total_rows == 0:
                    failures.append(
                        f"{family} ({table}): zero_rows_with_source_data -- no "
                        f"scope='repo' row with computed_at >= {args.run_start} "
                        f"for any of org {args.org_id}'s {len(repo_ids)} live "
                        "repo(s)."
                    )
                if stray:
                    failures.append(
                        f"{family} ({table}): wrote scope_id(s) {sorted(stray)} "
                        "that are not in ClickHouse repos for any org at all -- "
                        "the exact CHAOS-4263 dead-id shape."
                    )
                continue

            table = REPO_DAY_FAMILIES[family]
            readback = family_readback(client, table, args.org_id, repo_ids, run_start)
            total_rows = sum(int(v["rows"]) for v in readback.values())
            stray = unscoped_repo_ids(client, table, run_start) - repo_ids
            summary[family] = {
                "table": table,
                "org_id": args.org_id,
                "rows_written": total_rows,
                "repos_with_rows": sorted(readback),
                "repo_ids_outside_org": sorted(stray),
            }
            if total_rows == 0:
                failures.append(
                    f"{family} ({table}): zero_rows_with_source_data -- no "
                    f"row with computed_at >= {args.run_start} for any of "
                    f"org {args.org_id}'s {len(repo_ids)} live repo(s)."
                )
            if stray:
                failures.append(
                    f"{family} ({table}): wrote repo_id(s) {sorted(stray)} "
                    "that are not in ClickHouse repos for any org at all -- "
                    "the exact CHAOS-4263 dead-id shape."
                )

        print(json.dumps(summary, indent=2, default=str))
        if args.summary_json:
            with open(args.summary_json, "w") as f:
                json.dump(summary, f, indent=2, default=str)

        if failures:
            print("\nFAILED:", file=sys.stderr)
            for line in failures:
                print(f"  - {line}", file=sys.stderr)
            return 1

        print(
            f"\nOK: {len(args.families)} family(ies) produced rows for "
            f"org {args.org_id}, all within its {len(repo_ids)} live repo id(s)."
        )
        return 0
    finally:
        sink.close()


if __name__ == "__main__":
    raise SystemExit(main())
