"""Migration 027: Add org_id to ClickHouse sorting keys for multi-tenant query performance.

Migration 024 added org_id as a regular column to all analytics tables, but left it
out of the sorting key (ORDER BY). This means WHERE org_id = ... queries cannot use
ClickHouse index pruning and must scan all granules.

Because org_id already exists as a column, ALTER TABLE ... MODIFY ORDER BY cannot be
used (ClickHouse only allows MODIFY ORDER BY to reference columns added in the same
ALTER statement). This migration uses the shadow table pattern (Altinity pattern):

    1. SHOW CREATE TABLE to get full DDL (preserves all settings, indexes, etc.)
    2. Modify DDL: rename to table_new, prepend org_id to ORDER BY
    3. INSERT INTO table_new SELECT * FROM table
    4. EXCHANGE TABLES table AND table_new (atomic swap)
    5. DROP TABLE table_new (which now holds the old structure)

Materialized views from migration 026 are dropped before rebuilding their target
tables, then recreated with org_id in the SELECT/GROUP BY.

The 3 AggregatingMergeTree rollup tables (from migration 026) were created after
migration 024, so they don't have org_id yet — it's added via ALTER before rebuild.

Idempotent: tables already having org_id first in ORDER BY are skipped.
"""

import logging
import re

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table catalog: table_name -> new ORDER BY with org_id prepended
# ---------------------------------------------------------------------------

TABLES = {
    # --- MergeTree tables (19) ---
    "repo_metrics_daily": "(org_id, repo_id, day)",
    "user_metrics_daily": "(org_id, repo_id, author_email, day)",
    "commit_metrics": "(org_id, repo_id, day, author_email, commit_hash)",
    "team_metrics_daily": "(org_id, team_id, day)",
    "work_item_metrics_daily": "(org_id, provider, day, work_scope_id, team_id)",
    "work_item_user_metrics_daily": "(org_id, provider, work_scope_id, user_identity, day)",
    "work_item_state_durations_daily": "(org_id, provider, work_scope_id, team_id, status, day)",
    "file_metrics_daily": "(org_id, repo_id, day, path)",
    "review_edges_daily": "(org_id, repo_id, reviewer, author, day)",
    "cicd_metrics_daily": "(org_id, repo_id, day)",
    "deploy_metrics_daily": "(org_id, repo_id, day)",
    "incident_metrics_daily": "(org_id, repo_id, day)",
    "file_complexity_snapshots": "(org_id, repo_id, as_of_day, file_path)",
    "repo_complexity_daily": "(org_id, repo_id, day)",
    "file_hotspot_daily": "(org_id, repo_id, day, file_path)",
    "investment_classifications_daily": "(org_id, day, provider, artifact_type, investment_area, project_stream, artifact_id)",
    "investment_metrics_daily": "(org_id, day, team_id, investment_area, project_stream)",
    "issue_type_metrics_daily": "(org_id, day, provider, team_id, issue_type_norm)",
    "dora_metrics_daily": "(org_id, repo_id, day, metric_name)",
    # --- ReplacingMergeTree tables (31) ---
    "repos": "(org_id, id)",
    "git_files": "(org_id, repo_id, path)",
    "git_commits": "(org_id, repo_id, hash)",
    "git_commit_stats": "(org_id, repo_id, commit_hash, file_path)",
    "git_blame": "(org_id, repo_id, path, line_no)",
    "git_pull_requests": "(org_id, repo_id, number)",
    "git_pull_request_reviews": "(org_id, repo_id, number, review_id)",
    "ci_pipeline_runs": "(org_id, repo_id, run_id)",
    "deployments": "(org_id, repo_id, deployment_id)",
    "incidents": "(org_id, repo_id, incident_id)",
    "ic_landscape_rolling_30d": "(org_id, repo_id, team_id, map_name, as_of_day, identity_id)",
    "work_item_cycle_times": "(org_id, provider, work_item_id)",
    "work_items": "(org_id, repo_id, work_item_id)",
    "work_item_transitions": "(org_id, repo_id, work_item_id, occurred_at)",
    "work_item_dependencies": "(org_id, source_work_item_id, target_work_item_id, relationship_type)",
    "work_item_reopen_events": "(org_id, work_item_id, occurred_at)",
    "work_item_interactions": "(org_id, work_item_id, occurred_at, interaction_type)",
    "sprints": "(org_id, provider, sprint_id)",
    "work_graph_edges": "(org_id, source_type, source_id, edge_type, target_type, target_id)",
    "work_graph_issue_pr": "(org_id, repo_id, work_item_id, pr_number)",
    "work_graph_pr_commit": "(org_id, repo_id, pr_number, commit_hash)",
    "work_unit_investments": "(org_id, work_unit_id)",
    "work_unit_investment_quotes": "(org_id, work_unit_id, source_id, quote)",
    "investment_explanations": "(org_id, cache_key)",
    "atlassian_ops_incidents": "(org_id, id)",
    "atlassian_ops_alerts": "(org_id, id)",
    "atlassian_ops_schedules": "(org_id, id)",
    "worklogs": "(org_id, provider, worklog_id)",
    "capacity_forecasts": "(org_id, forecast_id)",
    "teams": "(org_id, id)",
    "jira_project_ops_team_links": "(org_id, project_key, ops_team_id)",
    # --- AggregatingMergeTree tables (3, from migration 026) ---
    "commit_daily_rollup": "(org_id, repo_id, day)",
    "ci_daily_rollup": "(org_id, repo_id, day)",
    "deployment_daily_rollup": "(org_id, repo_id, day)",
}

# Tables created in migration 026 (after 024) that still need the org_id column.
TABLES_NEEDING_ORG_ID_COLUMN = [
    "commit_daily_rollup",
    "ci_daily_rollup",
    "deployment_daily_rollup",
]

# Materialized views to drop before rebuilding target tables, recreate after.
MATERIALIZED_VIEWS_DROP = [
    "commit_daily_rollup_mv",
    "ci_daily_rollup_mv",
    "deployment_daily_rollup_mv",
]


MATERIALIZED_VIEWS_CREATE = [
    """\
CREATE MATERIALIZED VIEW IF NOT EXISTS commit_daily_rollup_mv
TO commit_daily_rollup
AS SELECT
    gc.org_id AS org_id,
    gc.repo_id AS repo_id,
    toDate(gc.author_when) AS day,
    toUInt64(count()) AS commit_count,
    toUInt64(sum(if(gcs.additions > 0, gcs.additions, 0))) AS loc_added,
    toUInt64(sum(if(gcs.deletions > 0, abs(gcs.deletions), 0))) AS loc_deleted,
    toUInt64(count(DISTINCT gcs.file_path)) AS files_changed
FROM git_commits gc
LEFT JOIN git_commit_stats gcs
    ON gc.repo_id = gcs.repo_id AND gc.hash = gcs.commit_hash
GROUP BY gc.org_id, gc.repo_id, toDate(gc.author_when)""",
    """\
CREATE MATERIALIZED VIEW IF NOT EXISTS ci_daily_rollup_mv
TO ci_daily_rollup
AS SELECT
    org_id,
    repo_id,
    toDate(started_at) AS day,
    toUInt64(count()) AS total_runs,
    toUInt64(countIf(status = 'success')) AS success_runs,
    toUInt64(countIf(status = 'failure')) AS failure_runs
FROM ci_pipeline_runs
GROUP BY org_id, repo_id, toDate(started_at)""",
    """\
CREATE MATERIALIZED VIEW IF NOT EXISTS deployment_daily_rollup_mv
TO deployment_daily_rollup
AS SELECT
    org_id,
    repo_id,
    toDate(coalesce(deployed_at, started_at)) AS day,
    toUInt64(count()) AS total_deployments,
    toUInt64(countIf(status = 'success')) AS success_deployments,
    toUInt64(countIf(status = 'failure')) AS failure_deployments
FROM deployments
GROUP BY org_id, repo_id, toDate(coalesce(deployed_at, started_at))""",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Regex: ORDER BY (col, col, ...) | ORDER BY tuple(col, ...) | ORDER BY col
_ORDER_BY_RE = re.compile(r"ORDER BY\s+(?:tuple\([^)]+\)|\([^)]+\)|\S+)", re.IGNORECASE)


# Regex: table name in CREATE TABLE statement (handles optional db prefix + backticks)
def _table_name_re(table: str) -> re.Pattern:
    return re.compile(
        rf"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        rf"(?:`?[\w\d_]+`?\.)?`?){re.escape(table)}(`?\s|`?\()",
        re.IGNORECASE,
    )


def _has_org_id_first_in_order_by(ddl: str) -> bool:
    """Check if org_id is already the first column in the ORDER BY clause."""
    match = _ORDER_BY_RE.search(ddl)
    if not match:
        return False
    order_clause = match.group(0)
    # Check if org_id appears right after ORDER BY (possibly inside parens)
    return bool(
        re.search(r"ORDER BY\s+(?:tuple)?\(?\s*org_id", order_clause, re.IGNORECASE)
    )


def _replace_order_by(ddl: str, new_order_by: str) -> str:
    """Replace the ORDER BY clause in a CREATE TABLE DDL string."""
    result, count = _ORDER_BY_RE.subn(f"ORDER BY {new_order_by}", ddl, count=1)
    if count == 0:
        raise ValueError(f"Could not find ORDER BY in DDL: {ddl[:300]}...")
    return result


def _replace_table_name(ddl: str, old_name: str, new_name: str) -> str:
    """Replace the table name in a CREATE TABLE DDL string."""
    pattern = _table_name_re(old_name)
    result, count = pattern.subn(rf"\g<1>{new_name}\g<2>", ddl, count=1)
    if count == 0:
        raise ValueError(
            f"Could not replace table name '{old_name}' in DDL: {ddl[:300]}..."
        )
    return result


def _table_exists(client, table: str) -> bool:
    try:
        res = client.query(
            "SELECT count() FROM system.tables "
            "WHERE database = currentDatabase() AND name = {name:String}",
            parameters={"name": table},
        )
        rows = getattr(res, "result_rows", None) or []
        return bool(rows and rows[0] and rows[0][0] > 0)
    except Exception:
        return False


def _rebuild_table(client, table: str, new_order_by: str) -> None:
    """Rebuild a single table with org_id prepended to its ORDER BY.

    Uses the shadow table pattern:
      1. SHOW CREATE TABLE → get full DDL
      2. Modify: rename to table_new, set new ORDER BY
      3. CREATE TABLE table_new ...
      4. INSERT INTO table_new SELECT * FROM table
      5. EXCHANGE TABLES table AND table_new
      6. DROP TABLE table_new
    """
    shadow = f"{table}_new"

    if not _table_exists(client, table):
        log.warning(f"  {table}: table does not exist, skipping")
        return


    res = client.query(f"SHOW CREATE TABLE `{table}`")
    ddl = res.result_rows[0][0]


    if _has_org_id_first_in_order_by(ddl):
        log.info(f"  {table}: org_id already first in ORDER BY, skipping")
        return


    new_ddl = _replace_table_name(ddl, table, shadow)
    new_ddl = _replace_order_by(new_ddl, new_order_by)


    log.info(f"  {table}: creating shadow table")
    client.command(f"DROP TABLE IF EXISTS `{shadow}`")
    client.command(new_ddl)


    log.info(f"  {table}: copying data")
    client.command(f"INSERT INTO `{shadow}` SELECT * FROM `{table}`")


    log.info(f"  {table}: atomic swap via EXCHANGE TABLES")
    client.command(f"EXCHANGE TABLES `{table}` AND `{shadow}`")


    client.command(f"DROP TABLE `{shadow}`")

    log.info(f"  {table}: done")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def upgrade(client):
    """Add org_id to the sorting key of all ClickHouse analytics tables."""
    log.info("=== Migration 027: Add org_id to sorting keys ===")

    # ------------------------------------------------------------------
    # Step 1: Drop materialized views before touching their target tables
    # ------------------------------------------------------------------
    log.info("Step 1/4: Dropping materialized views")
    for mv in MATERIALIZED_VIEWS_DROP:
        log.info(f"  Dropping {mv}")
        client.command(f"DROP VIEW IF EXISTS `{mv}`")

    # ------------------------------------------------------------------
    # Step 2: Add org_id column to rollup tables that missed migration 024
    # ------------------------------------------------------------------
    log.info("Step 2/4: Adding org_id column to rollup tables (from migration 026)")
    for table in TABLES_NEEDING_ORG_ID_COLUMN:
        if not _table_exists(client, table):
            log.warning(f"  {table}: does not exist, skipping column add")
            continue
        log.info(f"  {table}: ALTER TABLE ADD COLUMN IF NOT EXISTS org_id")
        client.command(
            f"ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS "
            f"org_id String DEFAULT 'default'"
        )

    # ------------------------------------------------------------------
    # Step 3: Rebuild all tables with org_id in ORDER BY
    # ------------------------------------------------------------------
    log.info("Step 3/4: Rebuilding tables with org_id in sorting key")
    total = len(TABLES)
    for i, (table, new_order_by) in enumerate(TABLES.items(), 1):
        log.info(f"[{i}/{total}] {table}")
        try:
            _rebuild_table(client, table, new_order_by)
        except Exception as exc:
            log.error(f"FAILED on {table}: {exc}")
            # Clean up shadow table on failure
            try:
                client.command(f"DROP TABLE IF EXISTS `{table}_new`")
            except Exception:
                pass
            raise

    # ------------------------------------------------------------------
    # Step 4: Recreate materialized views with org_id
    # ------------------------------------------------------------------
    log.info("Step 4/4: Recreating materialized views with org_id")
    for mv_sql in MATERIALIZED_VIEWS_CREATE:
        # Extract view name for logging
        m = re.search(
            r"CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
            mv_sql,
            re.IGNORECASE,
        )
        name = m.group(1) if m else "<unknown>"
        log.info(f"  Creating {name}")
        client.command(mv_sql)

    log.info("=== Migration 027: Complete ===")
