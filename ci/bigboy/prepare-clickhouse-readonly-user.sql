-- CHAOS-7079 (D2938/D2939/D2943): prepared, NOT applied. A read-only
-- ClickHouse user for the `dho-clickhouse-readers` network's read-only
-- callers (MCP's acr-api / acr-projector; other read-only callers may join
-- the same network and use this same user later).
--
-- Table list source: .remember/acr-mcp/lanes/lane-mcp-venue-plan/TABLES.md,
-- Grant (a), 31 objects (30 tables + 1 view), ALL VERIFIED there at exact
-- file:line. Grant (b) (8 objects, `context_for_task` evidence reads) is
-- explicitly NOT included -- (a) and (b) are DISJOINT sets (grant (a) rows
-- 1-31, grant (b) rows 32-39 in TABLES.md's own numbering; (b) is not a
-- subset of (a)). Only (a) is prepared here, per D2943's instruction; if (b)
-- is ever needed for this venue, it is a separate, explicit ask.
--
-- Existence check (2026-09-28, gwc-bigboy-ops-3, read-only, names only):
-- all 31 objects of grant (a) exist in bigboy ClickHouse database `default`
-- today (`system.tables`, name+engine only, no data read). None missing.
--
-- Column lists: TABLES.md gives exact columns only for grant (b) (which is
-- excluded here). Grant (a)'s "Columns" field cites schema.go line ranges,
-- not a hand-picked list -- so this file grants whole-table/whole-view
-- SELECT for each of the 31 objects, not a column subset. Narrow further
-- only on an explicit ask with a real column list, the same way grant (b)
-- was verified.
--
-- Apply only on team-lead's word. The credential this creates lives in a
-- 0600 file under /var/lib/oci-cache at apply time, never printed here or
-- anywhere else. Database name is bigboy's `default` (verified above by
-- table lookup, never by reading a DSN secret).

-- readonly = 2, not 1: readonly = 1 refuses any per-query SETTINGS clause (Code 164), which the
-- MCP caller class sends on every query. readonly = 2 still forbids every write and forbids
-- changing `readonly` itself, and allows changing the other settings within their MAX bounds.
-- The MAX pins below are what keep those per-query changes bounded: the caller may lower the
-- limits, never raise them.
CREATE SETTINGS PROFILE IF NOT EXISTS mcp_readonly_profile
    SETTINGS readonly = 2,
             max_memory_usage = 4000000000 MAX 4000000000,   -- 4 GiB ceiling, named so the MCP lead can tune it
             max_execution_time = 30 MAX 30;                 -- seconds; matches "a bytes-read ceiling for the MCP caller class" intent (DESIGN-r5-clean.md J.7 #7)

-- Password/auth method chosen and set at apply time (never written here).
-- The literal string below is a syntax placeholder, not a real value.
-- D2947/D2953 (2026-09-28): compose network membership alone does not admit anything --
-- `clickhouse` stays a member of both `dev-health` and `dho-clickhouse-readers`, and the
-- stock ClickHouse image listens on every interface by default; a container on
-- `dev-health` alone could still open a TCP connection to it. The real admission control
-- for THIS user is HOST IP, a per-user ClickHouse setting checked at authentication
-- regardless of which interface accepted the connection -- restricted to
-- `dho-clickhouse-readers`' own fixed subnet (ci/bigboy/compose.bigboy.images.yml's
-- `networks.dho-clickhouse-readers.ipam.config`), so mcp_readonly cannot authenticate
-- from `dev-health` even with the correct password.
CREATE USER IF NOT EXISTS mcp_readonly
    IDENTIFIED WITH sha256_password BY '<SET AT APPLY TIME, NEVER COMMITTED>'
    HOST IP '10.199.80.0/28'
    DEFAULT DATABASE default
    SETTINGS PROFILE mcp_readonly_profile;

-- Grant (a), 31 objects, SELECT only, table-by-table (never a database-wide
-- GRANT SELECT ON default.* shortcut).
GRANT SELECT ON default.backfill_log TO mcp_readonly;
GRANT SELECT ON default.capacity_forecasts TO mcp_readonly;
GRANT SELECT ON default.ci_pipeline_runs TO mcp_readonly;
GRANT SELECT ON default.compounding_risk_daily TO mcp_readonly;
GRANT SELECT ON default.deployments TO mcp_readonly;
GRANT SELECT ON default.estimate_coverage_metrics_daily TO mcp_readonly;
GRANT SELECT ON default.git_pull_request_reviews TO mcp_readonly;
GRANT SELECT ON default.git_pull_requests TO mcp_readonly;
GRANT SELECT ON default.ic_landscape_rolling_30d TO mcp_readonly;
GRANT SELECT ON default.investment_metrics_daily TO mcp_readonly;
GRANT SELECT ON default.operational_incidents TO mcp_readonly;
GRANT SELECT ON default.operational_service_repository_mappings TO mcp_readonly;
GRANT SELECT ON default.recommendations_daily TO mcp_readonly;
GRANT SELECT ON default.repo_metrics_daily TO mcp_readonly;
GRANT SELECT ON default.team_metrics_daily TO mcp_readonly;
GRANT SELECT ON default.cicd_metrics_daily TO mcp_readonly;
GRANT SELECT ON default.deploy_metrics_daily TO mcp_readonly;
GRANT SELECT ON default.repos TO mcp_readonly;
GRANT SELECT ON default.work_graph_issue_pr TO mcp_readonly;
GRANT SELECT ON default.work_graph_deployment_incident_edges TO mcp_readonly;
GRANT SELECT ON default.work_item_dependencies TO mcp_readonly;
GRANT SELECT ON default.teams TO mcp_readonly;
GRANT SELECT ON default.project_membership_transitions TO mcp_readonly;
GRANT SELECT ON default.projects TO mcp_readonly;
GRANT SELECT ON default.work_unit_investments TO mcp_readonly;
GRANT SELECT ON default.work_item_team_attributions TO mcp_readonly;
GRANT SELECT ON default.team_project_ownership TO mcp_readonly;
GRANT SELECT ON default.team_repo_ownership TO mcp_readonly;
GRANT SELECT ON default.work_item_metrics_daily TO mcp_readonly;
GRANT SELECT ON default.work_items TO mcp_readonly;
GRANT SELECT ON default.project_membership_presence TO mcp_readonly;  -- VIEW, not a base table

-- Explicit deny-by-omission: no INSERT/ALTER/DROP/CREATE grant of any kind
-- is issued to this user anywhere in this file, and no other table/database
-- is named.
