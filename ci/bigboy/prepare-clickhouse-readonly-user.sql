-- CHAOS-7079 (D2938/D2939/D2943): prepared, NOT applied. A read-only
-- ClickHouse user for the `dho-clickhouse-readers` network's read-only
-- callers (MCP's acr-api / acr-projector; other read-only callers may join
-- the same network and use this same user later).
--
-- Grant shape (GWC ruling 2026-10-17 01:0xZ, CHAOS-10004): bigboy rehearses prod in its real form, and prod's acr reads ClickHouse
-- through the read-only user acr_ro with SELECT on the whole database. So this user has database-wide SELECT on `default` plus
-- SELECT on system.tables and system.columns (the schema reads the readers do), instead of the 34 per-table lines this file held
-- (33 tables and 1 view from lane-mcp-venue-plan TABLES.md grant (a), with three work-unit membership tables added later). The
-- per-table list drifted behind the readers' code and broke the acr projector (code 497 on work_item_relations_read). Writes stay
-- impossible: the profile below is readonly = 2 and no INSERT/ALTER/DROP/CREATE grant of any kind is issued. gwc-pin72 applies the
-- live grant on bigboy; this file is the record of it.
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
-- `dho-clickhouse-readers`' own fixed subnet (ci/bigboy/compose.bigboy.yml's
-- `networks.dho-clickhouse-readers.ipam.config`), so mcp_readonly cannot authenticate
-- from `dev-health` even with the correct password.
CREATE USER IF NOT EXISTS mcp_readonly
    IDENTIFIED WITH sha256_password BY '<SET AT APPLY TIME, NEVER COMMITTED>'
    HOST IP '10.199.80.0/28'
    DEFAULT DATABASE default
    SETTINGS PROFILE mcp_readonly_profile;

-- SELECT only: the database the readers use, and the two system tables they read the schema from.
GRANT SELECT ON default.* TO mcp_readonly;
GRANT SELECT ON system.tables TO mcp_readonly;
GRANT SELECT ON system.columns TO mcp_readonly;

-- Explicit deny-by-omission: no INSERT/ALTER/DROP/CREATE grant of any kind
-- is issued to this user anywhere in this file, and no database other than
-- `default` (and the two system tables above) is named.
