-- Hand-run rollback of 147_project_team_ownership_groups.sql. The refresh view goes first, so no refresh appends
-- while the tables are dropped; then the copy view, then the tables.
DROP VIEW IF EXISTS project_team_ownership_groups_refresh SYNC;
DROP VIEW IF EXISTS project_team_ownership_groups_current_mv SYNC;
DROP TABLE IF EXISTS project_team_ownership_groups_current SYNC;
DROP TABLE IF EXISTS project_team_ownership_groups_log SYNC;
ALTER TABLE schema_migrations DELETE WHERE version = '147_project_team_ownership_groups.sql' SETTINGS mutations_sync = 2;
