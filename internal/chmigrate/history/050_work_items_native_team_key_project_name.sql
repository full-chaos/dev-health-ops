-- Migration 050: work_items native_team_key + project_name (CHAOS-2467)
--
-- The "normalize" phase of the Org/Team/Project/Member attribution epic adds
-- two additive raw columns to work_items so provider normalizers can stop
-- conflating Team and Project:
--   * native_team_key - raw provider team key (Linear `issue.team.key`
--     only). Empty for GitHub/GitLab/Jira, where the item has no native team.
--   * project_name    - real provider project name (Linear `project.name`,
--     Jira `project.name`), moved out of the previously overloaded project_id.
--
-- Additive only: the existing project_key / project_id columns are unchanged.
-- Plain String (ClickHouse default '') matches the existing
-- project_key / project_id convention. The sinks write "" for absent values.
ALTER TABLE work_items ADD COLUMN IF NOT EXISTS native_team_key String AFTER project_id;
ALTER TABLE work_items ADD COLUMN IF NOT EXISTS project_name String AFTER native_team_key;
