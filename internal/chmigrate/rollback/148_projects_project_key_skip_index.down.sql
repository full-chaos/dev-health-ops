-- Hand-run rollback of 148_projects_project_key_skip_index.sql.
ALTER TABLE projects DROP INDEX IF EXISTS project_key_bf;
ALTER TABLE schema_migrations DELETE WHERE version = '148_projects_project_key_skip_index.sql' SETTINGS mutations_sync = 2;
