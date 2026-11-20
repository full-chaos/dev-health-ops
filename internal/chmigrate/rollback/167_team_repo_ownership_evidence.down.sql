-- Rollback of 167_team_repo_ownership_evidence.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator
-- runs it by hand, and only after the worker image is back on a build without 167 (a worker build with 167 refuses to start
-- on a schema that lacks it).
--
-- The recorded evidence is dropped with the table. A build without 167 neither writes nor reads it; a later build with 167
-- finds no recorded key and closes the claims of that time on the kind guard alone (no_recorded_evidence) until it records
-- them again. The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a
-- build that has the migration reads it as applied and never applies it again.
DROP TABLE IF EXISTS team_repo_ownership_evidence SYNC;
ALTER TABLE schema_migrations DELETE WHERE version = '167_team_repo_ownership_evidence.sql' SETTINGS mutations_sync = 2;
