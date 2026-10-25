-- Revision 0159 (down_revision 0158), written by hand.
-- The migrator runs this file and the alembic_version update in one transaction, so a build here cannot be
-- CONCURRENT: on a table with writers it holds a SHARE lock (unit writes wait) until the run commits. Where
-- sync_run_units is large, `dho migrate postgres build-index failure-streak` builds the same index without
-- blocking writes BEFORE this revision runs (the chart's migrations.hook.streakIndex Job); the IF NOT EXISTS
-- below then finds it and does nothing. An empty, fresh or small table builds it here at once.
-- The index serves the per-source LATERAL top-N of providersync.RecentTerminalUnitsSQL: its equality columns, then its
-- ORDER BY, over its status predicate. The definition is FailureStreakIndexBody (internal/pgmigrate/indexbuild.go);
-- TestFailureStreakIndexDefinitionMatchesRevision0159 keeps the two equal.

CREATE INDEX IF NOT EXISTS ix_sync_run_units_failure_streak ON public.sync_run_units (org_id, integration_id, source_id, dataset_key, updated_at DESC, id DESC)
WHERE status IN ('success', 'failed');
