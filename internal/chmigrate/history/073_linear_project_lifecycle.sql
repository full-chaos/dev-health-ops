-- CHAOS-3365: carry a provider project's own lifecycle alongside its subject row.
--
-- `is_active` deliberately stays a RETIREMENT flag -- 0 only when the provider
-- archived or deleted the project -- because `scope_catalog` filters the Ask Dev
-- subject catalog on `is_active = 1`. Encoding lifecycle into activity would make
-- every project unresolvable the moment it completed, which is exactly when
-- people start asking what it delivered. Lifecycle therefore gets its own column.
--
-- Additive only: every column has a default, so existing rows (the team-derived
-- attribution rows) read back as empty/NULL rather than needing a backfill.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS state LowCardinality(String) DEFAULT '' AFTER is_active;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_date Nullable(Date) AFTER state;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS url String DEFAULT '' AFTER target_date;
