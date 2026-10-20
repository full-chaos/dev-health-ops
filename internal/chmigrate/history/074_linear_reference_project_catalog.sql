-- CHAOS-3716: preserve the native Linear project reference projection.
--
-- ``projects`` already carries the provider lifecycle/retirement columns. The
-- catalog query also returns the native team edges and lead identity. Keeping
-- those as typed columns makes the projection queryable without a JSON blob
-- and lets the Go sink read back the exact normalized row. Defaults keep the
-- existing team-derived rows valid and make this additive for other providers.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS team_ids Array(String) DEFAULT [] AFTER url;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS team_keys Array(String) DEFAULT [] AFTER team_ids;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS lead_id Nullable(String) AFTER team_keys;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS lead_name Nullable(String) AFTER lead_id;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS lead_email Nullable(String) AFTER lead_name;
