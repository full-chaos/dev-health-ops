ALTER TABLE deployments ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
