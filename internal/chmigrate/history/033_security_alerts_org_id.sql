-- Migration 033: Add org_id column to security_alerts.
-- The security_alerts table was created in 032 after 024_add_org_id but
-- missed this column, so the ClickHouse _insert_rows helper (which auto-
-- injects org_id for every table) fails with "Unrecognized column 'org_id'".
-- Uses the same String DEFAULT 'default' pattern as 024 for consistency.

ALTER TABLE security_alerts ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
