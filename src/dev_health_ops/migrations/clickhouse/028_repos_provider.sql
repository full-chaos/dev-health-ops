-- Migration 028: Add provider column to repos table.
-- Stores the sync provider (github, gitlab, local, synthetic, jira, linear).
-- DEFAULT 'unknown' for backward compatibility with existing rows.
ALTER TABLE repos ADD COLUMN IF NOT EXISTS provider String DEFAULT 'unknown';
