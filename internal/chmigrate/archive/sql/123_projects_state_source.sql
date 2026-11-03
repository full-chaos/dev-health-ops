-- CHAOS-9438: carry the provider fact that decided a project's `state` category
-- (for example `gitlab.project.archived=true`), so a consumer can say "appears
-- completed (archived)". `state` itself is the closed set backlog, planned,
-- started, paused, completed, canceled (lowercase). Additive: existing rows read
-- back as '' until their next sync.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS state_source LowCardinality(String) DEFAULT '' AFTER state;
