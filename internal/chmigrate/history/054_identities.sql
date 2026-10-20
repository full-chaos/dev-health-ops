-- CHAOS-2600 CS5 — ClickHouse-native identities (parallel to the CH teams
-- table). Named `identities` so it does NOT collide with the Postgres
-- `identity_mappings` table during org-deletion purge (each layer must own a
-- unique table name, like CH `teams` vs Postgres `team_mappings`).
-- ClickHouse is the team system of record. Identity records (canonical_id ->
-- provider identities + team membership) move off Postgres so the admin
-- identity surface has full replacement semantics — updating an identity's
-- teams removes it from teams it left, including when email/provider-ids
-- changed. ReplacingMergeTree(updated_at) keyed on (org_id, canonical_id):
-- the logical identity is the sort key, so FINAL reads are well-formed and the
-- latest write wins. provider_identities is a JSON-encoded dict[str, list[str]].
CREATE TABLE IF NOT EXISTS identities (
    org_id String DEFAULT 'default',
    canonical_id String,
    identity_uuid UUID,
    display_name Nullable(String),
    email Nullable(String),
    provider_identities String DEFAULT '{}',
    team_ids Array(String) DEFAULT [],
    is_active UInt8 DEFAULT 1,
    updated_at DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, canonical_id);
