-- 0055 stable repository id for grants (CHAOS-12720, part of CHAOS-10708). ADDITIVE ONLY.
--
-- A provider rename or transfer changes the path of a repository and leaves its id. A repository
-- grant is keyed on the path, so a rename drops the grant. This migration adds:
--
--   resource_grants.resource_repo_id   "<provider>:<number>", NULL for a legacy row. A row with an
--                                      id matches on the id only (the path is display); a row
--                                      without one matches on the path, as before. Nullable, no
--                                      default, no rewrite: the one-shot dho backfill fills it.
--   repository_identities              the copy (D6651) of the provider ids per organization. The
--                                      source stays the ClickHouse repos table (provider_repo_id);
--                                      the dho sync verb writes this table. Authorize reads it to
--                                      turn the live path of a request into an id.
--
-- The path index is not unique: a new repository may take the path of a renamed one before the
-- next sync moves the old row. Authorize treats a path with two rows as unresolved and denies.
-- repository_identities is a security table written only inside audit.Commit (the 0006 guard).
ALTER TABLE resource_grants
    ADD COLUMN resource_repo_id text,
    ADD CONSTRAINT resource_grants_repo_id_shape
    CHECK (resource_repo_id IS NULL OR (resource_kind = 'repository' AND resource_id <> '*' AND resource_repo_id ~ '^[a-z]{1,16}:[0-9]{1,20}$'));

CREATE TABLE repository_identities (
    organization_id  uuid        NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    provider         text        NOT NULL CHECK (provider ~ '^[a-z]{1,16}$'),
    provider_repo_id text        NOT NULL CHECK (provider_repo_id ~ '^[0-9]{1,20}$'),
    current_path     text        NOT NULL CHECK (octet_length(current_path) <= 255 AND current_path ~ '^[a-z0-9][a-z0-9._/-]*$'),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, provider, provider_repo_id)
);

CREATE INDEX repository_identities_path_idx ON repository_identities (organization_id, current_path);

CREATE TRIGGER repository_identities_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON repository_identities
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER repository_identities_truncate_guard
    BEFORE TRUNCATE ON repository_identities
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
