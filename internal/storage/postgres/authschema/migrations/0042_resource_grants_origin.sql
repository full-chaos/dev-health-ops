-- 0042 resource_grants: origin. ADDITIVE ONLY.
--
-- origin: who made the grant. 'api' is a grant made through the grant routes (granted_by
-- names the actor, enforced by the grant Store); 'backfill' is a grant the W4 4.22 backfill
-- writes, so its lineage is a value and not a convention. Existing rows are 'api'.
-- The 0020 kind and key CHECKs were validated by 0024.
--
-- resource_grants_history_guard also freezes origin of a revoked row.

ALTER TABLE resource_grants
    ADD COLUMN origin text NOT NULL DEFAULT 'api',
    ADD CONSTRAINT resource_grants_origin_closed CHECK (origin IN ('api', 'backfill'));

CREATE OR REPLACE FUNCTION resource_grants_history_guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF OLD.revoked_at IS NOT NULL AND (
        NEW.id              IS DISTINCT FROM OLD.id
        OR NEW.principal_id    IS DISTINCT FROM OLD.principal_id
        OR NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.role_key        IS DISTINCT FROM OLD.role_key
        OR NEW.resource_kind   IS DISTINCT FROM OLD.resource_kind
        OR NEW.resource_id     IS DISTINCT FROM OLD.resource_id
        OR NEW.granted_by      IS DISTINCT FROM OLD.granted_by
        OR NEW.expires_at      IS DISTINCT FROM OLD.expires_at
        OR NEW.created_at      IS DISTINCT FROM OLD.created_at
        OR NEW.revoked_at      IS DISTINCT FROM OLD.revoked_at
        OR NEW.revoked_by      IS DISTINCT FROM OLD.revoked_by
        OR NEW.origin          IS DISTINCT FROM OLD.origin
    ) THEN
        RAISE EXCEPTION 'a revoked grant is history and is not changed'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
