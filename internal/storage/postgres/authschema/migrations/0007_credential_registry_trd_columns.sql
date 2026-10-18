-- 0007 credential_registry: the TRD section 9/12 columns 0003 left out.
-- ADDITIVE ONLY. See 0001's header.
--
-- TRD section 9 specifies credential_registry(id, class, principal_id,
-- domain_owner, external_reference, fingerprint, audiences, scopes, status,
-- rotation_parent_id, timestamps). 0003 created the table without lineage,
-- audiences, scopes, domain owner, external reference and fingerprint. The wire
-- contract for credential metadata is derived from this table, so the producer
-- gets the columns first.
--
-- `status` is deliberately NOT a column: it is DERIVED, so it can never
-- disagree with the timestamps that define it.
--     revoked_at IS NOT NULL      -> 'revoked'
--     else expires_at <= now()    -> 'expired'
--     else                        -> 'active'
-- Revocation outranks expiry, and revocation is monotonic (below).

-- credential_token_name is the one shape an audience or a scope may have:
-- lowercase, bounded, no whitespace. A domain, so every array element is
-- checked (a CHECK over the array would need a subquery).
CREATE DOMAIN credential_token_name AS text
    CONSTRAINT credential_token_name_shape CHECK (VALUE ~ '^[a-z][a-z0-9._:-]{0,63}$');

ALTER TABLE credential_registry
    -- Rotation lineage (TRD section 12). NULL for a first-issue credential.
    ADD COLUMN rotation_parent_id uuid REFERENCES credential_registry (id),
    ADD COLUMN audiences credential_token_name[] NOT NULL DEFAULT '{}',
    ADD COLUMN scopes    credential_token_name[] NOT NULL DEFAULT '{}',
    -- Which validator owns the credential. ACP-ADR-08/09: ACR and External Push
    -- keep their domain validators; this column is metadata, never a second
    -- validator.
    ADD COLUMN domain_owner text NOT NULL DEFAULT 'platform',
    -- The domain system's own id for a projected credential: an opaque id, never
    -- secret material.
    ADD COLUMN external_reference text,
    -- A non-secret fingerprint of the credential's public identifier, fixed
    -- length, distinct from display_prefix.
    ADD COLUMN fingerprint text,
    ADD CONSTRAINT credential_registry_audiences_bounded
        CHECK (cardinality(audiences) <= 16 AND array_position(audiences, NULL) IS NULL),
    ADD CONSTRAINT credential_registry_scopes_bounded
        CHECK (cardinality(scopes) <= 64 AND array_position(scopes, NULL) IS NULL),
    ADD CONSTRAINT credential_registry_domain_owner_closed
        CHECK (domain_owner IN ('platform', 'acr', 'external_push')),
    ADD CONSTRAINT credential_registry_external_reference_shape
        CHECK (external_reference IS NULL OR external_reference ~ '^[A-Za-z0-9_-]{1,128}$'),
    -- A projected credential names its domain record; a platform credential has
    -- none to name.
    ADD CONSTRAINT credential_registry_external_reference_matches_owner
        CHECK ((domain_owner = 'platform') = (external_reference IS NULL)),
    ADD CONSTRAINT credential_registry_fingerprint_shape
        CHECK (fingerprint IS NULL OR fingerprint ~ '^[0-9a-f]{16}$'),
    ADD CONSTRAINT credential_registry_not_its_own_parent
        CHECK (rotation_parent_id IS DISTINCT FROM id);

CREATE INDEX credential_registry_rotation_parent_idx
    ON credential_registry (rotation_parent_id) WHERE rotation_parent_id IS NOT NULL;

-- Rotation preserves or narrows privilege (TRD section 12). A rotated row must
-- name a live parent of the same principal, class and domain owner, and may
-- carry only scopes and audiences its parent carries. Widening is a NEW
-- credential issued under its own authorization, not a rotation.
CREATE FUNCTION credential_registry_rotation_narrows() RETURNS trigger
    LANGUAGE plpgsql
AS $$
DECLARE
    parent credential_registry%ROWTYPE;
BEGIN
    IF NEW.rotation_parent_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO parent FROM credential_registry WHERE id = NEW.rotation_parent_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'rotation parent % does not exist', NEW.rotation_parent_id USING ERRCODE = '23503';
    END IF;
    IF parent.principal_id <> NEW.principal_id
       OR parent.credential_class <> NEW.credential_class
       OR parent.domain_owner <> NEW.domain_owner THEN
        RAISE EXCEPTION 'a rotated credential keeps its parent''s principal, class and domain owner'
            USING ERRCODE = '23514';
    END IF;
    IF parent.revoked_at IS NOT NULL THEN
        RAISE EXCEPTION 'rotation parent % is revoked', NEW.rotation_parent_id USING ERRCODE = '23514';
    END IF;
    IF NOT (NEW.scopes <@ parent.scopes AND NEW.audiences <@ parent.audiences) THEN
        RAISE EXCEPTION 'rotation may preserve or narrow scopes and audiences, never widen them'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER credential_registry_rotation_narrows
    BEFORE INSERT ON credential_registry
    FOR EACH ROW EXECUTE FUNCTION credential_registry_rotation_narrows();

-- Revocation is idempotent and monotonic (TRD section 12): re-writing the same
-- revoked_at and revoked_reason is a no-op, and a revoked row can never change
-- them or return to active. Privilege never widens in place, and identity and
-- lineage columns never change after insert.
CREATE FUNCTION credential_registry_lifecycle_is_monotonic() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.revoked_at IS NOT NULL
       AND (NEW.revoked_at IS DISTINCT FROM OLD.revoked_at
            OR NEW.revoked_reason IS DISTINCT FROM OLD.revoked_reason) THEN
        RAISE EXCEPTION 'credential % is revoked; revocation is monotonic', OLD.id USING ERRCODE = '23514';
    END IF;
    IF NEW.principal_id <> OLD.principal_id
       OR NEW.credential_class <> OLD.credential_class
       OR NEW.domain_owner <> OLD.domain_owner
       OR NEW.external_reference IS DISTINCT FROM OLD.external_reference
       OR NEW.rotation_parent_id IS DISTINCT FROM OLD.rotation_parent_id THEN
        RAISE EXCEPTION 'credential % identity and lineage are immutable', OLD.id USING ERRCODE = '23514';
    END IF;
    IF NOT (NEW.scopes <@ OLD.scopes AND NEW.audiences <@ OLD.audiences) THEN
        RAISE EXCEPTION 'credential % scopes and audiences may be narrowed in place, never widened', OLD.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER credential_registry_lifecycle_is_monotonic
    BEFORE UPDATE ON credential_registry
    FOR EACH ROW EXECUTE FUNCTION credential_registry_lifecycle_is_monotonic();
