-- signing_keys becomes the rotation state machine's durable store
-- (internal/auth/keyrotation, ACP-ADR-02 §6). Nothing wrote this table before:
-- the runtime role holds SELECT only and no operator path existed.
--
-- status takes the five rotation states of the key-set manifest; 'revoked'
-- is replaced by 'compromised', and revoked_at records when a key was
-- compromised. A compromised row is terminal: it is never updated or deleted,
-- so no rollback re-enables it. A kid, its algorithm and its public key never
-- change.
--
-- A row already 'revoked' (or carrying revoked_at) was written outside any
-- product path, since nothing wrote this table before. The migration refuses
-- it rather than guess its meaning: an operator reviews the row, removes it and
-- migrates again (`auth-migrate keys` then records the key's state).
DO $$
DECLARE
    revoked_rows bigint;
BEGIN
    SELECT count(*) INTO revoked_rows FROM signing_keys WHERE status = 'revoked' OR revoked_at IS NOT NULL;
    IF revoked_rows > 0 THEN
        RAISE EXCEPTION 'migration 0021 refused: signing_keys holds % revoked row(s); review and remove them, then migrate again', revoked_rows
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

ALTER TABLE signing_keys DROP CONSTRAINT signing_keys_status_check;
ALTER TABLE signing_keys ADD CONSTRAINT signing_keys_status_check
    CHECK (status IN ('pending', 'active', 'retiring', 'retired', 'compromised'));
ALTER TABLE signing_keys ADD CONSTRAINT signing_keys_compromised_revoked
    CHECK ((status = 'compromised') = (revoked_at IS NOT NULL));

-- state_since starts the publish-before-issue and retire-after-expiry floors.
-- activated_at/deactivated_at bound the window in which the key signed, so a
-- compromise revokes the sessions that minted under it ('-infinity' = signed
-- since before the table knew it; NULL activated_at = never signed).
ALTER TABLE signing_keys
    ADD COLUMN state_since    timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN activated_at   timestamptz,
    ADD COLUMN deactivated_at timestamptz;

-- At most one active key (one active issuer per token class), and one public
-- key under one kid only: a compromised key cannot return under a new kid.
CREATE UNIQUE INDEX signing_keys_one_active ON signing_keys ((status)) WHERE status = 'active';
CREATE UNIQUE INDEX signing_keys_public_key_x_key ON signing_keys ((public_key_jwk ->> 'x'));

CREATE FUNCTION signing_keys_compromised_terminal() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'signing key % is compromised; it cannot change or be removed', OLD.kid
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER signing_keys_compromised_terminal
    BEFORE UPDATE OR DELETE ON signing_keys
    FOR EACH ROW WHEN (OLD.status = 'compromised')
    EXECUTE FUNCTION signing_keys_compromised_terminal();

CREATE FUNCTION signing_keys_key_immutable() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'signing key % cannot change its kid, algorithm or public key', OLD.kid
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER signing_keys_key_immutable
    BEFORE UPDATE OF kid, algorithm, public_key_jwk ON signing_keys
    FOR EACH ROW WHEN (OLD.kid IS DISTINCT FROM NEW.kid OR OLD.algorithm IS DISTINCT FROM NEW.algorithm
                       OR OLD.public_key_jwk IS DISTINCT FROM NEW.public_key_jwk)
    EXECUTE FUNCTION signing_keys_key_immutable();

-- Every write goes through audit.Commit, with its audit row. TRUNCATE skips
-- row triggers, so it gets the statement guard 0006 gives every guarded
-- table, and it is refused outright while a compromised row exists (inside
-- audit.Commit too): a truncate would erase the deny list.
CREATE TRIGGER signing_keys_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON signing_keys
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER signing_keys_truncate_guard
    BEFORE TRUNCATE ON signing_keys
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();

CREATE FUNCTION signing_keys_compromised_no_truncate() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM signing_keys WHERE status = 'compromised') THEN
        RAISE EXCEPTION 'signing_keys holds compromised keys; it cannot be truncated'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER signing_keys_compromised_no_truncate
    BEFORE TRUNCATE ON signing_keys
    FOR EACH STATEMENT EXECUTE FUNCTION signing_keys_compromised_no_truncate();
