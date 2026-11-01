-- 0041 delegated (impersonation) sessions. ADDITIVE ONLY. See 0001's header.
--
-- A delegated session is a row of auth.sessions: principal_id is the target
-- (the effective subject), organization_id the target organization, id the
-- delegation id, and these columns add the real actor (G-49, G-52). Revoke, pin,
-- password stamp, the organization verbs and go-api's sid check then work on it
-- with no new grant. It has no refresh_credentials row, so nothing can extend it.
--
-- Locks in writer order: auth.principals first, then auth.sessions. The foreign
-- key to principals needs SHARE ROW EXCLUSIVE there. Taken second, it could wait
-- behind a writer that waits for sessions, and auth-migrate does not retry a
-- deadlock. apply.go's lock_timeout bounds both waits (CHAOS-12200).
LOCK TABLE principals IN SHARE ROW EXCLUSIVE MODE;

-- The ALTER takes ACCESS EXCLUSIVE on sessions. The table is small (3 rows on
-- prod at this writing), so the CHECK's scan takes no time to speak of.
-- ON DELETE CASCADE: RESTRICT would block deleting the base's organization, and
-- SET NULL would break the CHECK. A gone row reads as a dead session.
ALTER TABLE sessions
    ADD COLUMN actor_principal_id uuid REFERENCES principals (id) ON DELETE CASCADE,
    ADD COLUMN delegated_from_session_id uuid REFERENCES sessions (id) ON DELETE CASCADE,
    ADD COLUMN delegation_reason text,
    ADD COLUMN delegation_permitted_actions text[],
    ADD CONSTRAINT sessions_delegation_shape CHECK (
        (actor_principal_id IS NULL AND delegated_from_session_id IS NULL
         AND delegation_reason IS NULL AND delegation_permitted_actions IS NULL)
        OR (actor_principal_id IS NOT NULL AND delegated_from_session_id IS NOT NULL
            AND delegation_reason IS NOT NULL AND delegation_permitted_actions IS NOT NULL
            AND organization_id IS NOT NULL
            AND actor_principal_id <> principal_id
            AND expires_at <= created_at + interval '15 minutes'
            AND char_length(btrim(delegation_reason)) BETWEEN 1 AND 500));

CREATE FUNCTION sessions_delegation_immutable() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'the delegation of session % cannot change', OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER sessions_delegation_immutable
    BEFORE UPDATE OF actor_principal_id, delegated_from_session_id, delegation_reason, delegation_permitted_actions ON sessions
    FOR EACH ROW WHEN (OLD.actor_principal_id IS DISTINCT FROM NEW.actor_principal_id
        OR OLD.delegated_from_session_id IS DISTINCT FROM NEW.delegated_from_session_id
        OR OLD.delegation_reason IS DISTINCT FROM NEW.delegation_reason
        OR OLD.delegation_permitted_actions IS DISTINCT FROM NEW.delegation_permitted_actions)
    EXECUTE FUNCTION sessions_delegation_immutable();

-- One unrevoked row per actor. No time term: a prior row that expired unrevoked
-- still counts, so Start ends it first. Plain indexes: CONCURRENTLY cannot run in
-- the migration's transaction.
CREATE UNIQUE INDEX sessions_one_live_delegation ON sessions (actor_principal_id)
    WHERE actor_principal_id IS NOT NULL AND revoked_at IS NULL;
CREATE INDEX sessions_delegated_from_idx ON sessions (delegated_from_session_id)
    WHERE delegated_from_session_id IS NOT NULL;
