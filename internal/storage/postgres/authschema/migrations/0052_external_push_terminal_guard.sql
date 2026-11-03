-- 0051 append_external_push_lifecycle_event ends a credential once
-- (CHAOS-12332). A revoked or expired event is terminal: the projection
-- refuses any event after one and parks the row. Until now nothing stopped a
-- second terminal event (a sweeper's expired after an earlier expired, a route
-- revoke or rotate of a token the sweeper already expired, an expired after a
-- revoke), so each wrote a new revision that the consumer then parked.
--
-- aggregate_revisions gains terminal_event (NULL: not ended). The function
-- stores the event type that first ended the credential. A later revoked or
-- expired event for an ended credential writes nothing and returns the
-- credential's current revision. The check and the first write are ONE
-- statement: the upsert's DO UPDATE ... WHERE re-reads the row under its lock,
-- so two transactions that end the same credential cannot both write.
--
-- ADD COLUMN of a nullable column with no default is additive and keeps the
-- table's grants (the writer role's SELECT/INSERT/UPDATE are table-wide). The
-- function is REPLACED, not dropped, so its owner, EXECUTE grants, proconfig and
-- SECURITY DEFINER flag stay (restated exactly as 0047 has them); the migration
-- login must INHERIT the writer role, as for 0047. Signature, event list,
-- payload members, revision and key assignment, snapshot rule and commit guard
-- handling are 0047's.

ALTER TABLE aggregate_revisions ADD COLUMN terminal_event text;

DO $migration$
BEGIN
    EXECUTE format($definition$
CREATE OR REPLACE FUNCTION %1$I.append_external_push_lifecycle_event(
    p_event_type   text,
    p_aggregate_id text,
    p_payload      jsonb,
    p_snapshot     jsonb DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = %1$I, pg_temp
AS $body$
DECLARE
    v_type     constant text := 'external_push_credential';
    v_revision bigint;
    v_first    boolean;
    v_mark     text;
    v_member   text;
    v_binding  text;
BEGIN
    IF p_event_type IS NULL OR p_event_type NOT IN (
        'external_push_credential.created', 'external_push_credential.scoped',
        'external_push_credential.rotated', 'external_push_credential.revoked',
        'external_push_credential.expired') THEN
        RAISE EXCEPTION 'external push lifecycle: event_type is not one of the contract''s' USING ERRCODE = '22023';
    END IF;
    IF p_aggregate_id IS NULL OR p_aggregate_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'external push lifecycle: aggregate_id is not a canonical lowercase UUID' USING ERRCODE = '22023';
    END IF;
    IF p_payload IS NULL OR jsonb_typeof(p_payload) <> 'object'
       OR jsonb_typeof(p_payload -> 'change') IS DISTINCT FROM 'object'
       OR jsonb_typeof(p_payload -> 'organization_id') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_payload -> 'binding') IS DISTINCT FROM 'string' THEN
        RAISE EXCEPTION 'external push lifecycle: payload must be an object with organization_id, binding and change' USING ERRCODE = '22023';
    END IF;
    v_binding := p_payload ->> 'binding';
    IF v_binding NOT IN ('source', 'organization') THEN
        RAISE EXCEPTION 'external push lifecycle: binding is not source or organization' USING ERRCODE = '22023';
    END IF;
    IF v_binding = 'source' AND jsonb_typeof(p_payload -> 'source_id') IS DISTINCT FROM 'string' THEN
        RAISE EXCEPTION 'external push lifecycle: a source binding needs a source_id' USING ERRCODE = '22023';
    END IF;
    IF v_binding = 'organization' AND jsonb_exists(p_payload, 'source_id') THEN
        RAISE EXCEPTION 'external push lifecycle: an organization binding carries no source_id' USING ERRCODE = '22023';
    END IF;
    FOR v_member IN SELECT jsonb_object_keys(p_payload) LOOP
        IF v_member NOT IN ('organization_id', 'binding', 'source_id', 'change') THEN
            RAISE EXCEPTION 'external push lifecycle: payload member %% is not allowed', v_member USING ERRCODE = '22023';
        END IF;
    END LOOP;
    FOR v_member IN SELECT jsonb_object_keys(p_payload -> 'change') LOOP
        IF v_member NOT IN ('status', 'fingerprint', 'scopes', 'rotation_parent_id', 'expires_at') THEN
            RAISE EXCEPTION 'external push lifecycle: change member %% is not allowed', v_member USING ERRCODE = '22023';
        END IF;
    END LOOP;
    IF p_snapshot IS NOT NULL THEN
        IF jsonb_typeof(p_snapshot) <> 'object' THEN
            RAISE EXCEPTION 'external push lifecycle: the snapshot must be an object' USING ERRCODE = '22023';
        END IF;
        FOR v_member IN SELECT jsonb_object_keys(p_snapshot) LOOP
            IF v_member NOT IN ('status', 'fingerprint', 'scopes', 'expires_at') THEN
                RAISE EXCEPTION 'external push lifecycle: snapshot member %% is not allowed', v_member USING ERRCODE = '22023';
            END IF;
        END LOOP;
    END IF;

    IF current_setting('dev_health.audit_txid', true) IS NOT DISTINCT FROM pg_current_xact_id()::text THEN
        RAISE EXCEPTION 'external push lifecycle: the transaction is already armed for a guarded write' USING ERRCODE = '55000';
    END IF;
    PERFORM set_config('dev_health.audit_txid', pg_current_xact_id()::text, true);

    v_first := p_event_type IN ('external_push_credential.created', 'external_push_credential.rotated');
    v_mark := CASE WHEN p_event_type IN ('external_push_credential.revoked', 'external_push_credential.expired')
                   THEN p_event_type END;
    INSERT INTO %1$I.aggregate_revisions AS r (aggregate_type, aggregate_id, revision, terminal_event)
    VALUES (v_type, p_aggregate_id, 1, v_mark)
    ON CONFLICT (aggregate_type, aggregate_id)
    DO UPDATE SET revision = r.revision + 1, updated_at = clock_timestamp(),
                  terminal_event = coalesce(r.terminal_event, v_mark)
    WHERE v_mark IS NULL OR r.terminal_event IS NULL
    RETURNING r.revision INTO v_revision;

    IF v_revision IS NULL THEN
        -- The credential is already ended and this event ends it again: write nothing.
        SELECT r.revision INTO v_revision FROM %1$I.aggregate_revisions r
        WHERE r.aggregate_type = v_type AND r.aggregate_id = p_aggregate_id;
        PERFORM set_config('dev_health.audit_txid', '', true);
        RETURN v_revision;
    END IF;

    IF v_first AND v_revision <> 1 THEN
        RAISE EXCEPTION 'external push lifecycle: %% needs a credential with no earlier event, found revision %%', p_event_type, v_revision
            USING ERRCODE = '55000';
    END IF;
    IF NOT v_first AND v_revision = 1 THEN
        IF p_snapshot IS NULL THEN
            RAISE EXCEPTION 'external push lifecycle: %% for a credential with no earlier event needs its snapshot', p_event_type
                USING ERRCODE = '55000';
        END IF;
        INSERT INTO %1$I.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key)
        VALUES (v_type, p_aggregate_id, 'external_push_credential.created',
                jsonb_build_object('revision', 1, 'organization_id', p_payload -> 'organization_id',
                                   'binding', v_binding, 'change', p_snapshot)
                    || CASE WHEN v_binding = 'source'
                            THEN jsonb_build_object('source_id', p_payload -> 'source_id') ELSE '{}'::jsonb END,
                v_type || ':' || p_aggregate_id || ':1');
        UPDATE %1$I.aggregate_revisions SET revision = revision + 1, updated_at = clock_timestamp()
        WHERE aggregate_type = v_type AND aggregate_id = p_aggregate_id
        RETURNING revision INTO v_revision;
    END IF;

    INSERT INTO %1$I.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key)
    VALUES (v_type, p_aggregate_id, p_event_type,
            jsonb_build_object('revision', v_revision, 'organization_id', p_payload -> 'organization_id',
                               'binding', v_binding, 'change', p_payload -> 'change')
                || CASE WHEN v_binding = 'source'
                        THEN jsonb_build_object('source_id', p_payload -> 'source_id') ELSE '{}'::jsonb END,
            v_type || ':' || p_aggregate_id || ':' || v_revision::text);

    PERFORM set_config('dev_health.audit_txid', '', true);
    RETURN v_revision;
END;
$body$
$definition$, current_schema());
END
$migration$;
