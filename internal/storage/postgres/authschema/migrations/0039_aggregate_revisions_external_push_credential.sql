-- 0039 aggregate_revisions admits the external_push_credential aggregate.
-- NOT ADDITIVE in two places, on purpose (additiveExemptions): it REDEFINES the
-- aggregate_type CHECK and the aggregate_id CHECK, and only WIDENS them
-- (nothing is removed from the 0033 type list; every id the 0013 pattern
-- admitted is still admitted), so no stored row can violate either and no
-- pre-flight count is needed.
--
-- external_push_credential is the aggregate of the External Push credential
-- lifecycle events (external-push-lifecycle-event.v1, CHAOS-10894): the
-- customer-push token writes append them to auth_outbox_events in their own
-- transaction (CHAOS-10895). The outbox table has no aggregate_type CHECK;
-- the per-aggregate revision counter does.
--
-- The credential's wire id is the External Push domain's own id, a UUID in
-- canonical text (the registry's external_reference), not a prefixed id like
-- the provisioning aggregates, so the id CHECK admits that shape for this one
-- aggregate type only.
--
-- The ALTER takes ACCESS EXCLUSIVE on aggregate_revisions for as long as the
-- scan to re-validate the CHECK takes.
ALTER TABLE aggregate_revisions
    DROP CONSTRAINT aggregate_revisions_aggregate_id_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_id_check
    CHECK (aggregate_id ~ '^[a-z]{3}_[A-Za-z0-9_-]{1,64}$'
        OR (aggregate_type = 'external_push_credential'
            AND aggregate_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')),
    DROP CONSTRAINT aggregate_revisions_aggregate_type_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_type_check
    CHECK (aggregate_type IN ('principal', 'user', 'external_identity',
        'organization', 'membership', 'platform_role_assignment', 'service_account', 'workload', 'resource_grant', 'invite',
        'external_push_credential'));

-- The only door the customer-push domain has to the outbox (CHAOS-10895).
--
-- go-api writes the External Push credential rows and, in the same transaction,
-- the credential's lifecycle events. The outbox and revision tables are
-- guarded by the 0006 trigger, but the trigger compares a setting any role may
-- set, so it is a lint against mistakes and not a role boundary. go-api's
-- login therefore holds NO table privilege here. It holds EXECUTE on this one
-- function, which runs as a dedicated NOLOGIN owner (auth-migrate hands the
-- ownership over after the migration; see applyPushLifecycleWriter) that
-- holds exactly INSERT on the outbox and SELECT/INSERT/UPDATE on the revision
-- table.
--
-- What the function fixes, so a caller cannot choose it: the aggregate type,
-- the closed event list, the members the payload and its change may carry, the
-- revision (the next one, from the same upsert audit.Commit uses) and the
-- idempotency key (aggregate_type:aggregate_id:revision). The row's created_at
-- is the column default, the database clock. No dynamic SQL runs in the body:
-- the schema name is spliced in once, here, when the function is created, so
-- every object in the body is schema-qualified and search_path is pinned.
--
-- A credential that predates the stream has no event yet. A scoped, revoked or
-- expired event for it carries p_snapshot (the credential in its created
-- form); the function then writes created at revision 1 and the event at 2,
-- both under the aggregate's revision row lock.
--
-- Until auth-migrate gives the function to the dedicated owner it is owned by
-- the migration role and nobody but its owner can execute it.
DO $migration$
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.append_external_push_lifecycle_event(
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
    v_member   text;
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
       OR jsonb_typeof(p_payload -> 'source_id') IS DISTINCT FROM 'string' THEN
        RAISE EXCEPTION 'external push lifecycle: payload must be an object with organization_id, source_id and change' USING ERRCODE = '22023';
    END IF;
    FOR v_member IN SELECT jsonb_object_keys(p_payload) LOOP
        IF v_member NOT IN ('organization_id', 'source_id', 'change') THEN
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
    INSERT INTO %1$I.aggregate_revisions AS r (aggregate_type, aggregate_id, revision)
    VALUES (v_type, p_aggregate_id, 1)
    ON CONFLICT (aggregate_type, aggregate_id)
    DO UPDATE SET revision = r.revision + 1, updated_at = clock_timestamp()
    RETURNING r.revision INTO v_revision;

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
                                   'source_id', p_payload -> 'source_id', 'change', p_snapshot),
                v_type || ':' || p_aggregate_id || ':1');
        UPDATE %1$I.aggregate_revisions SET revision = revision + 1, updated_at = clock_timestamp()
        WHERE aggregate_type = v_type AND aggregate_id = p_aggregate_id
        RETURNING revision INTO v_revision;
    END IF;

    INSERT INTO %1$I.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key)
    VALUES (v_type, p_aggregate_id, p_event_type,
            jsonb_build_object('revision', v_revision, 'organization_id', p_payload -> 'organization_id',
                               'source_id', p_payload -> 'source_id', 'change', p_payload -> 'change'),
            v_type || ':' || p_aggregate_id || ':' || v_revision::text);

    PERFORM set_config('dev_health.audit_txid', '', true);
    RETURN v_revision;
END;
$body$
$definition$, current_schema());
    EXECUTE format('REVOKE ALL ON FUNCTION %1$I.append_external_push_lifecycle_event(text, text, jsonb, jsonb) FROM PUBLIC',
                   current_schema());
END
$migration$;
