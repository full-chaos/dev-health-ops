-- 0056 repository identity outbox events (CHAOS-13057, part of CHAOS-13034).
-- NOT ADDITIVE in two places, on purpose (additiveExemptions): it REDEFINES the aggregate_type CHECK and the
-- aggregate_id CHECK of aggregate_revisions and only WIDENS them (the 0048 type list plus one value; every id the 0044
-- pattern admitted is still admitted), so no stored row can violate either.
--
-- The provider sync (the worker) appends one repository_identity.observed event per synced repository that has a
-- provider id, in the same transaction that records the repos effect as committed. The event carries the full current
-- state (provider, provider id, live path, observed_at), so an add and a rename are the same event. The auth service
-- reads the events and upserts repository_identities (the copy, D6651; ClickHouse repos stays the source).
--
-- repository_identities gains the revision and the observed_at of the event that wrote the row. A row the one-shot
-- backfill wrote has revision 0 and no observed_at; the first event for it wins.
--
-- The aggregate is one repository of one organization: aggregate_id "<organization uuid>/<provider>/<provider id>".
ALTER TABLE aggregate_revisions
    DROP CONSTRAINT aggregate_revisions_aggregate_id_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_id_check
    CHECK (aggregate_id ~ '^[a-z]{3}_[A-Za-z0-9_-]{1,64}$'
        OR (aggregate_type = 'external_push_credential'
            AND aggregate_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        OR (aggregate_type = 'repository_identity'
            AND aggregate_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/[a-z]{1,16}/[0-9]{1,20}$')),
    DROP CONSTRAINT aggregate_revisions_aggregate_type_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_type_check
    CHECK (aggregate_type IN ('principal', 'user', 'external_identity',
        'organization', 'membership', 'platform_role_assignment', 'service_account', 'workload', 'resource_grant', 'invite',
        'external_push_credential', 'signing_key', 'repository_identity'));

ALTER TABLE repository_identities
    ADD COLUMN source_revision bigint NOT NULL DEFAULT 0 CHECK (source_revision >= 0),
    ADD COLUMN observed_at timestamptz;

-- The only door the worker has to the outbox for this aggregate. The worker's login holds EXECUTE on this function and
-- no table privilege in this schema; the function runs as a dedicated NOLOGIN owner (auth-migrate hands it over, see
-- the repository identity writer in authschema) that holds exactly INSERT on the outbox, USAGE on its sequence and
-- SELECT/INSERT/UPDATE on the revision table. It holds nothing on repository_identities: the auth service writes that.
--
-- What the function fixes, so a caller cannot choose it: the aggregate type, the event type, the payload members, the
-- shapes (the 0055 CHECKs of repository_identities), the revision (the next one, under the aggregate's revision row
-- lock) and the idempotency key (aggregate_type:aggregate_id:revision). created_at is the column default, the database
-- clock. The schema name is spliced in once, here, so every object in the body is schema-qualified and search_path
-- is pinned.
--
-- Until auth-migrate gives the function to the dedicated owner it is owned by the migration role and nobody but its
-- owner can execute it.
DO $migration$
BEGIN
    EXECUTE format($definition$
CREATE FUNCTION %1$I.append_repository_identity_event(
    p_organization_id  uuid,
    p_provider         text,
    p_provider_repo_id text,
    p_path             text,
    p_observed_at      timestamptz
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = %1$I, pg_temp
AS $body$
DECLARE
    v_type     constant text := 'repository_identity';
    v_id       text;
    v_revision bigint;
BEGIN
    IF p_organization_id IS NULL THEN
        RAISE EXCEPTION 'repository identity: organization_id is required' USING ERRCODE = '22023';
    END IF;
    IF p_provider IS NULL OR p_provider !~ '^[a-z]{1,16}$' THEN
        RAISE EXCEPTION 'repository identity: provider must match ^[a-z]{1,16}$' USING ERRCODE = '22023';
    END IF;
    IF p_provider_repo_id IS NULL OR p_provider_repo_id !~ '^[0-9]{1,20}$' THEN
        RAISE EXCEPTION 'repository identity: provider_repo_id must match ^[0-9]{1,20}$' USING ERRCODE = '22023';
    END IF;
    IF p_path IS NULL OR octet_length(p_path) > 255 OR p_path !~ '^[a-z0-9][a-z0-9._/-]*$' THEN
        RAISE EXCEPTION 'repository identity: path is not a canonical repository key' USING ERRCODE = '22023';
    END IF;
    IF p_observed_at IS NULL OR NOT isfinite(p_observed_at) THEN
        RAISE EXCEPTION 'repository identity: observed_at must be a finite instant' USING ERRCODE = '22023';
    END IF;

    IF current_setting('dev_health.audit_txid', true) IS NOT DISTINCT FROM pg_current_xact_id()::text THEN
        RAISE EXCEPTION 'repository identity: the transaction is already armed for a guarded write' USING ERRCODE = '55000';
    END IF;
    PERFORM set_config('dev_health.audit_txid', pg_current_xact_id()::text, true);

    v_id := p_organization_id::text || '/' || p_provider || '/' || p_provider_repo_id;
    INSERT INTO %1$I.aggregate_revisions AS r (aggregate_type, aggregate_id, revision)
    VALUES (v_type, v_id, 1)
    ON CONFLICT (aggregate_type, aggregate_id)
    DO UPDATE SET revision = r.revision + 1, updated_at = clock_timestamp()
    RETURNING r.revision INTO v_revision;

    INSERT INTO %1$I.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key)
    VALUES (v_type, v_id, 'repository_identity.observed',
            jsonb_build_object('revision', v_revision, 'organization_id', p_organization_id::text,
                               'change', jsonb_build_object('provider', p_provider, 'provider_repo_id', p_provider_repo_id,
                                                            'path', p_path,
                                                            'observed_at', to_char(p_observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))),
            v_type || ':' || v_id || ':' || v_revision::text);

    PERFORM set_config('dev_health.audit_txid', '', true);
    RETURN v_revision;
END;
$body$
$definition$, current_schema());
    EXECUTE format('REVOKE ALL ON FUNCTION %1$I.append_repository_identity_event(uuid, text, text, text, timestamptz) FROM PUBLIC',
                   current_schema());
END
$migration$;
