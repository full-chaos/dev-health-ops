-- Validates the four CHECK constraints that 0019 and 0020 added NOT VALID, so
-- the database proves them for every stored row, not only for new writes:
-- memberships_active_has_role, resource_grants_kind_closed,
-- resource_grants_repository_key_canonical and service_accounts_named_owner.
-- No constraint definition changes.
--
-- No code path writes these tables today, and the NOT VALID constraints have
-- refused every violating write since 0019/0020, so only rows older than those
-- migrations can violate. The pre-check counts them per constraint and refuses
-- with the constraint name and the count (never row data) before any VALIDATE,
-- so an operator sees a clear message instead of a raw VALIDATE error. A
-- refusal fails the migration job and records nothing.
--
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE: reads and writes continue.
-- There is no down step: the lineage is forward-only.
DO $$
DECLARE
    findings text := '';
    violating bigint;
BEGIN
    SELECT count(*) INTO violating FROM memberships WHERE NOT (status <> 'active' OR role IS NOT NULL);
    IF violating > 0 THEN
        findings := findings || format(' memberships_active_has_role has %s violating row(s);', violating);
    END IF;

    SELECT count(*) INTO violating FROM resource_grants WHERE NOT (resource_kind IN ('repository'));
    IF violating > 0 THEN
        findings := findings || format(' resource_grants_kind_closed has %s violating row(s);', violating);
    END IF;

    SELECT count(*) INTO violating FROM resource_grants
    WHERE NOT (resource_kind <> 'repository' OR resource_id ~ '^([a-z0-9][a-z0-9._-]{0,99}/[a-z0-9][a-z0-9._-]{0,99}|\*)$');
    IF violating > 0 THEN
        findings := findings || format(' resource_grants_repository_key_canonical has %s violating row(s);', violating);
    END IF;

    SELECT count(*) INTO violating FROM service_accounts WHERE owner_principal_id IS NULL;
    IF violating > 0 THEN
        findings := findings || format(' service_accounts_named_owner has %s violating row(s);', violating);
    END IF;

    IF findings <> '' THEN
        RAISE EXCEPTION 'migration 0024 refused:%', findings || ' fix or remove them, then migrate again'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

ALTER TABLE memberships VALIDATE CONSTRAINT memberships_active_has_role;
ALTER TABLE resource_grants VALIDATE CONSTRAINT resource_grants_kind_closed;
ALTER TABLE resource_grants VALIDATE CONSTRAINT resource_grants_repository_key_canonical;
ALTER TABLE service_accounts VALIDATE CONSTRAINT service_accounts_named_owner;
