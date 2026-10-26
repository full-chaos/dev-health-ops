-- 0027 workload registry: an audience list and per-deployment bindings.
-- NOT ADDITIVE, on purpose (additiveExemptions): workloads loses audience,
-- trust_domain, issuer, namespace and service_account_name (and with them the
-- index workloads_federation_key). The registration is now the workload itself
-- (name, audiences, allowed_actions); WHERE it runs is a row of
-- workload_bindings, one per deployment, so one workload can run in more than
-- one environment and the federation tuple has one home.
--
-- workloads has no writer before this leaf (CHAOS-10980), so the drop loses no
-- data. The pre-flight below RAISES, naming the count, if any row exists: a row
-- written by hand must be re-registered through the writer, not converted here.
--
-- audiences: the audiences a workload token for this workload may carry
-- (audiences.json), at most 16, each in the provisioning-event.v1 audience
-- shape, distinct. Empty is allowed: a registry identity that never calls a
-- service (an assertion issuer, an envelope consumer) holds no workload audience.
--
-- workload_bindings: trust_domain, issuer, namespace and service_account_name
-- are the exact fields ACP-ADR-06 requires be matched against TokenReview's own
-- response. service_account_uid is optional; when set the exchange requires
-- equality. The tuple is UNIQUE across all workloads: one federated identity can
-- never resolve to two workloads. Only audit.Commit writes the table (the 0006
-- guard). The runtime role reads it and never writes it.

DO $$
DECLARE
    existing bigint;
BEGIN
    SELECT count(*) INTO existing FROM workloads;
    IF existing > 0 THEN
        RAISE EXCEPTION 'migration 0027: workloads holds % row(s); register them through the workload writer, then rerun', existing
            USING ERRCODE = '23000';
    END IF;
END
$$;

CREATE FUNCTION workload_audiences_are_valid(audiences text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $$
    SELECT cardinality(audiences) <= 16
       AND NOT EXISTS (SELECT 1 FROM unnest(audiences) AS a WHERE a !~ '^[a-z][a-z0-9._:-]{0,63}$')
       AND (SELECT count(DISTINCT a) FROM unnest(audiences) AS a) = cardinality(audiences)
$$;

ALTER TABLE workloads
    ADD COLUMN audiences text[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT workloads_audiences_valid
    CHECK (workload_audiences_are_valid(audiences)),
    DROP COLUMN audience,
    DROP COLUMN trust_domain,
    DROP COLUMN issuer,
    DROP COLUMN namespace,
    DROP COLUMN service_account_name;

CREATE TABLE workload_bindings (
    id                   uuid        PRIMARY KEY,
    workload_principal_id uuid       NOT NULL REFERENCES workloads (principal_id) ON DELETE RESTRICT,
    environment          text        NOT NULL CHECK (environment ~ '^[a-z][a-z0-9-]{0,31}$'),
    trust_domain         text        NOT NULL CHECK (trust_domain <> '' AND length(trust_domain) <= 253),
    issuer               text        NOT NULL CHECK (issuer <> '' AND length(issuer) <= 512),
    namespace            text        NOT NULL CHECK (namespace <> '' AND length(namespace) <= 253),
    service_account_name text        NOT NULL CHECK (service_account_name <> '' AND length(service_account_name) <= 253),
    service_account_uid  text        CHECK (service_account_uid IS NULL
        OR service_account_uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
    disabled_at          timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX workload_bindings_federation_key
    ON workload_bindings (trust_domain, issuer, namespace, service_account_name);
CREATE INDEX workload_bindings_workload_idx ON workload_bindings (workload_principal_id);

CREATE TRIGGER workload_bindings_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON workload_bindings
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER workload_bindings_truncate_guard
    BEFORE TRUNCATE ON workload_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
