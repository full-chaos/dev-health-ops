\set ON_ERROR_STOP on

-- Idempotent one-time provisioning for the three unprivileged Go runtime roles
-- (domain, queue-control, coordinator) of the CHAOS-3033 Option B split.
-- Run this with the database owner's/admin connection before the pinned River
-- migration. Passwords are prompted without echo unless supplied as psql
-- variables by an external secret-aware automation.
\if :{?domain_role}
\else
  \set domain_role devhealth_domain
\endif
\if :{?queue_role}
\else
  \set queue_role devhealth_queue
\endif
\if :{?coordinator_role}
\else
  \set coordinator_role devhealth_coordinator
\endif
\if :{?domain_password}
\else
  \prompt -1 'Domain runtime role password: ' domain_password
\endif
\if :{?queue_password}
\else
  \prompt -1 'Queue-control runtime role password: ' queue_password
\endif
\if :{?coordinator_password}
\else
  \prompt -1 'Coordinator runtime role password: ' coordinator_password
\endif

SELECT (
         :'domain_role' = :'queue_role'
         OR :'domain_role' = :'coordinator_role'
         OR :'queue_role' = :'coordinator_role'
       ) AS roles_match
\gset
\if :roles_match
  \echo 'domain_role, queue_role, and coordinator_role must be distinct'
  -- \quit accepts an exit-code argument only on newer psql clients; verified
  -- empirically against the psql installed where this script runs (16.15):
  -- \quit 2 prints "extra argument "2" ignored" and still exits 0, silently
  -- defeating this whole distinctness check for any caller that only reads
  -- the exit code (a Helm hook, compose's go-river-provision). A forced SQL
  -- error under ON_ERROR_STOP (set at the top of this file) is the
  -- version-independent way to guarantee a nonzero exit; the \echo above
  -- already carries the operator-readable reason, so the error text itself
  -- (division by zero) never needs to be descriptive.
  SELECT 1/0;
\endif

SELECT current_database() AS app_database
\gset

SELECT format(
         'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
         :'domain_role',
         :'domain_password'
       )
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'domain_role')
\gexec

SELECT format(
         'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
         :'queue_role',
         :'queue_password'
       )
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'queue_role')
\gexec

SELECT format(
         'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
         :'coordinator_role',
         :'coordinator_password'
       )
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'coordinator_role')
\gexec

GRANT CONNECT ON DATABASE :"app_database" TO :"domain_role";
GRANT CONNECT ON DATABASE :"app_database" TO :"queue_role";
GRANT CONNECT ON DATABASE :"app_database" TO :"coordinator_role";
REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role", :"queue_role", :"coordinator_role";

-- The coordinator runtime role of the CHAOS-3033 Option B split. It gets no
-- per-table grants here on purpose: its table privileges are owned by the
-- pinned River migration (internal/storage/river/migrate.go
-- coordinatorGrantStatements), derived from postgres.CoordinatorPosture(), the
-- same declaration CheckCoordinatorAuthorization asserts at readiness. This
-- script only has to make the role exist and be connectable BEFORE that
-- migration runs -- the migration's preflight rejects a coordinator role that
-- is missing or not a least-privilege login.
GRANT USAGE ON SCHEMA public TO :"coordinator_role";
REVOKE CREATE ON SCHEMA public FROM :"coordinator_role";

-- CHAOS-4261: this script is bootstrap-only. It used to REVOKE ALL and then
-- re-GRANT a hand-maintained per-table whitelist for the domain and queue
-- roles -- a second copy of the grant manifest that silently drifted behind
-- postgres.domainPosture()/coordinatorPosture() (internal/storage/postgres/
-- domain_authorization.go) as tables were added over time. Any compose
-- service that reached this script WITHOUT then running go-river-migrate
-- (pgbouncer startup, an operator `docker compose run go-workerctl`, a
-- deploy pass that stopped after pass 1) wiped the previous, correct grants
-- down to that stale subset -- CHAOS-4261's prod incident.
--
-- The ONLY authority for per-table/sequence privileges on all three runtime
-- roles is now internal/storage/river/migrate.go's runtimeGrantStatements /
-- coordinatorGrantStatements, applied by go-river-migrate every time it runs
-- (idempotent: REVOKE ALL then re-GRANT the full declared posture in one
-- transaction). This script only has to make the three logins exist,
-- connectable, and unable to CREATEDB/CREATEROLE/self-grant TEMPORARY --
-- everything a login needs before that migration can even preflight it --
-- and it can therefore never again REVOKE a grant migrate already applied,
-- no matter how many times or in what order it is re-run.
GRANT USAGE ON SCHEMA public TO :"domain_role";
REVOKE CREATE ON SCHEMA public FROM :"domain_role";
GRANT USAGE ON SCHEMA public TO :"queue_role";
REVOKE CREATE ON SCHEMA public FROM :"queue_role";

-- The dho api Service's own role (CHAOS-6269, spec.md §4.4). Optional, same
-- shape as keda_role below -- only provisioned when the caller passes
-- api_role, so this script's behaviour for every EXISTING unmodified caller
-- (compose's go-river-provision service, the deploy chart's provision-roles
-- hook) is completely unchanged: neither passes api_role today, and making
-- it a REQUIRED 4th role here would have made every one of those callers
-- either block on an interactive \prompt with no terminal attached, or
-- silently create devhealth_api with an empty password -- a real regression
-- this script must not introduce as a side effect of a change nothing yet
-- depends on. An operator (today: chris, by hand, per the ticket's
-- paste-ready line) opts in explicitly by passing api_role/api_password.
--
-- Not part of the CHAOS-3033 Option B split and never opens a River pool, so
-- it gets no River-schema grant of any kind, here or anywhere --
-- CheckAPIAuthorization (internal/storage/postgres/api_authorization.go)
-- asserts zero River privilege the same way it does for the three roles
-- above. Its table privileges, once routes exist to need any, are owned by
-- postgres.APIPosture() and applied by the api Service's own
-- provisioning/rollout step -- exactly the relationship coordinatorPosture()
-- already has to coordinatorGrantStatements. This script's job for it is
-- identical to the three roles above's: make the login exist, connectable,
-- and unable to CREATEDB/CREATEROLE/self-grant TEMPORARY or CREATE on the
-- public schema -- nothing this script does can ever revoke a grant a later
-- step applied.
\if :{?api_role}
  \if :{?api_password}
  \else
    \prompt -1 'API Service role password: ' api_password
  \endif

  SELECT (:'api_role' = :'domain_role' OR :'api_role' = :'queue_role' OR :'api_role' = :'coordinator_role') AS api_role_collides
  \gset
  \if :api_role_collides
    \echo 'api_role must be distinct from domain_role, queue_role, and coordinator_role'
    -- See the roles_match check above for why this is SELECT 1/0, not \quit N.
    SELECT 1/0;
  \endif
  -- keda_role is a SEPARATE optional \if block (below) whose vars are still
  -- available for comparison here regardless of textual order -- psql
  -- resolves --set variables at invocation time, not at the point their
  -- \if block appears. Guarded on :{?keda_role} since it may be entirely
  -- unset (comparing an unset psql variable errors, it does not read as
  -- false). Without this check, api_role == keda_role creates ONE role that
  -- this block bootstraps to zero River privilege and the keda block below
  -- then grants USAGE on the River schema + SELECT on river_job to --
  -- CheckAPIAuthorization's unconditional "holds zero River privilege"
  -- assertion then refuses it, so the api Service could never become ready.
  \if :{?keda_role}
    SELECT (:'api_role' = :'keda_role') AS api_role_collides_keda
    \gset
    \if :api_role_collides_keda
      \echo 'api_role must be distinct from keda_role'
      -- See the roles_match check above for why this is SELECT 1/0, not \quit N.
      SELECT 1/0;
    \endif
  \endif

  SELECT format(
           'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
           :'api_role',
           :'api_password'
         )
   WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'api_role')
  \gexec

  GRANT CONNECT ON DATABASE :"app_database" TO :"api_role";
  -- PUBLIC holds TEMPORARY (and CONNECT) on every database by default in
  -- PostgreSQL; has_database_privilege resolves EFFECTIVE privilege,
  -- inherited-via-PUBLIC included, so revoking only from api_role leaves the
  -- role holding TEMPORARY anyway and fails CheckAPIAuthorization's
  -- unconditional "does not hold TEMPORARY" assertion. The domain/queue/
  -- coordinator block above already revokes this from PUBLIC database-wide,
  -- so in every real invocation of this script (api_role is never the ONLY
  -- thing provisioned -- the three roles above always run first) this line
  -- is a no-op by the time it runs. Revoked here too, explicitly, so this
  -- block is correct standing alone and does not depend on running after
  -- that one.
  REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"api_role";
  GRANT USAGE ON SCHEMA public TO :"api_role";
  -- Deliberately role-scoped only. A review round proved that naming PUBLIC
  -- here (mirroring the TEMPORARY revoke above) has real blast radius: the
  -- TEMPORARY revoke above is a no-op in every real invocation (the
  -- mandatory domain/queue/coordinator block earlier in this script already
  -- revokes TEMPORARY from PUBLIC database-wide before this block ever
  -- runs), but there is no earlier unconditional REVOKE CREATE FROM PUBLIC
  -- anywhere in this file -- adding one here would strip CREATE on the
  -- public schema from every OTHER role in the database that relies on
  -- PUBLIC's grant (the PostgreSQL default before v15, still possible on
  -- any target regardless of version) as an undocumented side effect of
  -- bootstrapping one unrelated role. Reproduced directly: granting PUBLIC
  -- CREATE, then running this script with api_role set, revoked an
  -- unrelated pre-existing role's CREATE privilege out from under it.
  -- On a target where PUBLIC still holds CREATE on the public schema,
  -- api_role effectively holds it too (has_schema_privilege resolves
  -- PUBLIC-inherited privilege), and CheckAPIAuthorization's "does not hold
  -- CREATE" assertion correctly REFUSES readiness -- the loud signal that
  -- this target needs its own deliberate, human REVOKE CREATE ... FROM
  -- PUBLIC, not a silent side effect of this script. The three roles above
  -- (domain/queue/coordinator) carry the identical, pre-existing gap;
  -- unfixed here, tracked as a follow-up, not silently dismissed.
  REVOKE CREATE ON SCHEMA public FROM :"api_role";
\endif

-- The query-api Service's own role (CHAOS-6804). Optional, same shape as
-- api_role above -- only provisioned when the caller passes query_api_role, so
-- every existing caller of this script behaves exactly as before. An operator
-- opts in explicitly with query_api_role/query_api_password (the paste-ready
-- line is in deploy/go-workers/README.md).
--
-- It never opens a River pool and gets no River-schema privilege of any kind.
-- Its table privileges are owned by postgres.QueryAPIPosture() and applied by
-- `dho migrate river` when QUERY_API_DATABASE_ROLE names this role (REVOKE ALL
-- then GRANT the manifest); this script only makes the login exist,
-- connectable, and unable to CREATEDB/CREATEROLE/TEMPORARY/CREATE on the public
-- schema, and can never revoke a grant a later step applied. Naming the
-- registry-table OWNER (or a superuser) as query_api_role is refused by the
-- migrate leg, never REVOKEd.
\if :{?query_api_role}
  \if :{?query_api_password}
  \else
    \prompt -1 'query-api role password: ' query_api_password
  \endif

  SELECT (
           :'query_api_role' = :'domain_role'
           OR :'query_api_role' = :'queue_role'
           OR :'query_api_role' = :'coordinator_role'
         ) AS query_api_role_collides
  \gset
  \if :query_api_role_collides
    \echo 'query_api_role must be distinct from domain_role, queue_role, and coordinator_role'
    -- See the roles_match check above for why this is SELECT 1/0, not \quit N.
    SELECT 1/0;
  \endif
  -- api_role and keda_role are separate optional blocks whose variables are
  -- available for comparison regardless of textual order (psql resolves --set
  -- variables at invocation time); each comparison is guarded on the variable
  -- being set, since comparing an unset psql variable errors.
  \if :{?api_role}
    SELECT (:'query_api_role' = :'api_role') AS query_api_role_collides_api
    \gset
    \if :query_api_role_collides_api
      \echo 'query_api_role must be distinct from api_role'
      SELECT 1/0;
    \endif
  \endif
  \if :{?keda_role}
    SELECT (:'query_api_role' = :'keda_role') AS query_api_role_collides_keda
    \gset
    \if :query_api_role_collides_keda
      \echo 'query_api_role must be distinct from keda_role'
      SELECT 1/0;
    \endif
  \endif

  SELECT format(
           'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
           :'query_api_role',
           :'query_api_password'
         )
   WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'query_api_role')
  \gexec

  GRANT CONNECT ON DATABASE :"app_database" TO :"query_api_role";
  -- See the api_role block for why TEMPORARY is revoked from PUBLIC here too,
  -- and why CREATE on the public schema is revoked from the role only.
  REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"query_api_role";
  GRANT USAGE ON SCHEMA public TO :"query_api_role";
  REVOKE CREATE ON SCHEMA public FROM :"query_api_role";
\endif

-- The KEDA postgresql scaler's read-only role. Optional -- only
-- provisioned when the caller passes keda_role (the Helm hook does this only
-- when a goWorkers group has autoscaling.enabled=true; Compose never sets
-- it). Unlike the three roles above, its password is re-applied every run
-- (ALTER ROLE, not just at creation): it authenticates a scaler credential
-- that a rotation should take effect on without a role drop/recreate. It
-- gets no public schema access at all -- only USAGE on the River schema and
-- SELECT on river_job, nothing else.
\if :{?keda_role}
  \if :{?keda_password}
  \else
    \prompt -1 'KEDA read-only role password: ' keda_password
  \endif
  \if :{?river_schema}
  \else
    \set river_schema river
  \endif

  SELECT format(
           'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
           :'keda_role',
           :'keda_password'
         )
   WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'keda_role')
  \gexec

  ALTER ROLE :"keda_role" PASSWORD :'keda_password';

  GRANT CONNECT ON DATABASE :"app_database" TO :"keda_role";
  REVOKE TEMPORARY ON DATABASE :"app_database" FROM :"keda_role";

  GRANT USAGE ON SCHEMA :"river_schema" TO :"keda_role";
  GRANT SELECT ON :"river_schema".river_job TO :"keda_role";
\endif
