//go:build integration

package postgres

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	apiAuthorizationPass = "api_authorization_password"
	apiAuthorizationRole = "devhealth_api_authorization_test"
)

// bootstrapAPIRole creates the api role exactly the way provision_river_roles.sql's
// optional api_role block does: LOGIN, no elevated capabilities, CONNECT,
// USAGE on the public schema, no CREATE -- and nothing else. This is the
// declared baseline apiPosture()'s empty manifest expects CheckAPIAuthorization
// to accept.
//
// It also creates the River schema (IF NOT EXISTS) even though the api role
// never touches it: rolePostureQuery's has_schema_privilege($2, ...) calls
// raise a hard error on a schema name that does not exist at all (as opposed
// to one that exists but grants nothing), so the schema must be present for
// the "api role holds zero River privilege" assertion to evaluate to false
// rather than error -- every other suite in this package that calls a
// CheckRolePosture-family function creates this schema in its own fixture
// for the identical reason (see queue_authorization_integration_test.go's
// "CREATE SCHEMA river").
func bootstrapAPIRole(t *testing.T, ctx context.Context, admin *pgxpool.Pool, dbName, role string) {
	t.Helper()
	for _, statement := range []string{
		"CREATE SCHEMA IF NOT EXISTS river",
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + apiAuthorizationPass + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + role,
		// PUBLIC holds TEMPORARY on every database by default; has_database_
		// privilege resolves effective privilege including what a role
		// inherits via PUBLIC, so revoking only from the named role is not
		// enough -- provision_river_roles.sql's api_role block carries the
		// identical revoke, with the identical reasoning, for the same cause.
		"REVOKE TEMPORARY ON DATABASE " + dbName + " FROM PUBLIC, " + role,
		"GRANT USAGE ON SCHEMA public TO " + role,
		"REVOKE CREATE ON SCHEMA public FROM " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("bootstrap %s: %v", statement, err)
		}
	}
}

// acrEntitlementTables creates every table apiPosture() currently declares
// (CHAOS-6244's four SELECT-only entitlement/principal tables, and
// CHAOS-6247's webhook_deliveries/worker_job_outbox/pagerduty_webhook_bindings)
// and, by iterating apiPosture().RequiredTables itself rather than a
// hand-written per-table list, GRANTs each its exact declared privilege set
// to role -- standing in for the api-role grant-apply step
// api_authorization.go's NOTE says does not exist yet: this is the SAME
// manual/future-automated step, executed here so a test proves the
// readiness check accepts a role that genuinely holds the declared
// manifest, not just the pre-CHAOS-6244 empty one. Kept as one shared
// helper (not a second copy per route PR) since the grant loop already
// derives from apiPosture() generically; a new route PR adds only its own
// CREATE TABLE statements above.
func acrEntitlementTables(t *testing.T, ctx context.Context, admin *pgxpool.Pool, role string) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS organizations (id uuid PRIMARY KEY, tier text NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS feature_flags (
  id uuid PRIMARY KEY, key text UNIQUE NOT NULL, min_tier text NOT NULL,
  is_enabled boolean NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS org_feature_overrides (
  org_id uuid NOT NULL, feature_id uuid NOT NULL, is_enabled boolean NOT NULL,
  expires_at timestamptz, PRIMARY KEY (org_id, feature_id))`,
		`CREATE TABLE IF NOT EXISTS org_licenses (
  org_id uuid PRIMARY KEY, tier text NOT NULL, features_override json)`,
		`CREATE TABLE IF NOT EXISTS users (id uuid PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS memberships (id uuid PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS impersonation_sessions (id uuid PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS webhook_deliveries (id uuid PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS worker_job_outbox (id uuid PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS pagerduty_webhook_bindings (id uuid PRIMARY KEY)`,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("bootstrap %s: %v", statement, err)
		}
	}
	// The grants derive from apiPosture() itself, as the River migration's
	// api leg derives its GRANT statements; a declared table this helper
	// does not model gets a stand-in.
	for _, table := range apiPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}
	for _, table := range apiPosture().RequiredTables {
		privileges := "SELECT"
		if table.AllowInsert {
			privileges += ", INSERT"
		}
		if table.AllowUpdate {
			privileges += ", UPDATE"
		}
		if table.AllowDelete {
			privileges += ", DELETE"
		}
		if _, err := admin.Exec(ctx, "GRANT "+privileges+" ON "+table.TableName+" TO "+role); err != nil {
			t.Fatalf("grant on %s: %v", table.TableName, err)
		}
	}
}

// externalIngestTables creates and grants the 7 tables CHAOS-6246's
// apiPosture() addition declares (external_ingest_tokens/_sources/_batches/
// _batch_payloads/_rejections, integration_sources, integrations -- see
// api_authorization.go), with write grants matching exactly what the
// external-ingest route's own queries issue: UPDATE on tokens (bumping
// last_used_at/last_used_ip), INSERT+UPDATE on batches and batch_payloads
// (the CC22 accept sequence), SELECT-only on the rest. apiPosture()'s
// manifest now spans both this function's tables and
// acrEntitlementTables'; every caller below that asserts CheckAPIAuthorization
// succeeds calls both.
func externalIngestTables(t *testing.T, ctx context.Context, admin *pgxpool.Pool, role string) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS external_ingest_tokens (
  id uuid PRIMARY KEY, org_id text NOT NULL, source_id uuid, token_hash text UNIQUE NOT NULL,
  scopes jsonb NOT NULL, expires_at timestamptz, revoked_at timestamptz,
  last_used_at timestamptz, last_used_ip text)`,
		`CREATE TABLE IF NOT EXISTS external_ingest_sources (
  id uuid PRIMARY KEY, org_id text NOT NULL, system text NOT NULL, instance text NOT NULL,
  entity_family text NOT NULL DEFAULT 'legacy', mode text NOT NULL DEFAULT 'disabled',
  enabled boolean NOT NULL DEFAULT true)`,
		`CREATE TABLE IF NOT EXISTS external_ingest_batches (
  ingestion_id uuid PRIMARY KEY, org_id text NOT NULL, idempotency_key text NOT NULL,
  payload_hash text NOT NULL, source_system text NOT NULL, source_instance text NOT NULL,
  entity_family text NOT NULL DEFAULT 'legacy', producer text, producer_version text,
  schema_version text NOT NULL, window_started_at timestamptz, window_ended_at timestamptz,
  status text NOT NULL DEFAULT 'accepted', attempts integer NOT NULL DEFAULT 1,
  items_received integer NOT NULL DEFAULT 0, items_accepted integer NOT NULL DEFAULT 0,
  items_rejected integer NOT NULL DEFAULT 0, record_counts jsonb, error_summary jsonb,
  created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, completed_at timestamptz,
  recompute_status text NOT NULL DEFAULT 'not_applicable', recompute_scope jsonb,
  recompute_dispatched_at timestamptz, recompute_completed_at timestamptz, recompute_error text)`,
		`CREATE TABLE IF NOT EXISTS external_ingest_batch_payloads (
  ingestion_id uuid PRIMARY KEY, org_id text NOT NULL, schema_version text NOT NULL,
  payload_json bytea NOT NULL, byte_size integer NOT NULL, created_at timestamptz NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS external_ingest_rejections (
  id uuid PRIMARY KEY, org_id text NOT NULL, ingestion_id uuid NOT NULL, record_index integer NOT NULL,
  record_kind text NOT NULL, external_id text, code text NOT NULL, message text NOT NULL,
  path text, created_at timestamptz NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS integrations (id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, is_active boolean NOT NULL DEFAULT true, config jsonb NOT NULL DEFAULT '{}')`,
		`CREATE TABLE IF NOT EXISTS integration_sources (
  id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
  provider text NOT NULL, external_id text NOT NULL, name text NOT NULL, full_name text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}', is_enabled boolean NOT NULL DEFAULT true)`,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("bootstrap %s: %v", statement, err)
		}
	}
	grants := map[string]string{
		"external_ingest_tokens":         "SELECT, UPDATE",
		"external_ingest_sources":        "SELECT",
		"external_ingest_batches":        "SELECT, INSERT, UPDATE",
		"external_ingest_batch_payloads": "SELECT, INSERT, UPDATE",
		"external_ingest_rejections":     "SELECT",
		"integrations":                   "SELECT",
		"integration_sources":            "SELECT",
	}
	for table, privileges := range grants {
		if _, err := admin.Exec(ctx, "GRANT "+privileges+" ON "+table+" TO "+role); err != nil {
			t.Fatalf("grant %s on %s: %v", privileges, table, err)
		}
	}
}

// TestCheckAPIAuthorizationAcceptsTheBaselineBootstrap proves a role
// provisioned exactly the way provision_river_roles.sql's optional api_role
// block provisions it -- CONNECT, USAGE on public -- PLUS acrEntitlementTables'
// grants for apiPosture()'s full current manifest passes CheckAPIAuthorization.
// Before CHAOS-6244 the manifest was empty and the baseline alone was
// sufficient; TestCheckAPIAuthorizationRefusesTheBareBaselineNow below is the
// regression proof that the bare baseline is no longer enough.
func TestCheckAPIAuthorizationAcceptsTheBaselineBootstrap(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName(apiAuthorizationRole, instance)
	if err != nil {
		t.Fatal(err)
	}

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	bootstrapAPIRole(t, ctx, admin, dbName, role)
	acrEntitlementTables(t, ctx, admin, role)
	externalIngestTables(t, ctx, admin, role)

	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); err != nil {
		t.Fatalf("baseline-bootstrapped api role failed readiness: %v", err)
	}
}

// TestCheckAPIAuthorizationRefusesTheBareBaselineNow is the regression proof
// that apiPosture()'s declared manifest is genuinely enforced: a role
// bootstrapped the OLD way (CONNECT + USAGE only, no acrEntitlementTables
// grants) now FAILS readiness, where before CHAOS-6244 (apiPosture() empty)
// it passed. provision_river_roles.sql's api_role block still produces
// exactly this bare baseline (see TestProvisionScriptAPIRoleOptInMatchesTheDeclaredPosture's
// updated doc comment) -- the manifest is declared here, ahead of any
// automated step that applies it as GRANTs.
func TestCheckAPIAuthorizationRefusesTheBareBaselineNow(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName(apiAuthorizationRole+"_bare", instance)
	if err != nil {
		t.Fatal(err)
	}

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	bootstrapAPIRole(t, ctx, admin, dbName, role)

	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	err = CheckAPIAuthorization(ctx, api, role, grantSchema)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("bare-baseline api role readiness error = %v, want ErrUnavailable", err)
	}
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("bare-baseline api role readiness error = %v, want ErrPostureRefused", err)
	}
}

// TestCheckAPIAuthorizationRefusesAnExtraPrivilege proves the "no more" half
// of apiPosture()'s manifest: a role that holds every declared grant
// (acrEntitlementTables) PLUS an undeclared privilege on some other
// relation, fails readiness -- exactly the property that stops the api role
// from silently accumulating access no route has asked for.
func TestCheckAPIAuthorizationRefusesAnExtraPrivilege(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName(apiAuthorizationRole+"_extra", instance)
	if err != nil {
		t.Fatal(err)
	}

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	bootstrapAPIRole(t, ctx, admin, dbName, role)
	acrEntitlementTables(t, ctx, admin, role)
	externalIngestTables(t, ctx, admin, role)
	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); err != nil {
		t.Fatalf("baseline-bootstrapped api role failed readiness before the extra grant: %v", err)
	}

	if _, err := admin.Exec(
		ctx, "CREATE TABLE public.api_authorization_extra_table (id uuid PRIMARY KEY)",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(
		ctx, "GRANT SELECT ON public.api_authorization_extra_table TO "+role,
	); err != nil {
		t.Fatal(err)
	}

	err = CheckAPIAuthorization(ctx, api, role, grantSchema)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("api authorization with an extra table grant error = %v, want ErrUnavailable", err)
	}
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("api authorization with an extra table grant error = %v, want ErrPostureRefused", err)
	}
}

// TestCheckAPIAuthorizationRefusesAMissingBaselinePrivilege proves the
// "no less" half: even with every acrEntitlementTables grant held, the
// baseline rolePostureQuery itself requires of every runtime role
// unconditionally -- USAGE on the public schema -- is still checked.
// Revoking it must fail readiness regardless of the declared table manifest.
func TestCheckAPIAuthorizationRefusesAMissingBaselinePrivilege(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName(apiAuthorizationRole+"_missing", instance)
	if err != nil {
		t.Fatal(err)
	}

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	bootstrapAPIRole(t, ctx, admin, dbName, role)
	acrEntitlementTables(t, ctx, admin, role)
	externalIngestTables(t, ctx, admin, role)
	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); err != nil {
		t.Fatalf("baseline-bootstrapped api role failed readiness before the revoke: %v", err)
	}

	// PostgreSQL grants USAGE on the public schema to PUBLIC by default, and
	// has_schema_privilege resolves effective privilege, inherited-via-PUBLIC
	// included -- revoking only from the named role leaves it holding USAGE
	// anyway (it still passed, revoking from just itself, on the first
	// attempt at this test). Revoking from PUBLIC too is what actually
	// removes the role's effective USAGE, the same class of default-grant
	// this suite already accounts for on TEMPORARY (see bootstrapAPIRole).
	if _, err := admin.Exec(ctx, "REVOKE USAGE ON SCHEMA public FROM PUBLIC, "+role); err != nil {
		t.Fatal(err)
	}

	err = CheckAPIAuthorization(ctx, api, role, grantSchema)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("api authorization missing public-schema USAGE error = %v, want ErrUnavailable", err)
	}
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("api authorization missing public-schema USAGE error = %v, want ErrPostureRefused", err)
	}
}

// TestProvisionScriptAPIRoleOptInMatchesTheDeclaredPosture runs the REAL
// scripts/worker/provision_river_roles.sql through psql with api_role set --
// not a Go re-implementation of it -- and proves the role it produces
// matches bootstrapAPIRole's own hand-rolled mimic: CONNECT + USAGE on
// public, nothing else. Since CHAOS-6244, that bare baseline is no longer
// apiPosture()'s whole manifest (api_authorization.go's NOTE: no automated
// step yet applies acrEntitlementTables' grants), so the role the
// script produces correctly FAILS readiness here -- this test's job is
// proving the SQL file produces exactly the bare baseline, the same thing
// TestCheckAPIAuthorizationRefusesTheBareBaselineNow proves for the
// hand-rolled version, not that provisioning alone is sufficient for
// readiness.
func TestProvisionScriptAPIRoleOptInMatchesTheDeclaredPosture(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	role, err := containers.RoleName("provision_api_opt_in", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	// See bootstrapAPIRole's doc comment: CheckAPIAuthorization's riverSchema
	// argument requires the schema to literally exist, even for a role that
	// holds no privilege on it.
	if _, err := admin.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS river"); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(ctx, "psql",
		instance.URI,
		"--set=ON_ERROR_STOP=1",
		"--set=domain_role=provision_api_opt_in_domain_unused",
		"--set=queue_role=provision_api_opt_in_queue_unused",
		"--set=coordinator_role=provision_api_opt_in_coordinator_unused",
		"--set=domain_password=unused",
		"--set=queue_password=unused",
		"--set=coordinator_password=unused",
		"--set=api_role="+role,
		"--set=api_password="+apiAuthorizationPass,
		"--file="+provisionScriptPath(t),
	)
	t.Cleanup(func() {
		for _, unused := range []string{
			"provision_api_opt_in_domain_unused",
			"provision_api_opt_in_queue_unused",
			"provision_api_opt_in_coordinator_unused",
		} {
			containers.DropRole(admin, unused, t.Logf)
		}
	})
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("psql --file=provision_river_roles.sql (api_role opt-in) failed: %v\n%s", err, output)
	}

	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	err = CheckAPIAuthorization(ctx, api, role, grantSchema)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("provision_river_roles.sql's api_role opt-in readiness error = %v, want ErrUnavailable", err)
	}
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("provision_river_roles.sql's api_role opt-in readiness error = %v, want ErrPostureRefused", err)
	}

	// And the converse: granting acrEntitlementTables' privileges the
	// script itself does not (yet) apply is what actually gets the
	// script-provisioned role to readiness -- proving the refusal above is
	// caused exactly by the missing table grants, nothing else about the
	// script's output.
	acrEntitlementTables(t, ctx, admin, role)
	externalIngestTables(t, ctx, admin, role)
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); err != nil {
		t.Fatalf("provision_river_roles.sql's api_role opt-in failed readiness after the missing grants were added: %v", err)
	}
}

// TestProvisionScriptDefaultRunNeverCreatesAnAPIRole is the regression proof
// for the near-miss this optional shape exists to avoid: every EXISTING
// caller of provision_river_roles.sql (compose's go-river-provision
// service, the deploy chart's provision-roles hook) does not pass api_role
// today. A version of this script that made api_role a REQUIRED 4th role
// alongside domain/queue/coordinator would have made every one of those
// unmodified callers either block on an interactive \prompt with no
// terminal attached, or silently create devhealth_api with an empty
// password. This proves the default (api_role unset) invocation creates no
// role named devhealth_api at all and behaves identically to before this
// change.
func TestProvisionScriptDefaultRunNeverCreatesAnAPIRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	domainRole, err := containers.RoleName("provision_default_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("provision_default_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorRole, err := containers.RoleName("provision_default_coordinator", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		containers.DropRole(admin, domainRole, t.Logf)
		containers.DropRole(admin, queueRole, t.Logf)
		containers.DropRole(admin, coordinatorRole, t.Logf)
		containers.DropRole(admin, "devhealth_api", t.Logf)
	})

	cmd := exec.CommandContext(ctx, "psql",
		instance.URI,
		"--set=ON_ERROR_STOP=1",
		"--set=domain_role="+domainRole,
		"--set=queue_role="+queueRole,
		"--set=coordinator_role="+coordinatorRole,
		"--set=domain_password=unused",
		"--set=queue_password=unused",
		"--set=coordinator_password=unused",
		"--file="+provisionScriptPath(t),
	)
	// cmd.Stdin is left at its zero value (nil), matching every real caller
	// (compose, the chart hook) and go's own default: if a future edit
	// reintroduces an unconditional \prompt for api_password, this run
	// blocks on read from an already-closed stdin and fails loudly here
	// (via the 2-minute context timeout) rather than an existing production
	// caller discovering it live.
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("psql --file=provision_river_roles.sql (default, no api_role) failed: %v\n%s", err, output)
	}

	var count int
	if err := admin.QueryRow(
		ctx, "SELECT count(*) FROM pg_roles WHERE rolname = 'devhealth_api'",
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("provision_river_roles.sql's default (api_role unset) run created a devhealth_api role; it must not")
	}
}

// TestProvisionScriptRefusesAPIRoleCollidingWithKedaRole is the executed
// regression proof for a real defect a codex review round found and
// reproduced against this script (round pr2820-r1, VOID IN FORM but its
// findings independently reproduced here): api_role was checked for
// collision against domain_role/queue_role/coordinator_role but not against
// keda_role, so api_role=X + keda_role=X created ONE role the api block
// bootstraps to zero River privilege and the keda block then grants River
// USAGE + river_job SELECT to -- CheckAPIAuthorization's "holds zero River
// privilege" assertion then refuses it forever. The script must reject the
// collision outright rather than provision a role that can never become
// ready.
func TestProvisionScriptRefusesAPIRoleCollidingWithKedaRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	role, err := containers.RoleName("provision_api_keda_collision", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	cmd := exec.CommandContext(ctx, "psql",
		instance.URI,
		"--set=ON_ERROR_STOP=1",
		"--set=domain_role=provision_api_keda_collision_domain_unused",
		"--set=queue_role=provision_api_keda_collision_queue_unused",
		"--set=coordinator_role=provision_api_keda_collision_coordinator_unused",
		"--set=domain_password=unused",
		"--set=queue_password=unused",
		"--set=coordinator_password=unused",
		"--set=api_role="+role,
		"--set=api_password="+apiAuthorizationPass,
		"--set=keda_role="+role,
		"--set=keda_password=unused",
		"--file="+provisionScriptPath(t),
	)
	t.Cleanup(func() {
		for _, unused := range []string{
			"provision_api_keda_collision_domain_unused",
			"provision_api_keda_collision_queue_unused",
			"provision_api_keda_collision_coordinator_unused",
		} {
			containers.DropRole(admin, unused, t.Logf)
		}
	})
	output, runErr := cmd.CombinedOutput()
	if runErr == nil {
		t.Fatalf("provision_river_roles.sql must refuse api_role == keda_role, but exited 0:\n%s", output)
	}
	if !strings.Contains(string(output), "api_role must be distinct from keda_role") {
		t.Fatalf("expected the api/keda collision message, got:\n%s", output)
	}

	var count int
	if err := admin.QueryRow(
		ctx, "SELECT count(*) FROM pg_roles WHERE rolname = $1", role,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("the refused run must not have created role %s, but it exists", role)
	}
}

// TestProvisionScriptAPIRoleRefusedWhenPublicHoldsCreate is the executed
// regression proof for a defect a LATER review round found in the first fix
// attempt: naming PUBLIC in the api_role block's `REVOKE CREATE ON SCHEMA
// public` (mirroring the TEMPORARY revoke a few lines above) reproducibly
// stripped CREATE from every OTHER role relying on PUBLIC's grant, not just
// api_role -- a database-wide side effect of bootstrapping one unrelated
// role. The script now revokes CREATE from api_role alone. This test proves
// the consequence of that scoping choice: on a database where PUBLIC has
// been granted CREATE on the public schema (not PostgreSQL's own default
// since v15, but a state this script cannot assume away), api_role
// effectively holds CREATE too (has_schema_privilege resolves effective
// privilege, PUBLIC-inherited included), and CheckAPIAuthorization's "does
// not hold CREATE" assertion correctly REFUSES readiness -- the intended,
// loud signal that this target needs its own deliberate `REVOKE CREATE ...
// FROM PUBLIC`, never a silent side effect of this script.
func TestProvisionScriptAPIRoleRefusedWhenPublicHoldsCreate(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	role, err := containers.RoleName("provision_api_public_create", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := admin.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS river"); err != nil {
		t.Fatal(err)
	}

	// The anomalous precondition the review round's reproduction used: grant
	// PUBLIC CREATE on the public schema before provisioning. Restored to
	// REVOKE at cleanup so this test does not leave the shared instance (torn
	// down anyway, but explicit) in a state other tests don't expect.
	if _, err := admin.Exec(ctx, "GRANT CREATE ON SCHEMA public TO PUBLIC"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(closeCtx, "REVOKE CREATE ON SCHEMA public FROM PUBLIC")
	})

	cmd := exec.CommandContext(ctx, "psql",
		instance.URI,
		"--set=ON_ERROR_STOP=1",
		"--set=domain_role=provision_api_public_create_domain_unused",
		"--set=queue_role=provision_api_public_create_queue_unused",
		"--set=coordinator_role=provision_api_public_create_coordinator_unused",
		"--set=domain_password=unused",
		"--set=queue_password=unused",
		"--set=coordinator_password=unused",
		"--set=api_role="+role,
		"--set=api_password="+apiAuthorizationPass,
		"--file="+provisionScriptPath(t),
	)
	t.Cleanup(func() {
		for _, unused := range []string{
			"provision_api_public_create_domain_unused",
			"provision_api_public_create_queue_unused",
			"provision_api_public_create_coordinator_unused",
		} {
			containers.DropRole(admin, unused, t.Logf)
		}
	})
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("psql --file=provision_river_roles.sql (PUBLIC CREATE precondition) failed: %v\n%s", err, output)
	}

	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	err = CheckAPIAuthorization(ctx, api, role, grantSchema)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("api readiness with PUBLIC CREATE granted before provisioning: error = %v, want ErrUnavailable", err)
	}
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("api readiness with PUBLIC CREATE granted before provisioning: error = %v, want ErrPostureRefused", err)
	}

	// And the converse, same connection/role: revoking PUBLIC's grant (the
	// deliberate, human step this test's docstring says the script must not
	// take silently) PLUS granting acrEntitlementTables' privileges
	// the script itself does not yet apply (api_authorization.go's NOTE) lets
	// the identical role pass readiness -- proving the refusal above is
	// caused by PUBLIC's CREATE grant, not some other defect, and that it is
	// the ONLY thing standing between this role and readiness once the table
	// grants are present.
	if _, err := admin.Exec(ctx, "REVOKE CREATE ON SCHEMA public FROM PUBLIC"); err != nil {
		t.Fatal(err)
	}
	acrEntitlementTables(t, ctx, admin, role)
	externalIngestTables(t, ctx, admin, role)
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); err != nil {
		t.Fatalf("api role failed readiness after PUBLIC's CREATE grant was revoked: %v", err)
	}
}

// closePostgresInstanceInternal mirrors domain_authorization_integration_test.go's
// closePostgresInstance (package postgres_test, not importable from here).
func closePostgresInstanceInternal(t *testing.T, instance *containers.Instance) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := instance.Close(ctx); err != nil {
		t.Errorf("terminate PostgreSQL test dependency: %v", err)
	}
}

// TestCheckAPIAuthorizationRefusesAMissingDeclaredGrant is the "no less" half
// for every declared table: a role missing any one declared grant is not
// ready.
func TestCheckAPIAuthorizationRefusesAMissingDeclaredGrant(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName(apiAuthorizationRole+"_declared", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	bootstrapAPIRole(t, ctx, admin, dbName, role)
	acrEntitlementTables(t, ctx, admin, role)
	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); err != nil {
		t.Fatalf("fully granted api role failed readiness: %v", err)
	}
	for _, table := range apiPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "REVOKE ALL ON TABLE public."+table.TableName+" FROM "+role); err != nil {
			t.Fatal(err)
		}
		if err := CheckAPIAuthorization(ctx, api, role, grantSchema); !errors.Is(err, ErrPostureRefused) {
			t.Fatalf("api role missing its %s grant: readiness error %v, want ErrPostureRefused", table.TableName, err)
		}
		acrEntitlementTables(t, ctx, admin, role)
	}
}

// TestCheckAPIAuthorizationRefusesAWithheldBackfillJobsInsert withholds the one
// privilege CHAOS-6871 added to the api role, INSERT on backfill_jobs (the
// sync configuration's backfill POST records its history row): a role that
// keeps SELECT and DELETE but lacks INSERT must not be ready, or the route
// would answer 503 on the first backfill.
func TestCheckAPIAuthorizationRefusesAWithheldBackfillJobsInsert(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName(apiAuthorizationRole+"_bfjobs", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	bootstrapAPIRole(t, ctx, admin, dbName, role)
	acrEntitlementTables(t, ctx, admin, role)
	api := connectAs(t, ctx, instance.URI, role, apiAuthorizationPass)
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); err != nil {
		t.Fatalf("fully granted api role failed readiness: %v", err)
	}
	declared := false
	for _, table := range apiPosture().RequiredTables {
		if table.TableName == "backfill_jobs" {
			declared = table.AllowInsert
		}
	}
	if !declared {
		t.Fatal("the api posture no longer declares INSERT on backfill_jobs: the backfill route records its history row")
	}
	if _, err := admin.Exec(ctx, "REVOKE INSERT ON TABLE public.backfill_jobs FROM "+role); err != nil {
		t.Fatal(err)
	}
	if err := CheckAPIAuthorization(ctx, api, role, grantSchema); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("api role without INSERT on backfill_jobs: readiness error %v, want ErrPostureRefused", err)
	}
}
