//go:build integration

package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/producttelemetry"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// CHAOS-6804, the completeness proof for the query-api manifest. The manifest
// (postgres.QueryAPIPosture) is a static read of the code; this test is what
// says it is enough. On the REAL migrated schema, a role provisioned the way a
// deployment provisions it (the real migrate leg: REVOKE ALL then GRANT the
// manifest) drives every Postgres path `dho query-api` reaches, through the
// real package code, and every one must succeed:
//
//   - routeswitch.PostgresSwitch.Enabled and the registry drift log
//   - reports Reader List/Get/Runs and Writer Create/Update/Clone/Trigger/Delete
//   - data-health connectors, the home freshness read, the product-telemetry
//     org names
//   - the BYO-LLM feature gate and settings read
//
// then the posture check passes on the same login. A path that is missing from
// the manifest fails HERE as a permission denial, not in production.
func TestQueryAPIRoleServesEveryPostgresPathItReaches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIRoleFixture(t, ctx)
	pool := fixture.rolePool

	if err := postgresstore.CheckQueryAPIAuthorization(ctx, pool, fixture.role, "river"); err != nil {
		t.Fatalf("the provisioned role must satisfy the posture before it serves anything: %v", err)
	}
	fixture.driveEveryPath(t, ctx, pool)
	if err := postgresstore.CheckQueryAPIAuthorization(ctx, pool, fixture.role, "river"); err != nil {
		t.Fatalf("the role stopped satisfying the posture after serving: %v", err)
	}
}

// TestQueryAPIRoleCannotReadColumnsOutsideItsGrant is D2910's explicit
// requirement: least privilege means a column-scoped table's OTHER
// columns -- users.password_hash above all -- must be unreachable as the
// restricted role, not merely undeclared. Column-level grants are additive
// (there is no "REVOKE ALL" equivalent to prove a negative against), so
// this asserts the negative directly: SELECT on a column this role was
// never granted fails with a real Postgres permission denial.
func TestQueryAPIRoleCannotReadColumnsOutsideItsGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIRoleFixture(t, ctx)

	for _, column := range []string{"password_hash", "username", "full_name", "avatar_url", "auth_provider_id"} {
		var value any
		err := fixture.rolePool.QueryRow(ctx, "SELECT "+column+" FROM users WHERE id = $1", uuid.MustParse(pathsAdminUser)).Scan(&value)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("SELECT users.%s as the restricted role: got %v, want a 42501 permission denial", column, err)
		}
	}

	// The columns this role SHOULD hold must still work -- a test that
	// only proves the negative could pass on a role denied everything.
	var isActive bool
	if err := fixture.rolePool.QueryRow(ctx, "SELECT is_active FROM users WHERE id = $1", uuid.MustParse(pathsAdminUser)).Scan(&isActive); err != nil {
		t.Errorf("SELECT users.is_active (a granted column) as the restricted role: %v", err)
	}
}

// TestQueryAPIRoleDriverObservesAMissingColumnGrant is
// TestQueryAPIRoleDriverObservesAMissingGrant's column-scoped counterpart
// (D2910): withhold each declared column SELECT in turn and confirm the
// specific policy call that reads it fails with a permission denial
// naming that column's table.
func TestQueryAPIRoleDriverObservesAMissingColumnGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIRoleFixture(t, ctx)
	policyStore := policy.PGStore{Pool: fixture.rolePool}

	probe := func() error {
		if _, _, err := policyStore.UserState(ctx, uuid.MustParse(pathsAdminUser)); err != nil {
			return err
		}
		if _, _, err := policyStore.Membership(ctx, uuid.MustParse(pathsAdminUser), uuid.MustParse(pathsOrg)); err != nil {
			return err
		}
		if _, err := policyStore.ActiveImpersonation(ctx, uuid.MustParse(pathsAdminUser)); err != nil {
			return err
		}
		return nil
	}

	for _, column := range postgresstore.QueryAPIPosture().ColumnScoped {
		revoke := "REVOKE SELECT (" + column.ColumnName + ") ON public." + column.TableName + " FROM " + fixture.role
		if _, err := fixture.admin.Exec(ctx, revoke); err != nil {
			t.Fatalf("%s: %v", revoke, err)
		}
		err := probe()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" || !strings.Contains(pgErr.Message, column.TableName) {
			t.Errorf("with SELECT on %s.%s withheld, probe = %v, want a 42501 permission denial naming %s",
				column.TableName, column.ColumnName, err, column.TableName)
		}
		grant := "GRANT SELECT (" + column.ColumnName + ") ON public." + column.TableName + " TO " + fixture.role
		if _, err := fixture.admin.Exec(ctx, grant); err != nil {
			t.Fatalf("%s: %v", grant, err)
		}
		if err := probe(); err != nil {
			t.Fatalf("after restoring %s.%s, probe still fails: %v", column.TableName, column.ColumnName, err)
		}
	}
}

// The driver above is only worth anything if it can fail. Withhold each
// declared SELECT in turn (one role each would be slow; one relation at a time
// on one role is enough) and the driver must report a permission denial for the
// path that reads that relation: a driver that passed anyway would be a test
// that cannot fail.
func TestQueryAPIRoleDriverObservesAMissingGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIRoleFixture(t, ctx)
	for _, table := range postgresstore.QueryAPIPosture().RequiredTables {
		if _, err := fixture.admin.Exec(ctx, "REVOKE SELECT ON public."+table.TableName+" FROM "+fixture.role); err != nil {
			t.Fatalf("revoke %s: %v", table.TableName, err)
		}
		failures := fixture.driveEveryPathCollecting(t, ctx, fixture.rolePool)
		if !hasPermissionDenied(failures, table.TableName) {
			t.Errorf("with SELECT on %s withheld the driver saw no permission denial naming it (saw %v): the driver does not reach that relation", table.TableName, failures)
		}
		if _, err := fixture.admin.Exec(ctx, "GRANT SELECT ON public."+table.TableName+" TO "+fixture.role); err != nil {
			t.Fatalf("restore %s: %v", table.TableName, err)
		}
	}
}

func hasPermissionDenied(failures []error, table string) bool {
	for _, failure := range failures {
		// The outbox producer deliberately hides the database error behind its
		// own bounded "unavailable" error, so a denied outbox read surfaces as
		// that, from the triggerReport path only.
		if table == "worker_job_outbox" && strings.Contains(failure.Error(), "triggerReport") &&
			strings.Contains(failure.Error(), "worker outbox database unavailable") {
			return true
		}
		var pgErr *pgconn.PgError
		if errors.As(failure, &pgErr) && pgErr.Code == "42501" && strings.Contains(pgErr.Message, table) {
			return true
		}
	}
	return false
}

type queryAPIRoleFixture struct {
	// runs makes each driver pass name its rows uniquely: a report schedule and
	// a sync configuration are unique per (org, name).
	runs     int
	admin    *pgxpool.Pool
	rolePool *pgxpool.Pool
	role     string
	org      string
}

const (
	pathsOrg      = "0a0a0a0a-0a0a-4a0a-8a0a-0a0a0a0a0a0a"
	pathsSchema   = "paths-schema-digest"
	pathsDocument = "paths-document-digest"
	pathsOp       = "pathsOperation"

	// CHAOS-6263 PR (a): the identity fixtures /query's edge-carrier path
	// reads (internal/api/policy.PGStore) -- an active superuser (also the
	// impersonation session's admin), the org they are really a member of,
	// and a target user/org for the impersonation session.
	pathsAdminUser         = "0b0b0b0b-0b0b-4b0b-8b0b-0b0b0b0b0b0b"
	pathsTargetUser        = "0c0c0c0c-0c0c-4c0c-8c0c-0c0c0c0c0c0c"
	pathsTargetOrg         = "0d0d0d0d-0d0d-4d0d-8d0d-0d0d0d0d0d0d"
	pathsMembershipID      = "0e0e0e0e-0e0e-4e0e-8e0e-0e0e0e0e0e0e"
	pathsImpersonationID   = "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f"
	pathsAdminTokenVersion = 3
)

func startQueryAPIRoleFixture(t *testing.T, ctx context.Context) *queryAPIRoleFixture {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}

	domain, err := containers.RoleName("paths_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := containers.RoleName("paths_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	role, err := containers.RoleName("paths_query_api", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "paths_role_password"
	statements := []string{"REVOKE TEMPORARY ON DATABASE " + dbName + " FROM PUBLIC"}
	for _, name := range []string{domain, queue, role} {
		statements = append(statements,
			"CREATE ROLE "+name+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'",
			"GRANT CONNECT ON DATABASE "+dbName+" TO "+name,
		)
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	for _, name := range []string{domain, queue, role} {
		name := name
		t.Cleanup(func() { containers.DropRole(admin, name, t.Logf) })
	}

	grants := make([]riverstore.TableGrant, 0)
	for _, table := range postgresstore.QueryAPIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	columnGrants := make([]riverstore.ColumnGrant, 0)
	for _, column := range postgresstore.QueryAPIPosture().ColumnScoped {
		columnGrants = append(columnGrants, riverstore.ColumnGrant{
			TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege,
		})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domain, QueueRole: queue,
		QueryAPIRole: role, QueryAPIGrants: grants, QueryAPIColumnGrants: columnGrants,
	}); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the query-api leg: %v", err)
	}

	rolePool, err := pgxpool.New(ctx, withRole(t, instance.URI, role, password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rolePool.Close)

	pgseed.Org(ctx, t, admin, pathsOrg, "enterprise")
	pgseed.SetFeatureFlag(ctx, t, admin, "1b1b1b1b-1b1b-4b1b-8b1b-1b1b1b1b1b1b", "byo_llm", "free", true)
	pgseed.OrgLicense(ctx, t, admin, pathsOrg, "enterprise", "{}")
	pgseed.Setting(ctx, t, admin, pathsOrg, "llm", "provider", "openai", false)
	pgseed.RoutingState(ctx, t, admin, pathsSchema, pathsDocument, pathsOp, "primary")

	// CHAOS-6263 PR (a): identity fixtures for /query's edge-carrier
	// PGStore reads (see the consts' own doc comment).
	pgseed.Org(ctx, t, admin, pathsTargetOrg, "enterprise")
	pgseed.User(ctx, t, admin, pathsAdminUser)
	pgseed.SetUserState(ctx, t, admin, pathsAdminUser, true, true, pathsAdminTokenVersion)
	pgseed.User(ctx, t, admin, pathsTargetUser)
	pgseed.Membership(ctx, t, admin, pathsMembershipID, pathsAdminUser, pathsOrg, "member")
	pgseed.ImpersonationSession(ctx, t, admin, pathsImpersonationID, pathsAdminUser, pathsTargetUser, pathsTargetOrg, "member",
		time.Now().Add(time.Hour), nil)

	return &queryAPIRoleFixture{admin: admin, rolePool: rolePool, role: role, org: pathsOrg}
}

// driveEveryPath fails the test on the first path that errors.
func (f *queryAPIRoleFixture) driveEveryPath(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, failure := range f.driveEveryPathCollecting(t, ctx, pool) {
		t.Errorf("a query-api Postgres path failed as the provisioned role: %v", failure)
	}
}

// driveEveryPathCollecting runs every path and returns each failure instead of
// stopping, so a caller can ask WHICH relation a withheld grant broke.
func (f *queryAPIRoleFixture) driveEveryPathCollecting(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []error {
	t.Helper()
	f.runs++
	suffix := fmt.Sprintf(" #%d", f.runs)
	var failures []error
	fail := func(what string, err error) {
		if err != nil {
			failures = append(failures, err)
			t.Logf("path %s: %v", what, err)
		}
	}

	// Routing registry: Enabled swallows a query error into "false", so assert
	// the STATE it exists to reach (the seeded "go" row reads as reachable).
	registry := routeswitch.NewPostgresSwitch(pool, pathsSchema, map[string]string{pathsOp: pathsDocument})
	if !registry.Enabled(pathsOp) {
		fail("routeswitch.Enabled", errors.New("Enabled() = false for a seeded reachable routing row (a swallowed permission error reads exactly like this)"))
		// Surface the underlying denial, which Enabled logs and hides.
		var mode string
		fail("routeswitch.Enabled/direct", pool.QueryRow(ctx,
			`SELECT mode FROM go_api_routing_state WHERE schema_digest = $1 AND document_digest = $2 AND selected_operation = $3`,
			pathsSchema, pathsDocument, pathsOp).Scan(&mode))
	}
	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	logRoutingStateDrift(pool, pathsSchema)
	log.SetOutput(previous)
	if strings.Contains(logged.String(), "failed") {
		var pgErr error
		_, pgErr = pool.Exec(ctx, `SELECT schema_digest, count(*) FROM go_api_routing_state GROUP BY schema_digest`)
		fail("registry drift", pgErr)
	}

	// Saved reports: every mutation, then every read.
	writer := newReportWriter(pool, filepath.Join(repoRootFromHere(t), "contracts", "jobs", "v1"))
	if writer == nil || writer.Outbox == nil {
		fail("report writer", errors.New("the writer has no outbox: triggerReport would not be exercised"))
		return failures
	}
	cron, tz := "0 9 * * 1", "UTC"
	description := "paths"
	created, err := writer.Create(ctx, f.org, reports.CreateInput{
		Name: "paths report" + suffix, Description: &description, ReportPlan: []byte(`{"steps":[]}`),
		Parameters: []byte(`{}`), ScheduleCron: &cron, ScheduleTimezone: tz,
	})
	fail("writer.Create", err)
	if err == nil && created != nil {
		newName := "paths report renamed" + suffix
		_, err = writer.Update(ctx, f.org, created.ID, reports.UpdateInput{Name: &newName})
		fail("writer.Update", err)
		clone, cloneErr := writer.Clone(ctx, f.org, reports.CloneInput{SourceReportID: created.ID})
		fail("writer.Clone", cloneErr)
		_, err = writer.Trigger(ctx, f.org, created.ID)
		fail("writer.Trigger", err)

		reader := &reports.Reader{Postgres: pool}
		_, err = reader.List(ctx, f.org, 10, 0)
		fail("reader.List", err)
		_, err = reader.Get(ctx, f.org, created.ID)
		fail("reader.Get", err)
		_, err = reader.Runs(ctx, f.org, created.ID, 10)
		fail("reader.Runs", err)

		// Data health connectors: a sync configuration with a job attached.
		_, err = f.admin.Exec(ctx, `
INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'paths connector' || $2::text, 'github', '[]'::json, '{}'::json, TRUE, now(), now())`, f.org, suffix)
		if err != nil {
			t.Fatalf("seed sync_configurations: %v", err)
		}
		dh := &datahealth.Reader{Postgres: pool}
		_, err = dh.Connectors(ctx, f.org)
		fail("datahealth.Connectors", err)

		if clone != nil {
			if _, err := writer.Delete(ctx, f.org, clone.ID); err != nil {
				fail("writer.Delete", err)
			}
		}
	}

	_, err = home.FetchLatestSuccessfulSyncAt(ctx, pool, f.org)
	fail("home.FetchLatestSuccessfulSyncAt", err)
	_, err = producttelemetry.LoadOrgNames(ctx, pool)
	fail("producttelemetry.LoadOrgNames", err)
	_, err = llmorgsettings.Store{Pool: pool}.ResolveUsableProvider(ctx, f.org)
	fail("llmorgsettings.ResolveUsableProvider", err)

	// CHAOS-6263 PR (a): /query's edge-carrier PGStore reads, driven AS THE
	// RESTRICTED ROLE (pool), against internal/api/policy.PGStore directly
	// -- the same store buildQueryEdgeAuthenticatorFromEnv constructs, not
	// a second reimplementation.
	policyStore := policy.PGStore{Pool: pool}
	state, found, err := policyStore.UserState(ctx, uuid.MustParse(pathsAdminUser))
	fail("policy.UserState", err)
	if err == nil && (!found || !state.IsActive || !state.IsSuperuser || state.TokenVersion != pathsAdminTokenVersion) {
		fail("policy.UserState", fmt.Errorf("unexpected state: found=%v state=%+v", found, state))
	}
	_, isMember, err := policyStore.Membership(ctx, uuid.MustParse(pathsAdminUser), uuid.MustParse(pathsOrg))
	fail("policy.Membership", err)
	if err == nil && !isMember {
		fail("policy.Membership", errors.New("expected the seeded membership row, found none"))
	}
	session, err := policyStore.ActiveImpersonation(ctx, uuid.MustParse(pathsAdminUser))
	fail("policy.ActiveImpersonation", err)
	if err == nil && session == nil {
		fail("policy.ActiveImpersonation", errors.New("expected the seeded active impersonation session, found none"))
	}

	// CHAOS-7096: /query/proof-write's org-allowlist read. go_api_proof_orgs
	// is otherwise never reached by anything above -- without this, revoking
	// SELECT on it produces no failure naming it (caught live: this driver
	// originally had no path to that relation at all, per TestQueryAPIRole
	// DriverObservesAMissingGrant's own "the driver does not reach that
	// relation" message).
	_, err = postgresstore.ProofOrgAllowed(ctx, pool, f.org)
	fail("postgresstore.ProofOrgAllowed", err)
	return failures
}

// CHAOS-6804 r1: the production readiness path (queryAPIPostureCheck ->
// NewCachedQueryAPIPostureCheck -> CheckNoWait) must run the strengthened proofs,
// not only the shared manifest check. A role that satisfies the manifest but holds
// a privilege in a third schema must end up NotReady through THAT path, and a
// clean role must end up ready.
func TestQueryAPIPostureCheckInProductionRefusesAGrantOutsideTheManagedSchemas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIRoleFixture(t, ctx)
	env := func(key string) string {
		if key == "QUERY_API_DATABASE_ROLE" {
			return fixture.role
		}
		return ""
	}
	// The production composition with a short freshness window: the default
	// re-proves a passing answer every 300 s (CHAOS-6937), and this test watches a
	// grant change reach readiness within a minute.
	check := queryAPIPostureCheckWith(env, fixture.rolePool, postgresstore.PostureCheckOptions{
		TTL: time.Second, MaxStale: time.Minute, Jitter: -1,
	})
	waitReady := func(what string, want func(error) bool) error {
		deadline := time.Now().Add(60 * time.Second)
		for {
			err := check(ctx)
			if want(err) {
				return err
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: last answer %v", what, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitReady("a clean role becomes ready", func(err error) bool { return err == nil })

	for _, statement := range []string{
		"CREATE SCHEMA third_party",
		"CREATE TABLE third_party.secrets (id int)",
		"GRANT USAGE ON SCHEMA third_party TO " + fixture.role,
		"GRANT SELECT ON third_party.secrets TO " + fixture.role,
	} {
		if _, err := fixture.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	// The cache serves the earlier pass until it ages out (TTL 30 s, refreshed in
	// the background), so this waits for the refresh to see the grant.
	err := waitReady("the grant outside the managed schemas is refused", func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "third_party.secrets")
	})
	if !errors.Is(err, postgresstore.ErrPostureRefused) {
		t.Fatalf("the refusal must be a posture refusal: %v", err)
	}
}
