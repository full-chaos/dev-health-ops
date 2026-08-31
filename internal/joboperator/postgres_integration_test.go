//go:build integration

package joboperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	operatorIntegrationDomainPass      = "operator_domain_runtime_password"
	operatorIntegrationQueuePass       = "operator_queue_runtime_password"
	operatorIntegrationCoordinatorPass = "operator_coordinator_runtime_password"
	operatorIntegrationToken           = "svc_worker_0123456789abcdefghijklmnopqrstuvwxyzAB"
	operatorIntegrationCredential      = "00000000-0000-4000-8000-000000000303"
)

// operatorIntegrationCoordinatorGrants translates postgres.CoordinatorPosture()
// into the migration's grant shape. It is derived, never restated: the posture
// is the same single authority CheckCoordinatorAuthorization asserts against
// and cmd/dev-health-worker-migrate provisions from, so what this fixture
// grants cannot drift from what production grants.
func operatorIntegrationCoordinatorGrants() ([]riverstore.TableGrant, []riverstore.ColumnGrant, []string) {
	posture := postgresstore.CoordinatorPosture()
	tables := make([]riverstore.TableGrant, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		tables = append(tables, riverstore.TableGrant{
			TableName:   table.TableName,
			AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate,
			AllowDelete: table.AllowDelete,
		})
	}
	columns := make([]riverstore.ColumnGrant, 0, len(posture.ColumnScoped))
	for _, column := range posture.ColumnScoped {
		columns = append(columns, riverstore.ColumnGrant{
			TableName:  column.TableName,
			ColumnName: column.ColumnName,
			Privilege:  column.Privilege,
		})
	}
	return tables, columns, append([]string(nil), posture.RequiredSequences...)
}

type allowIntegrationDomainGuard struct{}

func (allowIntegrationDomainGuard) Check(context.Context, Action, JobSummary) error { return nil }

// operatorIntegrationPolicyRegistry changes only the fixture's insertion
// policy. The operator backend continues to use the checked-in, unmodified
// registry so this test cannot weaken or misrepresent production routing.
type operatorIntegrationPolicyRegistry struct {
	registry *jobruntime.Registry
}

func (policy operatorIntegrationPolicyRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := policy.registry.Descriptor(kind)
	if !ok {
		return jobruntime.Descriptor{}, false
	}
	if kind == jobcontract.KindHeartbeat {
		descriptor.MigrationState = "canary"
		descriptor.Route = "river_canary"
		descriptor.RollbackRoute = "celery"
	}
	return descriptor, true
}

func (policy operatorIntegrationPolicyRegistry) Descriptors() []jobruntime.Descriptor {
	descriptors := policy.registry.Descriptors()
	for index, descriptor := range descriptors {
		if descriptor.Kind == jobcontract.KindHeartbeat {
			descriptor.MigrationState = "canary"
			descriptor.Route = "river_canary"
			descriptor.RollbackRoute = "celery"
			descriptors[index] = descriptor
		}
	}
	return descriptors
}

type idleJobRouteQuiescer struct{}

func (idleJobRouteQuiescer) Quiesce(context.Context, string) error { return nil }

func TestPostgresOperatorAuthenticationBackendAndAudit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	adminPool := openOperatorIntegrationPool(t, ctx, instance.URI)
	defer adminPool.Close()
	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving every role name
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	operatorIntegrationDomainRole, operatorIntegrationQueueRole, operatorIntegrationCoordinatorRole :=
		createOperatorIntegrationSchema(t, ctx, instance, adminPool)
	coordinatorTables, coordinatorColumns, coordinatorSequences := operatorIntegrationCoordinatorGrants()
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema:                  "river",
		DomainRole:              operatorIntegrationDomainRole,
		QueueRole:               operatorIntegrationQueueRole,
		CoordinatorRole:         operatorIntegrationCoordinatorRole,
		CoordinatorGrants:       coordinatorTables,
		CoordinatorColumnGrants: coordinatorColumns,
		CoordinatorSequences:    coordinatorSequences,
	}); err != nil {
		t.Fatal(err)
	}
	domainPool := openOperatorIntegrationRolePool(
		t, ctx, instance.URI, operatorIntegrationDomainRole, operatorIntegrationDomainPass,
	)
	defer domainPool.Close()
	queuePool := openOperatorIntegrationRolePool(
		t, ctx, instance.URI, operatorIntegrationQueueRole, operatorIntegrationQueuePass,
	)
	defer queuePool.Close()
	coordinatorPool := openOperatorIntegrationRolePool(
		t, ctx, instance.URI, operatorIntegrationCoordinatorRole, operatorIntegrationCoordinatorPass,
	)
	defer coordinatorPool.Close()
	if err := postgresstore.CheckDomainAuthorization(ctx, domainPool, operatorIntegrationDomainRole, "river"); err != nil {
		t.Fatalf("domain role authorization: %v", err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queuePool, operatorIntegrationQueueRole, "river"); err != nil {
		t.Fatalf("queue role authorization: %v", err)
	}
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	// CHAOS-3033 (PR #1292, ee2141eca) moved system.heartbeat (like every
	// checked-in kind except sync.provider_unit) to state go_default / route
	// river. This is a drift tripwire on the *current* checked-in policy, not
	// the pre-cutover celery baseline: it still fails loudly if a future edit
	// to migration-state.json silently changes heartbeat's routing.
	heartbeat, ok := registry.Descriptor(jobcontract.KindHeartbeat)
	if !ok || !heartbeat.Executable() || heartbeat.MigrationState != "go_default" || heartbeat.Route != "river" {
		t.Fatalf("checked-in heartbeat policy = %#v, want go_default/river executable", heartbeat)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	jobID := insertOperatorIntegrationJob(
		t,
		ctx,
		adminPool,
		operatorIntegrationPolicyRegistry{registry: registry},
		now,
	)
	if _, err := adminPool.Exec(ctx, `
		INSERT INTO river.river_queue (name, updated_at)
		VALUES ('heartbeat', $1), ('retention', $1)
		ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at`, now); err != nil {
		t.Fatal(err)
	}

	// CHAOS-3100. This used to authenticate through the fixture's ADMIN pool,
	// on the reasoning that the credential store sits outside the domain
	// allow-list. That reasoning was right and the fixture was still wrong:
	// an admin pool can run any statement, so the test proved nothing about
	// which runtime role can, and it would have passed identically while the
	// real CLI was 100% broken. The restricted coordinator login is the pool
	// cmd/dev-health-workerctl actually builds its authenticator on, so it is
	// the only connection that measures the deployed privilege.
	authenticator, err := NewAuthenticator(coordinatorPool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticator.Authenticate(ctx, "svc_worker_invalid"); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("invalid Authenticate() error = %v", err)
	}
	authentication, err := authenticator.Authenticate(ctx, operatorIntegrationToken)
	if err != nil {
		t.Fatal(err)
	}
	if authentication.Principal().ID != operatorIntegrationCredential {
		t.Fatalf("principal = %+v", authentication.Principal())
	}

	backend, err := NewDirectPostgresBackend(queuePool, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	providerJobID := insertOperatorProviderUnitIntegrationJob(t, ctx, adminPool, registry, now)
	if _, err := backend.Retry(ctx, providerJobID, Mutation{ExpectedState: StateDiscarded}); err != nil {
		t.Fatalf("current provider-unit delivery retry: %v", err)
	}
	if _, err := adminPool.Exec(ctx, `
		UPDATE river.river_job
		SET state = 'discarded', finalized_at = $2
		WHERE id = $1`, providerJobID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `
		UPDATE public.worker_job_outbox
		SET status = 'pending', river_job_id = NULL
		WHERE river_job_id = $1`, providerJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Retry(ctx, providerJobID, Mutation{ExpectedState: StateDiscarded}); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("stale provider-unit delivery retry error=%v, want ErrStateConflict", err)
	}
	auditor, err := NewPostgresAuditor(adminPool)
	if err != nil {
		t.Fatal(err)
	}
	productionGuard, err := NewPostgresDomainGuard(domainPool)
	if err != nil {
		t.Fatal(err)
	}
	routeRegistry, err := syncdispatchcontract.Load(filepath.Join("..", "..", "contracts", "sync-dispatch", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	routeCapabilities, err := syncroute.NewCapabilities(nil)
	if err != nil {
		t.Fatal(err)
	}
	routeController, err := syncroute.NewController(domainPool, routeRegistry, routeCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	jobRouteController, err := jobroute.NewControllerWithCeleryQuiescer(
		adminPool,
		operatorIntegrationPolicyRegistry{registry: registry},
		idleJobRouteQuiescer{},
		idleJobRouteQuiescer{},
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Dependencies{
		Registry: registry, Backend: backend, Authorizer: authentication.Authorizer(),
		DomainGuard: allowIntegrationDomainGuard{}, Auditor: auditor, Clock: func() time.Time { return now },
		RouteController: routeController, JobRouteController: jobRouteController,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := authentication.Principal()
	if err := service.Status(ctx, principal); err != nil {
		t.Fatalf("Status: %v", err)
	}
	job, err := service.Inspect(ctx, principal, jobID)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if job.Kind != jobcontract.KindHeartbeat || job.CorrelationID != "operator-integration-1" {
		t.Fatalf("sanitized job projection = %+v", job)
	}
	jobs, err := service.List(ctx, principal, ListFilter{States: []JobState{job.State}, Limit: 10})
	if err != nil || len(jobs) != 1 || jobs[0].ID != jobID {
		t.Fatalf("List() = %+v, %v", jobs, err)
	}
	queues, err := service.Queues(ctx, principal, "ops", []string{
		"coverage", "heartbeat", "retention", "webhooks",
	})
	if err != nil || len(queues) != 4 {
		t.Fatalf("Queues() = %+v, %v", queues, err)
	}
	for index, name := range []string{"coverage", "heartbeat", "retention", "webhooks"} {
		if queues[index].Name != name {
			t.Fatalf("Queues()[%d].Name = %q, want %q", index, queues[index].Name, name)
		}
	}
	if err := productionGuard.Check(ctx, ActionCancel, job); !errors.Is(err, ErrDomainPreconditionUnsupported) {
		t.Fatalf("production domain guard error = %v", err)
	}

	cancelled, err := service.Cancel(ctx, principal, jobID, "operator_request", "operator-integration-cancel")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelled.State != StateCancelled {
		t.Fatalf("cancelled job = %+v", cancelled)
	}
	assertOperatorIntegrationAudit(t, ctx, adminPool, 1, "jobs.cancel", "succeeded")

	if err := service.PauseQueue(ctx, principal, "heartbeat", "incident_response", "operator-integration-pause"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	queues, err = service.Queues(ctx, principal, "ops", []string{
		"coverage", "heartbeat", "retention", "webhooks",
	})
	if err != nil || len(queues) != 4 || queues[1].Name != "heartbeat" || !queues[1].Paused {
		t.Fatalf("paused Queues() = %+v, %v", queues, err)
	}
	if err := service.ResumeQueue(ctx, principal, "heartbeat", "incident_response", "operator-integration-resume"); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}
	assertOperatorIntegrationAudit(t, ctx, adminPool, 3, "queues.resume", "succeeded")

	activated, err := service.ApplyCheckedInJobRoute(
		ctx, principal, jobcontract.KindHeartbeat, "canary", "operator-integration-apply",
	)
	if err != nil {
		t.Fatalf("ApplyCheckedInJobRoute: %v", err)
	}
	if activated.Transport != "river_canary" || activated.Generation != 2 {
		t.Fatalf("activated route = %+v", activated)
	}
	restored, err := service.RollbackJobRoute(
		ctx, principal, jobcontract.KindHeartbeat, "rollback", "operator-integration-rollback",
	)
	if err != nil {
		t.Fatalf("RollbackJobRoute: %v", err)
	}
	if restored.Transport != "celery" || restored.Generation != 3 {
		t.Fatalf("restored route = %+v", restored)
	}
	assertOperatorIntegrationAudit(t, ctx, adminPool, 5, "job_routes.rollback", "succeeded")

	var auditColumns []string
	rows, err := adminPool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'worker_operator_audits'
		ORDER BY column_name`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		auditColumns = append(auditColumns, column)
	}
	rows.Close()
	for _, forbidden := range []string{"args", "encoded_args", "error_text", "token"} {
		for _, column := range auditColumns {
			if column == forbidden {
				t.Fatalf("audit schema exposes forbidden column %q", column)
			}
		}
	}
}

// TestOperatorAuthenticationIsCoordinatorOnlyAndNamesPrivilegeDenials is the
// CHAOS-3100 regression, and it deliberately proves BOTH halves of the fact
// the ticket says must land together. Proving only that some role can
// authenticate is what recreates the trap: the cheap way to make
// authentication work is to widen the domain role, and that trades a runtime
// 42501 for a permanent readiness failure across every domain worker, because
// CheckDomainAuthorization asserts the domain role holds NOTHING outside its
// own manifest.
//
//   - Half one: the restricted coordinator role -- the login
//     cmd/dev-health-workerctl builds its authenticator on -- completes a real
//     authentication against the real grants ApplyPinnedMigrations emits.
//     Grants are derived from CoordinatorPosture(), so this fails if the
//     migration and the posture ever disagree.
//   - Half two: the domain role is still refused, and the domain role's own
//     readiness still passes afterwards. A change that bought half one by
//     granting the domain role fails the refusal; a change that bought it by
//     granting without the matching allowlist row fails the readiness call.
//
// The refusal is also asserted by reason CODE, not merely by error identity.
// Before this change auth.go collapsed a 42501 into a bare ErrAuthentication,
// so workerctl printed `authentication_failed` for a missing grant and sent
// operators to rotate a token that was never wrong. That assertion is the one
// that fails without the auth.go half of this fix.
func TestOperatorAuthenticationIsCoordinatorOnlyAndNamesPrivilegeDenials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	adminPool := openOperatorIntegrationPool(t, ctx, instance.URI)
	t.Cleanup(adminPool.Close)
	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving every role name
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	operatorIntegrationDomainRole, operatorIntegrationQueueRole, operatorIntegrationCoordinatorRole :=
		createOperatorIntegrationSchema(t, ctx, instance, adminPool)
	coordinatorTables, coordinatorColumns, coordinatorSequences := operatorIntegrationCoordinatorGrants()
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema:                  "river",
		DomainRole:              operatorIntegrationDomainRole,
		QueueRole:               operatorIntegrationQueueRole,
		CoordinatorRole:         operatorIntegrationCoordinatorRole,
		CoordinatorGrants:       coordinatorTables,
		CoordinatorColumnGrants: coordinatorColumns,
		CoordinatorSequences:    coordinatorSequences,
	}); err != nil {
		t.Fatal(err)
	}

	coordinatorPool := openOperatorIntegrationRolePool(
		t, ctx, instance.URI, operatorIntegrationCoordinatorRole, operatorIntegrationCoordinatorPass,
	)
	t.Cleanup(coordinatorPool.Close)
	domainPool := openOperatorIntegrationRolePool(
		t, ctx, instance.URI, operatorIntegrationDomainRole, operatorIntegrationDomainPass,
	)
	t.Cleanup(domainPool.Close)

	// Half one. Authenticate runs SELECT and UPDATE in one CTE, so a role
	// holding only SELECT would fail here too -- authentication is a write.
	coordinatorAuthenticator, err := NewAuthenticator(coordinatorPool)
	if err != nil {
		t.Fatal(err)
	}
	authentication, err := coordinatorAuthenticator.Authenticate(ctx, operatorIntegrationToken)
	if err != nil {
		t.Fatalf("restricted coordinator role cannot authenticate the operator token: %v", err)
	}
	if authentication.Principal().ID != operatorIntegrationCredential {
		t.Fatalf("principal = %+v", authentication.Principal())
	}

	// Half two, part one: the domain role is refused, and says why. Note the
	// token supplied is the VALID one -- so `authentication_failed` would be
	// an actively false statement about it, not merely a vague one.
	domainAuthenticator, err := NewAuthenticator(domainPool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := domainAuthenticator.Authenticate(ctx, operatorIntegrationToken); err == nil {
		t.Fatal("the domain role authenticated against the coordinator-exclusive credential store, " +
			"so it now holds a privilege its own readiness posture forbids")
	} else {
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("domain-role denial error = %v, want it to remain an ErrAuthentication", err)
		}
		if reason := AuthenticationReason(err); reason != ReasonCredentialStoreForbidden {
			t.Fatalf("domain-role denial reason = %q, want %q -- a missing grant reported as a "+
				"credential failure sends operators to rotate a token that was never wrong",
				reason, ReasonCredentialStoreForbidden)
		}
	}

	// Half two, part two: provisioning the coordinator did not widen the
	// domain role. This is the assertion that fails if a future change fixes
	// authentication by granting internal_service_credentials to the domain
	// role -- readiness would then find an undeclared relation and every
	// domain worker, not just workerctl, would fail closed at startup.
	if err := postgresstore.CheckDomainAuthorization(
		ctx, domainPool, operatorIntegrationDomainRole, "river",
	); err != nil {
		t.Fatalf("domain readiness broke once the coordinator was provisioned: %v", err)
	}

	// An invalid token on the correctly granted pool is the OTHER verdict, and
	// must stay distinguishable from the denial above -- otherwise the two
	// codes carry no information.
	if _, err := coordinatorAuthenticator.Authenticate(ctx, "svc_worker_"+strings.Repeat("z", 40)); err == nil {
		t.Fatal("an unknown token authenticated")
	} else if reason := AuthenticationReason(err); reason != ReasonAuthenticationFailed {
		t.Fatalf("unknown-token reason = %q, want %q", reason, ReasonAuthenticationFailed)
	}
}

func createOperatorIntegrationSchema(
	t *testing.T, ctx context.Context, instance *containers.Instance, pool *pgxpool.Pool,
) (domainRole, queueRole, coordinatorRole string) {
	t.Helper()
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainRole, err = containers.RoleName("operator_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err = containers.RoleName("operator_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorRole, err = containers.RoleName("operator_coordinator_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(operatorIntegrationToken))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	statements := []string{
		"REVOKE TEMPORARY ON DATABASE " + dbName + " FROM PUBLIC",
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + operatorIntegrationDomainPass + "'",
		"CREATE ROLE " + queueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + operatorIntegrationQueuePass + "'",
		// Created here, before ApplyPinnedMigrations, because the migration's
		// role-eligibility preflight rejects a coordinator role that does not
		// yet exist as a least-privilege login. This mirrors the real deploy
		// order: provision roles (scripts/worker/provision_river_roles.sql),
		// then migrate.
		"CREATE ROLE " + coordinatorRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + operatorIntegrationCoordinatorPass + "'",
		`CREATE TABLE public.internal_service_credentials (
			id uuid PRIMARY KEY,
			service_name text NOT NULL,
			token_hash text NOT NULL UNIQUE,
			scopes jsonb NOT NULL,
			revoked_at timestamptz,
			expires_at timestamptz,
			last_used_at timestamptz
		)`,
		`CREATE TABLE public.worker_operator_audits (
			id bigserial PRIMARY KEY,
			credential_id uuid REFERENCES public.internal_service_credentials(id) ON DELETE SET NULL,
			principal_type varchar(32) NOT NULL,
			principal_id varchar(128) NOT NULL,
			action varchar(32) NOT NULL,
			resource_type varchar(32) NOT NULL,
			resource_id varchar(256) NOT NULL,
			reason_code varchar(64) NOT NULL,
			correlation_id varchar(128) NOT NULL,
			status varchar(16) NOT NULL,
			created_at timestamptz NOT NULL,
			completed_at timestamptz,
			CONSTRAINT ck_worker_operator_audits_action CHECK (
				action IN (
					'jobs.cancel', 'jobs.retry', 'queues.pause', 'queues.resume',
					'workers.drain', 'job_routes.apply_checked_in', 'job_routes.rollback'
				)
			)
		)`,
		`CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY,
			state text NOT NULL,
			job_kind text,
			status text,
			river_job_id bigint UNIQUE
		)`,
		"CREATE TABLE public.worker_job_delivery_abandonments (dedupe_key text PRIMARY KEY)",
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY)",
		`CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY,
			job_kind text NOT NULL,
			status text NOT NULL
		)`,
		"CREATE TABLE public.worker_concurrency_leases (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_instances (instance_id uuid PRIMARY KEY)",
		`CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY,
			transport text NOT NULL,
			paused boolean NOT NULL,
			generation bigint NOT NULL,
			updated_at timestamptz NOT NULL
		)`,
		"CREATE TABLE public.integrations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_sources (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_datasets (id uuid PRIMARY KEY)",
		// Carries the columns coordinatorPosture scopes its grants to. A
		// column-level GRANT names the column, and PostgreSQL raises
		// undefined_column (42703) for one that does not exist, so a
		// one-column fixture would break the migration rather than exercise
		// it. credentials_encrypted is present and deliberately NOT granted:
		// it is the encrypted secret the coordinator must never reach.
		`CREATE TABLE public.integration_credentials (
			id uuid PRIMARY KEY, org_id text, provider text,
			is_active boolean, config json, credentials_encrypted text
		)`,
		"CREATE TABLE public.provider_oauth_credentials (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id uuid PRIMARY KEY, state text NOT NULL)",
		// CHAOS-4114: the executed-proof ledger joined domainPosture's manifest
		// (migration 0109). Every domain GRANT is wrapped in a to_regclass guard,
		// so a venue that never CREATEs it silently skips the grant and
		// CheckDomainAuthorization then fails closed with an opaque readiness
		// error naming neither the table nor this venue.
		`CREATE TABLE public.sync_executed_proof_ledger (
			provider text NOT NULL,
			dataset_key text NOT NULL,
			attempted_at timestamptz NOT NULL,
			proven_at timestamptz,
			PRIMARY KEY (provider, dataset_key),
			CONSTRAINT ck_sync_executed_proof_ledger_provider_normalized
				CHECK (provider = lower(provider) AND btrim(provider) <> ''),
			CONSTRAINT ck_sync_executed_proof_ledger_dataset_normalized
				CHECK (dataset_key = lower(dataset_key) AND btrim(dataset_key) <> '')
		)`,
		// Migration 0088 (#1529) added this table to domainPosture's manifest,
		// so the CheckDomainAuthorization readiness call each test in this
		// package makes fails closed on its absence — the
		// table has to exist here even though this fixture never writes to it,
		// exactly as it has to exist in a deployment before domain workers
		// start. ApplyPinnedMigrations' domain GRANT for it is guarded by
		// `IF to_regclass(...) IS NOT NULL`, so without this CREATE the role
		// silently receives no grant and readiness reports the opaque
		// "PostgreSQL readiness check failed".
		"CREATE TABLE public.sync_run_unit_effect_snapshots (sync_run_unit_id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_unit_chunk_checkpoints (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_run_unit_effect_chunks (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_watermarks (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_configurations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.scheduled_jobs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.scheduled_report_occurrences (occurrence_id text PRIMARY KEY)",
		"CREATE TABLE public.backfill_jobs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_coverage_projections (id uuid PRIMARY KEY)",
		"CREATE TABLE public.organizations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_requests (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_ledger (id bigint PRIMARY KEY)",
		// CHAOS-3033 Option B manifest additions — domain-exclusive tables
		// (role-partition manifest, removed in e23ede618; see git history at eda2d6b91).
		"CREATE TABLE public.billing_notifications (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_finalize_redrive_events (id uuid PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partition_recompute_events (id uuid PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batch_payloads (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batches (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_recompute_jobs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_rejections (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_sources (id bigint PRIMARY KEY)",
		"CREATE TABLE public.feature_flags (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_feature_overrides (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_licenses (id bigint PRIMARY KEY)",
		"CREATE TABLE public.dev_conversations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.dev_conversation_tombstones (id uuid PRIMARY KEY)",
		"CREATE TABLE public.provider_rate_limit_observations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.report_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.saved_reports (id bigint PRIMARY KEY)",
		"CREATE TABLE public.webhook_deliveries (id bigint PRIMARY KEY)",
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY,
			state text NOT NULL
		)`,
		`CREATE TABLE public.sync_dispatch_transport_routes (
			kind text PRIMARY KEY,
			generation bigint NOT NULL
		)`,
		// The remaining coordinator-exclusive relations. They exist here for
		// the same reason the domain tables above do, and the ordering is
		// load-bearing in the same way: every coordinator GRANT in
		// coordinatorGrantStatements is wrapped in `IF to_regclass(...) IS NOT
		// NULL`, so a table created after ApplyPinnedMigrations would silently
		// receive no grant at all. This fixture would then still pass while
		// asserting nothing -- the exact vacuity that let CHAOS-3100 survive.
		"CREATE TABLE public.sync_run_reference_discoveries (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_post_dispatches (id uuid PRIMARY KEY)",
		"CREATE TABLE public.scheduled_sync_occurrences (id uuid PRIMARY KEY)",
		"CREATE TABLE public.fixed_schedule_occurrences (id uuid PRIMARY KEY)",
		"CREATE TABLE public.tier_limits (id bigint PRIMARY KEY)",
		"CREATE TABLE public.job_runs (id uuid PRIMARY KEY)",
		// CHAOS-4209: the finalize service's compute-input checkpoint entered
		// domainPosture, so this venue must create it for the same to_regclass
		// reason the coordinator tables above are created before the migration.
		"CREATE TABLE public.sync_compute_checkpoints (id uuid PRIMARY KEY)",
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ($1, 'celery', FALSE, 1, statement_timestamp())`, jobcontract.KindHeartbeat); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO public.internal_service_credentials (id, service_name, token_hash, scopes)
		VALUES ($1, $2, $3, '["workers:read", "workers:operate"]'::jsonb)`,
		operatorIntegrationCredential,
		WorkerOperatorService,
		hex.EncodeToString(digest[:]),
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return domainRole, queueRole, coordinatorRole
}

func insertOperatorIntegrationJob(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	registry joboutbox.PolicyRegistry,
	now time.Time,
) int64 {
	t.Helper()
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		CorrelationID:   "operator-integration-1",
		IdempotencyKey:  "operator:heartbeat:integration:1",
		Domain: jobcontract.DomainLink{
			Type: "schedule_occurrence",
			ID:   "00000000-0000-4000-8000-000000000304",
		},
		Payload: jobcontract.HeartbeatPayload{ScheduledFor: now.Format(time.RFC3339)},
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	inserter, err := joboutbox.NewRiverInserter(pool, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	jobID, err := inserter.Insert(ctx, tx, joboutbox.Row{
		ID:              "00000000-0000-4000-8000-000000000305",
		DedupeKey:       envelope.IdempotencyKey,
		JobKind:         jobcontract.KindHeartbeat,
		ContractVersion: 1,
		Args:            encoded,
		PayloadHash:     "sha256:" + hex.EncodeToString(digest[:]),
		Queue:           "heartbeat",
		Priority:        2,
		MaxAttempts:     1,
		ScheduledAt:     now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return jobID
}

func insertOperatorProviderUnitIntegrationJob(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	registry joboutbox.PolicyRegistry,
	now time.Time,
) int64 {
	t.Helper()
	organizationID := "00000000-0000-4000-8000-000000000310"
	unitID := "00000000-0000-4000-8000-000000000311"
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		OrganizationID:  &organizationID,
		CorrelationID:   "sync-run:00000000-0000-4000-8000-000000000312",
		IdempotencyKey:  "sync.provider_unit:" + unitID,
		Domain: jobcontract.DomainLink{
			Type: "sync_run_unit",
			ID:   unitID,
		},
		Payload: jobcontract.ProviderUnitPayload{UnitID: unitID},
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	inserter, err := joboutbox.NewRiverInserter(pool, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	outboxID := "00000000-0000-4000-8000-000000000313"
	jobID, err := inserter.Insert(ctx, tx, joboutbox.Row{
		ID:              outboxID,
		DedupeKey:       envelope.IdempotencyKey,
		JobKind:         jobcontract.KindSyncProviderUnit,
		ContractVersion: 1,
		Args:            encoded,
		PayloadHash:     "sha256:" + hex.EncodeToString(digest[:]),
		Queue:           "sync_provider",
		Priority:        2,
		MaxAttempts:     5,
		ScheduledAt:     now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO public.worker_job_outbox (id, state, job_kind, status, river_job_id)
		VALUES ($1, 'current', $2, 'delivered', $3)`, outboxID, jobcontract.KindSyncProviderUnit, jobID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE river.river_job
		SET state = 'discarded', finalized_at = $2
		WHERE id = $1`, jobID, now); err != nil {
		t.Fatal(err)
	}
	return jobID
}

func assertOperatorIntegrationAudit(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	wantCount int,
	wantLastAction string,
	wantLastStatus string,
) {
	t.Helper()
	var count int
	var action, status, principalID, correlationID string
	if err := pool.QueryRow(ctx, `
		SELECT count(*) OVER (), action, status, principal_id, correlation_id
		FROM public.worker_operator_audits
		ORDER BY id DESC LIMIT 1`).Scan(&count, &action, &status, &principalID, &correlationID); err != nil {
		t.Fatal(err)
	}
	if count != wantCount || action != wantLastAction || status != wantLastStatus ||
		principalID != operatorIntegrationCredential || correlationID == "" {
		t.Fatalf("audit = count=%d action=%q status=%q principal=%q correlation=%q",
			count, action, status, principalID, correlationID)
	}
}

func openOperatorIntegrationPool(t *testing.T, ctx context.Context, uri string) *pgxpool.Pool {
	t.Helper()
	configuration, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	configuration.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(fmt.Errorf("ping PostgreSQL: %w", err))
	}
	return pool
}

func openOperatorIntegrationRolePool(
	t *testing.T,
	ctx context.Context,
	uri string,
	role string,
	password string,
) *pgxpool.Pool {
	t.Helper()
	configuration, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	configuration.ConnConfig.User = role
	configuration.ConnConfig.Password = password
	configuration.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(fmt.Errorf("ping PostgreSQL runtime role %q: %w", role, err))
	}
	return pool
}
