//go:build integration

package joboperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
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
	operatorIntegrationDomainRole = "operator_domain_runtime"
	operatorIntegrationQueueRole  = "operator_queue_runtime"
	operatorIntegrationDomainPass = "operator_domain_runtime_password"
	operatorIntegrationQueuePass  = "operator_queue_runtime_password"
	operatorIntegrationToken      = "svc_worker_0123456789abcdefghijklmnopqrstuvwxyzAB"
	operatorIntegrationCredential = "00000000-0000-4000-8000-000000000303"
)

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
	createOperatorIntegrationSchema(t, ctx, adminPool)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: operatorIntegrationDomainRole,
		QueueRole:  operatorIntegrationQueueRole,
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

	// The operator credential store is intentionally outside the domain runtime
	// allow-list, so authenticate through the fixture's operator/admin pool.
	authenticator, err := NewAuthenticator(adminPool)
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
	queues, err := service.Queues(ctx, principal, "ops")
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
	queues, err = service.Queues(ctx, principal, "ops")
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

func createOperatorIntegrationSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	digest := sha256.Sum256([]byte(operatorIntegrationToken))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	statements := []string{
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"CREATE ROLE " + operatorIntegrationDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + operatorIntegrationDomainPass + "'",
		"CREATE ROLE " + operatorIntegrationQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + operatorIntegrationQueuePass + "'",
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
			status text
		)`,
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY)",
		`CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY,
			job_kind text NOT NULL,
			status text NOT NULL
		)`,
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
		"CREATE TABLE public.integration_credentials (id uuid PRIMARY KEY)",
		"CREATE TABLE public.provider_oauth_credentials (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id uuid PRIMARY KEY, state text NOT NULL)",
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
		"CREATE TABLE public.sync_watermarks (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_configurations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.scheduled_jobs (id uuid PRIMARY KEY)",
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
