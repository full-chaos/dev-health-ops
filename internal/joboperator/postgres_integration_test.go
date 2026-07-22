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
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
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
	now := time.Now().UTC().Truncate(time.Microsecond)
	jobID := insertOperatorIntegrationJob(t, ctx, adminPool, registry, now)
	if _, err := adminPool.Exec(ctx, `
		INSERT INTO river.river_queue (name, updated_at)
		VALUES ('heartbeat', $1), ('retention', $1)
		ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at`, now); err != nil {
		t.Fatal(err)
	}

	authenticator, err := NewAuthenticator(domainPool)
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
	auditor, err := NewPostgresAuditor(domainPool)
	if err != nil {
		t.Fatal(err)
	}
	productionGuard, err := NewPostgresDomainGuard(domainPool)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Dependencies{
		Registry: registry, Backend: backend, Authorizer: authentication.Authorizer(),
		DomainGuard: allowIntegrationDomainGuard{}, Auditor: auditor, Clock: func() time.Time { return now },
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
	if err != nil || len(queues) != 2 {
		t.Fatalf("Queues() = %+v, %v", queues, err)
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
	if err != nil || !queues[0].Paused {
		t.Fatalf("paused Queues() = %+v, %v", queues, err)
	}
	if err := service.ResumeQueue(ctx, principal, "heartbeat", "incident_response", "operator-integration-resume"); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}
	assertOperatorIntegrationAudit(t, ctx, adminPool, 3, "queues.resume", "succeeded")

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
			completed_at timestamptz
		)`,
		`CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY,
			state text NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
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
	registry *jobruntime.Registry,
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
