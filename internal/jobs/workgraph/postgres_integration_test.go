//go:build integration

package workgraph

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresStoreCrashRecoveryReplacesRequestAndLedgerToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createExecutionTables(t, ctx, pool)
	now := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO work_graph_execution_requests (id, org_id, kind, scope, llm_concurrency, spend_limit_microunits, correlation_id, idempotency_key, state) VALUES ($1,$2,'workgraph.build','{}',1,0,'crash-recovery','workgraph:crash-recovery','pending')`, testRequestID, testOrgID); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	first, err := store.Claim(ctx, testRequestID, KindBuild)
	if err != nil || first == nil {
		t.Fatalf("first claim = %#v, %v", first, err)
	}
	if duplicate, err := store.Claim(ctx, testRequestID, KindBuild); err != nil || duplicate != nil {
		t.Fatalf("unexpired duplicate = %#v, %v", duplicate, err)
	}
	// A process crash leaves the first claimant with no completion write. A fresh
	// store instance must reclaim only after the persisted lease expires and fence
	// both tables with one new token.
	now = now.Add(store.lease + time.Second)
	fresh, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	fresh.now = func() time.Time { return now }
	reclaimed, err := fresh.Claim(ctx, testRequestID, KindBuild)
	if err != nil || reclaimed == nil || reclaimed.Token == first.Token {
		t.Fatalf("reclaimed = %#v, %v", reclaimed, err)
	}
	var requestToken, ledgerToken string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT claim_token::text FROM work_graph_execution_requests WHERE id=$1`, testRequestID).Scan(&requestToken); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT claim_token::text, attempt_count FROM work_graph_execution_ledger WHERE request_id=$1`, testRequestID).Scan(&ledgerToken, &attempts); err != nil {
		t.Fatal(err)
	}
	if requestToken != reclaimed.Token || ledgerToken != reclaimed.Token || attempts != 2 {
		t.Fatalf("request=%s ledger=%s attempts=%d, want new token=%s attempts=2", requestToken, ledgerToken, attempts, reclaimed.Token)
	}
	if err := fresh.Complete(ctx, *first, []byte(`{"edges":1}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion = %v", err)
	}
	if err := fresh.Complete(ctx, *reclaimed, []byte(`{"edges":1}`)); err != nil {
		t.Fatal(err)
	}
	var completionFences int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM worker_job_completion_fences
WHERE completion_key = $1`,
		"work_graph_execution_request:"+testRequestID,
	).Scan(&completionFences); err != nil {
		t.Fatal(err)
	}
	if completionFences != 1 {
		t.Fatalf("completion fences=%d want=1", completionFences)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM worker_job_completion_fences`); err != nil {
		t.Fatal(err)
	}
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewRequestWriter(registry)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteTx(ctx, tx, Request{
		ID: testRequestID, OrganizationID: testOrgID, Kind: KindBuild,
		Scope: []byte(`{}`), LLMConcurrency: 1, SpendLimitMicrounits: 0,
		CorrelationID: "crash-recovery", IdempotencyKey: "workgraph:crash-recovery",
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("terminal replay: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM worker_job_completion_fences
WHERE completion_key = $1`,
		"work_graph_execution_request:"+testRequestID,
	).Scan(&completionFences); err != nil {
		t.Fatal(err)
	}
	if completionFences != 1 {
		t.Fatalf("restored completion fences=%d want=1", completionFences)
	}
	if completed, err := fresh.Claim(ctx, testRequestID, KindBuild); err != nil || completed != nil {
		t.Fatalf("terminal request re-claimed = %#v, %v", completed, err)
	}
}

func TestRequestWriterUsesCanonicalDeferredOutboxPolicyInsideCallerTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createExecutionTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewRequestWriter(registry)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		ID:                   testRequestID,
		OrganizationID:       testOrgID,
		Kind:                 KindBuild,
		Scope:                []byte(`{"from_date":"2026-07-01"}`),
		LLMConcurrency:       1,
		SpendLimitMicrounits: 5,
		CorrelationID:        "post-sync:1",
		IdempotencyKey:       "workgraph:post-sync:1",
	}
	for attempt := 0; attempt < 2; attempt++ {
		tx, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if err := writer.WriteTx(ctx, tx, request); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("duplicate attempt %d: %v", attempt, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var kind, queue, dedupe string
	if err := pool.QueryRow(ctx, `SELECT job_kind, queue, dedupe_key FROM worker_job_outbox`).Scan(&kind, &queue, &dedupe); err != nil {
		t.Fatal(err)
	}
	if kind != string(KindBuild) || queue != "workgraph" || dedupe != request.IdempotencyKey {
		t.Fatalf("outbox policy drift: kind=%s queue=%s dedupe=%s", kind, queue, dedupe)
	}
	var requests, handoffs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM work_graph_execution_requests`).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox`).Scan(&handoffs); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || handoffs != 1 {
		t.Fatalf("requests=%d handoffs=%d", requests, handoffs)
	}
	activeWriter, err := NewRequestWriter(activeWorkgraphRegistry{Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	activeRequest := request
	activeRequest.ID = "00000000-0000-4000-8000-000000000103"
	activeRequest.Kind = KindMaterialize
	activeRequest.IdempotencyKey = "investment:post-sync:1"
	activeRequest.PrerequisiteCompletionKey = "work_graph_execution_request:" + testRequestID
	activeTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := activeWriter.WriteTx(ctx, activeTx, activeRequest); err != nil {
		_ = activeTx.Rollback(ctx)
		t.Fatalf("active route publish: %v", err)
	}
	if err := activeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var prerequisite string
	if err := pool.QueryRow(ctx, `
SELECT prerequisite_completion_key
FROM worker_job_outbox
WHERE dedupe_key=$1`, activeRequest.IdempotencyKey).Scan(&prerequisite); err != nil {
		t.Fatal(err)
	}
	if prerequisite != activeRequest.PrerequisiteCompletionKey {
		t.Fatalf("prerequisite=%q want=%q", prerequisite, activeRequest.PrerequisiteCompletionKey)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request.Scope = []byte(`{"from_date":"2026-07-02"}`)
	if err := writer.WriteTx(ctx, tx, request); !errors.Is(err, ErrInvalidState) {
		_ = tx.Rollback(ctx)
		t.Fatalf("mutated duplicate err=%v", err)
	}
	_ = tx.Rollback(ctx)
}

type activeWorkgraphRegistry struct{ *jobruntime.Registry }

func (registry activeWorkgraphRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.Registry.Descriptor(kind)
	if ok && (kind == string(KindBuild) || kind == string(KindMaterialize)) {
		descriptor.MigrationState = "go_default"
		descriptor.Route = "river"
	}
	return descriptor, ok
}

func createExecutionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE work_graph_execution_requests (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, kind text NOT NULL, scope jsonb NOT NULL,
 model_ref text NULL, prompt_ref text NULL, llm_concurrency integer NOT NULL,
 spend_limit_microunits bigint NOT NULL, correlation_id text NOT NULL, idempotency_key text NOT NULL UNIQUE,
 state text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT statement_timestamp(), updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
CREATE TABLE work_graph_execution_ledger (
 request_id uuid PRIMARY KEY REFERENCES work_graph_execution_requests(id), claim_token uuid NOT NULL,
 state text NOT NULL, attempt_count integer NOT NULL DEFAULT 1, output_evidence jsonb NULL,
 failure_detail text NULL, last_attempt_at timestamptz NOT NULL DEFAULT statement_timestamp(), completed_at timestamptz NULL
);
CREATE TABLE worker_job_outbox (
 id uuid PRIMARY KEY, dedupe_key varchar(256) NOT NULL UNIQUE, job_kind varchar(96) NOT NULL,
 contract_version integer NOT NULL, args json NOT NULL, payload_hash varchar(71) NOT NULL,
 queue varchar(96) NOT NULL, priority smallint NOT NULL, max_attempts smallint NOT NULL,
 scheduled_at timestamptz NOT NULL, status varchar(16) NOT NULL, attempt_count integer NOT NULL,
 next_attempt_at timestamptz NOT NULL, prerequisite_completion_key text NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
) ;
CREATE TABLE worker_job_completion_fences (
 completion_key text PRIMARY KEY,
 completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
)`)
	if err != nil {
		t.Fatal(err)
	}
}
