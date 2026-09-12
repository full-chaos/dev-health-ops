//go:build integration

package workgraph

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	coalesceOrgID    = "00000000-0000-4000-8000-000000000009"
	coalesceOtherOrg = "00000000-0000-4000-8000-00000000000a"
	coalesceScope    = `{"force":false,"from_date":"2026-09-01","to_date":"2026-09-12"}`
)

// TestRequestWriterCoalescesPendingMaterializeRequests is the ticket's whole
// claim, measured end to end against the real schema -- including alembic
// 0060's terminal-immutability trigger, which is what turns "superseded" from
// a state a later pass could walk back into a fact.
//
// THE INVARIANT, and why 'canceled' specifically. internal/joboutbox/
// strand_repair.go re-arms a delivered outbox row whenever its work-graph
// request is 'pending', or 'running' past its lease. Those are the only two
// states repairStrandedWorkGraphSQL accepts, and its own integration matrix
// (TestStrandRepairAgainstLivePostgres, "work graph requests") pins that a
// 'canceled' request is refused. So a supersede that leaves the row pending
// and merely kills its River job does not supersede anything: the next
// reconciler tick resurrects it. Moving the row to a state that query cannot
// select -- and that the trigger then freezes -- is the supersede.
func TestRequestWriterCoalescesPendingMaterializeRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := startCoalescePostgres(t, ctx)
	writer := newCoalesceWriter(t)

	first := coalesceRequest("00000000-0000-4000-8000-000000000201", "ext-recompute:bridge-1")
	second := coalesceRequest("00000000-0000-4000-8000-000000000202", "ext-recompute:bridge-2")

	if outcome := writeCoalescing(t, ctx, pool, writer, first); len(outcome.SupersededRequestIDs) != 0 {
		t.Fatalf("the first flush superseded %v; there was nothing queued to supersede",
			outcome.SupersededRequestIDs)
	}
	outcome := writeCoalescing(t, ctx, pool, writer, second)
	if len(outcome.SupersededRequestIDs) != 1 || outcome.SupersededRequestIDs[0] != first.ID {
		t.Fatalf("second flush superseded %v, want exactly [%s]",
			outcome.SupersededRequestIDs, first.ID)
	}

	if state := requestState(t, ctx, pool, first.ID); state != "canceled" {
		t.Fatalf("superseded request state = %q, want canceled: any other state is one "+
			"the strand repair or a claim can still reach", state)
	}
	if state := requestState(t, ctx, pool, second.ID); state != "pending" {
		t.Fatalf("superseding request state = %q, want pending", state)
	}
	// One pending materialization per key is the ticket, stated as a count.
	var pending int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM work_graph_execution_requests
WHERE org_id = $1::uuid AND kind = $2 AND state = 'pending'`,
		coalesceOrgID, string(KindMaterialize)).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("pending materialize requests = %d, want exactly 1", pending)
	}

	// Terminal means terminal: alembic 0060's trigger must refuse to walk the
	// superseded row back to a state anything could execute. Without this the
	// supersede would be a suggestion.
	if _, err := pool.Exec(ctx, `
UPDATE work_graph_execution_requests SET state = 'pending' WHERE id = $1::uuid`,
		first.ID); err == nil {
		t.Fatal("a superseded request was walked back to pending; terminal immutability is not real")
	}

	// The stale River job for the superseded request no-ops instead of failing
	// loudly: Claim reports a canceled request the way it reports a succeeded
	// one, so the handler retires the job without executing it. Reporting it as
	// an error instead would trade a backlog for an alert per stale job.
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.Claim(ctx, first.ID, KindMaterialize)
	if err != nil || claim != nil {
		t.Fatalf("claim on a superseded request = %#v, %v; want (nil, nil)", claim, err)
	}
}

// TestRequestWriterSupersedesOnlyWhatIsSafeToSupersede walks every conjunct of
// the coalescing predicate. Each row here is a way the supersede could be too
// EAGER, and each is a bug with a worse blast radius than the duplicate work
// the coalescing exists to remove: cancelling running work strands a lease,
// cancelling another producer's request strands whatever fences on it, and
// cancelling across scopes silently drops a recompute nobody asked to drop.
func TestRequestWriterSupersedesOnlyWhatIsSafeToSupersede(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := startCoalescePostgres(t, ctx)
	writer := newCoalesceWriter(t)

	for _, testCase := range []struct {
		name    string
		prepare func(t *testing.T, queued Request)
		arrival func(request Request) Request
	}{
		{
			name: "a running request is left alone",
			prepare: func(t *testing.T, queued Request) {
				if _, err := pool.Exec(ctx, `
UPDATE work_graph_execution_requests
SET state = 'running', claim_token = $2::uuid, lease_expires_at = $3
WHERE id = $1::uuid`, queued.ID, "00000000-0000-4000-8000-0000000009f1",
					time.Now().UTC().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "another producer's request is left alone",
			arrival: func(request Request) Request { request.CorrelationID = "post-sync:run-7"; return request },
		},
		{
			name: "a different scope is left alone",
			arrival: func(request Request) Request {
				request.Scope = []byte(`{"force":false,"from_date":"2026-08-01","to_date":"2026-09-12"}`)
				return request
			},
		},
		{
			name:    "another organization's request is left alone",
			arrival: func(request Request) Request { request.OrganizationID = coalesceOtherOrg; return request },
		},
		{
			name:    "a producer that did not opt in supersedes nothing",
			arrival: func(request Request) Request { request.Coalesce = false; return request },
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			resetCoalesceTables(t, ctx, pool)
			queued := coalesceRequest("00000000-0000-4000-8000-000000000301", "ext-recompute:bridge-1")
			writeCoalescing(t, ctx, pool, writer, queued)
			if testCase.prepare != nil {
				testCase.prepare(t, queued)
			}
			arrival := coalesceRequest("00000000-0000-4000-8000-000000000302", "ext-recompute:bridge-2")
			if testCase.arrival != nil {
				arrival = testCase.arrival(arrival)
			}
			outcome := writeCoalescing(t, ctx, pool, writer, arrival)
			if len(outcome.SupersededRequestIDs) != 0 {
				t.Fatalf("superseded %v, want nothing", outcome.SupersededRequestIDs)
			}
			if state := requestState(t, ctx, pool, queued.ID); state == "canceled" {
				t.Fatal("the queued request was cancelled anyway")
			}
		})
	}
}

// TestRequestWriterCoalescingSurvivesAnIdempotentRewrite is the retry path, and
// it is the one a coalescing key gets wrong by default.
//
// The drain's correlation is deterministic per bridge row, so a lease reclaim
// re-runs Enqueue with the SAME request id and the SAME scope. A supersede
// predicate that did not exclude the incoming id would match that row -- it
// agrees on org, kind, scope and producer with itself -- cancel it, and then
// hit ON CONFLICT DO NOTHING, leaving the flush with nothing pending at all.
// That failure is silent: the drain commits, the bridge row is marked done,
// and the recompute simply never happens.
func TestRequestWriterCoalescingSurvivesAnIdempotentRewrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := startCoalescePostgres(t, ctx)
	writer := newCoalesceWriter(t)

	request := coalesceRequest("00000000-0000-4000-8000-000000000401", "ext-recompute:bridge-1")
	for attempt := range 2 {
		outcome := writeCoalescing(t, ctx, pool, writer, request)
		if len(outcome.SupersededRequestIDs) != 0 {
			t.Fatalf("attempt %d superseded %v; a re-write must never cancel itself",
				attempt, outcome.SupersededRequestIDs)
		}
	}
	if state := requestState(t, ctx, pool, request.ID); state != "pending" {
		t.Fatalf("re-written request state = %q, want pending", state)
	}
}

// TestRequestWriterBoundsAnUnboundedMaterializeStart proves the bound reaches
// the PERSISTED row, not just the return value: the scope in the table is what
// the executor reads and what the coalescing key compares, so a bound that
// existed only in the outcome would be decorative.
func TestRequestWriterBoundsAnUnboundedMaterializeStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := startCoalescePostgres(t, ctx)
	writer := newCoalesceWriter(t)

	request := coalesceRequest("00000000-0000-4000-8000-000000000501", "post-sync:run-1")
	request.Coalesce = false
	request.Scope = []byte(`{"to_date":"2026-09-12"}`)
	outcome := writeCoalescing(t, ctx, pool, writer, request)
	if outcome.BoundedFromDate != "2026-08-14" {
		t.Fatalf("BoundedFromDate = %q, want 2026-08-14", outcome.BoundedFromDate)
	}
	var storedFromDate string
	if err := pool.QueryRow(ctx, `
SELECT scope ->> 'from_date' FROM work_graph_execution_requests WHERE id = $1::uuid`,
		request.ID).Scan(&storedFromDate); err != nil {
		t.Fatal(err)
	}
	if storedFromDate != "2026-08-14" {
		t.Fatalf("persisted from_date = %q, want 2026-08-14", storedFromDate)
	}
}

func coalesceRequest(id, correlation string) Request {
	return Request{
		ID:             id,
		OrganizationID: coalesceOrgID,
		Kind:           KindMaterialize,
		Scope:          []byte(coalesceScope),
		LLMConcurrency: 1,
		CorrelationID:  correlation,
		IdempotencyKey: correlation + ":investment.materialize",
		Coalesce:       true,
	}
}

func writeCoalescing(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *RequestWriter, request Request,
) WriteOutcome {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := writer.WriteRequestTx(ctx, tx, request)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("write %s: %v", request.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return outcome
}

func requestState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM work_graph_execution_requests WHERE id = $1::uuid`,
		requestID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func newCoalesceWriter(t *testing.T) *RequestWriter {
	t.Helper()
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewRequestWriter(activeWorkgraphRegistry{Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func resetCoalesceTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// The requests table is TRUNCATEd rather than DELETEd FROM because the
	// terminal-immutability trigger this fixture deliberately installs fires on
	// UPDATE only -- but the ledger's foreign key does not care, so both go in
	// one statement with CASCADE.
	if _, err := pool.Exec(ctx, `
TRUNCATE work_graph_execution_requests, work_graph_execution_ledger,
         worker_job_outbox, worker_job_completion_fences CASCADE`); err != nil {
		t.Fatal(err)
	}
}

// startCoalescePostgres stands up the schema with alembic 0060's CHECK
// constraints AND its terminal-immutability trigger, which createExecutionTables
// omits. Both are load-bearing here rather than incidental: the state CHECK is
// what proves 'canceled' is a state the column actually admits, and the trigger
// is what makes a superseded request unrecoverable by anything downstream.
func startCoalescePostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
CREATE TABLE work_graph_execution_requests (
 id uuid PRIMARY KEY, org_id uuid NOT NULL,
 kind text NOT NULL CHECK (kind IN ('workgraph.build', 'investment.materialize')),
 scope jsonb NOT NULL,
 model_ref text NULL, prompt_ref text NULL,
 llm_concurrency integer NOT NULL CHECK (llm_concurrency BETWEEN 1 AND 16),
 spend_limit_microunits bigint NOT NULL CHECK (spend_limit_microunits >= 0),
 correlation_id text NOT NULL CHECK (length(correlation_id) BETWEEN 1 AND 128),
 idempotency_key text NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 256),
 state text NOT NULL DEFAULT 'pending' CHECK (state IN (
   'pending', 'running', 'succeeded', 'failed', 'ambiguous', 'canceled')),
 claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
 CHECK ((state = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
     OR (state <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL))
);
CREATE TABLE work_graph_execution_ledger (
 request_id uuid PRIMARY KEY REFERENCES work_graph_execution_requests(id) ON DELETE CASCADE,
 claim_token uuid NOT NULL, state text NOT NULL, attempt_count integer NOT NULL DEFAULT 1,
 output_evidence jsonb NULL, failure_detail text NULL,
 last_attempt_at timestamptz NOT NULL DEFAULT statement_timestamp(), completed_at timestamptz NULL
);
CREATE TABLE worker_job_outbox (
 id uuid PRIMARY KEY, dedupe_key varchar(256) NOT NULL UNIQUE, job_kind varchar(96) NOT NULL,
 contract_version integer NOT NULL, args json NOT NULL, payload_hash varchar(71) NOT NULL,
 queue varchar(96) NOT NULL, priority smallint NOT NULL, max_attempts smallint NOT NULL,
 scheduled_at timestamptz NOT NULL, status varchar(16) NOT NULL, attempt_count integer NOT NULL,
 next_attempt_at timestamptz NOT NULL, prerequisite_completion_key text NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE TABLE worker_job_completion_fences (
 completion_key text PRIMARY KEY,
 completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
CREATE OR REPLACE FUNCTION forbid_work_graph_terminal_mutation()
RETURNS trigger AS $$
BEGIN
    IF OLD.state IN ('succeeded', 'failed', 'canceled') THEN
        RAISE EXCEPTION 'terminal work graph execution request is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER work_graph_execution_terminal_immutable
BEFORE UPDATE ON work_graph_execution_requests
FOR EACH ROW EXECUTE FUNCTION forbid_work_graph_terminal_mutation()`); err != nil {
		t.Fatal(err)
	}
	return pool
}
