//go:build integration

package repair

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ddl is the ledger schema exactly as the migrations create it (the columns the
// runs and partitions carry are those the liveness reads name).
var ddl = []string{
	`CREATE TABLE work_graph_execution_requests (
		id uuid PRIMARY KEY, org_id uuid NOT NULL,
		kind text NOT NULL CHECK (kind IN ('workgraph.build','investment.materialize','investment.dispatch','investment.chunk','investment.finalize')),
		scope jsonb NOT NULL,
		model_ref text NULL, prompt_ref text NULL, llm_concurrency integer NOT NULL CHECK (llm_concurrency BETWEEN 1 AND 16),
		spend_limit_microunits bigint NOT NULL CHECK (spend_limit_microunits >= 0),
		correlation_id text NOT NULL, idempotency_key text NOT NULL UNIQUE,
		state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','running','succeeded','failed','ambiguous','canceled')),
		claim_token uuid NULL, lease_expires_at timestamptz NULL,
		attempt_count integer NOT NULL DEFAULT 0,
		created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
		updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
		CHECK ((state = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
			OR (state <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL)))`,
	`CREATE TABLE work_graph_execution_ledger (
		request_id uuid PRIMARY KEY REFERENCES work_graph_execution_requests(id) ON DELETE CASCADE,
		claim_token uuid NOT NULL,
		state text NOT NULL CHECK (state IN ('executing','succeeded','failed','ambiguous','repaired')),
		attempt_count integer NOT NULL DEFAULT 1 CHECK (attempt_count >= 1),
		output_evidence jsonb NULL,
		failure_detail text NULL CHECK (failure_detail IS NULL OR length(failure_detail) BETWEEN 1 AND 1024),
		last_attempt_at timestamptz NOT NULL DEFAULT statement_timestamp(),
		completed_at timestamptz NULL,
		CHECK ((state = 'succeeded' AND completed_at IS NOT NULL AND output_evidence IS NOT NULL)
			OR (state <> 'succeeded' AND completed_at IS NULL)))`,
	`CREATE TABLE work_graph_execution_repairs (
		id uuid PRIMARY KEY,
		request_id uuid NOT NULL REFERENCES work_graph_execution_requests(id) ON DELETE CASCADE,
		expected_attempt_count integer NOT NULL CHECK (expected_attempt_count >= 1),
		resolution text NOT NULL CHECK (resolution IN ('retry_safe','confirm_succeeded')),
		review_evidence text NOT NULL CHECK (length(review_evidence) BETWEEN 1 AND 2048),
		output_evidence jsonb NULL,
		created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
		UNIQUE (request_id, expected_attempt_count, resolution),
		CHECK ((resolution = 'confirm_succeeded' AND output_evidence IS NOT NULL)
			OR (resolution = 'retry_safe' AND output_evidence IS NULL)))`,
	`CREATE OR REPLACE FUNCTION forbid_work_graph_terminal_mutation() RETURNS trigger AS $$
		BEGIN
			IF OLD.state IN ('succeeded','failed','canceled') THEN
				RAISE EXCEPTION 'terminal work graph execution request is immutable';
			END IF;
			RETURN NEW;
		END; $$ LANGUAGE plpgsql`,
	`CREATE TRIGGER work_graph_execution_terminal_immutable BEFORE UPDATE ON work_graph_execution_requests
		FOR EACH ROW EXECUTE FUNCTION forbid_work_graph_terminal_mutation()`,
	`CREATE TABLE metric_compatibility_executions (
		id uuid PRIMARY KEY,
		worker_kind text NOT NULL CHECK (worker_kind IN ('daily','remaining')),
		operation text NOT NULL CHECK (operation IN ('partition','finalize')),
		run_id uuid NOT NULL, partition_id uuid NULL,
		family text NOT NULL, generation text NOT NULL, scope_digest text NOT NULL,
		claim_token uuid NOT NULL,
		state text NOT NULL CHECK (state IN ('executing','succeeded','ambiguous','retry_authorized')),
		attempt_count integer NOT NULL DEFAULT 1 CHECK (attempt_count >= 1),
		output_evidence jsonb NULL,
		failure_detail text NULL,
		created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
		last_attempt_at timestamptz NOT NULL DEFAULT statement_timestamp(),
		completed_at timestamptz NULL,
		CHECK ((operation = 'partition' AND partition_id IS NOT NULL) OR (operation = 'finalize' AND partition_id IS NULL)),
		CHECK ((state = 'succeeded' AND completed_at IS NOT NULL AND output_evidence IS NOT NULL)
			OR (state <> 'succeeded' AND completed_at IS NULL)))`,
	`CREATE TABLE metric_compatibility_execution_repairs (
		id uuid PRIMARY KEY,
		execution_id uuid NOT NULL REFERENCES metric_compatibility_executions(id) ON DELETE CASCADE,
		expected_state text NOT NULL CHECK (expected_state IN ('executing','ambiguous')),
		expected_attempt_count integer NOT NULL CHECK (expected_attempt_count >= 1),
		resolution text NOT NULL CHECK (resolution IN ('retry_safe','confirm_succeeded')),
		review_evidence text NOT NULL CHECK (length(review_evidence) BETWEEN 1 AND 2048),
		output_evidence jsonb NULL,
		created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
		CHECK ((resolution = 'confirm_succeeded' AND output_evidence IS NOT NULL)
			OR (resolution = 'retry_safe' AND output_evidence IS NULL)),
		UNIQUE (execution_id, expected_state, expected_attempt_count, resolution))`,
	`CREATE TABLE daily_metrics_runs (
		id uuid PRIMARY KEY, status text NOT NULL,
		finalization_status text NULL, finalization_claim_token uuid NULL, finalization_lease_expires_at timestamptz NULL)`,
	`CREATE TABLE daily_metrics_partitions (
		id uuid PRIMARY KEY, run_id uuid NOT NULL, status text NOT NULL,
		claim_token uuid NULL, lease_expires_at timestamptz NULL)`,
	`CREATE TABLE remaining_metric_runs (id uuid PRIMARY KEY, status text NOT NULL, canceled_at timestamptz NULL)`,
	`CREATE TABLE remaining_metric_partitions (
		id uuid PRIMARY KEY, run_id uuid NOT NULL, status text NOT NULL,
		claim_token uuid NULL, lease_expires_at timestamptz NULL)`,
}

func startDB(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)
	inst, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, stmt := range ddl {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("ddl: %v\n%s", err, stmt)
		}
	}
	return ctx, pool
}

func mustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// ambiguousRequest seeds one work-graph execution in the given request and
// ledger states.
func ambiguousRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestState, ledgerState string, attempt int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	claim, lease := "NULL", "NULL"
	if requestState == "running" {
		claim, lease = "'"+uuid.NewString()+"'", "now() + interval '1 hour'"
	}
	mustExec(t, ctx, pool, `INSERT INTO work_graph_execution_requests
		(id, org_id, kind, scope, llm_concurrency, spend_limit_microunits, correlation_id, idempotency_key, state, claim_token, lease_expires_at, attempt_count)
		VALUES ($1, $2, 'investment.materialize', '{}', 1, 0, 'c', $3, `+`$4`+`, `+claim+`, `+lease+`, $5)`,
		id, uuid.New(), id.String(), requestState, attempt)
	failure := "NULL"
	if ledgerState == "ambiguous" {
		failure = "'unclassified outcome'"
	}
	mustExec(t, ctx, pool, `INSERT INTO work_graph_execution_ledger (request_id, claim_token, state, attempt_count, failure_detail)
		VALUES ($1, $2, $3, $4, `+failure+`)`, id, uuid.New(), ledgerState, attempt)
	return id
}

type wgRow struct {
	RequestState, LedgerState string
	Output                    *string
	Failure                   *string
	Completed                 bool
	Repairs                   int
}

func readWG(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) wgRow {
	t.Helper()
	var r wgRow
	err := pool.QueryRow(ctx, `SELECT req.state, l.state, l.output_evidence::text, l.failure_detail, l.completed_at IS NOT NULL,
		(SELECT count(*) FROM work_graph_execution_repairs WHERE request_id = req.id)
		FROM work_graph_execution_requests req JOIN work_graph_execution_ledger l ON l.request_id = req.id WHERE req.id = $1`, id).
		Scan(&r.RequestState, &r.LedgerState, &r.Output, &r.Failure, &r.Completed, &r.Repairs)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func wantRefusal(t *testing.T, err error, status int, detail string) {
	t.Helper()
	refusal, ok := AsRefusal(err)
	if !ok || refusal.Status != status || (detail != "" && refusal.Detail != detail) {
		t.Fatalf("error = %v, want refusal %d %q", err, status, detail)
	}
}

func evidence(t *testing.T, raw string) map[string]any {
	t.Helper()
	m, err := decodeObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func decodeObject(raw string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var out map[string]any
	return out, decoder.Decode(&out)
}

func TestWorkgraphRepair(t *testing.T) {
	ctx, pool := startDB(t)

	t.Run("retry_safe returns the request to pending and marks the ledger repaired", func(t *testing.T) {
		id := ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 2)
		got, err := RepairWorkgraph(ctx, pool, WorkgraphRequest{RequestID: id, ExpectedAttemptCount: 2, Resolution: ResolutionRetrySafe, ReviewEvidence: "checked the target"}, false)
		if err != nil || got.Status != "repaired" || got.RequestID != id.String() {
			t.Fatalf("%+v %v", got, err)
		}
		r := readWG(t, ctx, pool, id)
		if r.RequestState != "pending" || r.LedgerState != "repaired" || r.Output != nil || r.Failure != nil || r.Completed || r.Repairs != 1 {
			t.Fatalf("%+v", r)
		}
	})

	t.Run("confirm_succeeded settles both with the canonical evidence", func(t *testing.T) {
		id := ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 1)
		out := evidence(t, `{"b": 2, "a": {"z": 1.5, "y": "é"}}`)
		if _, err := RepairWorkgraph(ctx, pool, WorkgraphRequest{RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionConfirmSucceeded, ReviewEvidence: "verified", OutputEvidence: out}, false); err != nil {
			t.Fatal(err)
		}
		r := readWG(t, ctx, pool, id)
		if r.RequestState != "succeeded" || r.LedgerState != "succeeded" || !r.Completed || r.Output == nil || r.Repairs != 1 {
			t.Fatalf("%+v", r)
		}
		var stored map[string]any
		if err := json.Unmarshal([]byte(*r.Output), &stored); err != nil || stored["b"] != float64(2) {
			t.Fatalf("stored evidence %s", *r.Output)
		}
	})

	t.Run("repairing twice is refused the second time and changes nothing", func(t *testing.T) {
		id := ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 1)
		req := WorkgraphRequest{RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: "x"}
		if _, err := RepairWorkgraph(ctx, pool, req, false); err != nil {
			t.Fatal(err)
		}
		_, err := RepairWorkgraph(ctx, pool, req, false)
		wantRefusal(t, err, 409, "Only unleased ambiguous executions can be repaired")
		if r := readWG(t, ctx, pool, id); r.Repairs != 1 || r.RequestState != "pending" {
			t.Fatalf("%+v", r)
		}
	})

	t.Run("every ineligible row is refused with the same reason and untouched", func(t *testing.T) {
		cases := map[string]struct {
			id      uuid.UUID
			attempt int
		}{
			"request pending":     {ambiguousRequest(t, ctx, pool, "pending", "ambiguous", 1), 1},
			"request running":     {ambiguousRequest(t, ctx, pool, "running", "ambiguous", 1), 1},
			"ledger executing":    {ambiguousRequest(t, ctx, pool, "ambiguous", "executing", 1), 1},
			"ledger failed":       {ambiguousRequest(t, ctx, pool, "ambiguous", "failed", 1), 1},
			"stale attempt count": {ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 3), 2},
			"absent":              {uuid.New(), 1},
		}
		for name, c := range cases {
			before := ""
			if name != "absent" {
				r := readWG(t, ctx, pool, c.id)
				before = r.RequestState + "/" + r.LedgerState
			}
			_, err := RepairWorkgraph(ctx, pool, WorkgraphRequest{RequestID: c.id, ExpectedAttemptCount: c.attempt, Resolution: ResolutionRetrySafe, ReviewEvidence: "x"}, false)
			wantRefusal(t, err, 409, "Only unleased ambiguous executions can be repaired")
			if name != "absent" {
				r := readWG(t, ctx, pool, c.id)
				if r.RequestState+"/"+r.LedgerState != before || r.Repairs != 0 {
					t.Errorf("%s changed: %+v", name, r)
				}
			}
		}
	})

	t.Run("a failure midway rolls the whole repair back", func(t *testing.T) {
		id := ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 1)
		// an existing repair under the same unique key makes the insert fail
		mustExec(t, ctx, pool, `INSERT INTO work_graph_execution_repairs (id, request_id, expected_attempt_count, resolution, review_evidence)
			VALUES ($1, $2, 1, 'retry_safe', 'earlier')`, uuid.New(), id)
		_, err := RepairWorkgraph(ctx, pool, WorkgraphRequest{RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: "x"}, false)
		if err == nil {
			t.Fatal("expected the insert to fail")
		}
		if _, refused := AsRefusal(err); refused {
			t.Fatalf("a database failure is not a refusal: %v", err)
		}
		if r := readWG(t, ctx, pool, id); r.RequestState != "ambiguous" || r.LedgerState != "ambiguous" || r.Repairs != 1 {
			t.Fatalf("not rolled back: %+v", r)
		}
	})

	t.Run("invalid requests are refused before any read", func(t *testing.T) {
		id := ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 1)
		big := map[string]any{"k": strings.Repeat("x", OutputEvidenceMaxBytes)}
		for name, req := range map[string]WorkgraphRequest{
			"attempt below one":        {RequestID: id, ExpectedAttemptCount: 0, Resolution: ResolutionRetrySafe, ReviewEvidence: "x"},
			"unknown resolution":       {RequestID: id, ExpectedAttemptCount: 1, Resolution: "other", ReviewEvidence: "x"},
			"empty review evidence":    {RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: ""},
			"review evidence too long": {RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: strings.Repeat("é", 1025)},
			"confirm without output":   {RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionConfirmSucceeded, ReviewEvidence: "x"},
			"retry with output":        {RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: "x", OutputEvidence: map[string]any{}},
			"output over the bound":    {RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionConfirmSucceeded, ReviewEvidence: "x", OutputEvidence: big},
		} {
			_, err := RepairWorkgraph(ctx, pool, req, false)
			if _, ok := AsRefusal(err); !ok {
				t.Errorf("%s: %v", name, err)
			}
		}
		if r := readWG(t, ctx, pool, id); r.Repairs != 0 || r.RequestState != "ambiguous" {
			t.Fatalf("%+v", r)
		}
	})

	t.Run("a dry run performs the repair and rolls it back", func(t *testing.T) {
		id := ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 1)
		got, err := RepairWorkgraph(ctx, pool, WorkgraphRequest{RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: "x"}, true)
		if err != nil || got.Status != "repaired" {
			t.Fatalf("%+v %v", got, err)
		}
		if r := readWG(t, ctx, pool, id); r.RequestState != "ambiguous" || r.LedgerState != "ambiguous" || r.Repairs != 0 {
			t.Fatalf("dry run left changes: %+v", r)
		}
	})
}

// seedExecution inserts a metric compatibility execution and, when live, the
// run/partition rows that hold its claim under a live lease.
func seedExecution(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, operation, state string, attempt int, live bool) (execution uuid.UUID, run uuid.UUID) {
	t.Helper()
	execution, run = uuid.New(), uuid.New()
	partition, claim := uuid.New(), uuid.New()
	lease := "now() - interval '1 hour'"
	if live {
		lease = "now() + interval '1 hour'"
	}
	var partitionArg any
	if operation == "partition" {
		partitionArg = partition
	}
	mustExec(t, ctx, pool, `INSERT INTO metric_compatibility_executions
		(id, worker_kind, operation, run_id, partition_id, family, generation, scope_digest, claim_token, state, attempt_count)
		VALUES ($1,$2,$3,$4,$5,'f','g',$6,$7,$8,$9)`, execution, kind, operation, run, partitionArg, strings.Repeat("a", 64), claim, state, attempt)
	switch {
	case kind == "remaining":
		mustExec(t, ctx, pool, `INSERT INTO remaining_metric_runs (id, status) VALUES ($1, 'running')`, run)
		mustExec(t, ctx, pool, `INSERT INTO remaining_metric_partitions (id, run_id, status, claim_token, lease_expires_at) VALUES ($1,$2,'running',$3,`+lease+`)`, partition, run, claim)
	case operation == "partition":
		mustExec(t, ctx, pool, `INSERT INTO daily_metrics_runs (id, status) VALUES ($1, 'running')`, run)
		mustExec(t, ctx, pool, `INSERT INTO daily_metrics_partitions (id, run_id, status, claim_token, lease_expires_at) VALUES ($1,$2,'running',$3,`+lease+`)`, partition, run, claim)
	default:
		mustExec(t, ctx, pool, `INSERT INTO daily_metrics_runs (id, status, finalization_status, finalization_claim_token, finalization_lease_expires_at) VALUES ($1,'running','running',$2,`+lease+`)`, run, claim)
	}
	return execution, run
}

func readExecution(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (state string, output *string, repairs int) {
	t.Helper()
	err := pool.QueryRow(ctx, `SELECT state, output_evidence::text, (SELECT count(*) FROM metric_compatibility_execution_repairs WHERE execution_id = $1)
		FROM metric_compatibility_executions WHERE id = $1`, id).Scan(&state, &output, &repairs)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestMetricExecutionRepair(t *testing.T) {
	ctx, pool := startDB(t)
	retry := func(id uuid.UUID, state string, attempt int, review string) MetricRequest {
		return MetricRequest{ExecutionID: id, ExpectedState: state, ExpectedAttemptCount: attempt, Resolution: ResolutionRetrySafe, ReviewEvidence: review}
	}

	t.Run("retry_safe authorises a retry for every claim shape once the claim is dead", func(t *testing.T) {
		for _, c := range []struct{ kind, op string }{{"daily", "partition"}, {"daily", "finalize"}, {"remaining", "partition"}} {
			id, _ := seedExecution(t, ctx, pool, c.kind, c.op, "ambiguous", 1, false)
			got, err := RepairMetricExecution(ctx, pool, retry(id, "ambiguous", 1, "checked"), false)
			if err != nil || got.Status != "repaired" || got.State != "retry_authorized" || got.ExecutionID != id.String() {
				t.Fatalf("%v/%v: %+v %v", c.kind, c.op, got, err)
			}
			if state, out, n := readExecution(t, ctx, pool, id); state != "retry_authorized" || out != nil || n != 1 {
				t.Fatalf("%v/%v: %s %v %d", c.kind, c.op, state, out, n)
			}
		}
	})

	t.Run("a live claim is never repaired out from under its owner", func(t *testing.T) {
		for _, c := range []struct{ kind, op string }{{"daily", "partition"}, {"daily", "finalize"}, {"remaining", "partition"}} {
			id, _ := seedExecution(t, ctx, pool, c.kind, c.op, "executing", 1, true)
			_, err := RepairMetricExecution(ctx, pool, retry(id, "executing", 1, "x"), false)
			wantRefusal(t, err, 409, "Original execution claim is still active")
			if state, _, n := readExecution(t, ctx, pool, id); state != "executing" || n != 0 {
				t.Fatalf("%v/%v changed: %s %d", c.kind, c.op, state, n)
			}
		}
	})

	t.Run("a stuck executing row with a dead claim is repairable", func(t *testing.T) {
		id, _ := seedExecution(t, ctx, pool, "daily", "partition", "executing", 1, false)
		if got, err := RepairMetricExecution(ctx, pool, retry(id, "executing", 1, "x"), false); err != nil || got.State != "retry_authorized" {
			t.Fatalf("%+v %v", got, err)
		}
	})

	t.Run("confirm_succeeded settles with the evidence and stays idempotent", func(t *testing.T) {
		id, _ := seedExecution(t, ctx, pool, "daily", "partition", "ambiguous", 1, false)
		req := MetricRequest{ExecutionID: id, ExpectedState: "ambiguous", ExpectedAttemptCount: 1, Resolution: ResolutionConfirmSucceeded, ReviewEvidence: "verified",
			OutputEvidence: evidence(t, `{"rows": 12, "ratio": 0.5}`)}
		got, err := RepairMetricExecution(ctx, pool, req, false)
		if err != nil || got.State != "succeeded" {
			t.Fatalf("%+v %v", got, err)
		}
		again, err := RepairMetricExecution(ctx, pool, req, false)
		if err != nil || again.Status != "already_applied" || again.State != "succeeded" {
			t.Fatalf("second application: %+v %v", again, err)
		}
		if _, _, n := readExecution(t, ctx, pool, id); n != 1 {
			t.Fatalf("repairs = %d", n)
		}
		changed := req
		changed.ReviewEvidence = "verified differently"
		_, err = RepairMetricExecution(ctx, pool, changed, false)
		wantRefusal(t, err, 409, "Repair identity conflict")
		changed = req
		changed.OutputEvidence = evidence(t, `{"rows": 13, "ratio": 0.5}`)
		_, err = RepairMetricExecution(ctx, pool, changed, false)
		wantRefusal(t, err, 409, "Repair identity conflict")
	})

	t.Run("retry_safe applied twice is idempotent", func(t *testing.T) {
		id, _ := seedExecution(t, ctx, pool, "daily", "finalize", "ambiguous", 1, false)
		r := retry(id, "ambiguous", 1, "same")
		if _, err := RepairMetricExecution(ctx, pool, r, false); err != nil {
			t.Fatal(err)
		}
		again, err := RepairMetricExecution(ctx, pool, r, false)
		if err != nil || again.Status != "already_applied" || again.State != "retry_authorized" {
			t.Fatalf("%+v %v", again, err)
		}
	})

	t.Run("state, attempt and absence are refused", func(t *testing.T) {
		id, _ := seedExecution(t, ctx, pool, "daily", "partition", "ambiguous", 2, false)
		_, err := RepairMetricExecution(ctx, pool, retry(id, "executing", 2, "x"), false)
		wantRefusal(t, err, 409, "Execution state or attempt changed")
		_, err = RepairMetricExecution(ctx, pool, retry(id, "ambiguous", 1, "x"), false)
		wantRefusal(t, err, 409, "Execution state or attempt changed")
		_, err = RepairMetricExecution(ctx, pool, retry(uuid.New(), "ambiguous", 1, "x"), false)
		wantRefusal(t, err, 404, "Execution not found")
		if state, _, n := readExecution(t, ctx, pool, id); state != "ambiguous" || n != 0 {
			t.Fatalf("%s %d", state, n)
		}
	})

	t.Run("invalid requests are refused", func(t *testing.T) {
		id, _ := seedExecution(t, ctx, pool, "daily", "partition", "ambiguous", 1, false)
		for name, req := range map[string]MetricRequest{
			"state neither":          retry(id, "succeeded", 1, "x"),
			"attempt zero":           retry(id, "ambiguous", 0, "x"),
			"empty review":           retry(id, "ambiguous", 1, ""),
			"confirm without output": {ExecutionID: id, ExpectedState: "ambiguous", ExpectedAttemptCount: 1, Resolution: ResolutionConfirmSucceeded, ReviewEvidence: "x"},
			"retry with output":      {ExecutionID: id, ExpectedState: "ambiguous", ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: "x", OutputEvidence: map[string]any{}},
		} {
			_, err := RepairMetricExecution(ctx, pool, req, false)
			if _, ok := AsRefusal(err); !ok {
				t.Errorf("%s: %v", name, err)
			}
		}
	})

	t.Run("a dry run rolls back", func(t *testing.T) {
		id, _ := seedExecution(t, ctx, pool, "daily", "partition", "ambiguous", 1, false)
		if got, err := RepairMetricExecution(ctx, pool, retry(id, "ambiguous", 1, "x"), true); err != nil || got.Status != "repaired" {
			t.Fatalf("%+v %v", got, err)
		}
		if state, _, n := readExecution(t, ctx, pool, id); state != "ambiguous" || n != 0 {
			t.Fatalf("dry run left changes: %s %d", state, n)
		}
	})
}

func TestRedriveDaily(t *testing.T) {
	ctx, pool := startDB(t)

	dead, deadRun := seedExecution(t, ctx, pool, "daily", "partition", "ambiguous", 1, false)
	stuck, stuckRun := seedExecution(t, ctx, pool, "daily", "partition", "executing", 1, false)
	live, liveRun := seedExecution(t, ctx, pool, "daily", "partition", "executing", 1, true)
	finalize, finalizeRun := seedExecution(t, ctx, pool, "daily", "finalize", "ambiguous", 1, false)
	done, doneRun := seedExecution(t, ctx, pool, "daily", "partition", "ambiguous", 1, false)
	mustExec(t, ctx, pool, `UPDATE metric_compatibility_executions SET state = 'retry_authorized' WHERE id = $1`, done)
	remaining, remainingRun := seedExecution(t, ctx, pool, "remaining", "partition", "ambiguous", 1, false)

	runs := []uuid.UUID{deadRun, stuckRun, liveRun, finalizeRun, doneRun, remainingRun}
	states := func() map[string]string {
		out := map[string]string{}
		for name, id := range map[string]uuid.UUID{"dead": dead, "stuck": stuck, "live": live, "finalize": finalize, "done": done, "remaining": remaining} {
			out[name], _, _ = readExecution(t, ctx, pool, id)
		}
		return out
	}

	got, err := RedriveDaily(ctx, pool, RedriveRequest{RunIDs: runs, ReviewEvidence: "checked partition output"}, true)
	if err != nil || got.Repaired != 2 || got.SkippedClaimActive != 1 {
		t.Fatalf("dry run: %+v %v", got, err)
	}
	if s := states(); s["dead"] != "ambiguous" || s["stuck"] != "executing" {
		t.Fatalf("a dry run must change nothing: %v", s)
	}

	got, err = RedriveDaily(ctx, pool, RedriveRequest{RunIDs: runs, ReviewEvidence: "checked partition output"}, false)
	if err != nil || got.Repaired != 2 || got.SkippedClaimActive != 1 {
		t.Fatalf("partition redrive: %+v %v", got, err)
	}
	if s := states(); s["dead"] != "retry_authorized" || s["stuck"] != "retry_authorized" || s["live"] != "executing" ||
		s["finalize"] != "ambiguous" || s["done"] != "retry_authorized" || s["remaining"] != "ambiguous" {
		t.Fatalf("partition scope must not touch finalize or remaining rows: %v", s)
	}

	got, err = RedriveDaily(ctx, pool, RedriveRequest{RunIDs: runs, ReviewEvidence: "checked finalize output", Operations: []string{"finalize"}}, false)
	if err != nil || got.Repaired != 1 || got.SkippedClaimActive != 0 {
		t.Fatalf("finalize redrive: %+v %v", got, err)
	}
	if s := states(); s["finalize"] != "retry_authorized" {
		t.Fatalf("%v", s)
	}
	// a repeat finds nothing left to move for the same runs and scope except the live claim
	got, err = RedriveDaily(ctx, pool, RedriveRequest{RunIDs: runs, ReviewEvidence: "again"}, false)
	if err != nil || got.Repaired != 0 || got.SkippedClaimActive != 1 {
		t.Fatalf("repeat: %+v %v", got, err)
	}

	if got, err := RedriveDaily(ctx, pool, RedriveRequest{ReviewEvidence: "x"}, false); err != nil || got != (RedriveResult{}) {
		t.Fatalf("no runs: %+v %v", got, err)
	}
	_, err = RedriveDaily(ctx, pool, RedriveRequest{RunIDs: runs, ReviewEvidence: "x", Operations: []string{"partition", "partition"}}, false)
	wantRefusal(t, err, 422, "")
	_, err = RedriveDaily(ctx, pool, RedriveRequest{RunIDs: runs, ReviewEvidence: "x", Operations: []string{"other"}}, false)
	wantRefusal(t, err, 422, "")
	_, err = RedriveDaily(ctx, pool, RedriveRequest{RunIDs: runs, ReviewEvidence: ""}, false)
	wantRefusal(t, err, 422, "")
	tooMany := make([]uuid.UUID, MaxRedriveRuns+1)
	for i := range tooMany {
		tooMany[i] = uuid.New()
	}
	_, err = RedriveDaily(ctx, pool, RedriveRequest{RunIDs: tooMany, ReviewEvidence: "x"}, false)
	wantRefusal(t, err, 422, "")
}

// Two operators repairing the same row at once: exactly one moves it, the other
// is told it is already applied (metric, identical assertion) or refused
// (work graph), and the ledger records a single repair.
func TestRepairsRaceSafely(t *testing.T) {
	ctx, pool := startDB(t)
	race := func(n int, run func() error) []error {
		errs := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				errs[i] = run()
			}(i)
		}
		close(start)
		wg.Wait()
		return errs
	}

	t.Run("work graph", func(t *testing.T) {
		id := ambiguousRequest(t, ctx, pool, "ambiguous", "ambiguous", 1)
		errs := race(6, func() error {
			_, err := RepairWorkgraph(ctx, pool, WorkgraphRequest{RequestID: id, ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: "x"}, false)
			return err
		})
		won := 0
		for _, err := range errs {
			if err == nil {
				won++
			} else if r, ok := AsRefusal(err); !ok || r.Status != 409 {
				t.Fatalf("unexpected: %v", err)
			}
		}
		if r := readWG(t, ctx, pool, id); won != 1 || r.Repairs != 1 || r.RequestState != "pending" {
			t.Fatalf("won %d %+v", won, r)
		}
	})

	t.Run("metric execution", func(t *testing.T) {
		id, _ := seedExecution(t, ctx, pool, "daily", "partition", "ambiguous", 1, false)
		repaired, applied := 0, 0
		var mu sync.Mutex
		errs := race(6, func() error {
			got, err := RepairMetricExecution(ctx, pool, MetricRequest{ExecutionID: id, ExpectedState: "ambiguous", ExpectedAttemptCount: 1, Resolution: ResolutionRetrySafe, ReviewEvidence: "x"}, false)
			mu.Lock()
			defer mu.Unlock()
			switch got.Status {
			case "repaired":
				repaired++
			case "already_applied":
				applied++
			}
			return err
		})
		for _, err := range errs {
			if err != nil {
				t.Fatalf("an identical assertion never fails: %v", err)
			}
		}
		if state, _, n := readExecution(t, ctx, pool, id); repaired != 1 || applied != 5 || n != 1 || state != "retry_authorized" {
			t.Fatalf("repaired %d applied %d repairs %d state %s", repaired, applied, n, state)
		}
	})
}
