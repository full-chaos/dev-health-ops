//go:build integration

package workgraph

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// One case per error class of Execute. Transient classes must leave the
// request claimable; deterministic classes end 'failed' (never 'ambiguous').
func TestMaterializeErrorClassesLeaveTheRightState(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		err          error
		wantCategory jobruntime.ErrorCategory
		wantState    string
	}{
		{name: "deadline", err: context.DeadlineExceeded, wantCategory: jobruntime.CategoryRetryable, wantState: "pending"},
		{name: "network", err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}, wantCategory: jobruntime.CategoryRetryable, wantState: "pending"},
		{name: "unclassified infra", err: errors.New("write work_unit_investments: send batch"), wantCategory: jobruntime.CategoryRetryable, wantState: "pending"},
		{name: "deterministic", err: Deterministic(ClassLLMDeterministic, errors.New("provider said no")), wantCategory: jobruntime.CategoryPermanent, wantState: "failed"},
		{name: "invalid scope", err: Deterministic(ClassScopeInvalid, errors.New("bad scope")), wantCategory: jobruntime.CategoryPermanent, wantState: "failed"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			pool, store, _ := startWorkGraphReleaseLostFixture(t, ctx)
			insertMaterializeRequest(t, ctx, pool)
			handler, err := NewMaterializeHandler(store, funcExecutor{run: func(context.Context, Claim) ([]byte, error) {
				return nil, testCase.err
			}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			workErr := handler.Work(ctx, materializeExecution())
			if workErr == nil || !strings.Contains(workErr.Error(), string(testCase.wantCategory)) {
				t.Fatalf("Work = %v, want %s", workErr, testCase.wantCategory)
			}
			request, ledger := requestAndLedgerState(t, ctx, pool)
			if request != testCase.wantState || request == "ambiguous" || ledger == "ambiguous" {
				t.Fatalf("request=%s ledger=%s, want request %s", request, ledger, testCase.wantState)
			}
			if testCase.wantState == "pending" {
				// Claimable at once, without waiting out a 10 minute lease.
				claim, err := store.Claim(ctx, testRequestID, KindMaterialize)
				if err != nil || claim == nil {
					t.Fatalf("re-claim = %v, %v", claim, err)
				}
				// The claim counter survives the requeue: it is the clock the
				// retry-loop alert reads.
				if claim.Request.AttemptCount != 2 {
					t.Fatalf("claim count after one requeue = %d, want 2", claim.Request.AttemptCount)
				}
			}
		})
	}
}

// TestRetryBudgetEndsFailedAndDoesNotBlockTheNextRequest (CHAOS-8782, D4926)
// on the real handler and Postgres state code: a request that fails
// retryably on every claim is requeued through claim 8 and ends 'failed' on
// claim 9, and a LATER request (the next scheduled one: another id and another
// idempotency key) is claimed and completed normally.
func TestRetryBudgetEndsFailedAndDoesNotBlockTheNextRequest(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := startWorkGraphReleaseLostFixture(t, ctx)
	insertMaterializeRequest(t, ctx, pool)
	failing, err := NewMaterializeHandler(store, funcExecutor{run: func(context.Context, Claim) ([]byte, error) {
		return nil, errors.New("write work_unit_investments: send batch")
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for claim := 1; claim < retryBudgetClaims; claim++ {
		workErr := failing.Work(ctx, materializeExecution())
		if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryRetryable)) {
			t.Fatalf("claim %d: %v, want retryable", claim, workErr)
		}
		if request, _ := requestAndLedgerState(t, ctx, pool); request != "pending" {
			t.Fatalf("claim %d: request %s, want pending", claim, request)
		}
	}
	workErr := failing.Work(ctx, materializeExecution())
	if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("claim %d: %v, want permanent", retryBudgetClaims, workErr)
	}
	request, ledger := requestAndLedgerState(t, ctx, pool)
	if request != "failed" || ledger != "failed" {
		t.Fatalf("request=%s ledger=%s, want failed/failed (never ambiguous)", request, ledger)
	}
	var detail string
	if err := pool.QueryRow(ctx, `SELECT failure_detail FROM work_graph_execution_ledger WHERE request_id = $1`, testRequestID).Scan(&detail); err != nil {
		t.Fatal(err)
	}
	if detail != "retry budget exhausted: unclassified" {
		t.Fatalf("ledger detail = %q", detail)
	}
	// A failed request is not claimable again (strand repair does not re-arm it:
	// joboutbox TestStrandRepairAgainstLivePostgres "failed is refused").
	if _, err := store.Claim(ctx, testRequestID, KindMaterialize); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("re-claim of a failed request = %v, want ErrInvalidState", err)
	}

	// The next scheduled request for the same org and kind.
	const nextID = "00000000-0000-4000-8000-000000000102"
	if _, err := pool.Exec(ctx, `
INSERT INTO work_graph_execution_requests (
    id, org_id, kind, scope, llm_concurrency, spend_limit_microunits,
    correlation_id, idempotency_key, state
) VALUES ($1,$2,'investment.materialize','{}',1,0,'next-fixture','workgraph:next-fixture','pending')`, nextID, testOrgID); err != nil {
		t.Fatal(err)
	}
	next := materializeExecution()
	next.Args.Payload.RequestID = nextID
	next.Envelope.Domain.ID = nextID
	next.Args.Domain.ID = nextID
	succeeding, err := NewMaterializeHandler(store, succeedingExecutor(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := succeeding.Work(ctx, next); err != nil {
		t.Fatalf("next request = %v, want success", err)
	}
	var nextState string
	if err := pool.QueryRow(ctx, `SELECT state FROM work_graph_execution_requests WHERE id = $1`, nextID).Scan(&nextState); err != nil {
		t.Fatal(err)
	}
	if nextState != "succeeded" {
		t.Fatalf("next request state = %s, want succeeded", nextState)
	}
}
