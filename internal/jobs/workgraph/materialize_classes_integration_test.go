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
