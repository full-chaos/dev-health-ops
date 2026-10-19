package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func ledgerTotal(snapshot RequestLedgerSnapshot) int {
	total := 0
	for _, entry := range snapshot.Entries {
		total += entry.Requests
	}
	return total
}

// Every execution of a complete route returns the requests it made per route
// family and dimension, on every exit: the actuals of the unit's budget
// comparison. The route here sends through the executor's own client and
// labels nothing, so they are unclassified.
func TestExecuteReturnsTheUnitsRequestLedgerOnEveryExit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	errCollect := errors.New("route decode failed")
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	cases := []struct {
		name    string
		calls   int
		adjust  func(*CompleteRouteExecutor)
		wantErr func(error) bool
		want    int
	}{
		{"success", 2, nil, func(err error) bool { return err == nil }, 2},
		{"route error after three requests", 3, func(executor *CompleteRouteExecutor) {
			executor.Handler = &spendingCompleteRouteHandler{calls: 3, err: errCollect}
		}, func(err error) bool { return errors.Is(err, errCollect) }, 3},
		{"configuration rejected before any request", 0, func(executor *CompleteRouteExecutor) { executor.Handler = nil },
			func(err error) bool { return errors.Is(err, ErrInvalidConfiguration) }, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			claim, session := completeRouteSession(t, now, false)
			replies := make([]scriptedReply, tc.calls)
			for index := range replies {
				replies[index] = scriptedReply{status: 200}
			}
			executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{
				calls: tc.calls, batch: completeRouteFixture(t, claim),
			}, &memoryEffectLedger{}, &memoryEffectSink{})
			executor.Doer = fakehttp.Client(&scriptedDoer{replies: replies})
			if tc.adjust != nil {
				tc.adjust(&executor)
			}
			result, err := executor.Execute(context.Background(), session, descriptor)
			if !tc.wantErr(err) {
				t.Fatalf("err=%v", err)
			}
			if got := ledgerTotal(result.RequestLedger); got != tc.want || result.RequestLedger.Dropped != 0 {
				t.Fatalf("request ledger = %+v, want %d requests", result.RequestLedger, tc.want)
			}
			for _, entry := range result.RequestLedger.Entries {
				if entry.RouteFamily != providerfoundation.LedgerUnclassified || entry.Dimension != "rest_core" {
					t.Fatalf("entry = %+v, want unclassified/rest_core", entry)
				}
			}
		})
	}
}

func TestExecuteReturnsTheRequestLedgerOfAFailedChunkedStreamInvocation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	_, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, _ := Descriptor("github", "cicd")
	errStream := errors.New("job fetch exhausted")
	executor := completeRouteExecutor(now, &spendingChunkRouteHandler{calls: 2, err: errStream},
		newChunkMemoryStore(), &memoryEffectSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.Doer = fakehttp.Client(&scriptedDoer{replies: []scriptedReply{{status: 200}, {err: errScriptedTransport}}})
	result, err := executor.Execute(context.Background(), session, descriptor)
	if !errors.Is(err, errStream) {
		t.Fatalf("err=%v", err)
	}
	if got := ledgerTotal(result.RequestLedger); got != 2 {
		t.Fatalf("request ledger = %+v, want the 2 requests the chunked invocation made", result.RequestLedger)
	}
}
