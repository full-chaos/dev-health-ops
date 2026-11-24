package providerunit

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-14546, end to end. The rate-limited answer comes from a REAL
// HTTPClient built by the production executor (production retry policy),
// not from an error injected through BuildExecutor. Its retry wait
// (Retry-After 120 s) does not fit the ~300 ms left on the context, so the
// client returns at once with the context still live. The class and
// RetryAfter must survive executor -> route -> Handler.Work, which defers
// without consuming the attempt.

const executorRetryAfter = 120 * time.Second

// retryDeadlineDoer answers the repository read and limits every other
// request with a 429 and Retry-After 120.
type retryDeadlineDoer struct {
	mu       sync.Mutex
	limited  int
	requests []string
}

func (doer *retryDeadlineDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	doer.requests = append(doer.requests, request.URL.Path)
	if request.URL.Path == "/repos/acme/api" {
		return jsonResponse(request, http.StatusOK,
			strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)), nil
	}
	doer.limited++
	response := jsonResponse(request, http.StatusTooManyRequests, strings.NewReader(`{"message":"limited"}`))
	response.Header.Set("Retry-After", "120")
	return response, nil
}

// recordingGate keeps the penalties the client arms, as the production
// Valkey gate does on every rate-limited answer.
type recordingGate struct {
	mu        sync.Mutex
	penalties []time.Duration
}

func (*recordingGate) Wait(context.Context) (time.Duration, error) { return 0, nil }

func (gate *recordingGate) Penalize(_ context.Context, penalty time.Duration) error {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.penalties = append(gate.penalties, penalty)
	return nil
}

func retryDeadlineExecutor(
	now time.Time, handler providersync.CompleteRouteHandler, doer providerfoundation.HTTPDoer, gate *recordingGate,
) ExecutorFactory {
	return func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
		return providersync.CompleteRouteExecutor{
			Credentials: providerfoundation.CredentialResolver{
				Repository: githubCredentialRepository{unit: session.Claim.Unit},
				Decryptor:  githubCredentialDecryptor{},
			},
			Doer: fakehttp.Client(doer),
			// The production policy (3 attempts): the 429 is retryable, so
			// only the deadline check can end the attempt early.
			Retry:        providerfoundation.DefaultRetryPolicy(),
			Budget:       testBudgetStore{},
			BudgetLimits: map[providersync.CostClass]int{providersync.CostMedium: 1, providersync.CostHeavy: 1},
			BudgetTTL:    time.Minute,
			Gate: func(providersync.Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
				return gate
			},
			Handler:           handler,
			Comparator:        providersync.ProductionContractComparator{},
			Committer:         providersync.EffectCommitter{Ledger: &testEffectLedger{}, Sink: testEffectSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now }},
			HeartbeatInterval: 10 * time.Second,
			Now:               func() time.Time { return now },
		}, nil
	}
}

func assertRetryWaitPastDeadlineDefersThroughTheExecutor(
	t *testing.T, unit providersync.Unit, route providersync.CompleteRouteHandler, wantChunked bool,
) {
	t.Helper()
	descriptor, ok := providersync.Descriptor(unit.Provider, unit.Dataset)
	if !ok || !descriptor.RouteReady || !descriptor.Plannable || descriptor.Chunked != wantChunked {
		t.Fatalf("%s/%s descriptor present=%v ready=%v plannable=%v chunked=%v, want chunked=%v",
			unit.Provider, unit.Dataset, ok, descriptor.RouteReady, descriptor.Plannable, descriptor.Chunked, wantChunked)
	}
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(unit)
	doer := &retryDeadlineDoer{}
	gate := &recordingGate{}
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: retryDeadlineExecutor(now, route, doer, gate),
	}
	execution := providerExecution(unit, now, 1)
	execution.Definition.MaxAttempts = 5
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := handler.Work(ctx, execution)

	if ctx.Err() != nil {
		t.Fatalf("the work context died before Work returned (%v): the client slept the wait out", ctx.Err())
	}
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed {
		t.Fatalf("Work() = %v; want an attempt-neutral snooze", err)
	}
	if delay < executorRetryAfter || delay >= executorRetryAfter+rateLimitMaxJitter {
		t.Fatalf("snooze=%s, want [%s, %s)", delay, executorRetryAfter, executorRetryAfter+rateLimitMaxJitter)
	}
	if retryAfter, rateLimited := providerRateLimitDelay(err); !rateLimited || retryAfter != executorRetryAfter {
		t.Fatalf("Work() error class rate_limited=%v RetryAfter=%s, want true and %s", rateLimited, retryAfter, executorRetryAfter)
	}
	if repository.rateLimitDeferrals != 1 || repository.releaseCalls != 0 || repository.failures != 0 {
		t.Fatalf("deferrals=%d releases=%d failures=%d, want 1 deferral and no attempt used",
			repository.rateLimitDeferrals, repository.releaseCalls, repository.failures)
	}
	if len(repository.rateLimitObservations) != 1 || repository.rateLimitObservations[0].RetryAfter != executorRetryAfter {
		t.Fatalf("rate-limit observations=%+v, want one with RetryAfter %s", repository.rateLimitObservations, executorRetryAfter)
	}
	if !repository.availableAt.Equal(now.Add(executorRetryAfter)) {
		t.Fatalf("not-before fence=%s, want %s", repository.availableAt, now.Add(executorRetryAfter))
	}
	// One 429 and no second try: the wait did not fit, so the client ended
	// the attempt at once instead of retrying.
	if doer.limited != 1 {
		t.Fatalf("rate-limited requests=%d (%v), want 1", doer.limited, doer.requests)
	}
	if len(gate.penalties) != 1 || gate.penalties[0] != executorRetryAfter {
		t.Fatalf("gate penalties=%v, want [%s]", gate.penalties, executorRetryAfter)
	}
}

// Chunked route: github/files (CollectChunks via the chunked executor). The
// 429 is on the branch read, after the repository read succeeded.
func TestRetryWaitPastTheDeadlineThroughTheChunkedExecutorDefersWithoutConsumingTheAttempt(t *testing.T) {
	t.Parallel()
	assertRetryWaitPastDeadlineDefersThroughTheExecutor(t, githubFilesUnit(), providersync.GitHubFilesRouteHandler{}, true)
}

// Non-chunked route: github/blame (one-batch Collect).
func TestRetryWaitPastTheDeadlineThroughTheOneBatchExecutorDefersWithoutConsumingTheAttempt(t *testing.T) {
	t.Parallel()
	assertRetryWaitPastDeadlineDefersThroughTheExecutor(t, githubBlameUnit(),
		providersync.GitHubBlameRouteHandler{Coverage: emptyBlameCoverage{}, MaxFiles: 2}, false)
}
