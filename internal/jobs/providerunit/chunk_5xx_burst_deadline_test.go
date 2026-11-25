package providerunit

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// gatewayDeadlineDoer answers the repository read and every other request with
// a gateway status and Retry-After 120, a wait that does not fit the time left.
type gatewayDeadlineDoer struct {
	mu       sync.Mutex
	status   int
	answered int
}

func (doer *gatewayDeadlineDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	if request.URL.Path == "/repos/acme/api" {
		return jsonResponse(request, http.StatusOK,
			strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)), nil
	}
	doer.answered++
	response := jsonResponse(request, doer.status, strings.NewReader(`{"message":"unavailable"}`))
	response.Header.Set("Retry-After", "120")
	return response, nil
}

func workGatewayAnswerPastTheDeadline(
	t *testing.T, unit providersync.Unit, route providersync.CompleteRouteHandler, status int,
) (error, *memoryUnitRepository, *gatewayDeadlineDoer, context.Context) {
	t.Helper()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(unit)
	doer := &gatewayDeadlineDoer{status: status}
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: retryDeadlineExecutor(now, route, doer, &recordingGate{}),
	}
	execution := providerExecution(unit, now, 1)
	execution.Definition.MaxAttempts = 5
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	t.Cleanup(cancel)
	return handler.Work(ctx, execution), repository, doer, ctx
}

// A gateway answer whose Retry-After wait does not fit the time left must
// still reach the gateway arm: the client ends the try at once with the class
// kept beside the deadline error, so the unit defers and keeps its checkpoint
// instead of ending as a time-limit failure (CHAOS-14696, the common outage
// shape: the provider answers 503 with a long Retry-After).
func TestChunkedUnitGatewayAnswerWithAWaitPastTheDeadlineStillDefers(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			err, repository, doer, ctx := workGatewayAnswerPastTheDeadline(t, githubFilesUnit(), providersync.GitHubFilesRouteHandler{}, status)
			if ctx.Err() != nil {
				t.Fatalf("the work context died before Work returned (%v): the client slept the wait out", ctx.Err())
			}
			if _, snoozed := jobruntime.SnoozeDelay(err); !snoozed {
				t.Fatalf("Work() = %v; want an attempt-neutral snooze", err)
			}
			if repository.rateLimitDeferrals != 1 || repository.releaseCalls != 0 || repository.failures != 0 || repository.status != "dispatching" {
				t.Fatalf("deferrals=%d releases=%d failures=%d status=%q, want 1 deferral, no attempt used, dispatching",
					repository.rateLimitDeferrals, repository.releaseCalls, repository.failures, repository.status)
			}
			if len(repository.rateLimitObservations) != 1 || repository.rateLimitObservations[0].Dimension != "" {
				t.Fatalf("observations=%+v, want one empty observation (no cooldown feed)", repository.rateLimitObservations)
			}
			if doer.answered != 1 {
				t.Fatalf("gateway answers=%d, want 1 (the wait did not fit, so no second try)", doer.answered)
			}
		})
	}
}

// CONTROL (passes today): an unchunked route keeps the slept-out wait and its
// plain deadline error for a 503, so no other caller's fallback changes.
func TestUnchunkedUnitGatewayAnswerWithAWaitPastTheDeadlineKeepsTheOrdinaryRetry(t *testing.T) {
	t.Parallel()
	err, repository, _, _ := workGatewayAnswerPastTheDeadline(t, githubBlameUnit(),
		providersync.GitHubBlameRouteHandler{Coverage: emptyBlameCoverage{}, MaxFiles: 2}, http.StatusServiceUnavailable)
	if _, snoozed := jobruntime.SnoozeDelay(err); snoozed || repository.rateLimitDeferrals != 0 {
		t.Fatalf("Work() = %v deferrals=%d; an unchunked unit must not defer", err, repository.rateLimitDeferrals)
	}
}
