package providerunit

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type retryDoerFunc func(*http.Request) (*http.Response, error)

func (f retryDoerFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

// retryWaitPastDeadlineFailure is the error a real client returns when the
// retry wait does not fit the time left.
func retryWaitPastDeadlineFailure(t *testing.T, status int, headers http.Header, wait time.Duration) error {
	t.Helper()
	doer := retryDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status, Header: headers, Request: request,
			Body: io.NopCloser(strings.NewReader(`{"message":"limited"}`)),
		}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.test", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: wait, MaxWait: wait},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err = client.Do(ctx, http.MethodGet, "/items", nil)
	if err == nil {
		t.Fatal("client.Do returned no error")
	}
	return err
}

func workWithFailure(t *testing.T, failure error) (error, *memoryUnitRepository) {
	t.Helper()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, failure
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5
	return handler.Work(context.Background(), execution), repository
}

// A rate-limited retry whose wait is longer than the time left defers without
// consuming the attempt (CHAOS-14546). At base the client slept to the
// deadline and returned a deadline error, which burned an attempt.
func TestRetryWaitPastTheDeadlineRateLimitDefersWithoutConsumingTheAttempt(t *testing.T) {
	t.Parallel()
	failure := retryWaitPastDeadlineFailure(t, http.StatusTooManyRequests, http.Header{"Retry-After": {"120"}}, 5*time.Millisecond)
	err, repository := workWithFailure(t, failure)
	if _, snoozed := jobruntime.SnoozeDelay(err); !snoozed {
		t.Fatalf("Work() = %v; want an attempt-neutral snooze", err)
	}
	if repository.rateLimitDeferrals != 1 || repository.releaseCalls != 0 || repository.failures != 0 {
		t.Fatalf("deferrals=%d releases=%d failures=%d, want 1 deferral and no attempt used",
			repository.rateLimitDeferrals, repository.releaseCalls, repository.failures)
	}
}

// A transient answer whose retry wait is longer than the time left keeps the
// ordinary retry (the work context is still live), not a rate-limit deferral.
func TestRetryWaitPastTheDeadlineTransientKeepsTheOrdinaryRetry(t *testing.T) {
	t.Parallel()
	failure := retryWaitPastDeadlineFailure(t, http.StatusServiceUnavailable, nil, 5*time.Second)
	err, repository := workWithFailure(t, failure)
	if repository.rateLimitDeferrals != 0 {
		t.Fatalf("transient answer was deferred as a rate limit: %d", repository.rateLimitDeferrals)
	}
	if _, snoozed := jobruntime.SnoozeDelay(err); snoozed || err == nil {
		t.Fatalf("Work() = %v, want a plain retryable failure, not a snooze", err)
	}
	if repository.failures != 1 || repository.status != "failed" {
		t.Fatalf("failures=%d status=%q, want one recorded failure attempt (pinned: the work context is still live)", repository.failures, repository.status)
	}
}
