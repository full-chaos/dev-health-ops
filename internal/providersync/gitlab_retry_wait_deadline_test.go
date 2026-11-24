package providersync

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type retryDeadlineGate struct {
	mu    sync.Mutex
	until time.Time
}

func (g *retryDeadlineGate) Wait(context.Context) (time.Duration, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return max(time.Until(g.until), 0), nil
}

func (g *retryDeadlineGate) Penalize(_ context.Context, d time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.until = time.Now().Add(d)
	return nil
}

func retryDeadlineGitLabClient(t *testing.T, doer providerfoundation.HTTPDoer, gate providerfoundation.BackoffGate, wait time.Duration) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: wait, MaxWait: wait},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	client.Gate = gate
	return client
}

// A 503 whose retry wait is longer than the time left must stop the security
// route as the slept-out deadline did: an error and no watermark, never the
// empty successful batch the route gives an ordinary provider failure.
func TestGitLabSecurityRetryWaitPastTheDeadlineIsAnErrorWithoutWatermark(t *testing.T) {
	doer := &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()}
	doer.responses["/api/v4/projects/123/vulnerability_findings"] = gitLabSecurityHTTPResponse{
		status: http.StatusServiceUnavailable, body: `{"message":"down"}`,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	batch, err := (GitLabSecurityRouteHandler{}).Collect(
		ctx, nativeTestClaim("gitlab", "security"), providerfoundation.Credential{},
		retryDeadlineGitLabClient(t, doer, nil, 25*time.Second), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &providerErr) ||
		providerErr.Class != providerfoundation.ErrorTransient || batch.Watermark != nil || len(batch.Effects) != 0 {
		t.Fatalf("err=%v watermark=%v effects=%d, want a transient deadline error, no watermark, no effects", err, batch.Watermark, len(batch.Effects))
	}
}

// Named limit: with the shared gate wired as in production, an epic rate limit
// whose cooldown outlasts the deadline arms the gate, so the required issue
// fetch is refused and Collect returns the rate_limited error. The watermark
// is held because the unit errors; the held partial result is not kept.
func TestGitLabEpicRateLimitUnderTheSharedGateErrorsAndHoldsTheWatermark(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	doer := &epicLimitDoer{inner: &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}, limit: true}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	batch, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 3, MaxPages: 10, NestedMaxPages: 10}).Collect(
		ctx, claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		retryDeadlineGitLabClient(t, doer, &retryDeadlineGate{}, 5*time.Millisecond), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited || batch.Watermark != nil {
		t.Fatalf("err=%v watermark=%v, want rate_limited and no watermark", err, batch.Watermark)
	}
}
