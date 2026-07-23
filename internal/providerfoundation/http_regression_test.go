package providerfoundation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestHTTPClientPreservesSuccessfulResponseBody(t *testing.T) {
	t.Parallel()
	const payload = `{"items":[{"id":"provider-1"}]}`
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusOK, nil, payload), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})

	response, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != payload {
		t.Fatalf("body=%q, want %q", body, payload)
	}
}

func TestHTTPClientClampsIntegerAndDateRetryAfter(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 2, InitialWait: time.Millisecond, MaxWait: 25 * time.Millisecond}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unexpected request")
		return nil, nil
	}), policy)
	now := time.Now()
	for name, raw := range map[string]string{
		"integer": "3600",
		"date":    now.Add(time.Hour).UTC().Format(http.TimeFormat),
	} {
		t.Run(name, func(t *testing.T) {
			parsed := ParseRetryAfter(raw, now)
			if parsed <= policy.MaxWait {
				t.Fatalf("fixture did not exceed max wait: %s", parsed)
			}
			if got := client.retryDelay(1, parsed); got != policy.MaxWait {
				t.Fatalf("retry delay=%s, want %s", got, policy.MaxWait)
			}
		})
	}
}

func TestHTTPClientPenalizesSharedGateWithLocalBackoff(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 1, InitialWait: 20 * time.Millisecond, MaxWait: 25 * time.Millisecond}
	gate := &recordingBackoffGate{}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusTooManyRequests, nil, `{"error":"limited"}`), nil
	}), policy)
	client.Gate = gate

	_, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited {
		t.Fatalf("error=%v", err)
	}
	if gate.penalty < policy.InitialWait || gate.penalty > policy.MaxWait {
		t.Fatalf("shared gate penalty=%s, want bounded local wait in [%s,%s]", gate.penalty, policy.InitialWait, policy.MaxWait)
	}
	if got, want := (ValkeyBackoffGate{Provider: "github"}).key(), "rate_limit:github:_:_"; got != want {
		t.Fatalf("shared key=%q, want %q", got, want)
	}
}

func TestHTTPClientBoundsAndObservesReservationRelease(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	reservation := &blockingReservation{}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Budget = staticBudgetStore{reservation: reservation}
	client.BudgetKey = BudgetKey{Provider: "github", OrgID: "org", CostClass: "rest", Limit: 1, TTL: time.Minute}
	client.Metrics = metrics
	client.ReservationReleaseTimeout = 10 * time.Millisecond

	started := time.Now()
	response, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	if !errors.Is(err, ErrBudgetUnavailable) || response != nil {
		t.Fatalf("response=%v error=%v", response, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("release exceeded bounded cleanup budget: %s", elapsed)
	}
	if !reservation.deadlineSeen {
		t.Fatal("release context did not carry a deadline")
	}
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `dev_health_provider_budget_release_errors_total{provider="github"} 1`) {
		t.Fatalf("missing release failure metric:\n%s", output.String())
	}
}

func TestHTTPClientReleaseSurvivesRequestCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	reservation := &contextCheckingReservation{}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		cancel()
		return testHTTPResponse(request, http.StatusOK, nil, `{}`), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Budget = staticBudgetStore{reservation: reservation}
	client.BudgetKey = BudgetKey{Provider: "github", OrgID: "org", CostClass: "rest", Limit: 1, TTL: time.Minute}
	client.ReservationReleaseTimeout = time.Second

	response, err := client.Do(ctx, http.MethodGet, "/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if !reservation.called || reservation.contextErr != nil {
		t.Fatalf("release called=%t context error=%v", reservation.called, reservation.contextErr)
	}
}

func TestHTTPClientPreservesRequestAndReleaseFailures(t *testing.T) {
	t.Parallel()
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusUnauthorized, nil, `{}`), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Budget = staticBudgetStore{reservation: failingReservation{}}
	client.BudgetKey = BudgetKey{Provider: "github", OrgID: "org", CostClass: "rest", Limit: 1, TTL: time.Minute}

	_, err := client.Do(context.Background(), http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorAuthentication {
		t.Fatalf("request failure missing from joined error: %v", err)
	}
	if !errors.Is(err, ErrBudgetUnavailable) {
		t.Fatalf("release failure missing from joined error: %v", err)
	}
}

func newTestHTTPClient(t *testing.T, doer HTTPDoer, retry RetryPolicy) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient(
		"github",
		"https://api.github.test",
		doer,
		TokenAuth("Authorization", "Bearer ", secrets.NewValue("token")),
		retry,
		LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testHTTPResponse(request *http.Request, status int, headers http.Header, body string) *http.Response {
	if headers == nil {
		headers = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

type HTTPDoerFunc func(*http.Request) (*http.Response, error)

func (f HTTPDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

type recordingBackoffGate struct {
	penalty time.Duration
}

func (*recordingBackoffGate) Wait(context.Context) (time.Duration, error) {
	return 0, nil
}

func (g *recordingBackoffGate) Penalize(_ context.Context, delay time.Duration) error {
	g.penalty = delay
	return nil
}

type staticBudgetStore struct {
	reservation Reservation
	err         error
}

func (s staticBudgetStore) Acquire(context.Context, BudgetKey) (Reservation, error) {
	return s.reservation, s.err
}

type blockingReservation struct {
	deadlineSeen bool
}

func (r *blockingReservation) Release(ctx context.Context) error {
	_, r.deadlineSeen = ctx.Deadline()
	<-ctx.Done()
	return ctx.Err()
}

type contextCheckingReservation struct {
	called     bool
	contextErr error
}

type failingReservation struct{}

func (failingReservation) Release(context.Context) error {
	return ErrBudgetUnavailable
}

func (r *contextCheckingReservation) Release(ctx context.Context) error {
	r.called = true
	r.contextErr = ctx.Err()
	return nil
}
