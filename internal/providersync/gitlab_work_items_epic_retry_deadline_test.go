package providersync

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// epicLimitDoer answers the group epics list with a 429 whose Retry-After is
// far longer than the context deadline; every other path goes to the fake.
type epicLimitDoer struct {
	inner *gitLabWorkItemsDoer
	limit bool
}

func (doer *epicLimitDoer) Do(request *http.Request) (*http.Response, error) {
	if doer.limit && request.URL.Path == "/api/v4/groups/acme/epics" {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests",
			Header: http.Header{"Retry-After": {"2"}},
			Body:   io.NopCloser(strings.NewReader(`{"message":"rate limit"}`)), Request: request,
		}, nil
	}
	return doer.inner.Do(request)
}

func epicRetryClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: 5 * time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func epicRetryCollect(t *testing.T, ctx context.Context, limit bool) CompleteRouteBatch {
	t.Helper()
	claim := nativeTestClaim("gitlab", "work-items")
	doer := &epicLimitDoer{inner: &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}, limit: limit}
	batch, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 3, MaxPages: 10, NestedMaxPages: 10}).Collect(
		ctx, claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID}, epicRetryClient(t, doer),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("Collect failed (limit=%v): %v", limit, err)
	}
	return batch
}

// A rate-limit wait longer than the deadline now reaches hold() with a live
// context: the epics part is incomplete and the watermark is held. The next
// run, with no rate limit, reaches the same final rows and advances.
func TestGitLabEpicsRateLimitNearTheDeadlineHoldsTheWatermarkAndTheNextRunCompletes(t *testing.T) {
	baseline := epicRetryCollect(t, context.Background(), false)
	if baseline.Watermark == nil {
		t.Fatal("baseline run did not advance the watermark")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	held := epicRetryCollect(t, ctx, true)
	if held.Watermark != nil {
		t.Fatalf("watermark = %v on a held run, want nil (not advanced)", held.Watermark)
	}
	incomplete, _ := held.Result["incomplete"].([]string)
	if !reflect.DeepEqual(incomplete, []string{"epics"}) {
		t.Fatalf("incomplete = %v, want [epics]", held.Result["incomplete"])
	}

	next := epicRetryCollect(t, context.Background(), false)
	if next.Watermark == nil || !next.Watermark.Equal(*baseline.Watermark) {
		t.Fatalf("next run watermark = %v, want %v", next.Watermark, baseline.Watermark)
	}
	if _, has := next.Result["incomplete"]; has {
		t.Fatalf("next run still incomplete: %v", next.Result["incomplete"])
	}
	if !reflect.DeepEqual(next.Effects, baseline.Effects) || !reflect.DeepEqual(next.Result["epics_synced"], baseline.Result["epics_synced"]) {
		t.Fatal("the next run's rows differ from a run that never saw the rate limit")
	}
}
