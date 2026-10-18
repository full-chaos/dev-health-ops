package providerfoundation

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func ledgerClient(t *testing.T, ledger *RequestLedger, statuses ...int) *HTTPClient {
	t.Helper()
	call := 0
	policy := RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusOK
		if call < len(statuses) {
			status = statuses[call]
		}
		call++
		return testHTTPResponse(request, status, http.Header{}, `{}`), nil
	})), policy)
	client.Ledger = ledger
	return client
}

func entriesOf(ledger *RequestLedger) map[LedgerKey]int {
	entries, _ := ledger.Snapshot()
	out := map[LedgerKey]int{}
	for _, entry := range entries {
		out[entry.LedgerKey] = entry.Requests
	}
	return out
}

func TestEveryWireAttemptIsCountedUnderItsFamilyAndDimension(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	// Two attempts on one call: a 503, then success. A retry is a wire request.
	client := ledgerClient(t, ledger, http.StatusServiceUnavailable, http.StatusOK)
	ctx := WithRequestAccount(context.Background(), "prs", "")
	response, err := client.Do(ctx, http.MethodGet, "/repos/acme/api/pulls", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	got := entriesOf(ledger)
	if got[LedgerKey{"prs", "rest_core"}] != 2 || len(got) != 1 {
		t.Fatalf("ledger = %v, want 2 requests under prs/rest_core", got)
	}
}

func TestTheDimensionComesFromTheWireAndAnOverrideWins(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	client := ledgerClient(t, ledger)
	do := func(ctx context.Context, path string) {
		t.Helper()
		response, err := client.Do(ctx, http.MethodPost, path, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	do(context.Background(), "/graphql")
	do(context.Background(), "/search/issues")
	do(WithBudgetDimension(context.Background(), "contents_blob"), "/graphql")
	do(WithRequestAccount(context.Background(), "files", "contents_blob"), "/repos/acme/api/git/trees/main")
	got := entriesOf(ledger)
	want := map[LedgerKey]int{
		{LedgerUnclassified, "graphql_cost"}: 1, {LedgerUnclassified, "contents_blob"}: 1,
		{LedgerUnclassified, "rest_core"}: 0, {"files", "contents_blob"}: 1,
	}
	delete(want, LedgerKey{LedgerUnclassified, "rest_core"})
	want[LedgerKey{LedgerUnclassified, "search"}] = 1
	if len(got) != len(want) {
		t.Fatalf("ledger = %v, want %v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("ledger = %v, want %v", got, want)
		}
	}
}

func TestUnauthenticatedRequestsAreCountedToo(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	client := ledgerClient(t, ledger)
	response, err := client.DoUnauthenticated(WithRequestAccount(context.Background(), "tests", ""), http.MethodGet, "https://storage.example/blob")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := entriesOf(ledger); got[LedgerKey{"tests", "rest_core"}] != 1 {
		t.Fatalf("ledger = %v, want the artifact download counted", got)
	}
}

func TestTheLedgerKeepsAtMostFiftyKeysAndSaysWhatItDropped(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	for index := 0; index < 60; index++ {
		ledger.Record(LedgerKey{RouteFamily: "family" + string(rune('A'+index/26)) + string(rune('a'+index%26)), Dimension: "rest_core"})
	}
	ledger.Record(LedgerKey{RouteFamily: "familyAa", Dimension: "rest_core"}) // an existing key still counts
	entries, dropped := ledger.Snapshot()
	if len(entries) != 50 || dropped != 10 {
		t.Fatalf("entries = %d dropped = %d, want 50 keys and 10 dropped requests", len(entries), dropped)
	}
	if entriesOf(ledger)[LedgerKey{"familyAa", "rest_core"}] != 2 {
		t.Fatalf("an existing key stopped counting once the cap was reached")
	}
}

func TestANilLedgerCountsNothingAndTheLedgerIsConcurrencySafe(t *testing.T) {
	t.Parallel()
	var none *RequestLedger
	none.Record(LedgerKey{"x", "rest_core"})
	if entries, dropped := none.Snapshot(); entries != nil || dropped != 0 {
		t.Fatalf("a nil ledger reported %v %d", entries, dropped)
	}
	ledger := NewRequestLedger()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := 0; index < 100; index++ {
				ledger.Record(LedgerKey{"prs", "rest_core"})
			}
		}()
	}
	wg.Wait()
	if got := entriesOf(ledger)[LedgerKey{"prs", "rest_core"}]; got != 800 {
		t.Fatalf("counted %d, want 800", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// redirectingClient follows redirects like a stock http.Client: /old answers a
// 302 to location, any other path answers 200. hits counts the requests that
// reached the wire.
func redirectingClient(t *testing.T, ledger *RequestLedger, location string, hits *int) *HTTPClient {
	t.Helper()
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		*hits++
		if r.URL.Path == "/old" {
			return testHTTPResponse(r, http.StatusFound, http.Header{"Location": {location}}, ``), nil
		}
		return testHTTPResponse(r, http.StatusOK, http.Header{}, `{}`), nil
	})}
	client := newTestHTTPClient(t, hc, RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Ledger = ledger
	return client
}

func TestAFollowedRedirectHopIsCountedAsAWireRequest(t *testing.T) {
	t.Parallel()
	ctx := WithRequestAccount(context.Background(), "prs", "")
	for name, call := range map[string]func(*HTTPClient) (*http.Response, error){
		"authenticated": func(c *HTTPClient) (*http.Response, error) { return c.Do(ctx, http.MethodGet, "/old", nil) },
		"unauthenticated": func(c *HTTPClient) (*http.Response, error) {
			return c.DoUnauthenticated(ctx, http.MethodGet, "https://api.github.test/old")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ledger, hits := NewRequestLedger(), 0
			client := redirectingClient(t, ledger, "https://api.github.test/next", &hits)
			response, err := call(client)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			got := entriesOf(ledger)
			if hits != 2 || got[LedgerKey{"prs", "rest_core"}] != 2 || len(got) != 1 {
				t.Fatalf("wire requests = %d, ledger = %v, want both requests of the redirect under prs/rest_core", hits, got)
			}
		})
	}
}

func TestARefusedRedirectHopIsNotCounted(t *testing.T) {
	t.Parallel()
	ledger, hits := NewRequestLedger(), 0
	client := redirectingClient(t, ledger, "https://elsewhere.test/x", &hits)
	ctx := WithRequestAccount(context.Background(), "prs", "")
	if response, err := client.Do(ctx, http.MethodGet, "/old", nil); err == nil {
		_ = response.Body.Close()
		t.Fatal("a redirect off the credential origin was followed")
	}
	got := entriesOf(ledger)
	if hits != 1 || got[LedgerKey{"prs", "rest_core"}] != 1 || len(got) != 1 {
		t.Fatalf("wire requests = %d, ledger = %v, want the one request that was sent", hits, got)
	}
}

func TestAZeroValueLedgerCounts(t *testing.T) {
	t.Parallel()
	ledger := &RequestLedger{}
	ledger.Record(LedgerKey{"prs", "rest_core"})
	ledger.Record(LedgerKey{"prs", "rest_core"})
	if got := entriesOf(ledger); got[LedgerKey{"prs", "rest_core"}] != 2 {
		t.Fatalf("ledger = %v, want 2", got)
	}
}

// contextDroppingDoer sends each request on a context that carries none of the
// caller's values, as a decorator that rebuilds the request would.
type contextDroppingDoer struct{ inner HTTPDoer }

func (d contextDroppingDoer) Do(r *http.Request) (*http.Response, error) {
	return d.inner.Do(r.WithContext(context.Background()))
}
func (d contextDroppingDoer) Unwrap() interface {
	Do(*http.Request) (*http.Response, error)
} {
	return d.inner
}
func (d contextDroppingDoer) Rewrap(inner interface {
	Do(*http.Request) (*http.Response, error)
}) interface {
	Do(*http.Request) (*http.Response, error)
} {
	return contextDroppingDoer{inner: inner}
}

func TestAFollowedRedirectHopIsCountedWhateverTheDecoratorsDoWithTheContext(t *testing.T) {
	t.Parallel()
	hits := 0
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		hits++
		if r.URL.Path == "/old" {
			return testHTTPResponse(r, http.StatusFound, http.Header{"Location": {"https://api.github.test/next"}}, ``), nil
		}
		return testHTTPResponse(r, http.StatusOK, http.Header{}, `{}`), nil
	})}
	build := func(doer HTTPDoer) *HTTPClient {
		client, err := NewHTTPClient("github", "https://api.github.test", doer,
			TokenAuth("Authorization", "Bearer ", secrets.NewValue("token")),
			RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
			LeaseGuardFunc(func(context.Context) error { return nil }))
		if err != nil {
			t.Fatal(err)
		}
		client.Ledger = NewRequestLedger()
		return client
	}
	ctx := WithRequestAccount(context.Background(), "prs", "")
	for name, client := range map[string]*HTTPClient{
		"a decorator that drops the context": build(contextDroppingDoer{inner: hc}),
		"a client built without NewHTTPClient": func() *HTTPClient {
			client := build(hc)
			client.Doer = hc
			return client
		}(),
	} {
		hits = 0
		response, err := client.Do(ctx, http.MethodGet, "/old", nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_ = response.Body.Close()
		if got := entriesOf(client.Ledger); hits != 2 || got[LedgerKey{"prs", "rest_core"}] != 2 || len(got) != 1 {
			t.Fatalf("%s: wire requests = %d, ledger = %v, want both requests under prs/rest_core", name, hits, got)
		}
	}
}

// opaqueCountingDoer is a route's counting decorator: it wraps the constructed
// client's Doer and is not an httpguard.Wrapper, so the per-call guard cannot
// reach through it.
type opaqueCountingDoer struct {
	delegate HTTPDoer
	attempts *int
}

func (d opaqueCountingDoer) Do(r *http.Request) (*http.Response, error) {
	*d.attempts++
	return d.delegate.Do(r)
}

func TestAFollowedRedirectHopIsCountedOnceBehindAnOpaqueRouteDecorator(t *testing.T) {
	t.Parallel()
	hits := 0
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		hits++
		if r.URL.Path == "/old" {
			return testHTTPResponse(r, http.StatusFound, http.Header{"Location": {"https://api.github.test/next"}}, ``), nil
		}
		return testHTTPResponse(r, http.StatusOK, http.Header{}, `{}`), nil
	})}
	client, err := NewHTTPClient("github", "https://api.github.test", hc,
		TokenAuth("Authorization", "Bearer ", secrets.NewValue("token")),
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	client.Ledger = NewRequestLedger()
	attempts := 0
	counted := *client
	counted.Doer = opaqueCountingDoer{delegate: client.Doer, attempts: &attempts}
	response, err := counted.Do(WithRequestAccount(context.Background(), "merge_requests", ""), http.MethodGet, "/old", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := entriesOf(client.Ledger); hits != 2 || got[LedgerKey{"merge_requests", "rest_core"}] != 2 || len(got) != 1 {
		t.Fatalf("wire requests = %d, ledger = %v, want both requests counted once under merge_requests/rest_core", hits, got)
	}
}

func TestAFamilyLabelKeepsTheDimensionAnOuterAccountSet(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	client := ledgerClient(t, ledger)
	ctx := WithRequestFamily(WithRequestAccount(context.Background(), "files", "contents_blob"), "issues")
	response, err := client.Do(ctx, http.MethodPost, "/graphql", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := entriesOf(ledger); got[LedgerKey{"issues", "contents_blob"}] != 1 || len(got) != 1 {
		t.Fatalf("ledger = %v, want issues/contents_blob", got)
	}
}
