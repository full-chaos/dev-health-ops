package providerfoundation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/spantest"
)

const (
	spanTestRequestName = "dev_health.provider.request"
	spanTestSecret      = "SECRETVALUE"
	spanTestOwner       = "acme-owner"
	spanTestRepo        = "widget-repo"
	spanTestArtifactID  = "4242"
	spanTestZipTemplate = "/repos/{owner}/{repo}/actions/artifacts/{artifact}/zip"
	spanTestFamily      = "tests_artifacts"
)

// spanTestAllowList is the whole key set of a provider request span: http.response.status_code only when a status
// arrived, error.type only on a failed call.
var spanTestAllowList = []string{
	"dev_health.provider.attempts", "dev_health.provider.budget_ms", "dev_health.provider.gate_wait_ms",
	"dev_health.provider.lease_ms", "dev_health.provider.target", "error.type", "family", "http.request.method",
	"http.response.status_code", "http.route", "outcome", "provider",
}

// The closed vocabulary of every string value of the span.
var (
	spanTestTargets    = []string{"api", "redirect"}
	spanTestOutcomes   = []string{"response", "failed", "budget_contended", "cancelled"}
	spanTestErrorTypes = []string{"authentication", "not_found", "conflict", "rate_limited", "transient", "cancelled", "permanent", "other"}
)

func spanTestWantKeys(status, failed bool) []string {
	var keys []string
	for _, key := range spanTestAllowList {
		if (key == "error.type" && !failed) || (key == "http.response.status_code" && !status) {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func spanTestString(t *testing.T, pair spantest.Pair, key string) string {
	t.Helper()
	value, ok := spantest.Value(pair.Raw.Attributes(), key)
	if !ok {
		t.Fatalf("%s has no %s", pair.Raw.Name(), key)
	}
	return value.AsString()
}

func spanTestInt(t *testing.T, pair spantest.Pair, key string) int64 {
	t.Helper()
	value, ok := spantest.Value(pair.Raw.Attributes(), key)
	if !ok {
		t.Fatalf("%s has no %s", pair.Raw.Name(), key)
	}
	return value.AsInt64()
}

// requireRequestEnums checks every string value of a request span against its closed source.
func requireRequestEnums(t *testing.T, pair spantest.Pair, provider, route, family string) {
	t.Helper()
	for key, allowed := range map[string][]string{
		"dev_health.provider.target": spanTestTargets, "outcome": spanTestOutcomes,
		"provider": {provider}, "http.route": {route}, "family": {family}, "http.request.method": {http.MethodGet},
	} {
		if got := spanTestString(t, pair, key); !slices.Contains(allowed, got) {
			t.Errorf("%s = %q, want one of %v", key, got, allowed)
		}
	}
	if value, ok := spantest.Value(pair.Raw.Attributes(), "error.type"); ok && !slices.Contains(spanTestErrorTypes, value.AsString()) {
		t.Errorf("error.type = %q, want one of %v", value.AsString(), spanTestErrorTypes)
	}
}

func noFollowClient() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func spanTestClientFor(t *testing.T, provider, base string, doer HTTPDoer, retry RetryPolicy) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient(provider, base, doer,
		func(request *http.Request) error {
			request.Header.Set("Authorization", "Bearer synthetic-test-value")
			return nil
		},
		retry, LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func spanTestClient(t *testing.T, base string, doer HTTPDoer, retry RetryPolicy) *HTTPClient {
	t.Helper()
	return spanTestClientFor(t, "github", base, doer, retry)
}

func oneAttempt() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}
}

func closeBody(response *http.Response) {
	if response != nil && response.Body != nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
}

// C1: the API request that answers 302 and the unauthenticated request to the signed blob URL give one span each. Raw
// and exported, the key set is the allow-list exactly, both legs carry the artifact zip route template, and no text names
// the signature, the hosts, the owner, the repository or the artifact.
func TestProviderRequestSpansOfAnArtifactRedirectCarryOnlyTheAllowList(t *testing.T) {
	pipeline := spantest.Install(t)
	blob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "PK-synthetic-archive")
	}))
	t.Cleanup(blob.Close)
	location := blob.URL + "/containers/" + spanTestOwner + "/" + spanTestRepo + "/artifact.zip?sig=" + spanTestSecret + "&se=2026-01-01"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, location, http.StatusFound)
	}))
	t.Cleanup(api.Close)
	client := spanTestClient(t, api.URL, noFollowClient(), oneAttempt())
	ctx := WithRequestAccount(pipeline.Parent, spanTestFamily, "")

	response, err := client.DoRoute(ctx, http.MethodGet, spanTestZipTemplate,
		"/repos/"+spanTestOwner+"/"+spanTestRepo+"/actions/artifacts/"+spanTestArtifactID+"/zip", nil)
	if err != nil {
		t.Fatalf("api call: %v", err)
	}
	gotLocation := response.Header.Get("Location")
	closeBody(response)
	if response.StatusCode != http.StatusFound || gotLocation != location {
		t.Fatalf("api status=%d location match=%v, want 302 and the blob URL", response.StatusCode, gotLocation == location)
	}
	response, err = client.DoUnauthenticatedRoute(ctx, http.MethodGet, spanTestZipTemplate, gotLocation)
	if err != nil {
		t.Fatalf("blob call: %v", err)
	}
	closeBody(response)

	pairs := pipeline.Pairs(spanTestRequestName)
	if len(pairs) != 2 {
		t.Fatalf("request spans = %d, want 2 (api + redirect)", len(pairs))
	}
	apiURL, _ := url.Parse(api.URL)
	blobURL, _ := url.Parse(blob.URL)
	for index, want := range []struct {
		target string
		status int64
	}{{"api", http.StatusFound}, {"redirect", http.StatusOK}} {
		pair := pairs[index]
		spantest.Check(t, pair, spanTestWantKeys(true, false), spanTestSecret, "sig", apiURL.Host, blobURL.Host, "127.0.0.1",
			spanTestOwner, spanTestRepo, spanTestArtifactID, "containers", "artifact.zip", "Bearer", "synthetic-test-value", "Location")
		requireRequestEnums(t, pair, "github", spanTestZipTemplate, spanTestFamily)
		if got := spanTestString(t, pair, "dev_health.provider.target"); got != want.target {
			t.Errorf("span %d target = %q, want %q", index, got, want.target)
		}
		if got := spanTestString(t, pair, "outcome"); got != "response" {
			t.Errorf("span %d outcome = %q, want response", index, got)
		}
		if got := spanTestInt(t, pair, "http.response.status_code"); got != want.status {
			t.Errorf("span %d status = %d, want %d", index, got, want.status)
		}
		if got := spanTestInt(t, pair, "dev_health.provider.attempts"); got != 1 {
			t.Errorf("span %d attempts = %d, want 1", index, got)
		}
		if pair.Raw.Status().Code == codes.Error {
			t.Errorf("span %d has an Error status on a response", index)
		}
	}
}

// The request span is in the shared client: a provider other than GitHub gets the same key set, unrouted and
// unclassified when the caller names no template and no family.
func TestProviderRequestSpanKeySetIsTheSameForAnotherProvider(t *testing.T) {
	pipeline := spantest.Install(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `[]`) }))
	t.Cleanup(server.Close)
	client := spanTestClientFor(t, "gitlab", server.URL, noFollowClient(), oneAttempt())
	response, err := client.Do(pipeline.Parent, http.MethodGet, "/projects/"+spanTestOwner+"%2F"+spanTestRepo+"/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}
	closeBody(response)
	pairs := pipeline.Pairs(spanTestRequestName)
	if len(pairs) != 1 {
		t.Fatalf("request spans = %d, want 1", len(pairs))
	}
	spantest.Check(t, pairs[0], spanTestWantKeys(true, false), spanTestOwner, spanTestRepo, "/projects/", "127.0.0.1")
	requireRequestEnums(t, pairs[0], "gitlab", "unrouted", LedgerUnclassified)
}

// C2: a call with no recording parent makes no span (never a new root): no parent and a valid parent that is not
// sampled. The nil context is covered by TestRequestSpanGuardTakesANilContext: Do and DoUnauthenticated cannot send
// with a nil context.
func TestProviderRequestWithoutARecordingParentMakesNoSpan(t *testing.T) {
	pipeline := spantest.Install(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{}`) }))
	t.Cleanup(server.Close)
	client := spanTestClient(t, server.URL, noFollowClient(), oneAttempt())
	for name, ctx := range map[string]context.Context{"no parent": context.Background(), "unsampled parent": spantest.UnsampledParent()} {
		response, err := client.DoRoute(ctx, http.MethodGet, spanTestZipTemplate, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		closeBody(response)
		response, err = client.DoUnauthenticated(ctx, http.MethodGet, server.URL+"/blob?sig="+spanTestSecret)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		closeBody(response)
	}
	if spans := pipeline.Ended(); len(spans) != 0 {
		t.Fatalf("spans = %d (first %q), want 0", len(spans), spans[0].Name())
	}
}

// C3: one span per call, not per wire attempt: 503 then 200 is one span with attempts 2 and the final status.
func TestProviderRequestRetryIsOneSpanWithTheAttemptCount(t *testing.T) {
	pipeline := spantest.Install(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	t.Cleanup(server.Close)
	client := spanTestClient(t, server.URL, noFollowClient(), RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: 2 * time.Millisecond})

	response, err := client.Do(pipeline.Parent, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	closeBody(response)
	pairs := pipeline.Pairs(spanTestRequestName)
	if len(pairs) != 1 || calls.Load() != 2 {
		t.Fatalf("request spans = %d wire calls = %d, want 1 span over 2 calls", len(pairs), calls.Load())
	}
	spantest.Check(t, pairs[0], spanTestWantKeys(true, false), spanTestOwner, spanTestRepo, "127.0.0.1")
	requireRequestEnums(t, pairs[0], "github", "unrouted", LedgerUnclassified)
	if got := spanTestInt(t, pairs[0], "dev_health.provider.attempts"); got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
	if got := spanTestInt(t, pairs[0], "http.response.status_code"); got != http.StatusOK {
		t.Errorf("status = %d, want 200", got)
	}
}

type spanTestBudget struct {
	acquireErr error
	releaseErr error
}

type spanTestReservation struct{ err error }

func (r spanTestReservation) Release(context.Context) error { return r.err }

func (b spanTestBudget) Acquire(context.Context, BudgetKey) (Reservation, error) {
	if b.acquireErr != nil {
		return nil, b.acquireErr
	}
	return spanTestReservation{err: b.releaseErr}, nil
}

// C4: the span changes no return value. Each case runs the same call twice, under a recording parent with the real
// provider and with a noop provider: the errors are deeply equal, errors.Is keeps its answer and the response is nil
// both times. error.type is the closed class; ErrBudgetContended is no failure (outcome budget_contended, no Error
// status, no error.type).
func TestProviderRequestSpanChangesNoReturnValue(t *testing.T) {
	ok := HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(io.MultiReader()), Request: request}, nil
	})
	cases := []struct {
		name      string
		doer      HTTPDoer
		budget    BudgetStore
		is        error
		outcome   string
		errorType string
	}{
		{name: "transport failure", doer: HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp 10.9.8.7:443: refused near " + spanTestSecret)
		}), outcome: "failed", errorType: "transient"},
		{name: "budget contended", doer: ok, budget: spanTestBudget{acquireErr: ErrBudgetContended}, is: ErrBudgetContended, outcome: "budget_contended"},
		{name: "budget release failure", doer: ok, budget: spanTestBudget{releaseErr: errors.New("valkey down " + spanTestSecret)}, is: ErrBudgetUnavailable, outcome: "failed", errorType: "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, unauthenticated := range []bool{false, true} {
				call := func(ctx context.Context) (*http.Response, error) {
					client := spanTestClient(t, "https://api.example.test", fakehttp.Client(tc.doer), oneAttempt())
					if tc.budget != nil {
						client.Budget = tc.budget
					}
					if unauthenticated {
						return client.DoUnauthenticated(ctx, http.MethodGet, "https://objects.example.test/blob")
					}
					return client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
				}
				pipeline := spantest.Install(t)
				tracedResponse, traced := call(pipeline.Parent)
				otel.SetTracerProvider(noop.NewTracerProvider())
				untracedResponse, untraced := call(pipeline.Parent)
				closeBody(tracedResponse)
				closeBody(untracedResponse)
				if traced == nil || !reflect.DeepEqual(traced, untraced) || traced.Error() != untraced.Error() || tracedResponse != nil || untracedResponse != nil {
					t.Fatalf("unauthenticated=%v traced (%v, %#v), noop (%v, %#v): want the same error and nil responses", unauthenticated, tracedResponse, traced, untracedResponse, untraced)
				}
				if tc.is != nil && (!errors.Is(traced, tc.is) || !errors.Is(untraced, tc.is)) {
					t.Fatalf("errors.Is(%v) traced=%v noop=%v, want true", tc.is, errors.Is(traced, tc.is), errors.Is(untraced, tc.is))
				}
				pairs := pipeline.Pairs(spanTestRequestName)
				if len(pairs) != 1 {
					t.Fatalf("request spans = %d, want 1", len(pairs))
				}
				pair := pairs[0]
				var providerErr *ProviderError
				status := errors.As(traced, &providerErr) && providerErr.StatusCode > 0
				spantest.Check(t, pair, spanTestWantKeys(status, tc.errorType != ""), spanTestSecret, "10.9.8.7", "refused", "dial tcp",
					"valkey", "api.example.test", "objects.example.test", spanTestOwner, spanTestRepo)
				requireRequestEnums(t, pair, "github", "unrouted", LedgerUnclassified)
				if got := spanTestString(t, pair, "outcome"); got != tc.outcome {
					t.Errorf("outcome = %q, want %q", got, tc.outcome)
				}
				if tc.errorType != "" {
					if got := spanTestString(t, pair, "error.type"); got != tc.errorType {
						t.Errorf("error.type = %q, want %q", got, tc.errorType)
					}
					if pair.Raw.Status().Code != codes.Error || pair.Raw.Status().Description != tc.errorType {
						t.Errorf("status = %v %q, want Error with the class", pair.Raw.Status().Code, pair.Raw.Status().Description)
					}
				} else if pair.Raw.Status().Code == codes.Error || len(pair.Raw.Events()) != 0 {
					t.Errorf("status = %v events = %d, want no Error status and no event", pair.Raw.Status().Code, len(pair.Raw.Events()))
				}
			}
		})
	}
}

// C4b: error text never reaches a span. The blob leg fails with a transport error whose text (net/http's *url.Error)
// holds the signed URL; the API leg answers 404 on an owner/repo path; a budget store returns an error whose text holds
// the signed URL, which the client returns as it is. No name, attribute, event or status, raw or exported, holds the
// signature, the blob host, the owner, the repository or the path.
func TestProviderRequestErrorTextNeverReachesASpan(t *testing.T) {
	pipeline := spantest.Install(t)
	blobURL := "http://blob.test/x?sig=" + spanTestSecret
	failing := fakehttp.Client(HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset reading " + blobURL)
	}))
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"Not Found","path":"`+r.URL.Path+`"}`, http.StatusNotFound)
	}))
	t.Cleanup(api.Close)

	blobClient := spanTestClient(t, "https://api.example.test", failing, oneAttempt())
	response, blobErr := blobClient.DoUnauthenticated(pipeline.Parent, http.MethodGet, blobURL)
	closeBody(response)
	apiClient := spanTestClient(t, api.URL, noFollowClient(), oneAttempt())
	response, apiErr := apiClient.Do(pipeline.Parent, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo+"/actions/artifacts/"+spanTestArtifactID+"/zip", nil)
	closeBody(response)
	budgetClient := spanTestClient(t, "https://api.example.test", failing, oneAttempt())
	budgetClient.Budget = spanTestBudget{acquireErr: errors.New("budget store refused " + blobURL)}
	response, budgetErr := budgetClient.DoUnauthenticated(pipeline.Parent, http.MethodGet, blobURL)
	closeBody(response)
	if blobErr == nil || apiErr == nil || budgetErr == nil || !strings.Contains(budgetErr.Error(), spanTestSecret) {
		t.Fatalf("blob err=%v api err=%v budget err=%v, want all three to fail, the last with the URL in its text", blobErr, apiErr, budgetErr)
	}

	pairs := pipeline.Pairs(spanTestRequestName)
	if len(pairs) != 3 {
		t.Fatalf("request spans = %d, want 3", len(pairs))
	}
	for index, pair := range pairs {
		status := index == 1
		spantest.Check(t, pair, spanTestWantKeys(status, true), spanTestSecret, "sig", "blob.test", spanTestOwner, spanTestRepo,
			"/actions/", spanTestArtifactID, "connection reset", "Not Found")
		requireRequestEnums(t, pair, "github", "unrouted", LedgerUnclassified)
	}
	if got := spanTestString(t, pairs[1], "error.type"); got != string(ErrorNotFound) {
		t.Errorf("api error.type = %q, want not_found", got)
	}
}

// C2, nil context: the span start takes a nil context without a panic and makes no span (DoRoute passes a nil context
// to Do; Do and DoUnauthenticated cannot send with one).
func TestRequestSpanGuardTakesANilContext(t *testing.T) {
	pipeline := spantest.Install(t)
	client := spanTestClient(t, "https://api.example.test", fakehttp.Client(HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unused")
	})), oneAttempt())
	var nilCtx context.Context
	for _, target := range []string{requestTargetAPI, requestTargetRedirect} {
		ctx, probe := client.startRequestSpan(nilCtx, http.MethodGet, target)
		if ctx != nil || probe != nil {
			t.Fatalf("target %s: ctx=%v probe=%v, want nil and nil", target, ctx, probe)
		}
		probe.end(nil, nil)
	}
	if spans := pipeline.Ended(); len(spans) != 0 {
		t.Fatalf("spans = %d, want 0", len(spans))
	}
}
