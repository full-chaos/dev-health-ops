package providerfoundation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
// status, no error.type). status is the status that arrived on the wire (0: none): a budget release that fails after a
// 200 keeps the 200 on the span although the call returns no response.
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
		status    int64
	}{
		{name: "transport failure", doer: HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp 10.9.8.7:443: refused near " + spanTestSecret)
		}), outcome: "failed", errorType: "transient"},
		{name: "budget contended", doer: ok, budget: spanTestBudget{acquireErr: ErrBudgetContended}, is: ErrBudgetContended, outcome: "budget_contended"},
		{name: "budget release failure", doer: ok, budget: spanTestBudget{releaseErr: errors.New("valkey down " + spanTestSecret)}, is: ErrBudgetUnavailable, outcome: "failed", errorType: "other", status: http.StatusOK},
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
				spantest.Check(t, pair, spanTestWantKeys(tc.status > 0, tc.errorType != ""), spanTestSecret, "10.9.8.7", "refused", "dial tcp",
					"valkey", "api.example.test", "objects.example.test", spanTestOwner, spanTestRepo)
				requireRequestEnums(t, pair, "github", "unrouted", LedgerUnclassified)
				if tc.status > 0 {
					if got := spanTestInt(t, pair, "http.response.status_code"); got != tc.status {
						t.Errorf("status = %d, want %d", got, tc.status)
					}
				}
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
		probe := client.startRequestSpan(nilCtx, http.MethodGet, target)
		if probe != nil {
			t.Fatalf("target %s: probe=%v, want nil", target, probe)
		}
		probe.end(nilCtx, nil)
	}
	if spans := pipeline.Ended(); len(spans) != 0 {
		t.Fatalf("spans = %d, want 0", len(spans))
	}
}

// spanTestGate answers its scripted waits in order, then 0; Penalize returns penalizeErr.
type spanTestGate struct {
	waits       []time.Duration
	calls       int
	penalizeErr error
}

func (g *spanTestGate) Wait(context.Context) (time.Duration, error) {
	g.calls++
	if g.calls <= len(g.waits) {
		return g.waits[g.calls-1], nil
	}
	return 0, nil
}

func (g *spanTestGate) Penalize(context.Context, time.Duration) error { return g.penalizeErr }

func onlyRequestSpan(t *testing.T, pipeline *spantest.Pipeline) spantest.Pair {
	t.Helper()
	pairs := pipeline.Pairs(spanTestRequestName)
	if len(pairs) != 1 {
		t.Fatalf("request spans = %d, want 1", len(pairs))
	}
	return pairs[0]
}

// spanTestTransport is a transport that answers in place: it writes nothing on a wire, so no WroteRequest fires.
type spanTestTransport func(*http.Request) (*http.Response, error)

func (f spanTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// spanTestRedirectThenFail follows redirects: the first request gets a same-origin 302, the hop fails in the transport.
func spanTestRedirectThenFail() *http.Client {
	return &http.Client{Transport: spanTestTransport(func(request *http.Request) (*http.Response, error) {
		if request.Response == nil {
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {"/hop"}}, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		return nil, errors.New("aaaa")
	})}
}

func spanTestStatusDoer(status int, header http.Header) HTTPDoerFunc {
	return func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
	}
}

// The status on the request span is the status of the response to the LAST wire attempt sent, read at the wire: a
// status that arrived stays on the span when the call fails after it (a gate write, a cancelled backoff, a refused
// redirect), and a status this request never received (a token exchange 401 before the request was sent) is never on it.
func TestProviderRequestSpanKeepsTheStatusOfTheLastAttempt(t *testing.T) {
	cases := []struct {
		name      string
		call      func(t *testing.T, ctx context.Context) error
		status    int64
		attempts  int64
		outcome   string
		errorType string
	}{
		{name: "429 then the gate write fails", call: func(t *testing.T, ctx context.Context) error {
			client := spanTestClient(t, "https://api.example.test", fakehttp.Client(spanTestStatusDoer(http.StatusTooManyRequests, http.Header{"Retry-After": {"1"}})), oneAttempt())
			client.Gate = &spanTestGate{penalizeErr: errors.New("aaaa")}
			response, err := client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
			closeBody(response)
			return err
		}, status: http.StatusTooManyRequests, attempts: 1, outcome: "failed", errorType: "other"},
		{name: "503 then a cancelled backoff", call: func(t *testing.T, ctx context.Context) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			unavailable := spanTestStatusDoer(http.StatusServiceUnavailable, nil)
			client := spanTestClient(t, "https://api.example.test", fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
				cancel()
				return unavailable(request)
			})), RetryPolicy{MaxAttempts: 2, InitialWait: 10 * time.Second, MaxWait: 10 * time.Second})
			response, err := client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
			closeBody(response)
			return err
		}, status: http.StatusServiceUnavailable, attempts: 1, outcome: "cancelled"},
		{name: "302 refused by the origin guard", call: func(t *testing.T, ctx context.Context) error {
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{}`) }))
			t.Cleanup(other.Close)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, other.URL+"/elsewhere", http.StatusFound)
			}))
			t.Cleanup(api.Close)
			client := spanTestClient(t, api.URL, &http.Client{}, oneAttempt())
			response, err := client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
			closeBody(response)
			if !errors.Is(err, ErrCredentialInvalid) {
				t.Fatalf("err = %v, want ErrCredentialInvalid", err)
			}
			return err
		}, status: http.StatusFound, attempts: 1, outcome: "failed", errorType: "other"},
		{name: "a token exchange 401 before the request is sent", call: func(t *testing.T, ctx context.Context) error {
			client, err := NewHTTPClient("github", "https://api.example.test", fakehttp.Client(HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
				t.Error("the request was sent")
				return nil, errors.New("aaaa")
			})), func(*http.Request) error {
				return &ProviderError{Class: ErrorAuthentication, StatusCode: http.StatusUnauthorized}
			}, oneAttempt(), LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
			closeBody(response)
			return err
		}, attempts: 0, outcome: "failed", errorType: "authentication"},
		{name: "503 then a retry that fails before it is written", call: func(t *testing.T, ctx context.Context) error {
			calls := 0
			unavailable := spanTestStatusDoer(http.StatusServiceUnavailable, nil)
			client := spanTestClient(t, "https://api.example.test", fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return unavailable(request)
				}
				return nil, errors.New("aaaa")
			})), RetryPolicy{MaxAttempts: 2, InitialWait: time.Millisecond, MaxWait: 2 * time.Millisecond})
			response, err := client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
			closeBody(response)
			return err
		}, status: http.StatusServiceUnavailable, attempts: 2, outcome: "failed", errorType: "transient"},
		{name: "503 then a retry that is written and gets no response", call: func(t *testing.T, ctx context.Context) error {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				connection, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				_ = connection.Close()
			}))
			t.Cleanup(server.Close)
			client := spanTestClient(t, server.URL, &http.Client{}, RetryPolicy{MaxAttempts: 2, InitialWait: time.Millisecond, MaxWait: 2 * time.Millisecond})
			response, err := client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
			closeBody(response)
			return err
		}, attempts: 2, outcome: "failed", errorType: "transient"},
		{name: "a redirect, then the next hop fails before it is written", call: func(t *testing.T, ctx context.Context) error {
			client := spanTestClient(t, "https://api.example.test", spanTestRedirectThenFail(), oneAttempt())
			response, err := client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
			closeBody(response)
			return err
		}, status: http.StatusFound, attempts: 1, outcome: "failed", errorType: "transient"},
		{name: "an unauthenticated redirect, then the next hop fails before it is written", call: func(t *testing.T, ctx context.Context) error {
			client := spanTestClient(t, "https://api.example.test", spanTestRedirectThenFail(), oneAttempt())
			response, err := client.DoUnauthenticated(ctx, http.MethodGet, "https://objects.example.test/blob")
			closeBody(response)
			return err
		}, status: http.StatusFound, attempts: 1, outcome: "failed", errorType: "transient"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := spantest.Install(t)
			if err := tc.call(t, pipeline.Parent); err == nil {
				t.Fatal("call err = nil, want the failure")
			}
			pair := onlyRequestSpan(t, pipeline)
			spantest.Check(t, pair, spanTestWantKeys(tc.status > 0, tc.errorType != ""), spanTestOwner, spanTestRepo, "127.0.0.1", "elsewhere")
			if tc.status > 0 {
				if got := spanTestInt(t, pair, "http.response.status_code"); got != tc.status {
					t.Errorf("status = %d, want %d", got, tc.status)
				}
			}
			if got := spanTestInt(t, pair, "dev_health.provider.attempts"); got != tc.attempts {
				t.Errorf("attempts = %d, want %d", got, tc.attempts)
			}
			if got := spanTestString(t, pair, "outcome"); got != tc.outcome {
				t.Errorf("outcome = %q, want %q", got, tc.outcome)
			}
			if tc.errorType != "" {
				if got := spanTestString(t, pair, "error.type"); got != tc.errorType {
					t.Errorf("error.type = %q, want %q", got, tc.errorType)
				}
			}
		})
	}
}

// A call that ended only because its context was cancelled is outcome cancelled with no Error status, also when the
// transport turned the cancellation into a transient ProviderError. A cancel error under a live context (a context the
// code cancelled itself) and a cancellation joined with a failed budget release stay failures.
func TestProviderRequestSpanOfACancelledCallIsCancelled(t *testing.T) {
	cases := []struct {
		name         string
		cancel       bool
		budget       BudgetStore
		transportErr error
		outcome      string
		errorType    string
	}{
		{name: "cancelled during the attempt", cancel: true, outcome: "cancelled"},
		{name: "cancelled with a failure leaf in the transport error", cancel: true, transportErr: errors.Join(context.Canceled, io.ErrUnexpectedEOF), outcome: "failed", errorType: "transient"},
		{name: "a cancel error under a live context", outcome: "failed", errorType: "transient"},
		{name: "cancelled and the budget release fails", cancel: true, budget: spanTestBudget{releaseErr: errors.New("aaaa")}, outcome: "failed", errorType: "transient"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, unauthenticated := range []bool{false, true} {
				pipeline := spantest.Install(t)
				ctx, cancel := context.WithCancel(pipeline.Parent)
				client := spanTestClient(t, "https://api.example.test", fakehttp.Client(HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
					if tc.cancel {
						cancel()
					}
					if tc.transportErr != nil {
						return nil, tc.transportErr
					}
					return nil, context.Canceled
				})), oneAttempt())
				if tc.budget != nil {
					client.Budget = tc.budget
				}
				var response *http.Response
				var err error
				if unauthenticated {
					response, err = client.DoUnauthenticated(ctx, http.MethodGet, "https://objects.example.test/blob")
				} else {
					response, err = client.Do(ctx, http.MethodGet, "/repos/"+spanTestOwner+"/"+spanTestRepo, nil)
				}
				closeBody(response)
				cancel()
				if err == nil {
					t.Fatalf("unauthenticated=%v: err = nil, want the cancellation", unauthenticated)
				}
				pair := onlyRequestSpan(t, pipeline)
				spantest.Check(t, pair, spanTestWantKeys(false, tc.errorType != ""), spanTestOwner, spanTestRepo, "objects.example.test")
				if got := spanTestString(t, pair, "outcome"); got != tc.outcome {
					t.Errorf("unauthenticated=%v: outcome = %q, want %q", unauthenticated, got, tc.outcome)
				}
				if failed := pair.Raw.Status().Code == codes.Error; failed != (tc.errorType != "") {
					t.Errorf("unauthenticated=%v: Error status = %v, want %v", unauthenticated, failed, tc.errorType != "")
				}
				if tc.errorType != "" {
					if got := spanTestString(t, pair, "error.type"); got != tc.errorType {
						t.Errorf("unauthenticated=%v: error.type = %q, want %q", unauthenticated, got, tc.errorType)
					}
				}
			}
		})
	}
}

// gate_wait_ms is the time in the gate reads and the in-place gate sleeps only; the lease assert after a gate sleep is
// lease time like every other assert of the call. Both sides are durations measured in this process: the span against
// the asserts' own clock (D6418).
func TestProviderRequestGateWaitExcludesTheLeaseAssert(t *testing.T) {
	const assertTime = 100 * time.Millisecond
	for _, tc := range []struct {
		name            string
		unauthenticated bool
		asserts         int
	}{{"Do", false, 3}, {"DoUnauthenticated", true, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := spantest.Install(t)
			client := spanTestClient(t, "https://api.example.test", fakehttp.Client(spanTestStatusDoer(http.StatusOK, nil)), oneAttempt())
			client.Gate = &spanTestGate{waits: []time.Duration{time.Millisecond}}
			client.GateWaitMax = time.Second
			asserts := 0
			var assertTotal, assertShortest time.Duration
			client.Lease = LeaseGuardFunc(func(context.Context) error {
				started := time.Now()
				time.Sleep(assertTime)
				took := time.Since(started)
				asserts++
				assertTotal += took
				if assertShortest == 0 || took < assertShortest {
					assertShortest = took
				}
				return nil
			})
			var response *http.Response
			var err error
			if tc.unauthenticated {
				response, err = client.DoUnauthenticated(pipeline.Parent, http.MethodGet, "https://objects.example.test/blob")
			} else {
				response, err = client.Do(pipeline.Parent, http.MethodGet, "/items", nil)
			}
			closeBody(response)
			if err != nil {
				t.Fatal(err)
			}
			pair := onlyRequestSpan(t, pipeline)
			if asserts != tc.asserts {
				t.Fatalf("lease asserts = %d, want %d", asserts, tc.asserts)
			}
			// spent and waited are durations the probe measured in this process, compared with the asserts' own clock.
			spent, waited := spanTestInt(t, pair, "dev_health.provider.lease_ms"), spanTestInt(t, pair, "dev_health.provider.gate_wait_ms")
			if spent < assertTotal.Milliseconds() {
				t.Errorf("lease_ms = %d, want >= %d (the time of every assert, measured by the asserts)", spent, assertTotal.Milliseconds())
			}
			if waited >= assertShortest.Milliseconds() {
				t.Errorf("gate_wait_ms = %d, want < %d (shorter than one assert: no assert in it)", waited, assertShortest.Milliseconds())
			}
		})
	}
}

// Existing log lines written during a provider call keep the caller's span: the request span is never the span of the
// context those lines read.
func TestExistingProviderLogLinesKeepTheCallersSpan(t *testing.T) {
	for _, unauthenticated := range []bool{false, true} {
		pipeline := spantest.Install(t)
		client := spanTestClient(t, "https://api.example.test", fakehttp.Client(spanTestStatusDoer(http.StatusOK, nil)), oneAttempt())
		client.Gate = &spanTestGate{waits: []time.Duration{time.Millisecond}}
		client.GateWaitMax = time.Second
		var out bytes.Buffer
		func() {
			restore := logging.InstallDefault(logging.NewJSON(&out, slog.LevelInfo))
			defer restore()
			var response *http.Response
			var err error
			if unauthenticated {
				response, err = client.DoUnauthenticated(pipeline.Parent, http.MethodGet, "https://objects.example.test/blob")
			} else {
				response, err = client.Do(pipeline.Parent, http.MethodGet, "/items", nil)
			}
			closeBody(response)
			if err != nil {
				t.Fatal(err)
			}
		}()
		onlyRequestSpan(t, pipeline)
		parent := trace.SpanContextFromContext(pipeline.Parent)
		lines := 0
		for _, raw := range bytes.Split(out.Bytes(), []byte("\n")) {
			if len(bytes.TrimSpace(raw)) == 0 {
				continue
			}
			var line map[string]any
			if err := json.Unmarshal(raw, &line); err != nil {
				t.Fatalf("log line is not JSON: %v", err)
			}
			if line["msg"] != "provider_gate_wait_slept" {
				continue
			}
			lines++
			if line["span_id"] != parent.SpanID().String() || line["trace_id"] != parent.TraceID().String() {
				t.Errorf("unauthenticated=%v: gate line span_id=%v trace_id=%v, want the caller's %s %s",
					unauthenticated, line["span_id"], line["trace_id"], parent.SpanID(), parent.TraceID())
			}
		}
		if lines != 1 {
			t.Fatalf("unauthenticated=%v: provider_gate_wait_slept lines = %d, want 1", unauthenticated, lines)
		}
	}
}

type spanTestJoinErr []error

func (e spanTestJoinErr) Error() string   { return "aaaa" }
func (e spanTestJoinErr) Unwrap() []error { return e }

// CallCancelled: a cancelled context AND only cancellation leaves (raw, wrapped, the transport's canceled reason, or a
// named label beside one). Anything else is a failure.
func TestCallCancelledNeedsACancelledContextAndOnlyCancellationLeaves(t *testing.T) {
	label := errors.New("label")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer stop()
	transportCanceled := &ProviderError{Class: ErrorTransient, Reason: TransportReasonCanceled}
	cases := []struct {
		name   string
		ctx    context.Context
		err    error
		labels []error
		want   bool
	}{
		{"raw cancel", cancelled, context.Canceled, nil, true},
		{"wrapped cancel", cancelled, &url.Error{Op: "Get", URL: "x", Err: context.Canceled}, nil, true},
		{"wrapped failure under a cancelled context", cancelled, fmt.Errorf("aaaa: %w", errors.New("aaaa")), nil, false},
		{"transport canceled reason", cancelled, transportCanceled, nil, true},
		{"label beside a cancel", cancelled, spanTestJoinErr{label, context.Canceled}, []error{label}, true},
		{"cancel under a live context", context.Background(), context.Canceled, nil, false},
		{"nil context", nil, context.Canceled, nil, false},
		{"nil error", cancelled, nil, nil, false},
		{"deadline", expired, context.DeadlineExceeded, nil, false},
		{"label alone", cancelled, label, []error{label}, false},
		{"label not named", cancelled, spanTestJoinErr{label, context.Canceled}, nil, false},
		{"cancel joined with a failure", cancelled, errors.Join(transportCanceled, ErrBudgetUnavailable), nil, false},
		{"other transport reason", cancelled, &ProviderError{Class: ErrorTransient, Reason: TransportReasonTimeout}, nil, false},
		{"empty join", cancelled, spanTestJoinErr{}, nil, false},
	}
	for _, tc := range cases {
		if got := CallCancelled(tc.ctx, tc.err, tc.labels...); got != tc.want {
			t.Errorf("%s: CallCancelled = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A plain Do under a recording caller span is a child of that span in the caller's trace: never a new root.
func TestProviderRequestSpanIsAChildOfTheCallersSpan(t *testing.T) {
	pipeline := spantest.Install(t)
	client := spanTestClient(t, "https://api.example.test", fakehttp.Client(spanTestStatusDoer(http.StatusOK, nil)), oneAttempt())
	response, err := client.Do(pipeline.Parent, http.MethodGet, "/items", nil)
	closeBody(response)
	if err != nil {
		t.Fatal(err)
	}
	pair := onlyRequestSpan(t, pipeline)
	caller := trace.SpanContextFromContext(pipeline.Parent)
	if pair.Raw.Parent().SpanID() != caller.SpanID() || pair.Raw.SpanContext().TraceID() != caller.TraceID() {
		t.Errorf("request span parent=%s trace=%s, want the caller's span %s in the caller's trace %s",
			pair.Raw.Parent().SpanID(), pair.Raw.SpanContext().TraceID(), caller.SpanID(), caller.TraceID())
	}
	if pair.Exported.Parent.SpanID() != caller.SpanID() || pair.Exported.SpanContext.TraceID() != caller.TraceID() {
		t.Errorf("exported request span parent=%s trace=%s, want the caller's", pair.Exported.Parent.SpanID(), pair.Exported.SpanContext.TraceID())
	}
}
