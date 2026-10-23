package apiservice

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
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func TestSignatureOnlySetIsUnchangedByTheGate(t *testing.T) {
	want := map[string]string{
		"POST /api/v1/billing/webhooks/stripe":         "stripe_webhook_signature",
		"POST /api/v1/webhooks/github":                 "github_webhook_signature",
		"POST /api/v1/webhooks/gitlab":                 "gitlab_webhook_signature",
		"POST /api/v1/webhooks/jira":                   "jira_webhook_signature",
		"POST /api/v1/webhooks/pagerduty/{binding_id}": "pagerduty_webhook_signature",
	}
	if !reflect.DeepEqual(signatureOnlyRoutes, want) {
		t.Fatalf("the signature-only set changed: %v", signatureOnlyRoutes)
	}
}

func TestIntakeGateCountsEveryIdShapeOnceAheadOfScope(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	store := &countingStore{scopeStore: scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}}
	auth, err := policy.NewAuthenticator(verifier, store, quiet)
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quiet)
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(httpapi.NewMemoryCounters(nil), nil, quiet), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uuid.NewString(), "type": "access", "org_id": uuid.NewString(), "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "/api/v1/webhooks/pagerduty/"
	shapes := []struct {
		name   string
		id     func(int) string
		budget int
	}{
		{"canonical id", func(int) string { return uuid.NewString() }, 600},
		{"malformed id", func(i int) string { return "not-a-uuid-" + uuid.NewString()[:8] + string(rune('a'+i%26)) }, 60},
		{"too long id", func(int) string { return strings.Repeat("a", 200) + uuid.NewString() }, 60},
		{"percent segment", func(int) string { return "%31" + uuid.NewString() }, 60},
		{"dot segment", func(int) string { return "." }, 60},
		{"double dot segment", func(int) string { return ".." }, 60},
	}
	peer := 0
	for _, shape := range shapes {
		for _, withBearer := range []bool{false, true} {
			peer++
			address := fmt.Sprintf("198.51.100.%d:4242", peer)
			send := func(i int) *httptest.ResponseRecorder {
				request := httptest.NewRequest(http.MethodPost, prefix+shape.id(i), nil)
				request.RemoteAddr = address
				if withBearer {
					request.Header.Set("Authorization", "Bearer "+bearer)
					request.Header.Set("X-Org-Id", uuid.NewString())
				}
				recorder := httptest.NewRecorder()
				server.Handler().ServeHTTP(recorder, request)
				return recorder
			}
			passed := 0
			var refused *httptest.ResponseRecorder
			for i := 0; i < shape.budget+5; i++ {
				got := send(i)
				if got.Code == http.StatusTooManyRequests {
					refused = got
					break
				}
				passed++
			}
			if passed != shape.budget || refused == nil {
				t.Errorf("%s bearer=%v: %d deliveries passed before the first 429, want %d", shape.name, withBearer, passed, shape.budget)
				continue
			}
			again := send(shape.budget + 100)
			if again.Code != http.StatusTooManyRequests || again.Body.String() != refused.Body.String() || refused.Body.String() != `{"detail":{"message":"Rate limit exceeded. Please try again later."}}` {
				t.Errorf("%s bearer=%v: throttled answers differ: %d %s vs %s", shape.name, withBearer, again.Code, again.Body.String(), refused.Body.String())
			}
			if refused.Header().Get("X-Frame-Options") != "DENY" {
				t.Errorf("%s bearer=%v: the refusal lacks the security headers: %v", shape.name, withBearer, refused.Header())
			}
		}
	}
}

func TestIntakeGateCountsPercentEncodedRouteNamesInTheSameBucket(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(httpapi.NewMemoryCounters(nil), nil, quiet))
	if err != nil {
		t.Fatal(err)
	}
	binding := uuid.NewString()
	cases := []struct {
		name, literal, alias string
		budget               int
	}{
		{"github", "/api/v1/webhooks/github", "/api/v1/webhooks/%67ithub", 3000},
		{"gitlab", "/api/v1/webhooks/gitlab", "/api/v1/webhooks/%67itlab", 3000},
		{"jira", "/api/v1/webhooks/jira", "/api/v1/webhooks/%6aira", 3000},
		{"pagerduty", "/api/v1/webhooks/pagerduty/" + binding, "/api/v1/webhooks/%70agerduty/" + binding, 60},
	}
	for index, c := range cases {
		address := fmt.Sprintf("203.0.113.%d:4242", index+1)
		send := func(target string) int {
			request := httptest.NewRequest(http.MethodPost, target, nil)
			request.RemoteAddr = address
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			return recorder.Code
		}
		for i := 0; i < c.budget; i++ {
			if send(c.literal) == http.StatusTooManyRequests {
				t.Fatalf("%s: delivery %d throttled early", c.name, i+1)
			}
		}
		if got := send(c.literal); got != http.StatusTooManyRequests {
			t.Fatalf("%s: literal path past its budget = %d, want 429", c.name, got)
		}
		if got := send(c.alias); got != http.StatusTooManyRequests {
			t.Errorf("%s: percent-encoded alias past the budget = %d, want 429", c.name, got)
		}
	}
}

func TestConfigureInstallsTheIntakeGate(t *testing.T) {
	components, err := configure(context.Background(), apiTestConfig(), health.NewRegistry(time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	}()
	var server *httpapi.Server
	for _, component := range components {
		if candidate, ok := component.(*httpapi.Server); ok {
			server = candidate
		}
	}
	if server == nil {
		t.Fatal("no api server was configured")
	}
	target := "/api/v1/webhooks/pagerduty/" + uuid.NewString()
	var last int
	for i := 0; i < 61; i++ {
		request := httptest.NewRequest(http.MethodPost, target, nil)
		request.RemoteAddr = "203.0.113.200:4242"
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		last = recorder.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("the 61st delivery to one PagerDuty path = %d, want 429 from the configured server's gate", last)
	}
}

type countingCounters struct{ clientHits, exactHits, routeHits int }

func (c *countingCounters) Increment(_ context.Context, hit httpapi.Hit) (int64, error) {
	switch hit.Limit.ID {
	case "webhook_intake_client":
		c.clientHits++
	case "webhook_pagerduty":
		c.exactHits++
	case "webhook_pagerduty_route", "webhook_intake_route":
		c.routeHits++
	}
	return 1, nil
}
func (*countingCounters) Peek(context.Context, httpapi.Hit) (int64, error) { return 0, nil }
func (*countingCounters) Backend() string                                  { return "memory" }

// A request that reaches an intake handler is counted exactly once, whatever
// spelling of the route it used: the gate matches the path the router routes
// on, so no encoded, double-encoded, dot-segment or slash variant reaches a
// handler uncounted.
func TestEveryRequestThatReachesAnIntakeHandlerIsCountedOnce(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	var reached int
	var sawBinding bool
	var bindingValue string
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	for index := range routes {
		routes[index].Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached++
			sawBinding = strings.Contains(r.Pattern, "/pagerduty/")
			bindingValue = r.PathValue("binding_id")
			w.WriteHeader(http.StatusNoContent)
		})
	}
	counters := &countingCounters{}
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(counters, nil, quiet))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	targets := []string{
		"/api/v1/webhooks/github", "/api/v1/webhooks/gitlab", "/api/v1/webhooks/jira", "/api/v1/webhooks/pagerduty/" + id,
		"/api/v1/webhooks/%67ithub", "/api/v1/webhooks/%2567ithub", "/api/v1/webhooks/%6aira", "/api/v1/webhooks/%67itlab",
		"/api/v1/webhooks/%70agerduty/" + id, "/api/v1/webhooks/pagerduty/%2567" + id[2:], "/api/v1/webhooks/pagerduty/%31" + id[1:],
		"/api/v1/webhooks%2Fgithub", "/api/v1/webhooks%2fgithub", "/api%2Fv1/webhooks/github", "/api%2fv1%2Fwebhooks%2Fjira",
		"/api/v1/webhooks%2Fgitlab", "/api/v1/webhooks%2fpagerduty/" + id, "/api/v1/webhooks/pagerduty%2F" + id, "/api/v1/webhooks/pagerduty%2f" + id,
		"/api/v1/webhooks/pagerduty/" + id + "%2F", "/api/v1/webhooks/pagerduty/" + id + "%2f", "/api/v1/webhooks/github%2F", "/api/v1/webhooks/github%2f",
		"/api/v1/webhooks/github/", "/api/v1/webhooks/gitlab/", "/api/v1/webhooks/jira/", "/api/v1/webhooks/pagerduty/" + id + "/",
		"//api/v1/webhooks/github", "/api/v1/webhooks//github", "/api/v1/webhooks/pagerduty//" + id,
		"/api/./v1/webhooks/github", "/api/v1/webhooks/../webhooks/github", "/api/v1/webhooks/./jira",
		"/API/V1/WEBHOOKS/GITHUB", "/api/v1/webhooks/GitHub", "/api/v1/webhooks/github.", "/api/v1/webhooks/github;x=1",
	}
	reachedCount := 0
	for _, target := range targets {
		for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodHead} {
			reached, sawBinding, bindingValue = 0, false, ""
			counters.clientHits, counters.exactHits, counters.routeHits = 0, 0, 0
			request := httptest.NewRequest(method, target, nil)
			request.RequestURI = target
			request.URL.RawPath = ""
			if parsed, err := url.ParseRequestURI(target); err == nil {
				request.URL = parsed
			}
			request.RemoteAddr = "203.0.113.9:4242"
			server.Handler().ServeHTTP(httptest.NewRecorder(), request)
			if reached > 0 {
				reachedCount++
				// A valid PagerDuty id passes the client, route and exact buckets once
				// each; an id that is not one UUID is charged only to the shared
				// bucket for such ids; the other routes pass the client and route
				// buckets once each.
				c, e, r := counters.clientHits, counters.exactHits, counters.routeHits
				want := [3]int{1, 0, 1}
				if sawBinding {
					want = [3]int{1, 1, 1}
					if _, err := uuid.Parse(bindingValue); err != nil || strings.Contains(bindingValue, "/") {
						want = [3]int{0, 1, 0}
					}
				}
				if method == http.MethodPost && [3]int{c, e, r} != want {
					t.Errorf("POST %s reached an intake handler counted client=%d exact=%d route=%d, want one pass", target, c, e, r)
				}
			}
		}
	}
	if reachedCount < 8 {
		t.Fatalf("only %d probes reached an intake handler: the probe no longer reaches the routes", reachedCount)
	}
}

func TestIntakeGateLogsALimiterStoreFailureOnce(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	failure := errors.New("dial tcp 10.9.8.7:6379: connection refused: password=hunter2 org=11111111-2222-3333-4444-555555555555")
	deps := Deps{}
	gate := newIntakeGate(&deps, logger, func(Deps) (httpapi.CounterStore, error) { return nil, failure })
	if gate == nil || deps.Limits == nil {
		t.Fatal("the gate or the in-process store was not built")
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("want exactly one log line, got %d: %q", len(lines), logs.String())
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["level"] != "ERROR" || entry["msg"] != "api: the shared rate limiter is unavailable; intake deliveries count in the in-process limiter" {
		t.Fatalf("log entry %v", entry)
	}
	for _, leaked := range []string{"10.9.8.7", "6379", "hunter2", "11111111-2222", "connection refused", "dial tcp"} {
		if strings.Contains(logs.String(), leaked) {
			t.Errorf("the log carries %q", leaked)
		}
	}
	if _, ok := entry["error"].(map[string]any); !ok {
		t.Fatalf("the failure is not logged as the error class group: %v", entry)
	}
	server, err := NewServerWithGate(apiTestConfig(), logger, webhookintake.Routes(webhookintake.Deps{Logger: logger}), gate)
	if err != nil {
		t.Fatal(err)
	}
	last := 0
	for i := 0; i < 61; i++ {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+uuid.NewString()[:36], nil)
		request.URL.Path = "/api/v1/webhooks/pagerduty/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		request.RemoteAddr = "203.0.113.50:4242"
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		last = recorder.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("the fallback gate answered %d on the 61st delivery, want 429", last)
	}
}

type exactKeyRecorder struct {
	*httpapi.MemoryCounters
	keys []string
}

func newExactKeyRecorder() *exactKeyRecorder {
	return &exactKeyRecorder{MemoryCounters: httpapi.NewMemoryCounters(nil)}
}

func (r *exactKeyRecorder) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	if hit.Limit.ID == "webhook_pagerduty" {
		r.keys = append(r.keys, hit.Path)
	}
	return r.MemoryCounters.Increment(ctx, hit)
}

// The gate keys the per-binding bucket on the id the handler will read: driven
// through the real server and router, a request that reaches the handler is
// keyed on exactly the canonical form of the id the handler's path value holds,
// and no request, reached or not, is keyed on the canonical form of an id that
// is not the whole remainder of its path.
func TestGateAndHandlerAcceptTheSameIdSpellings(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	var reachedWith []string
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	for index := range routes {
		routes[index].Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reachedWith = append(reachedWith, r.PathValue("binding_id"))
			w.WriteHeader(http.StatusNoContent)
		})
	}
	recorder := newExactKeyRecorder()
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(recorder, nil, quiet))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	canonical := id.String()
	const prefix = "/api/v1/webhooks/pagerduty/"
	targets := []string{
		canonical, strings.ToUpper(canonical), "urn:uuid:" + canonical, "{" + canonical + "}", strings.ReplaceAll(canonical, "-", ""),
		strings.ToUpper(canonical[:18]) + canonical[18:], "%31" + canonical[1:], "%2" + "F" + canonical, canonical + "%2F", canonical + "%2f",
		canonical + "%2Fextra", canonical + "/", canonical + "/extra", "extra/" + canonical, "/" + canonical, canonical[:35], canonical + "0",
		"not-a-uuid", "..", ".", "", strings.Repeat("a", 3000), "%67" + canonical[1:],
	}
	reached := 0
	for _, tail := range targets {
		reachedWith, recorder.keys = nil, nil
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		parsed, err := url.ParseRequestURI(prefix + tail)
		if err != nil {
			t.Errorf("tail %q does not parse as a request target: %v", tail, err)
			continue
		}
		request.URL = parsed
		request.RequestURI = prefix + tail
		request.RemoteAddr = "203.0.113.77:4242"
		server.Handler().ServeHTTP(httptest.NewRecorder(), request)
		if len(recorder.keys) != 1 {
			t.Errorf("tail %q: %d exact-bucket keys, want one (reached the handler %d times)", tail, len(recorder.keys), len(reachedWith))
			continue
		}
		key := recorder.keys[0]
		reached += len(reachedWith)
		for _, pv := range reachedWith {
			want := prefix + "malformed"
			if got, err := uuid.Parse(pv); err == nil && !strings.Contains(pv, "/") {
				want = prefix + got.String()
			}
			if key != want {
				t.Errorf("tail %q: the handler read %q, the gate keyed %q, want %q", tail, pv, key, want)
			}
		}
		if key == prefix+canonical {
			decoded, err := url.PathUnescape(strings.TrimPrefix(request.URL.EscapedPath(), prefix))
			if got, perr := uuid.Parse(decoded); err != nil || perr != nil || got != id || strings.Contains(decoded, "/") {
				t.Errorf("tail %q produced the canonical key of the valid id without being exactly that id", tail)
			}
		}
	}
	if reached < 8 {
		t.Fatalf("only %d probes reached the handler: the check no longer sees the router", reached)
	}
}

func intakeServerWithStubHandlers(t *testing.T) (*httpapi.Server, func(path, peer string) int) {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	for index := range routes {
		routes[index].Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	}
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(httpapi.NewMemoryCounters(nil), nil, quiet))
	if err != nil {
		t.Fatal(err)
	}
	send := func(path, peer string) int {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		request.RemoteAddr = peer + ":4242"
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder.Code
	}
	return server, send
}

func TestAMalformedBurstDoesNotSpendTheClientOrRouteBudget(t *testing.T) {
	_, send := intakeServerWithStubHandlers(t)
	const prefix = "/api/v1/webhooks/pagerduty/"
	for i := 0; i < 700; i++ {
		send(prefix+uuid.NewString()+"/extra", "198.51.100.10")
	}
	if got := send(prefix+uuid.NewString(), "198.51.100.10"); got != http.StatusNoContent {
		t.Fatalf("a valid delivery from the address that sent 700 malformed ones answered %d, want 204", got)
	}
	if got := send(prefix+uuid.NewString()+"/extra", "198.51.100.10"); got != http.StatusTooManyRequests {
		t.Fatalf("the malformed bucket did not refuse: %d", got)
	}
}

func TestAFloodOfOneIdDoesNotSpendTheRouteBudgetOfOtherIds(t *testing.T) {
	_, send := intakeServerWithStubHandlers(t)
	const prefix = "/api/v1/webhooks/pagerduty/"
	flooded := uuid.NewString()
	for i := 0; i < 700; i++ {
		send(prefix+flooded, "198.51.100.11")
	}
	if got := send(prefix+flooded, "198.51.100.11"); got != http.StatusTooManyRequests {
		t.Fatalf("the flooded id was not refused: %d", got)
	}
	for i := 0; i < 500; i++ {
		if got := send(prefix+uuid.NewString(), "198.51.100.11"); got != http.StatusNoContent {
			t.Fatalf("other id %d from the same address answered %d, want 204", i+1, got)
		}
	}
}

func TestFreshIdsCreateNoMoreExactCountersThanTheRouteBudget(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	for index := range routes {
		routes[index].Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	}
	recorder := newExactKeyRecorder()
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(recorder, nil, quiet))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+uuid.NewString(), nil)
		request.RemoteAddr = "198.51.100.30:4242"
		server.Handler().ServeHTTP(httptest.NewRecorder(), request)
	}
	distinct := map[string]bool{}
	for _, key := range recorder.keys {
		distinct[key] = true
	}
	if len(distinct) > 600 {
		t.Fatalf("5000 fresh ids from one address created %d exact counters, want at most the route budget of 600", len(distinct))
	}
}

func TestARouteRefusedDeliveryChargesNoExactCounter(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	for index := range routes {
		routes[index].Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	}
	recorder := newExactKeyRecorder()
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(recorder, nil, quiet))
	if err != nil {
		t.Fatal(err)
	}
	send := func(id string) int {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+id, nil)
		request.RemoteAddr = "198.51.100.31:4242"
		recorderResponse := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorderResponse, request)
		return recorderResponse.Code
	}
	for i := 0; i < 600; i++ {
		if got := send(uuid.NewString()); got != http.StatusNoContent {
			t.Fatalf("delivery %d answered %d, want 204", i+1, got)
		}
	}
	before := len(recorder.keys)
	refused := uuid.NewString()
	if got := send(refused); got != http.StatusTooManyRequests {
		t.Fatalf("the 601st fresh id answered %d, want 429 from the route budget", got)
	}
	if len(recorder.keys) != before {
		t.Fatalf("a delivery refused by the route budget still charged its exact counter (%d -> %d)", before, len(recorder.keys))
	}
}
