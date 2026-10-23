package webhookintake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const throttledBody = `{"detail":{"message":"Rate limit exceeded. Please try again later."}}`

func withLimits(t *testing.T, client, route, providerRoute int) {
	t.Helper()
	previousClient, previousTemplate, previousProvider := intakeClientLimit, pagerdutyTemplateLimit, providerRouteLimit
	intakeClientLimit.Count, pagerdutyTemplateLimit.Count, providerRouteLimit.Count = client, route, providerRoute
	t.Cleanup(func() {
		intakeClientLimit, pagerdutyTemplateLimit, providerRouteLimit = previousClient, previousTemplate, previousProvider
	})
}

func newTestGate(deps Deps) *Gate {
	return &Gate{limiters: newRateLimiters(deps.Counters, deps.Now), logger: deps.logger()}
}

// gateHandler is the api's intake gate around one handler, as the server
// middleware runs it.
func gateHandler(gate *Gate, provider string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse := gate.Admit(r, provider); refuse != nil {
			refuse(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func intakeRoutes(t *testing.T, deps Deps) map[string]http.Handler {
	t.Helper()
	t.Setenv("TRUSTED_PROXIES", "")
	gate := newTestGate(deps)
	providers := map[string]string{}
	for provider, template := range routeTemplates {
		providers[template] = provider
	}
	out := map[string]http.Handler{}
	for _, route := range Routes(deps) {
		if provider, ok := providers[route.Pattern]; ok {
			out[route.Pattern] = gateHandler(gate, provider, route.Handler)
			continue
		}
		out[route.Pattern] = route.Handler
	}
	return out
}

func deliver(handler http.Handler, target, peer, forwarded string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, target, nil)
	if id, ok := strings.CutPrefix(target, routePrefix+"/pagerduty/"); ok {
		request.SetPathValue("binding_id", id)
	}
	request.RemoteAddr = peer + ":4242"
	if forwarded != "" {
		request.Header.Set("X-Forwarded-For", forwarded)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestPagerDutyRouteBucketBoundsIdsFromOneAddress(t *testing.T) {
	noBindings(t)
	withLimits(t, 100000, 5, 100000)
	now := time.Now()
	routes := intakeRoutes(t, Deps{Counters: httpapi.NewMemoryCounters(func() time.Time { return now })})
	handler := routes[routePrefix+"/pagerduty/{binding_id}"]
	for i := range 5 {
		if got := deliver(handler, routePrefix+"/pagerduty/"+uuid.NewString(), "192.0.2.1", "").Code; got != http.StatusUnauthorized {
			t.Fatalf("delivery %d = %d, want 401", i+1, got)
		}
	}
	for _, id := range []string{uuid.NewString(), uuid.NewString()} {
		got := deliver(handler, routePrefix+"/pagerduty/"+id, "192.0.2.1", "")
		if got.Code != http.StatusTooManyRequests || got.Body.String() != throttledBody {
			t.Fatalf("id %s past the route bucket = %d %s", id, got.Code, got.Body.String())
		}
	}
	if got := deliver(handler, routePrefix+"/pagerduty/not-a-uuid", "192.0.2.1", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("an id that is not a UUID spent the route budget: %d", got)
	}
	if got := deliver(handler, routePrefix+"/pagerduty/"+uuid.NewString(), "192.0.2.2", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("another address = %d, want its own budget", got)
	}
}

func TestClientBucketCountsEveryIntakeRoute(t *testing.T) {
	noBindings(t)
	withLimits(t, 6, 100000, 100000)
	now := time.Now()
	routes := intakeRoutes(t, Deps{Counters: httpapi.NewMemoryCounters(func() time.Time { return now })})
	targets := []string{routePrefix + "/github", routePrefix + "/gitlab", routePrefix + "/jira", routePrefix + "/pagerduty/" + uuid.NewString()}
	patterns := []string{routePrefix + "/github", routePrefix + "/gitlab", routePrefix + "/jira", routePrefix + "/pagerduty/{binding_id}"}
	for i := range 6 {
		if got := deliver(routes[patterns[i%4]], targets[i%4], "192.0.2.1", "").Code; got == http.StatusTooManyRequests {
			t.Fatalf("delivery %d throttled early", i+1)
		}
	}
	for index, pattern := range patterns {
		got := deliver(routes[pattern], targets[index], "192.0.2.1", "")
		if got.Code != http.StatusTooManyRequests || got.Body.String() != throttledBody {
			t.Fatalf("%s past the client bucket = %d %s", pattern, got.Code, got.Body.String())
		}
	}
	if got := deliver(routes[patterns[0]], targets[0], "192.0.2.2", "").Code; got == http.StatusTooManyRequests {
		t.Fatal("another address shares the bucket")
	}
}

func TestProviderRouteBucketIsPerRoute(t *testing.T) {
	withLimits(t, 100000, 100000, 3)
	now := time.Now()
	routes := intakeRoutes(t, Deps{Counters: httpapi.NewMemoryCounters(func() time.Time { return now })})
	for _, route := range []string{"/github", "/gitlab", "/jira"} {
		for i := range 3 {
			if got := deliver(routes[routePrefix+route], routePrefix+route, "192.0.2.1", "").Code; got == http.StatusTooManyRequests {
				t.Fatalf("%s delivery %d throttled early", route, i+1)
			}
		}
		if got := deliver(routes[routePrefix+route], routePrefix+route, "192.0.2.1", ""); got.Code != http.StatusTooManyRequests || got.Body.String() != throttledBody {
			t.Fatalf("%s past its bucket = %d %s", route, got.Code, got.Body.String())
		}
	}
}

func TestForgedForwardingHeaderFromAnUntrustedPeerMintsNoBucket(t *testing.T) {
	withLimits(t, 100000, 100000, 2)
	now := time.Now()
	routes := intakeRoutes(t, Deps{Counters: httpapi.NewMemoryCounters(func() time.Time { return now })})
	handler := routes[routePrefix+"/github"]
	var last *httptest.ResponseRecorder
	for i := range 4 {
		last = deliver(handler, routePrefix+"/github", "192.0.2.1", fmt.Sprintf("203.0.113.%d", i+1))
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("a changing X-Forwarded-For escaped the bucket: %d", last.Code)
	}
}

func TestThrottledAnswerIsTheSameForKnownAndUnknownIdsAndReadsNothing(t *testing.T) {
	f := newRefusalFixture(t, "active")
	withLimits(t, 100000, 2, 100000)
	now := time.Now()
	f.deps.Counters = httpapi.NewMemoryCounters(func() time.Time { return now })
	handler := gateHandler(newTestGate(f.deps), providerPagerDuty, f.deps.handlePagerDutyWebhook())
	for range 2 {
		hitPagerDuty(handler, uuid.NewString(), "192.0.2.1", "")
	}
	loads := f.loads
	var answers []string
	for _, id := range []string{f.bindingID, uuid.NewString(), uuid.NewString()} {
		got := hitPagerDuty(handler, id, "192.0.2.1", "")
		answers = append(answers, fmt.Sprintf("%d %s %v", got.Code, got.Body.String(), got.Header()))
	}
	if answers[0] != answers[1] || answers[1] != answers[2] || !strings.HasPrefix(answers[0], "429 "+throttledBody) {
		t.Fatalf("throttled answers differ: %q", answers)
	}
	if f.loads != loads {
		t.Fatalf("throttled deliveries read %d bindings", f.loads-loads)
	}
}

func TestThrottleLogsTheProviderAndReasonClassOnly(t *testing.T) {
	withLimits(t, 100000, 100000, 1)
	var logs bytes.Buffer
	now := time.Now()
	routes := intakeRoutes(t, Deps{Counters: httpapi.NewMemoryCounters(func() time.Time { return now }), Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	handler := routes[routePrefix+"/jira"]
	deliver(handler, routePrefix+"/jira", "192.0.2.77", "")
	logs.Reset()
	deliver(handler, routePrefix+"/jira", "192.0.2.77", "")
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry); err != nil {
		t.Fatalf("want one log line, got %q", logs.String())
	}
	if entry["level"] != "WARN" || entry["msg"] != "webhook_intake.throttled" || entry["provider"] != "jira" || entry["reason"] != "route_bucket" || len(entry) != 5 {
		t.Fatalf("log entry %v", entry)
	}
	if strings.Contains(logs.String(), "192.0.2.77") {
		t.Fatal("the log carries the client address")
	}
}

func TestStoreFailureLetsProviderDeliveriesThroughAndRefusesPagerDuty(t *testing.T) {
	noBindings(t)
	var logs bytes.Buffer
	routes := intakeRoutes(t, Deps{Counters: failingCounters{}, Secrets: Secrets{GitHub: "s"}, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	got := deliver(routes[routePrefix+"/github"], routePrefix+"/github", "192.0.2.1", "")
	if got.Code != http.StatusUnprocessableEntity {
		t.Fatalf("a delivery with the store down = %d %s, want the handler's own refusal of the missing event headers (422)", got.Code, got.Body.String())
	}
	if !strings.Contains(logs.String(), `"msg":"webhook_intake.limiter_unavailable"`) || !strings.Contains(logs.String(), `"provider":"github"`) {
		t.Fatalf("no warning for the unavailable limiter: %q", logs.String())
	}
	pd := deliver(routes[routePrefix+"/pagerduty/{binding_id}"], routePrefix+"/pagerduty/"+uuid.NewString(), "192.0.2.1", "")
	if pd.Code != http.StatusInternalServerError || pd.Body.String() != `{"detail":"Internal Server Error"}` {
		t.Fatalf("pagerduty with the store down = %d %s", pd.Code, pd.Body.String())
	}
}
