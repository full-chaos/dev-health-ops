package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// CHAOS-13073 red cells: QUERY_API_PDP_MODE=shadow feeds the 5.12 shadow core (internal/api/policy). Only the 5.12
// counters count in shadow mode; the legacy answer always wins.

const (
	shadowResultsMetric  = "dev_health_api_shadow_results_total"
	shadowMismatchMetric = "dev_health_api_shadow_mismatch_total"
	shadowInvalidMetric  = "dev_health_api_shadow_config_invalid"
	queryAPIResultsName  = "dev_health_query_api_pdp_results_total"
)

// shadowCounters reads the 5.12 instruments, which record through the global meter provider that TestMain binds to
// testMeterReader (main_test.go). The totals are cumulative over the test binary, so a test compares a snapshot taken
// before with one taken after.
func shadowCounters(t *testing.T) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := testMeterReader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					out[m.Name+"|"+attrKey(p.Attributes.ToSlice())] += p.Value
				}
			case metricdata.Gauge[int64]:
				for _, p := range data.DataPoints {
					out[m.Name+"|"+attrKey(p.Attributes.ToSlice())] = p.Value
				}
			}
		}
	}
	return out
}

func shadowDelta(before, after map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for key, v := range after {
		if !strings.HasPrefix(key, shadowResultsMetric+"|") && !strings.HasPrefix(key, shadowMismatchMetric+"|") {
			continue
		}
		if d := v - before[key]; d != 0 {
			out[key] = d
		}
	}
	return out
}

// settle waits for the shadow's background call to count; the shadow has no Wait, so a test polls.
func settle(t *testing.T, before map[string]int64, want map[string]int64) map[string]int64 {
	t.Helper()
	var got map[string]int64
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		got = shadowDelta(before, shadowCounters(t))
		if fmt.Sprint(got) == fmt.Sprint(want) {
			return got
		}
	}
	return got
}

// settleQuiet is a fixed grace period for cells that expect nothing to be counted.
func settleQuiet(t *testing.T, before map[string]int64) map[string]int64 {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	return shadowDelta(before, shadowCounters(t))
}

type funcDecider struct {
	decide func(context.Context, pep.Request) pep.Decision
}

func (d funcDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	return d.decide(ctx, req)
}
func (d funcDecider) DecideBatch(context.Context, []pep.Request) []pep.Decision {
	panic("DecideBatch is not used by the shadow")
}

func pepOf(t *testing.T, base string) pep.Decider {
	t.Helper()
	client, err := pep.New(pep.Config{BaseURL: base, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func knobs(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) { v, ok := env[name]; return v, ok }
}

// newShadowREST is the gated REST fixture with a shadow-mode gate over decider, the 5.12 knobs read from env.
func newShadowREST(t *testing.T, decider pep.Decider, env map[string]string) (*gatedREST, *bytes.Buffer) {
	t.Helper()
	quietStdLog(t)
	shadowCounters(t)
	m := newAccessMinter(t)
	f := newRESTFixture(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow))
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var logs bytes.Buffer
	gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: decider, Lookup: knobs(env), Meter: provider.Meter("test"),
		Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	return &gatedREST{handler: planeHandler(f.handler.(*http.ServeMux), gate), reader: reader, gate: gate, f: f, minter: m}, &logs
}

// get is one GET and the time the handler took; it does not wait for the shadow.
func (g *gatedREST) get(bearer string) (answer, time.Duration) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	start := time.Now()
	g.handler.ServeHTTP(rec, req)
	took := time.Since(start)
	body, _ := io.ReadAll(rec.Result().Body)
	return answer{rec.Code, strings.TrimSpace(string(body))}, took
}

func (g *gatedREST) orglessUserToken() string {
	return g.minter.mint(ecUser, map[string]any{"org_id": nil}, nil)
}

// 2a/2b: PDP deny is a mismatch and PDP allow a match, the answer is byte-equal to the gate-less answer, and the
// #5679 counter never carries the shadow label value.
func TestRESTShadowCountsOnTheCoreAndNeverChangesTheAnswer(t *testing.T) {
	for name, tc := range map[string]struct {
		deny bool
		want map[string]int64
	}{
		"deny": {true, map[string]int64{
			shadowResultsMetric + "|reason=none,result=mismatch":                        1,
			shadowMismatchMetric + "|caller=admin,class=unresolved,namespace=analytics": 1,
		}},
		"allow": {false, map[string]int64{shadowResultsMetric + "|reason=none,result=match": 1}},
	} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			if tc.deny {
				srv.Deny("no_membership")
			}
			g, _ := newShadowREST(t, pepOf(t, srv.URL), nil)
			want := g.f.do(http.MethodGet, g.userToken())
			before := shadowCounters(t)
			got, _ := g.get(g.userToken())
			if got != want || got != gatedOK {
				t.Fatalf("answer %+v, gate-less %+v, want %+v", got, want, gatedOK)
			}
			if d := settle(t, before, tc.want); fmt.Sprint(d) != fmt.Sprint(tc.want) {
				t.Fatalf("5.12 counters %v, want %v", d, tc.want)
			}
			if series := g.counter(t, queryAPIResultsName); len(series) != 0 {
				t.Fatalf("shadow counted on the #5679 counter: %v", series)
			}
			reqs := srv.Requests()
			if len(reqs) != 1 || reqs[0].Action != "analytics.read" || reqs[0].OrganizationID != ecOrg.String() || reqs[0].ResourceKey != ecOrg.String() {
				t.Fatalf("PDP calls %+v", reqs)
			}
		})
	}
}

// 2c: PDP down, 503, 429 and a slow PDP change nothing in the answer and the handler never waits for the PDP.
func TestRESTShadowPDPFailureHasNoEffect(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(3 * time.Second) }))
	t.Cleanup(slow.Close)
	fail503, fail429 := pdptest.New(t), pdptest.New(t)
	fail503.Fail(http.StatusServiceUnavailable)
	fail429.Fail(http.StatusTooManyRequests)
	for name, base := range map[string]string{"down": closedURL, "503": fail503.URL, "429": fail429.URL, "slow": slow.URL} {
		t.Run(name, func(t *testing.T) {
			g, _ := newShadowREST(t, pepOf(t, base), nil)
			before := shadowCounters(t)
			got, took := g.get(g.userToken())
			if got != gatedOK {
				t.Fatalf("answer %+v, want %+v", got, gatedOK)
			}
			//abstime:allow hang ceiling: the slow PDP sleeps 3s, so a request that waited for it is far over this
			if took > 500*time.Millisecond {
				t.Fatalf("the request waited %v for the PDP", took)
			}
			want := map[string]int64{shadowResultsMetric + "|reason=none,result=error": 1}
			if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
				t.Fatalf("5.12 counters %v, want %v", d, want)
			}
		})
	}
}

// 2d: a decider that panics.
func TestRESTShadowDeciderPanicHasNoEffect(t *testing.T) {
	g, _ := newShadowREST(t, funcDecider{func(context.Context, pep.Request) pep.Decision { panic("boom") }}, nil)
	before := shadowCounters(t)
	if got, _ := g.get(g.userToken()); got != gatedOK {
		t.Fatalf("answer %+v, want %+v", got, gatedOK)
	}
	want := map[string]int64{shadowResultsMetric + "|reason=none,result=error": 1}
	if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
		t.Fatalf("5.12 counters %v, want %v", d, want)
	}
}

// 2e: mode off builds no shadow, asks nothing and counts nothing on either family.
func TestRESTShadowOffBuildsNothing(t *testing.T) {
	srv := pdptest.New(t)
	for _, mode := range []string{"", pdp.ModeOff} {
		quietStdLog(t)
		before := shadowCounters(t)
		m := newAccessMinter(t)
		f := newRESTFixture(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow))
		gate := pdp.New(pdp.Config{Mode: mode, Decider: pepOf(t, srv.URL)})
		if gate.Shadow() != nil {
			t.Fatalf("mode %q built a shadow", mode)
		}
		h := planeHandler(f.handler.(*http.ServeMux), gate)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
		req.Header.Set("Authorization", "Bearer "+m.mint(ecUser, nil, nil))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("mode %q: status %d", mode, rec.Code)
		}
		if d := settleQuiet(t, before); len(d) != 0 {
			t.Fatalf("mode %q counted %v", mode, d)
		}
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("off asked the PDP %d times", n)
	}
}

// 2f: a user token with no organization is the legacy 403: unchanged, counted skipped, no PDP call.
func TestRESTShadowNoOrgDenyIsSkipped(t *testing.T) {
	srv := pdptest.New(t)
	g, _ := newShadowREST(t, pepOf(t, srv.URL), nil)
	before := shadowCounters(t)
	got, _ := g.get(g.orglessUserToken())
	if want := (answer{http.StatusForbidden, `{"detail":"Organization context required"}`}); got != want {
		t.Fatalf("answer %+v, want %+v", got, want)
	}
	want := map[string]int64{shadowResultsMetric + "|reason=none,result=skipped": 1}
	if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
		t.Fatalf("5.12 counters %v, want %v", d, want)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("PDP asked %d times", n)
	}
}

// 401 refusals (no header, a bad token) are not observed.
func TestRESTShadowDoesNotObserveA401(t *testing.T) {
	srv := pdptest.New(t)
	g, _ := newShadowREST(t, pepOf(t, srv.URL), nil)
	before := shadowCounters(t)
	good := g.userToken()
	tampered := good[:len(good)-4] + "AAAA"
	for name, token := range map[string]string{"no header": "", "bad token": "not-a-token", "tampered user token": tampered} {
		if got, _ := g.get(token); got.Status != http.StatusUnauthorized {
			t.Fatalf("%s: status %d, want 401", name, got.Status)
		}
	}
	if d := settleQuiet(t, before); len(d) != 0 {
		t.Fatalf("a 401 was observed: %v", d)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("PDP asked %d times", n)
	}
}

// 2g/2i: the observe seam. A legacy deny with a PDP allow is a mismatch of class unresolved; a delegated session is
// impersonation_refused with no call, as in go-api.
func TestRESTShadowObserveSeam(t *testing.T) {
	for name, tc := range map[string]struct {
		legacyAllowed, delegated, pdpDeny bool
		caller                            string
		want                              map[string]int64
		calls                             int
	}{
		"legacy deny, PDP allow": {false, false, false, "admin", map[string]int64{
			shadowResultsMetric + "|reason=none,result=mismatch":                        1,
			shadowMismatchMetric + "|caller=admin,class=unresolved,namespace=analytics": 1}, 1},
		"legacy allow, PDP allow": {true, false, false, "superuser", map[string]int64{shadowResultsMetric + "|reason=none,result=match": 1}, 1},
		"delegated":               {true, true, false, "member", map[string]int64{shadowResultsMetric + "|reason=none,result=impersonation_refused": 1}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			g, _ := newShadowREST(t, pepOf(t, srv.URL), nil)
			before := shadowCounters(t)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
			req.Pattern = "GET /api/v1/home"
			req = req.WithContext(pdp.WithGate(req.Context(), g.gate))
			observeRESTShadow(req, g.userToken(), ecOrg.String(), tc.caller, tc.delegated, tc.legacyAllowed)
			if d := settle(t, before, tc.want); fmt.Sprint(d) != fmt.Sprint(tc.want) {
				t.Fatalf("5.12 counters %v, want %v", d, tc.want)
			}
			if n := len(srv.Requests()); n != tc.calls {
				t.Fatalf("PDP asked %d times, want %d", n, tc.calls)
			}
		})
	}
}

// 2h and the acp condition: EVERY legacy-allow request on a mapped route is observed, whatever the credential class.
// A user token asks the PDP (match); the envelope is counted skipped; the #5679 exempt counter is unchanged.
func TestRESTShadowObservesEveryLegacyAllow(t *testing.T) {
	skippedEnvelope := map[string]int64{shadowResultsMetric + "|reason=envelope,result=skipped": 1}
	matched := map[string]int64{shadowResultsMetric + "|reason=none,result=match": 1}
	for name, routed := range map[string]bool{"routed": true, "unrouted": false} {
		for class, tc := range map[string]struct {
			token func(*gatedREST, *testing.T) string
			want  map[string]int64
			calls int
		}{
			"user token": {func(g *gatedREST, _ *testing.T) string { return g.userToken() }, matched, 1},
			"envelope":   {func(g *gatedREST, _ *testing.T) string { return g.f.envelope }, skippedEnvelope, 0},
		} {
			if class == "user token" && !routed {
				continue
			}
			t.Run(name+"/"+class, func(t *testing.T) {
				srv := pdptest.New(t)
				quietStdLog(t)
				m := newAccessMinter(t)
				setup := testAccess(m.keys, nil, accessTestNow)
				if !routed {
					setup = nil
				}
				f := newRESTFixture(t, newAccessStore(), setup)
				reader := sdkmetric.NewManualReader()
				provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
				t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
				gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: pepOf(t, srv.URL), Meter: provider.Meter("test"),
					Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
				g := &gatedREST{handler: planeHandler(f.handler.(*http.ServeMux), gate), reader: reader, gate: gate, f: f, minter: m}
				before := shadowCounters(t)
				if got, _ := g.get(tc.token(g, t)); got.Status != http.StatusOK {
					t.Fatalf("status %d, want 200", got.Status)
				}
				if d := settle(t, before, tc.want); fmt.Sprint(d) != fmt.Sprint(tc.want) {
					t.Fatalf("5.12 counters %v, want exactly %v (one observation per legacy allow)", d, tc.want)
				}
				if n := len(srv.Requests()); n != tc.calls {
					t.Fatalf("PDP asked %d times, want %d", n, tc.calls)
				}
				if class != "user token" {
					if got := g.counter(t, "dev_health_query_api_pdp_exempt_total"); len(got) != 1 {
						t.Fatalf("exempt counter %v, want one series as before", got)
					}
				}
			})
		}
	}
}

// 2i on the route: a user token that carries an actor (a delegated session) is impersonation_refused once, with no PDP call.
func TestRESTShadowDelegatedUserTokenIsImpersonationRefused(t *testing.T) {
	srv := pdptest.New(t)
	g, _ := newShadowREST(t, pepOf(t, srv.URL), nil)
	before := shadowCounters(t)
	actor := wirePrincipal(uuid.MustParse("22222222-2222-4222-8222-222222222222"))
	if got, _ := g.get(g.minter.mint(ecUser, map[string]any{"act": map[string]any{"sub": actor}}, nil)); got != gatedOK {
		t.Fatalf("answer %+v, want %+v", got, gatedOK)
	}
	want := map[string]int64{shadowResultsMetric + "|reason=none,result=impersonation_refused": 1}
	if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
		t.Fatalf("5.12 counters %v, want %v", d, want)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("PDP asked %d times for a delegated session", n)
	}
}

// 2j: the in-flight bound: a second call while the first is blocked is dropped, and the answer is unchanged.
func TestRESTShadowInFlightFullIsDropped(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	g, _ := newShadowREST(t, funcDecider{func(ctx context.Context, req pep.Request) pep.Decision {
		started <- struct{}{}
		<-release
		return pep.Decision{}
	}}, map[string]string{policy.EnvShadowPDPInFlight: "1"})
	before := shadowCounters(t)
	if got, _ := g.get(g.userToken()); got != gatedOK {
		t.Fatalf("first: %+v", got)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("the first call never reached the decider")
	}
	if got, _ := g.get(g.userToken()); got != gatedOK {
		t.Fatalf("second: %+v", got)
	}
	want := map[string]int64{shadowResultsMetric + "|reason=none,result=dropped": 1}
	got := settle(t, before, want)
	close(release)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("5.12 counters %v, want %v", got, want)
	}
}

// 2k: AUTH_SHADOW_PDP_SAMPLE=50 asks for 50 of 100 requests. The in-flight cap is 100 so a slow PDP drops none.
func TestRESTShadowSampleKnob(t *testing.T) {
	srv := pdptest.New(t)
	g, _ := newShadowREST(t, pepOf(t, srv.URL), map[string]string{policy.EnvShadowPDPSample: "50", policy.EnvShadowPDPInFlight: "100"})
	before := shadowCounters(t)
	for i := 0; i < 100; i++ {
		if got, _ := g.get(g.userToken()); got != gatedOK {
			t.Fatalf("request %d: %+v", i, got)
		}
	}
	g.gate.Shadow().Wait()
	if n := len(srv.Requests()); n != 50 {
		t.Fatalf("PDP asked %d times for a 50 percent sample of 100, want 50", n)
	}
	if dropped := shadowDelta(before, shadowCounters(t))[shadowResultsMetric+"|reason=none,result=dropped"]; dropped != 0 {
		t.Fatalf("%d sampled calls dropped, want 0", dropped)
	}
}

// 2l: the shadow's rows are every REST route's protected row with an action: none is a GraphQL or /query row, and the
// two-row POST /query does not fail the build.
func TestRESTShadowRowsAreTheRESTRoutes(t *testing.T) {
	rows, err := pdp.RESTShadowRows(authv1.QueryAPIEndpointProfiles)
	if err != nil {
		t.Fatalf("RESTShadowRows: %v", err)
	}
	patterns := map[string]bool{}
	for _, route := range RESTRoutes() {
		patterns[route.Method+" "+route.Pattern] = true
	}
	want := map[string]string{}
	for id, action := range routeActions {
		key, _, _ := strings.Cut(id, " @")
		if patterns[key] {
			want[key] = action
		}
	}
	for key, action := range want {
		if row, ok := rows[key]; !ok || row.Action != action {
			t.Errorf("REST route %q (action %s) has shadow row %+v", key, action, row)
		}
	}
	for key := range rows {
		if !patterns[key] {
			t.Errorf("%q is a shadow row but no REST route", key)
		}
	}
	for _, key := range []string{"GET /graphql", "POST /graphql", "POST /query", "POST /query/proof", "POST /query/run-operation"} {
		if _, ok := rows[key]; ok {
			t.Errorf("%q is a GraphQL or /query row, not a REST row", key)
		}
	}
	if _, ok := rows["GET /api/v1/home"]; !ok {
		t.Error("GET /api/v1/home has no shadow row")
	}
}

// 2m: a bad knob never fails the start: no shadow, one ERROR naming the variable and no value, the config-invalid
// gauge is 1, and the route still answers.
func TestRESTShadowBadKnob(t *testing.T) {
	srv := pdptest.New(t)
	for name, tc := range map[string]struct{ variable, value string }{
		"sample zero":    {policy.EnvShadowPDPSample, "0"},
		"sample letters": {policy.EnvShadowPDPSample, "abc-secret"},
		"in flight":      {policy.EnvShadowPDPInFlight, "zero-secret"},
		"high risk":      {policy.EnvShadowPDPHighRisk, "maybe-secret"},
	} {
		t.Run(name, func(t *testing.T) {
			g, logs := newShadowREST(t, pepOf(t, srv.URL), map[string]string{tc.variable: tc.value})
			if g.gate.Shadow() != nil {
				t.Fatal("a bad knob left a shadow built")
			}
			if got, _ := g.get(g.userToken()); got != gatedOK {
				t.Fatalf("answer %+v, want %+v", got, gatedOK)
			}
			text := logs.String()
			if strings.Count(text, "level=ERROR") != 1 || !strings.Contains(text, "variable="+tc.variable) || strings.Contains(text, "secret") {
				t.Fatalf("log %q, want one ERROR naming %s and no value", text, tc.variable)
			}
			if got := shadowCounters(t)[shadowInvalidMetric+"|"]; got != 1 {
				t.Fatalf("config_invalid = %d, want 1", got)
			}
		})
	}
}

// 2n: every label value either family records is from a closed set: no org, user, token, route or path value.
func TestRESTShadowLabelCensus(t *testing.T) {
	srv := pdptest.New(t)
	g, _ := newShadowREST(t, pepOf(t, srv.URL), nil)
	srv.Deny("no_membership")
	_, _ = g.get(g.userToken())
	srv.Fail(http.StatusServiceUnavailable)
	_, _ = g.get(g.userToken())
	_, _ = g.get(g.orglessUserToken())
	time.Sleep(200 * time.Millisecond)
	closed := map[string][]string{
		"result":    {"match", "mismatch", "error", "dropped", "skipped", "impersonation_refused"},
		"reason":    policy.ShadowReasons(),
		"caller":    {"member", "admin", "superuser"},
		"class":     {"legacy_bug", "new_bug", "unresolved", "intentional"},
		"namespace": {"admin", "analytics", "billing", "git", "integrations", "license", "members", "metrics", "org", "reports", "settings", "teams", "work_items"},
	}
	for key := range shadowCounters(t) {
		name, attrs, _ := strings.Cut(key, "|")
		if name != shadowResultsMetric && name != shadowMismatchMetric {
			continue
		}
		for _, pair := range strings.Split(attrs, ",") {
			k, v, _ := strings.Cut(pair, "=")
			if !slices.Contains(closed[k], v) {
				t.Errorf("%s: label %s=%q is outside its closed set", name, k, v)
			}
		}
	}
}

// Enforce mode is byte-for-byte what #5679 shipped: the same answers, the same counter series, and nothing on the
// 5.12 counters.
func TestRESTEnforceUnchangedByTheShadowSwitch(t *testing.T) {
	srv := pdptest.New(t)
	g := newGatedREST(t, pdp.ModeEnforce, "analytics", srv.URL, true)
	before := shadowCounters(t)
	if got := g.do(http.MethodGet, g.userToken()); got != gatedOK {
		t.Fatalf("allow: %+v", got)
	}
	srv.Deny("role_lacks_action")
	if got := g.do(http.MethodGet, g.userToken()); got != gatedForbidden {
		t.Fatalf("deny: %+v", got)
	}
	srv.Fail(http.StatusServiceUnavailable)
	if got := g.do(http.MethodGet, g.userToken()); got != gatedUnavailable {
		t.Fatalf("down: %+v", got)
	}
	want := fmt.Sprint(map[string]int64{"action=analytics.read,mode=enforce,result=allow": 1,
		"action=analytics.read,mode=enforce,result=deny": 1, "action=analytics.read,mode=enforce,result=unavailable": 1})
	if got := fmt.Sprint(g.counter(t, queryAPIResultsName)); got != want {
		t.Fatalf("enforce series %s, want %s", got, want)
	}
	if d := settleQuiet(t, before); len(d) != 0 {
		t.Fatalf("enforce counted on the 5.12 counters: %v", d)
	}
}

func attrKey(kvs []attribute.KeyValue) string {
	var parts []string
	for _, kv := range kvs {
		parts = append(parts, string(kv.Key)+"="+kv.Value.Emit())
	}
	return strings.Join(parts, ",")
}

// The caller label comes from the verified caller: a live superuser is counted as a superuser.
func TestRESTShadowCallerKindFromTheVerifiedCaller(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	g, _ := newShadowREST(t, pepOf(t, srv.URL), nil)
	store := newAccessStore()
	store.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	g.f = newRESTFixture(t, store, testAccess(g.minter.keys, nil, accessTestNow))
	g.handler = planeHandler(g.f.handler.(*http.ServeMux), g.gate)
	before := shadowCounters(t)
	if got, _ := g.get(g.userToken()); got != gatedOK {
		t.Fatalf("answer %+v, want %+v", got, gatedOK)
	}
	want := map[string]int64{
		shadowResultsMetric + "|reason=none,result=mismatch":                            1,
		shadowMismatchMetric + "|caller=superuser,class=unresolved,namespace=analytics": 1,
	}
	if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
		t.Fatalf("5.12 counters %v, want %v", d, want)
	}
}

// A credential class that never asks the PDP has one reason from the closed list; a user token has "none".
func TestRESTShadowSkipReasonsAreClosed(t *testing.T) {
	for class, want := range map[principal.AccessClass]string{
		principal.AccessClassEdge: "hs256", principal.AccessClassOther: "envelope", principal.AccessClassUser: "none",
	} {
		if got := shadowSkipReason(class); got != want {
			t.Errorf("class %v: reason %q, want %q", class, got, want)
		}
	}
}

// R1: the hs256 and envelope paths record the reason on the counter, from the one closed list the go-api core owns.
func TestRESTShadowSkipReasonsAreTheCoreList(t *testing.T) {
	for _, class := range []principal.AccessClass{principal.AccessClassEdge, principal.AccessClassOther, principal.AccessClassUser} {
		if reason := shadowSkipReason(class); !slices.Contains(policy.ShadowReasons(), reason) {
			t.Errorf("class %v: reason %q is outside %v", class, reason, policy.ShadowReasons())
		}
	}
}

// recordingHomeClient answers like an empty organization and keeps every statement with its bindings, in order.
type recordingHomeClient struct {
	mu         sync.Mutex
	statements []string
}

func (c *recordingHomeClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, fmt.Sprintf("%s | %v", query, bindings))
	return emptyOrSizingRow(query), nil
}

// R2: a PDP decision WITH obligations never changes the legacy answer (status and body byte-equal to the gate-less
// answer) and never changes the SQL the handler runs (recording client). It replaces #5679's
// TestPDPShadowIgnoresObligations on the new shadow path.
func TestRESTShadowDecisionWithObligationsNeverChangesTheLegacyAnswerOrTheSQL(t *testing.T) {
	serve := func(t *testing.T, gate *pdp.Gate) (answer, []string) {
		t.Helper()
		m := newAccessMinter(t)
		client := &recordingHomeClient{}
		f := newRESTFixtureServing(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), newHomeGetHandler(client, nil))
		mux := f.handler.(*http.ServeMux)
		var handler http.Handler = mux
		if gate != nil {
			handler = planeHandler(mux, gate)
		}
		req := httptest.NewRequest(http.MethodGet, "/api/v1/home?scope_type=org", nil)
		req.Header.Set("Authorization", "Bearer "+m.mint(ecUser, nil, nil))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if gate != nil {
			gate.Shadow().Wait()
		}
		body, _ := io.ReadAll(rec.Result().Body)
		client.mu.Lock()
		defer client.mu.Unlock()
		statements := slices.Clone(client.statements)
		slices.Sort(statements)
		return answer{rec.Code, strings.TrimSpace(string(body))}, statements
	}
	quietStdLog(t)
	baseAnswer, baseSQL := serve(t, nil)
	if baseAnswer.Status != http.StatusOK || len(baseSQL) == 0 {
		t.Fatalf("gate-less run: %+v with %d statements, want a 200 that reads", baseAnswer, len(baseSQL))
	}
	srv := pdptest.New(t)
	srv.Obligations(map[string]any{"tenant_scope": strings.ToLower(ecOrg.String()), "repository_allowlist": []string{"acme/widgets"}})
	gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: pepOf(t, srv.URL), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	before := shadowCounters(t)
	gotAnswer, gotSQL := serve(t, gate)
	if gotAnswer != baseAnswer {
		t.Fatalf("answer with obligations %+v, gate-less %+v: not byte-equal", gotAnswer, baseAnswer)
	}
	if !slices.Equal(gotSQL, baseSQL) {
		t.Fatalf("SQL with obligations %q, gate-less %q: not equal", gotSQL, baseSQL)
	}
	if n := len(srv.Requests()); n != 1 {
		t.Fatalf("PDP asked %d times, want 1 (the cell must reach the shadow)", n)
	}
	want := map[string]int64{shadowResultsMetric + "|reason=none,result=match": 1}
	if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
		t.Fatalf("5.12 counters %v, want %v", d, want)
	}
}
