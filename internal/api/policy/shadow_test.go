package policy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// shadowBearer matches the PDP's principal_reference pattern, so the pep client sends it.
const shadowBearer = "aaaa.bbbb.cccc-dddd-eeee-ffff"

type shadowRecorder struct {
	mu       sync.Mutex
	mismatch []string
	results  []string
}

func recordShadow(t *testing.T) *shadowRecorder {
	t.Helper()
	rec := &shadowRecorder{}
	prevMismatch, prevResult := shadowMismatchAdd, shadowResultAdd
	shadowMismatchAdd = func(_ context.Context, namespace, caller, class string) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.mismatch = append(rec.mismatch, namespace+"/"+caller+"/"+class)
	}
	shadowResultAdd = func(_ context.Context, result string) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.results = append(rec.results, result)
	}
	t.Cleanup(func() { shadowMismatchAdd, shadowResultAdd = prevMismatch, prevResult })
	return rec
}

func (r *shadowRecorder) snapshot() (mismatch, results []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.mismatch), slices.Clone(r.results)
}

type pdpSeen struct {
	Principal, Action, Type, Org, Key, Service string
	HasOrg                                     bool
}

type testPDP struct {
	srv     *httptest.Server
	verdict func(action string) (bool, string)
	// obligations, when set, ride on every allow.
	obligations map[string]any
	mu          sync.Mutex
	seen        []pdpSeen
}

func newTestPDP(t *testing.T, verdict func(action string) (bool, string)) *testPDP {
	t.Helper()
	p := &testPDP{verdict: verdict}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PrincipalReference string `json:"principal_reference"`
			Action             string `json:"action"`
			Resource           struct {
				Type, Key string
				Org       *string `json:"organization_id"`
			} `json:"resource"`
			Context struct {
				ClientService string `json:"client_service"`
			} `json:"context"`
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		seen := pdpSeen{Principal: body.PrincipalReference, Action: body.Action, Type: body.Resource.Type, Key: body.Resource.Key, Service: body.Context.ClientService}
		var org any
		if body.Resource.Org != nil {
			seen.HasOrg, seen.Org = true, *body.Resource.Org
			org = *body.Resource.Org
		}
		p.mu.Lock()
		p.seen = append(p.seen, seen)
		p.mu.Unlock()
		allowed, reason := p.verdict(body.Action)
		w.Header().Set("Content-Type", "application/json")
		answer := map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": allowed,
			"reason_code": reason, "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		}
		if allowed && p.obligations != nil {
			answer["obligations"] = p.obligations
		}
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *testPDP) client(t *testing.T) *pep.Client {
	t.Helper()
	c, err := pep.New(pep.Config{BaseURL: p.srv.URL, AllowPlainHTTP: true, HTTPClient: p.srv.Client(), ClientService: "dev-health-ops-api", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (p *testPDP) calls() []pdpSeen {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.seen)
}

func allowAll(string) (bool, string) { return true, "allowed_by_role" }

func denyWith(reason string) func(string) (bool, string) {
	return func(string) (bool, string) { return false, reason }
}

type funcDecider struct {
	decide func(ctx context.Context, req pep.Request) pep.Decision
}

func (f funcDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	return f.decide(ctx, req)
}

func (f funcDecider) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(reqs))
	for i, r := range reqs {
		out[i] = f.decide(ctx, r)
	}
	return out
}

var shadowTestRows = map[string]ShadowRow{
	"GET /api/v1/admin/teams":      {Action: "teams.read", Namespace: "teams", ResourceType: "organization"},
	"POST /api/v1/admin/teams":     {Action: "teams.write", Namespace: "teams", ResourceType: "organization", HighRisk: true},
	"GET /api/v1/billing/invoices": {Action: "billing.read", Namespace: "billing", ResourceType: "organization"},
	"GET /api/v1/admin/orgs":       {Action: "admin.orgs", Namespace: "admin", ResourceType: "platform", HighRisk: true},
}

const shadowTestClasses = "legacy\tpdp\treason\tnamespace\tclass\tnote\n" +
	"allow\tdeny\trole_lacks_action\tteams\tlegacy_bug\tfixture\n" +
	"deny\tallow\tallowed_by_role\t*\tnew_bug\tfixture\n" +
	"allow\tdeny\tdelegation_invalid\t*\tintentional\tfixture\n"

func newShadowFor(t *testing.T, decider pep.Decider, mutate func(*ShadowConfig)) (*Shadow, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	cfg := ShadowConfig{Decider: decider, Rows: shadowTestRows, Classes: []byte(shadowTestClasses), HighRisk: true,
		Logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	if mutate != nil {
		mutate(&cfg)
	}
	sh, err := NewShadow(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return sh, logs
}

type persona struct {
	role      string
	superuser bool
}

var (
	personaAdmin     = persona{role: "admin"}
	personaMember    = persona{role: "member"}
	personaSuperuser = persona{role: "admin", superuser: true}
)

func personaStore(p persona) *accessStore {
	store := newAccessStore()
	store.roles[[2]uuid.UUID{userID, ownOrgID}] = p.role
	store.users[userID] = UserState{IsActive: true, IsSuperuser: p.superuser}
	return store
}

func shadowHandler(t *testing.T, store *accessStore, claims *RoutedResult, sh *Shadow, wrap func(http.Handler) http.Handler) http.Handler {
	t.Helper()
	auth := routed(t, store, &fakeValidator{result: claims})
	guard := NewGuard(auth, quiet())
	guard.SetShadow(sh)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	mux.Handle("POST /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	mux.Handle("GET /api/v1/billing/invoices", guard.Wrap(Authenticated, echo))
	mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(ActionAdminOrgs, echo))
	mux.Handle("GET /api/v1/admin/other", guard.Wrap(Admin, echo))
	if wrap != nil {
		return wrap(mux)
	}
	return mux
}

func shadowRequest(h http.Handler, method, path string) response {
	return serve(h, method, path, map[string]string{"Authorization": "Bearer " + shadowBearer})
}

func TestShadowPDPDownRequestStillServed(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	client, err := pep.New(pep.Config{BaseURL: url, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: "dev-health-ops-api", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []persona{personaAdmin, personaMember} {
		rec := recordShadow(t)
		base := shadowRequest(shadowHandler(t, personaStore(p), userClaims(nil), nil, nil), http.MethodGet, "/api/v1/admin/teams")
		sh, _ := newShadowFor(t, client, nil)
		got := shadowRequest(shadowHandler(t, personaStore(p), userClaims(nil), sh, nil), http.MethodGet, "/api/v1/admin/teams")
		sh.wg.Wait()
		if got.status != base.status || got.body != base.body {
			t.Fatalf("%s: with a dead PDP %d %q, want %d %q", p.role, got.status, got.body, base.status, base.body)
		}
		if _, results := rec.snapshot(); !slices.Equal(results, []string{"error"}) {
			t.Fatalf("%s: results %v, want [error]", p.role, results)
		}
	}
}

func TestShadowErrorNeverChangesTheResponse(t *testing.T) {
	deciders := map[string]funcDecider{
		"panic":      {func(context.Context, pep.Request) pep.Decision { panic("pdp client bug") }},
		"local deny": {func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }},
	}
	for name, d := range deciders {
		for _, p := range []persona{personaAdmin, personaMember} {
			t.Run(name+"/"+p.role, func(t *testing.T) {
				rec := recordShadow(t)
				base := shadowRequest(shadowHandler(t, personaStore(p), userClaims(nil), nil, nil), http.MethodGet, "/api/v1/admin/teams")
				sh, _ := newShadowFor(t, d, nil)
				got := shadowRequest(shadowHandler(t, personaStore(p), userClaims(nil), sh, nil), http.MethodGet, "/api/v1/admin/teams")
				sh.wg.Wait()
				if got.status != base.status || got.body != base.body {
					t.Fatalf("%d %q, want %d %q", got.status, got.body, base.status, base.body)
				}
				mismatch, results := rec.snapshot()
				if !slices.Equal(results, []string{"error"}) || len(mismatch) != 0 {
					t.Fatalf("results %v mismatch %v, want [error] and none", results, mismatch)
				}
			})
		}
	}
}

func TestShadowIsAsyncTheResponseDoesNotWaitForThePDP(t *testing.T) {
	rec := recordShadow(t)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	sh, _ := newShadowFor(t, funcDecider{func(ctx context.Context, _ pep.Request) pep.Decision {
		started <- struct{}{}
		<-release
		return pep.Decision{}
	}}, nil)
	done := make(chan response, 1)
	go func() {
		done <- shadowRequest(shadowHandler(t, personaStore(personaAdmin), userClaims(nil), sh, nil), http.MethodGet, "/api/v1/admin/teams")
	}()
	select {
	case got := <-done:
		if got.status != http.StatusOK {
			t.Fatalf("status %d", got.status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the response waited for the PDP")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the shadow call never started")
	}
	close(release)
	sh.wg.Wait()
	if _, results := rec.snapshot(); !slices.Equal(results, []string{"error"}) {
		t.Fatalf("results %v", results)
	}
}

func TestShadowMismatchClasses(t *testing.T) {
	cases := []struct {
		name     string
		who      persona
		method   string
		path     string
		verdict  func(string) (bool, string)
		status   int
		mismatch string
	}{
		{"legacy_bug rule", personaAdmin, "GET", "/api/v1/admin/teams", denyWith("role_lacks_action"), 200, "teams/admin/legacy_bug"},
		{"new_bug rule", personaMember, "GET", "/api/v1/admin/teams", allowAll, 403, "teams/member/new_bug"},
		{"intentional rule", personaAdmin, "GET", "/api/v1/admin/teams", denyWith("delegation_invalid"), 200, "teams/admin/intentional"},
		{"unresolved default", personaAdmin, "GET", "/api/v1/admin/teams", denyWith("grant_absent"), 200, "teams/admin/unresolved"},
		{"superuser caller", personaSuperuser, "GET", "/api/v1/admin/orgs", denyWith("platform_role_absent"), 200, "admin/superuser/unresolved"},
		{"authenticated level member", personaMember, "GET", "/api/v1/billing/invoices", denyWith("grant_absent"), 200, "billing/member/unresolved"},
		{"match allow", personaAdmin, "GET", "/api/v1/admin/teams", allowAll, 200, ""},
		{"match deny", personaMember, "GET", "/api/v1/admin/teams", denyWith("role_lacks_action"), 403, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := recordShadow(t)
			pdp := newTestPDP(t, tc.verdict)
			sh, _ := newShadowFor(t, pdp.client(t), nil)
			got := shadowRequest(shadowHandler(t, personaStore(tc.who), userClaims(nil), sh, nil), tc.method, tc.path)
			sh.wg.Wait()
			if got.status != tc.status {
				t.Fatalf("legacy status %d, want %d (legacy always wins)", got.status, tc.status)
			}
			mismatch, results := rec.snapshot()
			if tc.mismatch == "" {
				if len(mismatch) != 0 || !slices.Equal(results, []string{"match"}) {
					t.Fatalf("mismatch %v results %v, want none and [match]", mismatch, results)
				}
				return
			}
			if !slices.Equal(mismatch, []string{tc.mismatch}) || !slices.Equal(results, []string{"mismatch"}) {
				t.Fatalf("mismatch %v results %v, want [%s] and [mismatch]", mismatch, results, tc.mismatch)
			}
		})
	}
}

func TestShadowSkipsAreCountedAndAskNothing(t *testing.T) {
	cases := []struct {
		name    string
		store   func() *accessStore
		claims  *RoutedResult
		method  string
		path    string
		mutate  func(*ShadowConfig)
		wrap    func(http.Handler) http.Handler
		results []string
	}{
		{"high-risk row with the second switch off", func() *accessStore { return personaStore(personaAdmin) }, userClaims(nil), "POST", "/api/v1/admin/teams",
			func(c *ShadowConfig) { c.HighRisk = false }, nil, []string{"skipped"}},
		{"impersonation in force", func() *accessStore { return personaStore(personaAdmin) }, userClaims(nil), "GET", "/api/v1/admin/teams", nil,
			func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					next.ServeHTTP(w, r.WithContext(WithImpersonation(r.Context(), &Impersonation{TargetOrgID: uuid.MustParse(memberOrg)})))
				})
			}, []string{"skipped"}},
		{"caller with no organization on an org row", func() *accessStore { return personaStore(personaSuperuser) },
			userClaims(func(c *AccessTokenClaims) { c.OrgID = nil }), "GET", "/api/v1/billing/invoices", nil, nil, []string{"skipped"}},
		{"route with no action row", func() *accessStore { return personaStore(personaAdmin) }, userClaims(nil), "GET", "/api/v1/admin/other", nil, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := recordShadow(t)
			pdp := newTestPDP(t, allowAll)
			sh, _ := newShadowFor(t, pdp.client(t), tc.mutate)
			shadowRequest(shadowHandler(t, tc.store(), tc.claims, sh, tc.wrap), tc.method, tc.path)
			sh.wg.Wait()
			if _, results := rec.snapshot(); !slices.Equal(results, tc.results) {
				t.Fatalf("results %v, want %v", results, tc.results)
			}
			if n := len(pdp.calls()); n != 0 {
				t.Fatalf("%d PDP calls, want 0", n)
			}
		})
	}
}

func TestShadowHighRiskRowAsksWhenTheSecondSwitchIsOn(t *testing.T) {
	rec := recordShadow(t)
	pdp := newTestPDP(t, allowAll)
	sh, _ := newShadowFor(t, pdp.client(t), nil)
	shadowRequest(shadowHandler(t, personaStore(personaAdmin), userClaims(nil), sh, nil), http.MethodPost, "/api/v1/admin/teams")
	sh.wg.Wait()
	if n := len(pdp.calls()); n != 1 {
		t.Fatalf("%d PDP calls, want 1", n)
	}
	if _, results := rec.snapshot(); !slices.Equal(results, []string{"match"}) {
		t.Fatalf("results %v", results)
	}
}

func TestShadowLegacyHS256BearerIsSkipped(t *testing.T) {
	rec := recordShadow(t)
	pdp := newTestPDP(t, allowAll)
	sh, _ := newShadowFor(t, pdp.client(t), nil)
	store := newStore()
	withClaimRole(store, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
	guard := NewGuard(authenticator(t, store), quiet())
	guard.SetShadow(sh)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	got := serve(mux, http.MethodGet, "/api/v1/admin/teams", validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" }))
	sh.wg.Wait()
	if got.status != http.StatusOK {
		t.Fatalf("status %d %s", got.status, got.body)
	}
	if _, results := rec.snapshot(); !slices.Equal(results, []string{"skipped"}) || len(pdp.calls()) != 0 {
		t.Fatalf("results %v calls %d, want [skipped] and 0", results, len(pdp.calls()))
	}
}

func TestShadowOffAsksNothing(t *testing.T) {
	rec := recordShadow(t)
	pdp := newTestPDP(t, allowAll)
	handler := shadowHandler(t, personaStore(personaSuperuser), userClaims(nil), nil, nil)
	for _, path := range []string{"/api/v1/admin/teams", "/api/v1/billing/invoices", "/api/v1/admin/orgs"} {
		shadowRequest(handler, http.MethodGet, path)
	}
	var nilShadow *Shadow
	nilShadow.Observe(context.Background(), ShadowObservation{Pattern: "GET /api/v1/admin/teams", Bearer: shadowBearer, OrgID: ownOrg, Caller: ShadowCallerAdmin, UserAccessToken: true, LegacyAllowed: true})
	if mismatch, results := rec.snapshot(); len(mismatch)+len(results) != 0 || len(pdp.calls()) != 0 {
		t.Fatalf("shadow off counted %v %v and made %d calls", mismatch, results, len(pdp.calls()))
	}
}

func TestShadowNeverChangesAnyGuardAnswer(t *testing.T) {
	routes := [][2]string{{"GET", "/api/v1/admin/teams"}, {"POST", "/api/v1/admin/teams"}, {"GET", "/api/v1/billing/invoices"}, {"GET", "/api/v1/admin/orgs"}, {"GET", "/api/v1/admin/other"}}
	for _, p := range []persona{personaAdmin, personaMember, personaSuperuser} {
		for _, verdict := range []func(string) (bool, string){allowAll, denyWith("grant_absent")} {
			for _, route := range routes {
				for _, header := range []bool{true, false} {
					recordShadow(t)
					pdp := newTestPDP(t, verdict)
					sh, _ := newShadowFor(t, pdp.client(t), nil)
					do := func(h http.Handler) response {
						headers := map[string]string{}
						if header {
							headers["Authorization"] = "Bearer " + shadowBearer
						}
						return serve(h, route[0], route[1], headers)
					}
					base := do(shadowHandler(t, personaStore(p), userClaims(nil), nil, nil))
					got := do(shadowHandler(t, personaStore(p), userClaims(nil), sh, nil))
					sh.wg.Wait()
					if got.status != base.status || got.body != base.body || got.header.Get("WWW-Authenticate") != base.header.Get("WWW-Authenticate") {
						t.Fatalf("%s %s %s header=%v: %d %q, want %d %q", p.role, route[0], route[1], header, got.status, got.body, base.status, base.body)
					}
				}
			}
		}
	}
}

func TestShadowCounterLabelSetsAreClosed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("shadow-test")
	newShadowMismatchAdder(meter)(context.Background(), "teams", "admin", "unresolved")
	newShadowResultAdder(meter)(context.Background(), "match")
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	keysByMetric := map[string][]string{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is not an int64 sum", m.Name)
			}
			for _, point := range sum.DataPoints {
				var keys []string
				for _, kv := range point.Attributes.ToSlice() {
					keys = append(keys, string(kv.Key))
				}
				sort.Strings(keys)
				keysByMetric[m.Name] = keys
			}
		}
	}
	if got := keysByMetric[shadowMismatchMetric]; !slices.Equal(got, []string{"caller", "class", "namespace"}) {
		t.Fatalf("mismatch counter keys %v, want [caller class namespace]", got)
	}
	if got := keysByMetric[shadowResultMetric]; !slices.Equal(got, []string{"result"}) {
		t.Fatalf("results counter keys %v, want [result]", got)
	}
	rec := recordShadow(t)
	pdp := newTestPDP(t, denyWith("grant_absent"))
	sh, _ := newShadowFor(t, pdp.client(t), nil)
	hostile := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Header.Set("X-Org-Id", strangerOrg)
			r.Header.Set("User-Agent", "secret-agent")
			h.ServeHTTP(w, r)
		})
	}
	for _, p := range []persona{personaAdmin, personaMember, personaSuperuser} {
		for _, route := range [][2]string{{"GET", "/api/v1/admin/teams?token=abc"}, {"GET", "/api/v1/billing/invoices"}, {"GET", "/api/v1/admin/orgs"}, {"POST", "/api/v1/admin/teams"}} {
			shadowRequest(shadowHandler(t, personaStore(p), userClaims(nil), sh, hostile), route[0], route[1])
		}
	}
	sh.wg.Wait()
	mismatch, results := rec.snapshot()
	if len(mismatch) == 0 || len(results) == 0 {
		t.Fatal("no counter was emitted")
	}
	for _, m := range mismatch {
		parts := strings.Split(m, "/")
		if len(parts) != 3 || !slices.Contains(shadowNamespaces, parts[0]) || !slices.Contains(shadowCallers, parts[1]) || !slices.Contains(shadowClasses, parts[2]) {
			t.Errorf("mismatch label triple %q is outside the closed sets", m)
		}
	}
	for _, r := range results {
		if !slices.Contains(shadowResults, r) {
			t.Errorf("result %q is outside the closed set", r)
		}
	}
}

func TestShadowRequestToThePDPCarriesOnlyTheRowAndTheCaller(t *testing.T) {
	recordShadow(t)
	pdp := newTestPDP(t, allowAll)
	sh, _ := newShadowFor(t, pdp.client(t), nil)
	shadowRequest(shadowHandler(t, personaStore(personaSuperuser), userClaims(nil), sh, nil), http.MethodGet, "/api/v1/admin/teams")
	shadowRequest(shadowHandler(t, personaStore(personaSuperuser), userClaims(nil), sh, nil), http.MethodGet, "/api/v1/admin/orgs")
	sh.wg.Wait()
	seen := pdp.calls()
	sort.Slice(seen, func(i, j int) bool { return seen[i].Action < seen[j].Action })
	want := []pdpSeen{
		{Principal: shadowBearer, Action: "admin.orgs", Type: "platform", Key: "platform", Service: "dev-health-ops-api"},
		{Principal: shadowBearer, Action: "teams.read", Type: "organization", Org: ownOrg, Key: ownOrg, Service: "dev-health-ops-api", HasOrg: true},
	}
	if !slices.Equal(seen, want) {
		t.Fatalf("PDP saw %+v, want %+v", seen, want)
	}
}

func TestShadowLogsHoldNoCredentialUserOrOrganization(t *testing.T) {
	recordShadow(t)
	pdp := newTestPDP(t, denyWith("grant_absent"))
	sh, logs := newShadowFor(t, pdp.client(t), nil)
	shadowRequest(shadowHandler(t, personaStore(personaAdmin), userClaims(nil), sh, nil), http.MethodGet, "/api/v1/admin/teams")
	down, _ := newShadowFor(t, funcDecider{func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }}, nil)
	shadowRequest(shadowHandler(t, personaStore(personaAdmin), userClaims(nil), down, nil), http.MethodGet, "/api/v1/admin/teams")
	sh.wg.Wait()
	down.wg.Wait()
	text := logs.String()
	if !strings.Contains(text, "namespace=teams") || !strings.Contains(text, "class=unresolved") || !strings.Contains(text, "reason_code=grant_absent") {
		t.Fatalf("the mismatch log lacks its safe fields: %q", text)
	}
	for _, secret := range []string{shadowBearer, userID.String(), ownOrg, "user@example.com"} {
		if strings.Contains(text, secret) {
			t.Fatalf("the log holds %q: %q", secret, text)
		}
	}
}

func TestShadowPoolFullDropsTheCallNotTheRequest(t *testing.T) {
	rec := recordShadow(t)
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	sh, _ := newShadowFor(t, funcDecider{func(context.Context, pep.Request) pep.Decision {
		started <- struct{}{}
		<-release
		return pep.Decision{}
	}}, func(c *ShadowConfig) { c.MaxInFlight = 1 })
	handler := shadowHandler(t, personaStore(personaAdmin), userClaims(nil), sh, nil)
	first := shadowRequest(handler, http.MethodGet, "/api/v1/admin/teams")
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the first shadow call never started")
	}
	second := shadowRequest(handler, http.MethodGet, "/api/v1/admin/teams")
	if first.status != 200 || second.status != 200 {
		t.Fatalf("statuses %d %d", first.status, second.status)
	}
	if _, results := rec.snapshot(); !slices.Equal(results, []string{"dropped"}) {
		t.Fatalf("results %v, want [dropped] while the one slot is busy", results)
	}
	close(release)
	sh.wg.Wait()
}

func TestShadowSamplePercent(t *testing.T) {
	for _, percent := range []int{100, 50} {
		recordShadow(t)
		pdp := newTestPDP(t, allowAll)
		sh, _ := newShadowFor(t, pdp.client(t), func(c *ShadowConfig) { c.SamplePercent, c.MaxInFlight = percent, 200 })
		handler := shadowHandler(t, personaStore(personaAdmin), userClaims(nil), sh, nil)
		for i := 0; i < 100; i++ {
			shadowRequest(handler, http.MethodGet, "/api/v1/admin/teams")
		}
		sh.wg.Wait()
		if n := len(pdp.calls()); n != percent {
			t.Fatalf("sample %d%%: %d calls over 100 requests", percent, n)
		}
	}
}

func TestShadowConfigRefusals(t *testing.T) {
	ok := funcDecider{func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }}
	for name, cfg := range map[string]ShadowConfig{
		"no decider":       {Rows: shadowTestRows},
		"no rows":          {Decider: ok},
		"sample over 100":  {Decider: ok, Rows: shadowTestRows, SamplePercent: 101},
		"negative sample":  {Decider: ok, Rows: shadowTestRows, SamplePercent: -1},
		"bad class table":  {Decider: ok, Rows: shadowTestRows, Classes: []byte("legacy\tpdp\treason\tnamespace\tclass\tnote\nallow\tdeny\t*\t*\tbogus\tx\n")},
		"row bad resource": {Decider: ok, Rows: map[string]ShadowRow{"GET /x": {Action: "teams.read", Namespace: "teams", ResourceType: "repository"}}},
	} {
		if _, err := NewShadow(cfg); err == nil {
			t.Errorf("%s: NewShadow accepted the config", name)
		}
	}
	if _, err := NewShadow(ShadowConfig{Decider: ok, Rows: shadowTestRows}); err != nil {
		t.Fatalf("a valid config was refused: %v", err)
	}
}

func TestShadowEmbeddedClassTable(t *testing.T) {
	rules, err := LoadShadowClasses(shadowClassTable)
	if err != nil || len(rules) == 0 {
		t.Fatalf("embedded class table: %d rules, err %v", len(rules), err)
	}
	reasons := map[string]bool{}
	for _, r := range pep.WireReasons() {
		reasons[string(r)] = true
	}
	for _, rule := range rules {
		if !slices.Contains(shadowClasses, rule.Class) {
			t.Errorf("rule %+v has a class outside the closed set", rule)
		}
		if rule.Reason != "*" && !reasons[rule.Reason] {
			t.Errorf("rule %+v names a reason the PDP does not send", rule)
		}
		if rule.Namespace != "*" && !slices.Contains(shadowNamespaces, rule.Namespace) {
			t.Errorf("rule %+v names an unknown namespace", rule)
		}
	}
	for name, raw := range map[string]string{
		"no header":      "allow\tdeny\t*\t*\tnew_bug\tx\n",
		"bad legacy":     "legacy\tpdp\treason\tnamespace\tclass\tnote\nmaybe\tdeny\t*\t*\tnew_bug\tx\n",
		"bad class":      "legacy\tpdp\treason\tnamespace\tclass\tnote\nallow\tdeny\t*\t*\tbogus\tx\n",
		"unknown reason": "legacy\tpdp\treason\tnamespace\tclass\tnote\nallow\tdeny\tnot_a_reason\t*\tnew_bug\tx\n",
		"no note":        "legacy\tpdp\treason\tnamespace\tclass\tnote\nallow\tdeny\t*\t*\tnew_bug\t\n",
	} {
		if _, err := LoadShadowClasses([]byte(raw)); err == nil {
			t.Errorf("%s: LoadShadowClasses accepted the table", name)
		}
	}
}

func TestShadowRowsCensusOverTheGoAPIProfiles(t *testing.T) {
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Rows []struct {
			Service, Method, Route, Action, Classification string
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	want := 0
	namespaces := map[string]bool{}
	for _, row := range document.Rows {
		if row.Service != "dev-health-ops-api" || row.Action == "" || row.Classification != "protected" {
			continue
		}
		want++
		got, ok := rows[row.Method+" "+row.Route]
		if !ok || got.Action != row.Action {
			t.Errorf("row %s %s (%s) is not in the shadow table: %+v", row.Method, row.Route, row.Action, got)
			continue
		}
		action, err := policycatalog.Default().Action(row.Action)
		if err != nil {
			t.Errorf("action %s: %v", row.Action, err)
			continue
		}
		if got.ResourceType != action.ResourceKind || got.HighRisk != (action.RiskClass == policycatalog.RiskHigh) {
			t.Errorf("row %s: %+v does not follow the catalog %+v", row.Route, got, action)
		}
		if got.Namespace != strings.SplitN(row.Action, ".", 2)[0] {
			t.Errorf("row %s: namespace %q", row.Route, got.Namespace)
		}
		namespaces[got.Namespace] = true
	}
	if want == 0 || len(rows) != want {
		t.Fatalf("shadow table holds %d rows, the profiles name %d", len(rows), want)
	}
	var got []string
	for ns := range namespaces {
		got = append(got, ns)
	}
	sort.Strings(got)
	if !slices.Equal(got, shadowNamespaces) {
		t.Fatalf("namespaces of the rows %v, closed set %v", got, shadowNamespaces)
	}
	for name, raw := range map[string]string{
		"duplicate row":     fmt.Sprintf(`{"rows":[%s,%s]}`, testProfileRow("teams.read"), testProfileRow("teams.read")),
		"action not in set": fmt.Sprintf(`{"rows":[%s]}`, testProfileRow("teams.nope")),
		"namespace outside": fmt.Sprintf(`{"rows":[%s]}`, testProfileRow("analytics.read")),
	} {
		if _, err := ParseShadowRows([]byte(raw), "dev-health-ops-api"); err == nil {
			t.Errorf("%s: ParseShadowRows accepted the document", name)
		}
	}
}

func testProfileRow(action string) string {
	return fmt.Sprintf(`{"service":"dev-health-ops-api","method":"GET","route":"/x","action":%q,"classification":"protected"}`, action)
}
