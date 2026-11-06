package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/drilldown"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The pepgate seam on real REST handlers (CHAOS-13081, PLAN-10864-v2 section 4 PR-1): one route per action
// namespace, its real handler over a recording ClickHouse client, behind the real REST authenticator and the plane
// handler. A refusal runs no read; an Organization-scope allow and every path the PDP does not decide run exactly the
// read of today's path (no gate at all).

const seamDates = "start_date=2026-01-01&end_date=2026-01-15"

type seamRoute struct {
	name, pattern, query, action string
	handler                      func(*testing.T, *teamScopeReachClient) http.HandlerFunc
}

func seamRoutes() []seamRoute {
	return []seamRoute{
		{"drilldown_prs", "/api/v1/drilldown/prs", "?" + seamDates, "git.read", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			reader, err := drilldown.NewReader(c)
			if err != nil {
				t.Fatal(err)
			}
			return newDrilldownPRsGetHandler(reader)
		}},
		{"drilldown_issues", "/api/v1/drilldown/issues", "?" + seamDates, "work_items.read", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			reader, err := drilldown.NewReader(c)
			if err != nil {
				t.Fatal(err)
			}
			return newDrilldownIssuesGetHandler(reader)
		}},
		{"sankey", "/api/v1/sankey", "?" + seamDates, "analytics.read", func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newSankeyGetHandler(c)
		}},
		{"explain", "/api/v1/explain", "?metric=" + explain.MetricNames()[0] + "&" + seamDates, "metrics.read", func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			reader, err := explain.NewReader(c)
			if err != nil {
				t.Fatal(err)
			}
			return newExplainGetHandler(reader)
		}},
	}
}

// seamStack is one route behind the REST authenticator with the edge verifier routed by row.
type seamStack struct {
	t        *testing.T
	route    seamRoute
	mux      *http.ServeMux
	client   *teamScopeReachClient
	seen     []authctx.Claims
	user     string
	envelope string
}

type seamRun struct {
	status     int
	body       string
	statements []string
}

func newSeamStack(t *testing.T, route seamRoute) *seamStack {
	t.Helper()
	quietStdLog(t)
	minter := newAccessMinter(t)
	verifier, priv := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(ecSecret, ecIssuer, ecAudience, newQueryAPIUserStoreFrom(newAccessStore()))
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, testAccess(minter.keys, nil, accessTestNow).opts, true); err != nil {
		t.Fatal(err)
	}
	s := &seamStack{t: t, route: route, mux: http.NewServeMux(), client: &teamScopeReachClient{}, user: minter.mint(ecUser, nil, nil),
		envelope: iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"})}
	inner := route.handler(t, s.client)
	s.mux.HandleFunc(route.pattern, func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edge, route.name)
		if !ok {
			return
		}
		s.seen = append(s.seen, claims)
		inner(w, r.WithContext(authctx.WithClaims(r.Context(), claims)))
	})
	return s
}

// run serves one GET through the plane handler with gate (nil: no gate, the path before the PDP client).
func (s *seamStack) run(gate *pdp.Gate, bearer string) seamRun {
	s.t.Helper()
	req := httptest.NewRequest(http.MethodGet, s.route.pattern+s.route.query, nil)
	req.Header.Set("Authorization", "Bearer "+bearer)
	rec := httptest.NewRecorder()
	planeHandler(s.mux, gate).ServeHTTP(rec, req)
	gate.Wait()
	body, _ := io.ReadAll(rec.Result().Body)
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	statements := make([]string, len(s.client.statements))
	for i, statement := range s.client.statements {
		statements[i] = statement + "\n-- bindings: " + fmt.Sprint(s.client.bindings[i])
	}
	sort.Strings(statements)
	return seamRun{status: rec.Code, body: string(body), statements: statements}
}

// legacyRun is what the route answers and reads with no gate at all.
func legacyRun(t *testing.T, route seamRoute, bearer func(*seamStack) string) seamRun {
	t.Helper()
	s := newSeamStack(t, route)
	run := s.run(nil, bearer(s))
	if run.status != http.StatusOK || len(run.statements) == 0 {
		t.Fatalf("%s legacy run: status %d, %d statements (body %q)", route.pattern, run.status, len(run.statements), run.body)
	}
	return run
}

func userBearer(s *seamStack) string { return s.user }

func (r seamRun) same(other seamRun) bool {
	if r.status != other.status || r.body != other.body || len(r.statements) != len(other.statements) {
		return false
	}
	for i := range r.statements {
		if r.statements[i] != other.statements[i] {
			return false
		}
	}
	return true
}

// seamDenier is a PEP client over the wire fake answering every request with a deny verdict.
func seamDenier(t *testing.T) *seamDecider {
	t.Helper()
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	client, err := pep.New(pep.Config{BaseURL: srv.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return &seamDecider{Decider: client}
}

// seamDecider is a decider counting its decisions.
type seamDecider struct {
	pep.Decider
	calls atomic.Int64
}

func (d *seamDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	d.calls.Add(1)
	return d.Decider.Decide(ctx, req)
}

func seamAllow(s *seamStack, edit func(*pepfake.Rule)) pepfake.Rule {
	org := ecOrg.String()
	r := pepfake.Rule{PrincipalReference: s.user, Action: s.route.action, ResourceType: "organization", ResourceKey: org,
		OrganizationID: org, TenantScope: org, RepositoryScope: "organization"}
	if edit != nil {
		edit(&r)
	}
	return r
}

func seamGate(t *testing.T, mode, cellList string, decider pep.Decider) (*pdp.Gate, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return pdp.New(pdp.Config{Mode: mode, Decider: decider, Switch: authzenforce.Parse(authzenforce.QueryAPI, cellList),
		Meter: provider.Meter("test"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}), reader
}

func namespaceOf(action string) string {
	namespace, _, _ := strings.Cut(action, ".")
	return namespace
}

// P1, P4, P5, P7, P8, P9, P11, P12 on every namespace: a refusal answers before the handler, so no read runs. P4 is the
// F-A form: an allow with repository_allowlist [] on a route not cut over is 503, never the org-wide read.
func TestSeamRESTRefusalsRunNoRead(t *testing.T) {
	cases := []struct {
		name    string
		mode    string
		rule    func(*pepfake.Rule)
		noRule  bool
		want    answer
		cellOff bool
	}{
		{"P1 tenant_scope absent", pdp.ModeEnforce, func(r *pepfake.Rule) { r.TenantScope = "" }, false, gatedUnavailable, false},
		{"P2 tenant_scope other org", pdp.ModeEnforce, func(r *pepfake.Rule) { r.TenantScope = liveOtherOrg.String() }, false, gatedUnavailable, false},
		{"P4 allowlist []", pdp.ModeEnforce, func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", []string{} }, false, gatedUnavailable, false},
		{"P5 allowlist [acme/a]", pdp.ModeEnforce, func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", []string{"acme/a"} }, false, gatedUnavailable, false},
		{"P7 no repository member", pdp.ModeEnforce, func(r *pepfake.Rule) { r.RepositoryScope = "" }, false, gatedUnavailable, false},
		{"P8 minimum_assurance", pdp.ModeEnforce, func(r *pepfake.Rule) { r.MinimumAssurance = pep.AssuranceAAL2 }, false, gatedUnavailable, false},
		{"P9 deny", pdp.ModeEnforce, nil, true, gatedForbidden, false},
		{"P12 cell on, mode off", pdp.ModeOff, nil, false, gatedUnavailable, false},
		{"P11 enforce, cell off", pdp.ModeEnforce, nil, false, gatedUnavailable, true},
	}
	for _, route := range seamRoutes() {
		for _, tc := range cases {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				s := newSeamStack(t, route)
				var rules []pepfake.Rule
				if !tc.noRule {
					rules = append(rules, seamAllow(s, tc.rule))
				}
				var decider pep.Decider
				switch {
				case tc.noRule:
					decider = seamDenier(t)
				case tc.mode != pdp.ModeOff:
					decider = pepfake.New(rules...)
				}
				cellList := namespaceOf(route.action)
				if tc.cellOff {
					cellList = ""
				}
				gate, _ := seamGate(t, tc.mode, cellList, decider)
				got := s.run(gate, s.user)
				if (answer{got.status, strings.TrimSpace(got.body)}) != tc.want {
					t.Fatalf("answered %d %q, want %+v", got.status, got.body, tc.want)
				}
				if len(got.statements) != 0 || len(s.seen) != 0 {
					t.Fatalf("a refused request reached the handler: %d statement(s), handler ran %d time(s):\n%v", len(got.statements), len(s.seen), got.statements)
				}
			})
		}
	}
}

// P3: an allow over every repository serves with Scope Organization(org), and the read is today's read.
func TestSeamRESTOrganizationScopeServesTodaysRead(t *testing.T) {
	for _, route := range seamRoutes() {
		t.Run(route.name, func(t *testing.T) {
			legacy := legacyRun(t, route, userBearer)
			s := newSeamStack(t, route)
			gate, _ := seamGate(t, pdp.ModeEnforce, namespaceOf(route.action), pepfake.New(seamAllow(s, nil)))
			got := s.run(gate, s.user)
			if !got.same(legacy) {
				t.Fatalf("enforced Organization scope differs from today's read:\n got %d %q %v\nwant %d %q %v", got.status, got.body, got.statements, legacy.status, legacy.body, legacy.statements)
			}
			if len(s.seen) != 1 || s.seen[0].Scope != pepgate.Organization(ecOrg.String()) || s.seen[0].OrgID != ecOrg.String() {
				t.Fatalf("handler saw %+v, want Scope Organization(%s)", s.seen, ecOrg)
			}
		})
	}
}

// P10, P11: every path the PDP does not decide is today's path: the same read, the same bytes, the Legacy scope. P10
// (both switches unset, the PDP configured) asks nothing; P11 (QUERY_API_PDP_MODE=shadow, no cell, a PDP that
// denies) asks in the background and still serves.
func TestSeamRESTCellOffIsTodaysPath(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	for _, route := range seamRoutes() {
		legacy := legacyRun(t, route, userBearer)
		for name, gate := range map[string]func(*seamStack) (*pdp.Gate, *seamDecider){
			"P10 all unset": func(*seamStack) (*pdp.Gate, *seamDecider) {
				env := map[string]string{pdp.EnvAuthProxyURL: srv.URL, workloadinput.EnvIssuer: pdptest.Issuer, workloadinput.EnvBootstrapFile: pdptest.BootstrapFile(t)}
				return pdp.FromSettings(func(k string) (string, bool) { v, ok := env[k]; return v, ok }, slog.New(slog.NewTextHandler(io.Discard, nil))), nil
			},
			"P11 shadow, cell off, PDP denies": func(*seamStack) (*pdp.Gate, *seamDecider) {
				d := seamDenier(t)
				g, _ := seamGate(t, pdp.ModeShadow, "", d)
				return g, d
			},
			"other namespaces on": func(*seamStack) (*pdp.Gate, *seamDecider) {
				var others []string
				for _, namespace := range authzenforce.Namespaces() {
					if namespace != namespaceOf(route.action) {
						others = append(others, namespace)
					}
				}
				d := seamDenier(t)
				g, _ := seamGate(t, pdp.ModeShadow, strings.Join(others, ","), d)
				return g, d
			},
		} {
			t.Run(route.name+"/"+name, func(t *testing.T) {
				s := newSeamStack(t, route)
				g, decider := gate(s)
				got := s.run(g, s.user)
				if !got.same(legacy) {
					t.Fatalf("differs from today's path:\n got %d %q %v\nwant %d %q %v", got.status, got.body, got.statements, legacy.status, legacy.body, legacy.statements)
				}
				if len(s.seen) != 1 || s.seen[0].Scope != pepgate.Legacy(ecOrg.String()) {
					t.Fatalf("handler saw %+v, want Scope Legacy(%s)", s.seen, ecOrg)
				}
				if decider == nil && len(srv.Requests()) != 0 {
					t.Fatalf("all unset asked the PDP %d times", len(srv.Requests()))
				}
				if decider != nil && decider.calls.Load() != 1 {
					t.Fatalf("shadow asked %d times, want 1", decider.calls.Load())
				}
			})
		}
	}
}

// P14: the exempt classes (HS256 edge token, envelope) never ask, with every cell on and a PDP that denies: today's
// read, the Legacy scope, one exempt count per class.
func TestSeamRESTExemptCallersStayOnTodaysPath(t *testing.T) {
	for _, route := range seamRoutes() {
		t.Run(route.name, func(t *testing.T) {
			edge := func(*seamStack) string { return (&gatedREST{}).edgeToken(t) }
			envelope := func(s *seamStack) string { return s.envelope }
			legacyEdge, legacyEnvelope := legacyRun(t, route, edge), legacyRun(t, route, envelope)
			decider := seamDenier(t)
			gate, reader := seamGate(t, pdp.ModeEnforce, strings.Join(authzenforce.Namespaces(), ","), decider)
			for name, tc := range map[string]struct {
				bearer func(*seamStack) string
				legacy seamRun
			}{"edge": {edge, legacyEdge}, "envelope": {envelope, legacyEnvelope}} {
				s := newSeamStack(t, route)
				got := s.run(gate, tc.bearer(s))
				if !got.same(tc.legacy) {
					t.Fatalf("%s differs from today's path:\n got %d %q\nwant %d %q", name, got.status, got.body, tc.legacy.status, tc.legacy.body)
				}
				if len(s.seen) != 1 || s.seen[0].Scope != pepgate.Legacy(ecOrg.String()) {
					t.Fatalf("%s: handler saw %+v, want Scope Legacy", name, s.seen)
				}
			}
			if decider.calls.Load() != 0 {
				t.Fatalf("an exempt caller asked the PDP %d times", decider.calls.Load())
			}
			counts := (&gatedREST{reader: reader}).counter(t, "dev_health_query_api_pdp_exempt_total")
			if counts["action="+route.action+",class=ops_access_token_hs256"] != 1 || counts["action="+route.action+",class=effective_principal_envelope"] != 1 {
				t.Fatalf("exempt counts %v, want one per class", counts)
			}
		})
	}
}
