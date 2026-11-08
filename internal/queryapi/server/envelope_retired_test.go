package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// unacceptedBearer has no class query-api accepts: three segments, no readable JOSE header.
const unacceptedBearer = "bm90.YSB0b2tlbg.c2ln"

// The effective-principal envelope is a retired class. On a REST row, in every PDP mode and with or without
// row-routed access, a validly signed envelope gets the bytes an unaccepted bearer gets: the 401 refusal, no handler,
// no PDP call, no exempt count.
func TestQueryAPIRESTRefusesTheEnvelope(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	for _, mode := range []string{pdp.ModeOff, pdp.ModeShadow, pdp.ModeEnforce} {
		for name, routed := range map[string]bool{"routed": true, "unrouted": false} {
			t.Run(mode+"/"+name, func(t *testing.T) {
				g := newGatedREST(t, mode, "analytics", srv.URL, routed)
				asked := len(srv.Requests())
				unaccepted := g.do(http.MethodGet, unacceptedBearer)
				if unaccepted != (answer{http.StatusUnauthorized, restRefusal}) {
					t.Fatalf("unaccepted bearer: got %+v, want 401 %s", unaccepted, restRefusal)
				}
				if got := g.do(http.MethodGet, g.f.envelope); got != unaccepted {
					t.Fatalf("envelope: got %+v, want the unaccepted bearer's %+v", got, unaccepted)
				}
				if got := g.counter(t, "dev_health_query_api_pdp_exempt_total"); len(got) != 0 {
					t.Fatalf("exempt counts %v, want none", got)
				}
				if n := len(srv.Requests()) - asked; n != 0 {
					t.Fatalf("the envelope asked the PDP %d times", n)
				}
			})
		}
	}
}

// On the /query family and /buildinfo, on both listeners that mount them, a validly signed envelope gets the bare 401
// an unaccepted bearer gets and runs nothing.
func TestQueryAPIInternalCarrierRefusesTheEnvelope(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	stack := newAccessStack(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow))
	for _, target := range []struct{ listener, method, path string }{
		{"query", http.MethodPost, "/query"},
		{"internal", http.MethodPost, "/query"},
		{"query", http.MethodPost, "/query/proof"},
		{"internal", http.MethodPost, "/query/proof"},
		{"internal", http.MethodPost, "/query/proof-write"},
		{"internal", http.MethodPost, "/query/run-operation"},
		{"query", http.MethodGet, "/buildinfo"},
		{"internal", http.MethodGet, "/buildinfo"},
	} {
		t.Run(target.listener+target.path, func(t *testing.T) {
			ran := len(*stack.seen)
			unaccepted := stack.send(target.listener, target.method, target.path, "", unacceptedBearer)
			if unaccepted != (answer{http.StatusUnauthorized, queryRefusal}) {
				t.Fatalf("unaccepted bearer: got %+v, want 401 %q", unaccepted, queryRefusal)
			}
			if got := stack.send(target.listener, target.method, target.path, "envelope", ""); got != unaccepted {
				t.Fatalf("envelope: got %+v, want the unaccepted bearer's %+v", got, unaccepted)
			}
			if n := len(*stack.seen) - ran; n != 0 {
				t.Fatalf("a refused request ran %d time(s)", n)
			}
		})
	}
	if got := stack.send("internal", http.MethodGet, "/buildinfo", "headers", ""); got.Status != http.StatusOK {
		t.Fatalf("/buildinfo with the internal identity headers: got %+v, want 200", got)
	}
}

// roleSnapshot is the state the decision point reads for one caller: a membership role, or none, and a platform role.
type roleSnapshot struct {
	role     string
	member   bool
	platform string
}

func (s roleSnapshot) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (s roleSnapshot) PlatformRole() string                  { return s.platform }
func (s roleSnapshot) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (s roleSnapshot) Membership(org string) (authorization.Membership, bool) {
	if !s.member {
		return authorization.Membership{}, false
	}
	return authorization.Membership{OrgID: org, Role: s.role, Status: authorization.StatusActive}, true
}

type roleCase struct {
	name  string
	store func(*accessStore)
	token func(*accessMinter) string
	// snap is what the decision point reads; nil for a caller with no organization, which the PDP is never asked about.
	snap *roleSnapshot
	// query is main's /query answer for the caller: the status, and the claims the resolver sees when it runs.
	query       int
	queryClaims authctx.Claims
}

func roleCases() []roleCase {
	org := ecOrg.String()
	member := func(role string) roleCase {
		return roleCase{
			name:  role,
			store: func(s *accessStore) { s.roles = map[[2]uuid.UUID]string{{ecUser, ecOrg}: role} },
			token: func(m *accessMinter) string { return m.mint(ecUser, nil, nil) },
			snap:  &roleSnapshot{role: role, member: true},
			query: http.StatusOK, queryClaims: authctx.Claims{OrgID: org, Role: role, Scope: pepgate.Legacy(org)},
		}
	}
	superuser := func(s *accessStore) {
		s.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
		s.members = nil
	}
	return []roleCase{
		member("viewer"), member("member"), member("admin"), member("owner"),
		{
			name: "platform admin with no membership row", store: superuser,
			token: func(m *accessMinter) string { return m.mint(ecUser, nil, nil) },
			snap:  &roleSnapshot{platform: rule.PlatformAdmin},
			query: http.StatusUnauthorized,
		},
		{
			name: "platform admin with no organization", store: superuser,
			token: func(m *accessMinter) string { return m.mint(ecUser, map[string]any{"org_id": nil}, nil) },
			query: http.StatusOK, queryClaims: authctx.Claims{IsSuperuser: true, Scope: pepgate.Legacy("")},
		},
	}
}

// roleRun serves route behind authenticateRESTRequest on the row-routed user token path, over store, with gate (nil:
// no gate, the legacy path).
func roleRun(t *testing.T, m *accessMinter, route seamRoute, c roleCase, gate *pdp.Gate) seamRun {
	t.Helper()
	store := newAccessStore()
	c.store(store)
	verifier, _ := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(newQueryAPIUserStoreFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, testAccess(m.keys, nil, accessTestNow).opts); err != nil {
		t.Fatal(err)
	}
	s := &seamStack{t: t, route: route, mux: http.NewServeMux(), client: &teamScopeReachClient{}, user: c.token(m)}
	inner := route.handler(t, s.client)
	s.mux.HandleFunc(route.pattern, func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edge, route.name)
		if !ok {
			return
		}
		s.seen = append(s.seen, claims)
		inner(w, r.WithContext(authctx.WithClaims(r.Context(), claims)))
	})
	return s.run(gate, s.user)
}

// Product rule 1 on each role of a user access token, after the envelope class left query-api: on every enforced REST
// read (all cells on, the decision taken by the real Evaluator over the catalog for the caller's state) the answer is
// the legacy path's, byte for byte with the same reads; on /query each caller gets main's answer.
func TestUserAccessTokenRolesKeepTheLegacyAnswer(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	for _, c := range roleCases() {
		for _, route := range seamRoutes() {
			t.Run(c.name+"/"+route.name, func(t *testing.T) {
				legacy := roleRun(t, m, route, c, nil)
				if legacy.status != http.StatusOK || len(legacy.statements) == 0 {
					t.Fatalf("legacy path: status %d, %d statements (body %q)", legacy.status, len(legacy.statements), legacy.body)
				}
				decider, asks := seamDenier(t), int64(0)
				if c.snap != nil {
					org := ecOrg.String()
					d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: route.action, ResourceKind: "organization", OrganizationID: org}, *c.snap)
					if !d.Allowed || d.Obligations == nil {
						t.Fatalf("decision point answered %+v for %+v", d, *c.snap)
					}
					rule := pepfake.Rule{PrincipalReference: c.token(m), Action: route.action, ResourceType: "organization", ResourceKey: org,
						OrganizationID: org, TenantScope: d.Obligations.TenantScope, RepositoryScope: string(d.Obligations.RepositoryScope),
						RepositoryAllowlist: d.Obligations.RepositoryAllowlist}
					decider, asks = &seamDecider{Decider: pepfake.New(rule)}, 1
				}
				gate, _ := seamGate(t, pdp.ModeEnforce, strings.Join(authzenforce.Namespaces(), ","), decider)
				if got := roleRun(t, m, route, c, gate); !got.same(legacy) {
					t.Fatalf("enforced differs from the legacy path:\n got %d %q\nwant %d %q", got.status, got.body, legacy.status, legacy.body)
				}
				if n := decider.calls.Load(); n != asks {
					t.Fatalf("PDP asked %d times, want %d", n, asks)
				}
			})
		}
		t.Run(c.name+"/query", func(t *testing.T) {
			store := newAccessStore()
			c.store(store)
			stack := newAccessStack(t, store, testAccess(m.keys, nil, accessTestNow))
			got := stack.send("query", http.MethodPost, "/query", "", c.token(m))
			if got.Status != c.query {
				t.Fatalf("/query: got %+v, want status %d", got, c.query)
			}
			if c.query != http.StatusOK {
				if len(*stack.seen) != 0 {
					t.Fatalf("/query refused but ran as %+v", *stack.seen)
				}
				return
			}
			if len(*stack.seen) != 1 || (*stack.seen)[0] != c.queryClaims {
				t.Fatalf("/query ran as %+v, want %+v", *stack.seen, c.queryClaims)
			}
		})
	}
}
