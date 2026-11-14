package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

var paSU0 = uuid.MustParse("33333333-3333-4333-8333-0000000000f0") // superuser, no membership

func paSID(u uuid.UUID) uuid.UUID { return uuid.NewSHA1(u, []byte("sid")) }

type paSnap struct {
	platformRole string
	members      map[string]string
	del          *authorization.Delegation
	suspended    bool
}

func (s paSnap) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (s paSnap) PlatformRole() string                  { return s.platformRole }
func (s paSnap) OrganizationStatus(string) authorization.Status {
	if s.suspended {
		return authorization.StatusSuspended
	}
	return authorization.StatusActive
}
func (s paSnap) Membership(org string) (authorization.Membership, bool) {
	role, ok := s.members[org]
	if !ok {
		return authorization.Membership{}, false
	}
	return authorization.Membership{OrgID: org, Role: role, Status: authorization.StatusActive}, true
}
func (s paSnap) Grants(string) ([]authorization.Grant, error) { return nil, nil }
func (s paSnap) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s paSnap) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (s paSnap) NowUnixNano() int64 { return time.Now().UnixNano() }
func (s paSnap) Delegation() (authorization.Delegation, bool) {
	if s.del == nil {
		return authorization.Delegation{}, false
	}
	return *s.del, true
}

type paPDP struct {
	srv   *httptest.Server
	mu    sync.Mutex
	snaps map[string]paSnap
	label map[string]string
	seen  []string
}

func newPAPDP(t *testing.T) *paPDP {
	t.Helper()
	p := &paPDP{snaps: map[string]paSnap{}, label: map[string]string{}}
	evaluator := authorization.NewEvaluator()
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PrincipalReference string `json:"principal_reference"`
			Action             string `json:"action"`
			Resource           struct {
				Type string  `json:"type"`
				Key  string  `json:"key"`
				Org  *string `json:"organization_id"`
			} `json:"resource"`
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		snap, known := p.snaps[body.PrincipalReference]
		label := p.label[body.PrincipalReference]
		p.mu.Unlock()
		req := authorization.Request{Action: body.Action, ResourceKind: body.Resource.Type}
		var org any
		if body.Resource.Org != nil {
			org = *body.Resource.Org
			req.OrganizationID = *body.Resource.Org
		}
		d := authorization.Decision{Reason: authorization.ReasonInvalidRequest}
		if known {
			d = evaluator.Evaluate(req, snap)
		}
		reason := string(d.Reason)
		answer := map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": d.Allowed,
			"reason_code": reason, "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		}
		obl := map[string]any{}
		if d.Allowed && d.Obligations != nil {
			o := d.Obligations
			if o.TenantScope != "" {
				obl["tenant_scope"] = o.TenantScope
			}
			if o.RepositoryScope != "" {
				obl["repository_scope"] = o.RepositoryScope
			}
			if o.RepositoryRestricted || len(o.RepositoryAllowlist) > 0 {
				obl["repository_allowlist"] = append([]string{}, o.RepositoryAllowlist...)
			}
			if len(o.Redaction) > 0 {
				obl["redaction"] = o.Redaction
			}
			if o.MinimumAssurance != "" {
				obl["minimum_assurance"] = o.MinimumAssurance
			}
			if len(obl) > 0 {
				answer["obligations"] = obl
			}
		}
		p.mu.Lock()
		p.seen = append(p.seen, fmt.Sprintf("%s %s %s/%v -> allowed=%v reason=%s(internal %s) obligations=%v", label, body.Action, body.Resource.Type, org, d.Allowed, reason, d.Reason, obl))
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *paPDP) register(token, label string, s paSnap) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snaps[token], p.label[token] = s, label
}

func (p *paPDP) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.seen
	p.seen = nil
	return out
}

// paRows are real query-api REST rows, one per action of the sweep.
var paRows = []string{"/api/v1/home", "/api/v1/flame", "/api/v1/drilldown/prs", "/api/v1/drilldown/issues"}

// paHandler is the routed REST authentication (authenticateRESTRequest, the edge verifier routed by row) in front of
// each row, behind the plane handler carrying gate. The handler past authentication is a stub: it echoes the
// organization and the scope the request may serve, its organization and repository ceiling (no ClickHouse read). The
// scope kind is left out: an enforced allow is KindOrganization where legacy is KindLegacy, and both serve every
// repository of the organization.
func paHandler(t *testing.T, store *accessStore, m *accessMinter, gate *pdp.Gate) http.Handler {
	t.Helper()
	verifier, _ := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(newQueryAPIUserStoreFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	setup := testAccess(m.keys, nil, accessTestNow)
	if err := routeEdgeVerifier(edge, setup.opts); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, row := range paRows {
		mux.HandleFunc(row, func(w http.ResponseWriter, r *http.Request) {
			claims, ok := authenticateRESTRequest(w, r, verifier, edge, "platform-admin-parity")
			if !ok {
				return
			}
			r = r.WithContext(authctx.WithClaims(r.Context(), claims))
			org, has := claims.Scope.OrgID()
			keys, _ := json.Marshal(claims.Scope.RepoKeys())
			_, _ = fmt.Fprintf(w, `{"org":%q,"scope_org":%q,"scope_has_org":%v,"restricted":%v,"repo_keys":%s}`, claims.OrgID, org, has,
				claims.Scope.Restricted(), keys)
		})
	}
	return planeHandler(mux, gate)
}

func paGate(t *testing.T, mode, cells string, p *paPDP) *pdp.Gate {
	t.Helper()
	provider := sdkmetric.NewMeterProvider()
	cfg := pdp.Config{Mode: mode, Switch: authzenforce.Parse(authzenforce.QueryAPI, cells), Meter: provider.Meter("test"),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if p != nil {
		client, err := pep.New(pep.Config{BaseURL: p.srv.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Decider = client
	}
	return pdp.New(cfg)
}

// Product rule 1 (D6773), CHAOS-13209 B3: a superuser with no membership row gets, with the analytics, metrics, git
// and work_items cells on, the answer the same token gets with the cells off, on one real REST row per action.
// No-org EdDSA was 503 with no PDP call (legacy 200). The org claim with no row passes the routed authenticator since
// CHAOS-13206, and the PDP gives the legacy 200 since CHAOS-13240, in a suspended organization too.
func TestPlatformAdminNoMembershipQueryAPIAnswersEqualLegacy(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	store := newAccessStore()
	org := ecOrg.String()
	store.states[paSU0] = policy.UserState{IsActive: true, TokenVersion: 0, IsSuperuser: true}
	store.found[paSU0] = true
	store.liveSessions[paSID(paSU0)] = policy.SessionRow{PrincipalID: paSU0, LegacyTokenVersion: i64(0)}
	p := newPAPDP(t)
	legacy := paHandler(t, store, m, paGate(t, pdp.ModeOff, "", nil))
	enforced := paHandler(t, store, m, paGate(t, pdp.ModeEnforce, "analytics,metrics,git,work_items", p))
	ed := func(orgClaim *string) string {
		extra := map[string]any{"sid": sessionid.Wire(paSID(paSU0)), "org_id": nil}
		if orgClaim != nil {
			extra["org_id"] = *orgClaim
		}
		return m.mint(paSU0, extra, nil)
	}
	su0 := paSnap{platformRole: "platform_admin", members: map[string]string{}}
	tokens := map[string]string{
		"EdDSA no org":            ed(nil),
		"EdDSA org claim, no row": ed(&org),
	}
	for _, label := range []string{"EdDSA no org", "EdDSA org claim, no row"} {
		token := tokens[label]
		p.register(token, label, su0)
		for _, row := range paRows {
			t.Run(label+" GET "+row, func(t *testing.T) {
				serve := func(h http.Handler) (int, string) {
					req := httptest.NewRequest(http.MethodGet, row, nil)
					req.Header.Set("Authorization", "Bearer "+token)
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, req)
					return rec.Code, strings.TrimSpace(rec.Body.String())
				}
				wantStatus, wantBody := serve(legacy)
				p.take()
				if label == "EdDSA org claim, no row" && wantStatus != http.StatusOK {
					t.Fatalf("legacy %d %s, want 200 (a platform admin with no membership row reads its org claim)", wantStatus, wantBody)
				}
				for _, suspended := range []bool{false, true} {
					p.register(token, label, paSnap{platformRole: su0.platformRole, members: su0.members, suspended: suspended})
					gotStatus, gotBody := serve(enforced)
					calls := p.take()
					if gotStatus != wantStatus || gotBody != wantBody {
						t.Errorf("suspended org %v: enforced %d %s (PDP %v), want the legacy %d %s", suspended, gotStatus, gotBody, calls,
							wantStatus, wantBody)
					}
				}
			})
		}
	}
}
