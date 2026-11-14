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

var (
	dlpT   = uuid.MustParse("33333333-3333-4333-8333-0000000000f5") // admin in ecOrg, delegated target
	dlpSUI = uuid.MustParse("33333333-3333-4333-8333-0000000000f2") // superuser actor
)

func dlpSID(u uuid.UUID) uuid.UUID { return uuid.NewSHA1(u, []byte("sid")) }

type dlpSnap struct {
	platformRole string
	members      map[string]string
	del          *authorization.Delegation
}

func (s dlpSnap) PrincipalStatus() authorization.Status          { return authorization.StatusActive }
func (s dlpSnap) PlatformRole() string                           { return s.platformRole }
func (s dlpSnap) OrganizationStatus(string) authorization.Status { return authorization.StatusActive }
func (s dlpSnap) Membership(org string) (authorization.Membership, bool) {
	role, ok := s.members[org]
	if !ok {
		return authorization.Membership{}, false
	}
	return authorization.Membership{OrgID: org, Role: role, Status: authorization.StatusActive}, true
}
func (s dlpSnap) Grants(string) ([]authorization.Grant, error) { return nil, nil }
func (s dlpSnap) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s dlpSnap) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (s dlpSnap) NowUnixNano() int64 { return time.Now().UnixNano() }
func (s dlpSnap) Delegation() (authorization.Delegation, bool) {
	if s.del == nil {
		return authorization.Delegation{}, false
	}
	return *s.del, true
}

type dlpPDP struct {
	srv   *httptest.Server
	mu    sync.Mutex
	snaps map[string]dlpSnap
	label map[string]string
	seen  []string
}

func newDLPPDP(t *testing.T) *dlpPDP {
	t.Helper()
	p := &dlpPDP{snaps: map[string]dlpSnap{}, label: map[string]string{}}
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

func (p *dlpPDP) register(token, label string, s dlpSnap) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snaps[token], p.label[token] = s, label
}

func (p *dlpPDP) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.seen
	p.seen = nil
	return out
}

// dlpRows are real query-api REST read rows, one per action of the delegated chain.
var dlpRows = []string{"/api/v1/home", "/api/v1/flame", "/api/v1/drilldown/prs", "/api/v1/drilldown/issues"}

// dlpHandler is the routed REST authentication (authenticateRESTRequest, the edge verifier routed by row) in front of
// each row, behind the plane handler carrying gate. The handler past authentication is a
// stub: it echoes the organization and the scope kind the request may serve (no ClickHouse read).
func dlpHandler(t *testing.T, store *accessStore, m *accessMinter, gate *pdp.Gate) http.Handler {
	t.Helper()
	return dlpHandlerFor(t, store, m, gate, dlpRows)
}

// dlpHandlerFor is dlpHandler over the route patterns routes.
func dlpHandlerFor(t *testing.T, store *accessStore, m *accessMinter, gate *pdp.Gate, routes []string) http.Handler {
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
	for _, row := range routes {
		mux.HandleFunc(row, func(w http.ResponseWriter, r *http.Request) {
			claims, ok := authenticateRESTRequest(w, r, verifier, edge, "parity")
			if !ok {
				return
			}
			r = r.WithContext(authctx.WithClaims(r.Context(), claims))
			org, has := claims.Scope.OrgID()
			_, _ = fmt.Fprintf(w, `{"org":%q,"scope_kind":%d,"scope_org":%q,"scope_has_org":%v}`, claims.OrgID, claims.Scope.Kind(), org, has)
		})
	}
	return planeHandler(mux, gate)
}

func dlpGate(t *testing.T, mode, cells string, p *dlpPDP) *pdp.Gate {
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

// dlpReadCells switches on the actions of dlpRows.
var dlpReadCells = "analytics,metrics,git,work_items"

// A level the session cannot meet stays no verdict: an aal2 allow is a 503 on every enforced read row.
func TestADelegatedReadWithAnAAL2AllowStaysUnavailable(t *testing.T) {
	quietStdLog(t)
	h := newDLPRig(t, "aal2")
	for _, row := range dlpRows {
		if got := h.serve(row, h.enforced); got.code != http.StatusServiceUnavailable {
			t.Errorf("GET %s delegated, aal2 allow: %d %s, want 503", row, got.code, got.body)
		}
		h.pdp.take()
	}
}

type dlpAnswer struct {
	code int
	body string
}

type dlpRig struct {
	pdp              *dlpPDP
	legacy, enforced http.Handler
	token            string
}

func newDLPRig(t *testing.T, assurance string) *dlpRig {
	t.Helper()
	return newDLPRigFor(t, assurance, dlpRows)
}

func newDLPRigFor(t *testing.T, assurance string, routes []string) *dlpRig {
	t.Helper()
	m := newAccessMinter(t)
	store := newAccessStore()
	org := ecOrg.String()
	for _, u := range []uuid.UUID{dlpT, dlpSUI} {
		store.states[u] = policy.UserState{IsActive: true, TokenVersion: 0, IsSuperuser: u == dlpSUI}
		store.found[u] = true
		store.liveSessions[dlpSID(u)] = policy.SessionRow{PrincipalID: u, LegacyTokenVersion: i64(0)}
	}
	if store.roles == nil {
		store.roles = map[[2]uuid.UUID]string{}
	}
	store.members[[2]uuid.UUID{dlpT, ecOrg}] = true
	store.roles[[2]uuid.UUID{dlpT, ecOrg}] = "admin"
	p := newDLPPDP(t)
	token := m.mint(dlpT, map[string]any{"sid": sessionid.Wire(dlpSID(dlpT)), "org_id": org,
		"act": map[string]any{"sub": wirePrincipal(dlpSUI)}}, nil)
	p.register(token, "T/EdDSA/act=SUI", dlpSnap{members: map[string]string{org: "admin"}, del: &authorization.Delegation{
		Chain:             []authorization.Hop{{ActorPrincipalID: wirePrincipal(dlpSUI), PermittedActions: []string{"analytics.read", "metrics.read", "git.read", "work_items.read"}}},
		ActorPlatformRole: "platform_admin", TargetOrganizationID: org, Assurance: assurance}})
	return &dlpRig{pdp: p, token: token,
		legacy:   dlpHandlerFor(t, store, m, dlpGate(t, pdp.ModeOff, "", nil), routes),
		enforced: dlpHandlerFor(t, store, m, dlpGate(t, pdp.ModeEnforce, dlpReadCells, p), routes)}
}

func (h *dlpRig) serve(row string, handler http.Handler) dlpAnswer {
	return h.serveAs(http.MethodGet, row, handler)
}

func (h *dlpRig) serveAs(method, path string, handler http.Handler) dlpAnswer {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Authorization", "Bearer "+h.token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return dlpAnswer{rec.Code, strings.TrimSpace(rec.Body.String())}
}
