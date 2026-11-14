//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

var (
	parOrgA = uuid.MustParse("00000000-0000-4000-8000-00000000aa01")
	parOrgB = uuid.MustParse("00000000-0000-4000-8000-00000000bb01")
	parSU0  = uuid.MustParse("00000000-0000-4000-8000-0000000000f0") // superuser, no membership anywhere
	parSU   = uuid.MustParse("00000000-0000-4000-8000-0000000000f1") // superuser, admin in A
	parSUI  = uuid.MustParse("00000000-0000-4000-8000-0000000000f2") // superuser, admin in A, live impersonation row -> T in B
	parOA   = uuid.MustParse("00000000-0000-4000-8000-0000000000f3") // admin in A only
	parOAB  = uuid.MustParse("00000000-0000-4000-8000-0000000000f4") // admin in A and admin in B
	parT    = uuid.MustParse("00000000-0000-4000-8000-0000000000f5") // admin in B (impersonation target)
	parM    = uuid.MustParse("00000000-0000-4000-8000-0000000000f6") // member in B (PATCH target)
)

func parSID(u uuid.UUID) uuid.UUID { return uuid.NewSHA1(u, []byte("sid")) }

// parityStore is policy.PGStore over the real migrated database, with the auth-service session rows (auth.sessions)
// held in memory: one live session per seeded user.
type parityStore struct {
	policy.PGStore
	sessions map[uuid.UUID]policy.SessionRow
}

func (s parityStore) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	row, ok := s.sessions[id]
	return row, ok, nil
}

// paritySnap is the principal as auth-service's evaluator reads it; the same facts the database holds.
type paritySnap struct {
	platformRole string
	members      map[string]string
	del          *authorization.Delegation
}

func (s paritySnap) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (s paritySnap) PlatformRole() string                  { return s.platformRole }
func (s paritySnap) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (s paritySnap) Membership(org string) (authorization.Membership, bool) {
	role, ok := s.members[org]
	if !ok {
		return authorization.Membership{}, false
	}
	return authorization.Membership{OrgID: org, Role: role, Status: authorization.StatusActive}, true
}
func (s paritySnap) Grants(string) ([]authorization.Grant, error) { return nil, nil }
func (s paritySnap) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s paritySnap) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (s paritySnap) NowUnixNano() int64 { return time.Now().UnixNano() }
func (s paritySnap) Delegation() (authorization.Delegation, bool) {
	if s.del == nil {
		return authorization.Delegation{}, false
	}
	return *s.del, true
}

// parityPDP answers /v1/authorize with the production evaluator (authorization.NewEvaluator) over the snapshot of
// the bearer; the wire reason is the evaluator's own (allowed_by_platform_admin included), as authapi sends it.
type parityPDP struct {
	srv   *httptest.Server
	mu    sync.Mutex
	snaps map[string]paritySnap
	label map[string]string
	seen  []string
}

func newParityPDP(t *testing.T) *parityPDP {
	t.Helper()
	p := &parityPDP{snaps: map[string]paritySnap{}, label: map[string]string{}}
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
		if body.Resource.Type == "repository" {
			req.ResourceKey = body.Resource.Key
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

func (p *parityPDP) register(token, label string, snap paritySnap) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snaps[token], p.label[token] = snap, label
}

func (p *parityPDP) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.seen
	p.seen = nil
	return out
}

func (p *parityPDP) client(t *testing.T) *pep.Client {
	t.Helper()
	c, err := pep.New(pep.Config{BaseURL: p.srv.URL, AllowPlainHTTP: true, HTTPClient: p.srv.Client(), ClientService: goAPIAccessService, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type parityRig struct {
	legacy, enforced http.Handler
	minter           *eddsaMinter
	pdp              *parityPDP
	pool             *pgxpool.Pool
	revoker          *parityRevoker
}

const parityNamespaces = "members,billing,license,settings,teams,integrations,org"

// parityRevoker stands in for the acr client ACR_REVOKE_URL / _TOKEN_URL / _SUBJECT_FILE build (newACRRevoker): it
// records each call and admits it (204), so the route's own checks decide every answer.
type parityRevoker struct {
	mu    sync.Mutex
	calls []string
}

func (r *parityRevoker) Revoke(_ context.Context, orgID, credentialID, actorType, actorID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("org=%s cred=%s actor_type=%s actor=%s", orgID, credentialID, actorType, actorID))
	return nil
}

func (r *parityRevoker) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.calls
	r.calls = nil
	return out
}

func newParityRig(t *testing.T, ctx context.Context) *parityRig {
	t.Helper()
	instance := pgschema.Start(ctx, t)
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgseed.Org(ctx, t, pool, parOrgA.String(), "community")
	pgseed.Org(ctx, t, pool, parOrgB.String(), "community")
	sessions := map[uuid.UUID]policy.SessionRow{}
	for _, u := range []uuid.UUID{parSU0, parSU, parSUI, parOA, parOAB, parT, parM} {
		pgseed.User(ctx, t, pool, u.String())
		sessions[parSID(u)] = policy.SessionRow{PrincipalID: u, LegacyTokenVersion: i64ptr(0)}
	}
	for _, u := range []uuid.UUID{parSU0, parSU, parSUI} {
		pgseed.SetUserState(ctx, t, pool, u.String(), true, true, 0)
	}
	member := func(u, org uuid.UUID, role string) {
		pgseed.Membership(ctx, t, pool, uuid.NewSHA1(u, org[:]).String(), u.String(), org.String(), role)
	}
	member(parSU, parOrgA, "admin")
	member(parSUI, parOrgA, "admin")
	member(parOA, parOrgA, "admin")
	member(parOAB, parOrgA, "admin")
	member(parOAB, parOrgB, "admin")
	member(parT, parOrgB, "admin")
	member(parM, parOrgB, "member")
	for _, inv := range []struct{ id, org, sid string }{{"a1000000-0000-4000-8000-0000000000a1", parOrgA.String(), "in_org_a"}, {"b1000000-0000-4000-8000-0000000000b1", parOrgB.String(), "in_org_b"}} {
		if _, err := pool.Exec(ctx, `INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, currency, metadata, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, 'cus_x', 'open', 100, 'usd', '{}', now(), now())`, inv.id, inv.org, inv.sid); err != nil {
			t.Fatal(err)
		}
	}
	pgseed.ImpersonationSession(ctx, t, pool, uuid.NewSHA1(parSUI, []byte("imp")).String(), parSUI.String(), parT.String(), parOrgB.String(), "admin",
		time.Now().Add(2*time.Hour), nil)

	rig := &parityRig{minter: newEdDSAMinter(t), pdp: newParityPDP(t), pool: pool, revoker: &parityRevoker{}}
	store := parityStore{PGStore: policy.PGStore{Pool: pool}, sessions: sessions}
	build := func(enforcer *policy.Enforcer) http.Handler {
		auth, err := policy.NewAuthenticator(store, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		delegated, err := goAPIDelegatedRows()
		if err != nil {
			t.Fatal(err)
		}
		auth.DelegatedRows(delegated)
		eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: rig.minter.keys})
		if err != nil {
			t.Fatal(err)
		}
		validator, err := accessverify.New(accessverify.Config{
			Rows: realRows(t), EdDSA: eddsa,
			Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: quietLogger(), Now: func() time.Time { return probeNow },
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
			t.Fatal(err)
		}
		clickHouse := unreachableClickHouse(t)
		guard := policy.NewGuard(auth, quietLogger())
		if enforcer != nil {
			guard.SetEnforcer(enforcer)
		}
		deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: guard, ACRRevoker: rig.revoker}
		scope := policy.NewScope(auth, quietLogger())
		server, err := NewServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()), scope.Layers()...)
		if err != nil {
			t.Fatal(err)
		}
		return server.Handler()
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	enf, err := policy.NewEnforcer(policy.EnforceConfig{Decider: rig.pdp.client(t), Rows: rows,
		Switch: authzenforce.Parse(authzenforce.GoAPI, parityNamespaces), Timeout: 2 * time.Second, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	rig.legacy, rig.enforced = build(nil), build(enf)
	return rig
}

func (rig *parityRig) hs256(t *testing.T, u uuid.UUID, org *uuid.UUID, role string) string {
	claims := jwt.MapClaims{"sub": u.String(), "tv": 0, "email": u.String() + "@example.test"}
	if u == parSU0 || u == parSU || u == parSUI {
		claims["is_superuser"] = true
	}
	if org != nil {
		claims["org_id"] = org.String()
		claims["role"] = role
	}
	return probeHS256(t, probeKey, claims)
}

func (rig *parityRig) eddsa(u uuid.UUID, org *uuid.UUID, actor *uuid.UUID) string {
	extra := map[string]any{"sid": sessionid.Wire(parSID(u)), "org_id": nil}
	if org != nil {
		extra["org_id"] = org.String()
	}
	if actor != nil {
		extra["act"] = map[string]any{"sub": wirePrincipal(*actor)}
	}
	return rig.minter.mint(u, extra)
}

func (rig *parityRig) do(h http.Handler, method, path, body, token string) (int, string, http.Header) {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	b := strings.TrimSpace(rec.Body.String())
	if len(b) > 400 {
		b = b[:400] + "..."
	}
	return rec.Code, b, rec.Header()
}

func parPtr(u uuid.UUID) *uuid.UUID { return &u }

// Product rule 1 (D6773), CHAOS-13209: on the real go-api Routes over a migrated database, with the members, billing
// and other namespaces enforced by the production evaluator, every B3/B4 row gives the platform admin and the
// cross-org callers the answer the same token gets with enforcement off (status and body); on the acr revoke route a
// no-membership platform admin revokes as the organization admin (F1, equal on both sides).
func TestPlatformAdminAndCrossOrgAnswersEqualLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newParityRig(t, ctx)
	A, B := parOrgA.String(), parOrgB.String()
	snap := func(platform string, members map[string]string) paritySnap {
		return paritySnap{platformRole: platform, members: members}
	}
	su0 := snap("platform_admin", map[string]string{})
	su := snap("platform_admin", map[string]string{A: "admin"})
	oa := snap("", map[string]string{A: "admin"})
	oab := snap("", map[string]string{A: "admin", B: "admin"})
	type tok struct{ label, token string }
	mk := func(label, token string, s paritySnap) tok {
		rig.pdp.register(token, label, s)
		return tok{label, token}
	}
	hsSU0b := mk("SU0/HS256/org=B", rig.hs256(t, parSU0, parPtr(parOrgB), ""), su0)
	hsSU0none := mk("SU0/HS256/no-org", rig.hs256(t, parSU0, nil, ""), su0)
	edSU0b := mk("SU0/EdDSA/org=B", rig.eddsa(parSU0, parPtr(parOrgB), nil), su0)
	edSU0none := mk("SU0/EdDSA/no-org", rig.eddsa(parSU0, nil, nil), su0)
	hsSU := mk("SU/HS256/org=A", rig.hs256(t, parSU, parPtr(parOrgA), "admin"), su)
	edSU := mk("SU/EdDSA/org=A", rig.eddsa(parSU, parPtr(parOrgA), nil), su)
	hsOA := mk("OA/HS256/org=A", rig.hs256(t, parOA, parPtr(parOrgA), "admin"), oa)
	edOA := mk("OA/EdDSA/org=A", rig.eddsa(parOA, parPtr(parOrgA), nil), oa)
	hsOAB := mk("OAB/HS256/org=A", rig.hs256(t, parOAB, parPtr(parOrgA), "admin"), oab)
	edOAB := mk("OAB/EdDSA/org=A", rig.eddsa(parOAB, parPtr(parOrgA), nil), oab)

	membersB := "/api/v1/admin/orgs/" + B + "/members"
	patchM := "/api/v1/admin/orgs/" + B + "/members/" + parM.String()
	invB := "/api/v1/billing/invoices?org_id=" + B
	subsB := "/api/v1/billing/subscriptions/list?org_id=" + B
	type cell struct {
		item, method, path, body string
		caller                   tok
	}
	var cells []cell
	for _, c := range []tok{hsSU0b, hsSU0none, edSU0b, edSU0none} {
		cells = append(cells, cell{"B3", http.MethodPatch, patchM, `{"role":"member"}`, c}, cell{"B3", http.MethodGet, membersB, "", c})
	}
	for _, c := range []tok{hsSU, edSU, hsOA, edOA, hsOAB, edOAB} {
		cells = append(cells, cell{"B4", http.MethodGet, membersB, "", c}, cell{"B4", http.MethodGet, invB, "", c},
			cell{"B4", http.MethodGet, subsB, "", c})
	}
	for _, c := range cells {
		// The enforced side first: a write it refuses changes nothing the legacy side then reads.
		gotStatus, gotBody, _ := rig.do(rig.enforced, c.method, c.path, c.body, c.caller.token)
		calls := rig.pdp.take()
		wantStatus, wantBody, _ := rig.do(rig.legacy, c.method, c.path, c.body, c.caller.token)
		rig.pdp.take()
		if gotStatus != wantStatus || gotBody != wantBody {
			t.Errorf("%s %s %s %s: enforced %d %s (PDP %v), want the legacy %d %s", c.item, c.caller.label, c.method, c.path,
				gotStatus, gotBody, calls, wantStatus, wantBody)
		}
	}

	// F1 is EQUAL: a holder of admin.orgs with an org claim and no membership row revokes as that organization's admin
	// on both paths (product rule 1, acrcredentials handlers.actor), and the acr call names the platform admin.
	revoke := "/api/v1/acr/credentials/cred_parity_1/revoke"
	wantCall := fmt.Sprintf("org=%s cred=cred_parity_1 actor_type=admin actor=%s", parOrgB, parSU0)
	for _, side := range []struct {
		name string
		h    http.Handler
	}{{"enforce-on", rig.enforced}, {"enforce-off", rig.legacy}} {
		status, body, _ := rig.do(side.h, http.MethodPost, revoke, "", edSU0b.token)
		calls := rig.revoker.take()
		if status != http.StatusNoContent || body != "" || len(calls) != 1 || calls[0] != wantCall {
			t.Errorf("F1 %s %s: %d %s, revoke calls %v, want 204 and the one call %s", side.name, edSU0b.label,
				status, body, calls, wantCall)
		}
	}
	var role string
	if err := rig.pool.QueryRow(ctx, `SELECT role FROM memberships WHERE user_id=$1 AND org_id=$2`, parM, parOrgB).Scan(&role); err != nil {
		t.Fatal(err)
	}
	if role != "member" {
		t.Errorf("M's role in B = %s, want member", role)
	}
}
