//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// Product rule 1 parity cells for a superuser with no membership row on go-api. Each cell serves one request on the
// real go-api Routes over a migrated database twice: legacy (no Enforcer) and enforced (the Enforcer over the real
// profile rows, every namespace of msrNamespaces on, the production evaluator behind the PDP wire). The enforced
// answer must equal the legacy answer (status and body). Each side's answer is also logged as a CELL line.

var (
	msrOrgB    = uuid.MustParse("00000000-0000-4000-8000-0000000013b1") // active, invoice + subscription seeded
	msrOrgU    = uuid.MustParse("00000000-0000-4000-8000-0000000013c1") // never had a row
	msrOrgR    = uuid.MustParse("00000000-0000-4000-8000-0000000013d1") // seeded, then its row deleted
	msrSU0     = uuid.MustParse("00000000-0000-4000-8000-0000000013f0") // superuser, no membership row anywhere
	msrPlain   = uuid.MustParse("00000000-0000-4000-8000-0000000013f1") // not a superuser, no membership row
	msrMemberB = uuid.MustParse("00000000-0000-4000-8000-0000000013f2") // not a superuser, member of B
	msrInvoice = "13b10000-0000-4000-8000-0000000000a1"
)

const msrNamespaces = "members,billing,license,settings,teams,integrations,org"

func msrSID(u uuid.UUID) uuid.UUID { return uuid.NewSHA1(u, []byte("msr-sid")) }

type msrStore struct {
	policy.PGStore
	sessions map[uuid.UUID]policy.SessionRow
}

func (s msrStore) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	row, ok := s.sessions[id]
	return row, ok, nil
}

// msrSnap is the principal as the evaluator reads it. The organization status is read from the same database at
// decision time with decisionstate's rule (row active -> Active, inactive -> Suspended, no row -> Unknown).
type msrSnap struct {
	platformRole string
	members      map[string]string
	orgStatus    authorization.Status
}

func (s msrSnap) PrincipalStatus() authorization.Status          { return authorization.StatusActive }
func (s msrSnap) PlatformRole() string                           { return s.platformRole }
func (s msrSnap) OrganizationStatus(string) authorization.Status { return s.orgStatus }
func (s msrSnap) Grants(string) ([]authorization.Grant, error)   { return nil, nil }
func (s msrSnap) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s msrSnap) NowUnixNano() int64 { return time.Now().UnixNano() }
func (s msrSnap) Delegation() (authorization.Delegation, bool) {
	return authorization.Delegation{}, false
}
func (s msrSnap) Membership(org string) (authorization.Membership, bool) {
	role, ok := s.members[org]
	if !ok {
		return authorization.Membership{}, false
	}
	return authorization.Membership{OrgID: org, Role: role, Status: authorization.StatusActive}, true
}
func (s msrSnap) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}

func msrOrgStatus(ctx context.Context, pool *pgxpool.Pool, org string) (authorization.Status, error) {
	var active bool
	err := pool.QueryRow(ctx, `SELECT COALESCE(is_active, false) FROM organizations WHERE id::text = $1`, org).Scan(&active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return authorization.StatusUnknown, nil
	case err != nil:
		return authorization.StatusUnknown, err
	case active:
		return authorization.StatusActive, nil
	}
	return authorization.StatusSuspended, nil
}

var msrStatusName = map[authorization.Status]string{
	authorization.StatusUnknown: "Unknown", authorization.StatusActive: "Active", authorization.StatusInvited: "Invited",
	authorization.StatusSuspended: "Suspended", authorization.StatusRemoved: "Removed",
}

type msrPDP struct {
	srv   *httptest.Server
	mu    sync.Mutex
	snaps map[string]msrSnap
	label map[string]string
	seen  []string
}

func newMsrPDP(t *testing.T, pool *pgxpool.Pool) *msrPDP {
	t.Helper()
	p := &msrPDP{snaps: map[string]msrSnap{}, label: map[string]string{}}
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
			status, err := msrOrgStatus(r.Context(), pool, *body.Resource.Org)
			if err != nil {
				http.Error(w, "store", http.StatusBadGateway)
				return
			}
			snap.orgStatus = status
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
		if d.Allowed && d.Obligations != nil && d.Obligations.TenantScope != "" {
			answer["obligations"] = map[string]any{"tenant_scope": d.Obligations.TenantScope}
		}
		p.mu.Lock()
		p.seen = append(p.seen, fmt.Sprintf("%s %s %s/%v org_status=%s -> allowed=%v reason=%s", label, body.Action, body.Resource.Type, org,
			msrStatusName[snap.orgStatus], d.Allowed, d.Reason))
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *msrPDP) register(token, label string, s msrSnap) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snaps[token], p.label[token] = s, label
}

func (p *msrPDP) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.seen
	p.seen = nil
	return out
}

type msrRig struct {
	legacy, enforced http.Handler
	minter           *eddsaMinter
	pdp              *msrPDP
	pool             *pgxpool.Pool
}

func newMsrRig(t *testing.T, ctx context.Context) *msrRig {
	t.Helper()
	instance := pgschema.Start(ctx, t)
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgseed.Org(ctx, t, pool, msrOrgB.String(), "community")
	sessions := map[uuid.UUID]policy.SessionRow{}
	for _, u := range []uuid.UUID{msrSU0, msrPlain, msrMemberB} {
		pgseed.User(ctx, t, pool, u.String())
		sessions[msrSID(u)] = policy.SessionRow{PrincipalID: u, LegacyTokenVersion: i64ptr(0)}
	}
	pgseed.SetUserState(ctx, t, pool, msrSU0.String(), true, true, 0)
	pgseed.Membership(ctx, t, pool, uuid.NewSHA1(msrMemberB, []byte("msr-membership")).String(), msrMemberB.String(), msrOrgB.String(), "member")
	for _, stmt := range []string{
		`INSERT INTO billing_plans (id, key, name, tier, is_active, display_order) VALUES ('13e00000-0000-4000-8000-000000000001', 'msr-plan', 'Msr plan', 'team', true, 0)`,
		`INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active) VALUES ('13ef0000-0000-4000-8000-000000000001', '13e00000-0000-4000-8000-000000000001', 'monthly', 100, 'usd', true)`,
		`INSERT INTO subscriptions (id, org_id, billing_plan_id, billing_price_id, stripe_subscription_id, stripe_customer_id, status, current_period_start, current_period_end, created_at, updated_at)
VALUES ('13b20000-0000-4000-8000-000000000001', '` + msrOrgB.String() + `', '13e00000-0000-4000-8000-000000000001', '13ef0000-0000-4000-8000-000000000001', 'sub_msr_b', 'cus_msr_b', 'active', now(), now() + interval '30 days', now(), now())`,
		`INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, currency, metadata, created_at, updated_at)
VALUES ('` + msrInvoice + `', '` + msrOrgB.String() + `', 'in_msr_b', 'cus_msr_b', 'open', 100, 'usd', '{}', now(), now())`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	pgseed.Org(ctx, t, pool, msrOrgR.String(), "community")
	if tag, err := pool.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, msrOrgR); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("delete org R: %v (%d rows)", err, tag.RowsAffected())
	}

	rig := &msrRig{minter: newEdDSAMinter(t), pool: pool}
	rig.pdp = newMsrPDP(t, pool)
	store := msrStore{PGStore: policy.PGStore{Pool: pool}, sessions: sessions}
	clickHouse := unreachableClickHouse(t)
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
		guard := policy.NewGuard(auth, quietLogger())
		if enforcer != nil {
			guard.SetEnforcer(enforcer)
		}
		scopeRows, err := goAPIScopeRows()
		if err != nil {
			t.Fatal(err)
		}
		scope := policy.NewScope(auth, quietLogger())
		scope.DeclaredRows(scopeRows)
		deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: guard}
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
	client, err := pep.New(pep.Config{BaseURL: rig.pdp.srv.URL, AllowPlainHTTP: true, HTTPClient: rig.pdp.srv.Client(), ClientService: goAPIAccessService, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	enf, err := policy.NewEnforcer(policy.EnforceConfig{Decider: client, Rows: rows,
		Switch: authzenforce.Parse(authzenforce.GoAPI, msrNamespaces), Timeout: 2 * time.Second, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	rig.legacy, rig.enforced = build(nil), build(enf)
	return rig
}

func (rig *msrRig) hs256(t *testing.T, u, org uuid.UUID) string {
	claims := jwt.MapClaims{"sub": u.String(), "tv": 0, "email": u.String() + "@example.test", "org_id": org.String(), "role": ""}
	if u == msrSU0 {
		claims["is_superuser"] = true
	}
	return probeHS256(t, probeKey, claims)
}

func (rig *msrRig) eddsa(u, org uuid.UUID) string {
	return rig.minter.mint(u, map[string]any{"sid": sessionid.Wire(msrSID(u)), "org_id": org.String()})
}

// eddsaNoOrg is a user access token with no org claim.
func (rig *msrRig) eddsaNoOrg(u uuid.UUID) string {
	return rig.minter.mint(u, map[string]any{"sid": sessionid.Wire(msrSID(u)), "org_id": nil})
}

func (rig *msrRig) do(h http.Handler, method, path, body, token string) (int, string) {
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
	return rec.Code, b
}

// msrVerdict: 401, 403 and 503 refuse the caller; any other status is an answer past the guard.
func msrVerdict(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusServiceUnavailable:
		return "REFUSE"
	}
	return "ADMIT"
}

type msrCaller struct {
	label, token string
}

type msrRow struct {
	action, method, path, body string
}

// msrAnswer is one side's answer to one request.
type msrAnswer struct {
	status int
	body   string
	calls  []string
}

// msrServe serves the row to one side and returns its answer with the PDP calls it made.
func (rig *msrRig) msrServe(h http.Handler, r msrRow, c msrCaller) msrAnswer {
	status, body := rig.do(h, r.method, r.path, r.body, c.token)
	return msrAnswer{status: status, body: body, calls: rig.pdp.take()}
}

// msrParity serves the row to both sides and fails unless the enforced answer equals the legacy answer.
func (rig *msrRig) msrParity(t *testing.T, cell, org string, c msrCaller, r msrRow) (legacy, enforced msrAnswer) {
	t.Helper()
	legacy, enforced = rig.msrAnswers(t, cell, org, c, r)
	if enforced.status != legacy.status || enforced.body != legacy.body {
		t.Errorf("%s %s %s %s: enforced %d %s, legacy %d %s; product rule 1 wants the legacy answer", c.label, org, r.method,
			r.path, enforced.status, enforced.body, legacy.status, legacy.body)
	}
	return legacy, enforced
}

// msrAnswers serves the row to both sides. A write row goes to the enforced side first (a write it refuses changes
// nothing the legacy side then reads); a read row goes to the legacy side first. It logs one CELL line per side and
// returns both answers.
func (rig *msrRig) msrAnswers(t *testing.T, cell, org string, c msrCaller, r msrRow) (legacy, enforced msrAnswer) {
	t.Helper()
	if r.method == http.MethodGet {
		legacy = rig.msrServe(rig.legacy, r, c)
		enforced = rig.msrServe(rig.enforced, r, c)
	} else {
		enforced = rig.msrServe(rig.enforced, r, c)
		legacy = rig.msrServe(rig.legacy, r, c)
	}
	if enforced.status == 0 || legacy.status == 0 {
		t.Fatalf("%s %s %s %s %s: no answer (enforced %d, legacy %d)", cell, org, c.label, r.method, r.path, enforced.status, legacy.status)
	}
	t.Logf("CELL|%s|go-api|%s|%s|%s|%s %s|legacy|%d|%s|%s|pdp_calls=%d", cell, org, c.label, r.action, r.method, r.path,
		legacy.status, msrVerdict(legacy.status), legacy.body, len(legacy.calls))
	t.Logf("CELL|%s|go-api|%s|%s|%s|%s %s|enforced|%d|%s|%s|pdp=%v", cell, org, c.label, r.action, r.method, r.path,
		enforced.status, msrVerdict(enforced.status), enforced.body, enforced.calls)
	if len(legacy.calls) != 0 {
		t.Errorf("%s %s %s %s: the legacy side asked the PDP: %v", c.label, org, r.method, r.path, legacy.calls)
	}
	return legacy, enforced
}

// msrWantJudgedAllow fails unless the enforced side asked the PDP exactly once, for the row's action on org, and got
// an allow: the answer equals legacy because the decision point admits the caller, not because it was not asked.
func msrWantJudgedAllow(t *testing.T, label string, r msrRow, org uuid.UUID, enforced msrAnswer) {
	t.Helper()
	want := " " + r.action + " organization/" + org.String() + " "
	if len(enforced.calls) != 1 || !strings.Contains(enforced.calls[0], want) || !strings.Contains(enforced.calls[0], "allowed=true") {
		t.Errorf("%s %s %s: enforced PDP calls %v, want one allow of %s on %s", label, r.method, r.path, enforced.calls, r.action, org)
	}
}

// msrBillingLicenseRows are the 5 billing.read and 2 license.read go-api rows, asked as the token's org would be
// (no org_id; the {org_id} path segment = org). The invoice is org B's.
func msrBillingLicenseRows(org uuid.UUID) []msrRow {
	o := org.String()
	return []msrRow{
		{"billing.read", http.MethodGet, "/api/v1/billing/invoices", ""},
		{"billing.read", http.MethodGet, "/api/v1/billing/invoices/" + msrInvoice, ""},
		{"billing.read", http.MethodGet, "/api/v1/billing/subscriptions", ""},
		{"billing.read", http.MethodGet, "/api/v1/billing/subscriptions/history", ""},
		{"billing.read", http.MethodGet, "/api/v1/billing/subscriptions/list", ""},
		{"license.read", http.MethodGet, "/api/v1/billing/entitlements/" + o, ""},
		{"license.read", http.MethodGet, "/api/v1/licensing/entitlements/" + o, ""},
	}
}

// msrOrgIDRows are the 5 billing.read query rows naming org in org_id.
func msrOrgIDRows(org uuid.UUID) []msrRow {
	var out []msrRow
	for _, r := range msrBillingLicenseRows(org)[:5] {
		sep := "?"
		if strings.Contains(r.path, "?") {
			sep = "&"
		}
		r.path += sep + "org_id=" + org.String()
		out = append(out, r)
	}
	return out
}

// C3: a superuser with no membership row whose token names active org B, on the 5 billing.read and 2 license.read
// rows, without and with org_id=B. The EdDSA user access token is the judged class: the PDP is asked and allows (the
// platform_admin flag), and the legacy answer is an answer past the guard. HS256 is the cross-check: the retired class
// on a routed row, refused 401 on both sides.
func TestPlatformAdminNoMembershipBillingAndLicenseReadsEqualLegacyWired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newMsrRig(t, ctx)
	su0 := msrSnap{platformRole: rule.PlatformAdmin, members: map[string]string{}}
	ed := msrCaller{"SU0/EdDSA/org=B", rig.eddsa(msrSU0, msrOrgB)}
	hs := msrCaller{"SU0/HS256/org=B", rig.hs256(t, msrSU0, msrOrgB)}
	rig.pdp.register(ed.token, ed.label, su0)
	rig.pdp.register(hs.token, hs.label, su0)
	rows := append(msrBillingLicenseRows(msrOrgB), msrOrgIDRows(msrOrgB)...)
	for _, c := range []msrCaller{ed, hs} {
		for _, r := range rows {
			t.Run(c.label+" "+r.method+" "+r.path, func(t *testing.T) {
				legacy, enforced := rig.msrParity(t, "C3", "org B active", c, r)
				if c.label == ed.label {
					if msrVerdict(legacy.status) != "ADMIT" {
						t.Errorf("legacy %d %s: want an answer past the guard", legacy.status, legacy.body)
					}
					msrWantJudgedAllow(t, c.label, r, msrOrgB, enforced)
				}
			})
		}
	}
}

// The billing.read rows for a superuser with no membership row whose token has no org claim, without org_id and
// with org_id=B (the 13148 venue rows the shadow skipped). A member of B with a token for B is the control: it keeps
// its own answer, so the change is not wider.
func TestPlatformAdminNoOrgClaimBillingReadsEqualLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newMsrRig(t, ctx)
	su0 := msrSnap{platformRole: rule.PlatformAdmin, members: map[string]string{}}
	noOrg := msrCaller{"SU0/EdDSA/no org", rig.eddsaNoOrg(msrSU0)}
	rig.pdp.register(noOrg.token, noOrg.label, su0)
	member := msrCaller{"member of B/EdDSA/org=B", rig.eddsa(msrMemberB, msrOrgB)}
	rig.pdp.register(member.token, member.label, msrSnap{members: map[string]string{msrOrgB.String(): "member"}})
	billing := msrBillingLicenseRows(msrOrgB)[:5]
	rows := append(append([]msrRow{}, billing...), msrOrgIDRows(msrOrgB)...)
	for _, r := range rows {
		t.Run(noOrg.label+" "+r.method+" "+r.path, func(t *testing.T) {
			rig.msrParity(t, "no-org", "org B active", noOrg, r)
		})
	}
	for _, r := range billing {
		t.Run(member.label+" "+r.method+" "+r.path, func(t *testing.T) {
			rig.msrParity(t, "member", "org B active", member, r)
		})
	}
}

// A superuser with no membership row whose token names an organization with no row (U: never existed; R: its row
// deleted) gets the legacy answer on one row per platform_admin-flagged go-api action and on the 7 billing.read and
// license.read rows: the decision point admits a holder in an unknown organization. Control: a user without the
// platform action, same orgs, is refused on both sides, except the EdDSA billing.read row: the legacy side keeps a
// token org without a membership row as a member and admits it, while the decision point refuses the unknown
// organization (known divergence CHAOS-13261, kept stricter).
func TestPlatformAdminInAnUnknownOrDeletedOrganizationGoAPIAnswersEqualLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newMsrRig(t, ctx)
	su0 := msrSnap{platformRole: rule.PlatformAdmin, members: map[string]string{}}
	plain := msrSnap{members: map[string]string{}}
	for _, o := range []struct {
		label string
		id    uuid.UUID
	}{{"org U no row (never existed)", msrOrgU}, {"org R no row (deleted)", msrOrgR}} {
		org := o.id.String()
		flagged := []msrRow{
			{"billing.manage", http.MethodPost, "/api/v1/billing/subscriptions/cancel?org_id=" + org, "{}"},
			{"integrations.read", http.MethodGet, "/api/v1/admin/integrations", ""},
			{"integrations.write", http.MethodPost, "/api/v1/admin/integrations", "{}"},
			{"members.invite", http.MethodPost, "/api/v1/admin/orgs/" + org + "/invites", "{}"},
			{"members.manage", http.MethodPatch, "/api/v1/admin/orgs/" + org + "/members/" + msrPlain.String(), `{"role":"member"}`},
			{"members.read", http.MethodGet, "/api/v1/admin/orgs/" + org + "/members", ""},
			{"settings.write", http.MethodGet, "/api/v1/admin/settings/general", ""},
			{"teams.write", http.MethodGet, "/api/v1/admin/teams", ""},
		}
		rows := append(append(flagged, msrBillingLicenseRows(o.id)...), msrOrgIDRows(o.id)...)
		callers := []msrCaller{
			{"SU0/EdDSA/" + o.label, rig.eddsa(msrSU0, o.id)},
			{"SU0/HS256/" + o.label, rig.hs256(t, msrSU0, o.id)},
		}
		for _, c := range callers {
			rig.pdp.register(c.token, c.label, su0)
			for _, r := range rows {
				t.Run(c.label+" "+r.method+" "+r.path, func(t *testing.T) {
					legacy, _ := rig.msrParity(t, "M-go", o.label, c, r)
					if strings.Contains(c.label, "/EdDSA/") && msrVerdict(legacy.status) != "ADMIT" {
						t.Errorf("legacy %d %s: want an answer past the guard", legacy.status, legacy.body)
					}
				})
			}
		}
		for _, c := range []msrCaller{
			{"control plain/EdDSA/" + o.label, rig.eddsa(msrPlain, o.id)},
			{"control plain/HS256/" + o.label, rig.hs256(t, msrPlain, o.id)},
		} {
			rig.pdp.register(c.token, c.label, plain)
			for _, r := range []msrRow{rows[5], rows[8]} {
				t.Run(c.label+" "+r.method+" "+r.path, func(t *testing.T) {
					if r.action == "billing.read" && strings.Contains(c.label, "/EdDSA/") {
						legacy, enforced := rig.msrAnswers(t, "M-go", o.label, c, r)
						if legacy.status != http.StatusOK || !strings.HasPrefix(legacy.body, `{"items":[`) {
							t.Fatalf("control: legacy %d %s; want the measured legacy answer 200 {\"items\":[...", legacy.status, legacy.body)
						}
						if enforced.status == legacy.status && enforced.body == legacy.body {
							t.Fatalf("control: enforced now equals legacy (%d); remove the CHAOS-13261 row of this subtest from known_divergences.tsv", legacy.status)
						}
						if enforced.status != http.StatusForbidden || enforced.body != `{"detail":"Access denied"}` || len(enforced.calls) != 1 ||
							!strings.Contains(enforced.calls[0], " billing.read ") || !strings.HasSuffix(enforced.calls[0], " -> allowed=false reason=organization_inactive") {
							t.Fatalf("control: enforced %d %s pdp=%v is not the known CHAOS-13261 answer (403 Access denied, one billing.read PDP call refused organization_inactive)",
								enforced.status, enforced.body, enforced.calls)
						}
						venueoracle.KnownDivergenceSkip(t, "CHAOS-13261", strconv.Itoa(legacy.status), strconv.Itoa(enforced.status),
							fmt.Sprintf("legacy %s; enforced %s pdp=%v", legacy.body, enforced.body, enforced.calls))
					}
					legacy, enforced := rig.msrParity(t, "M-go", o.label, c, r)
					if msrVerdict(legacy.status) != "REFUSE" || msrVerdict(enforced.status) != "REFUSE" {
						t.Errorf("control: legacy %d, enforced %d; want both refused", legacy.status, enforced.status)
					}
				})
			}
		}
	}
}
