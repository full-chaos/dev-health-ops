package billing

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

const impersonationRefusalBody = `{"detail":"Not allowed in an impersonation session"}`

// sessionStore is the policy store of the route rig: users rows, memberships and live impersonation sessions.
type sessionStore struct {
	mu       sync.Mutex
	states   map[uuid.UUID]policy.UserState
	roles    map[[2]uuid.UUID]string
	sessions map[uuid.UUID]*policy.Impersonation
}

func (s *sessionStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, found := s.states[id]
	return state, found, nil
}

func (s *sessionStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	role, found := s.roles[[2]uuid.UUID{user, org}]
	return role, found, nil
}

func (s *sessionStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, found, err := s.Membership(ctx, user, org)
	return role, found, true, err
}

func (s *sessionStore) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[admin], nil
}

func (*sessionStore) ReadsPlatformRoles() bool { return false }

// routeRig serves the billing Routes behind the real Principal, OrgScope and Impersonation scope and the real
// Guard, with the go-api delegated_bearer rows of the contract and a fake Stripe. It has no pool: a request
// that reaches a handler's database read answers 500.
type routeRig struct {
	store  *sessionStore
	stripe *fakeStripeAPI
	signer *edgetoken.Signer
	server *httptest.Server
}

func newRouteRig(t *testing.T) *routeRig {
	t.Helper()
	const key = "billing-impersonation-route-test-key-32b!"
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	store := &sessionStore{states: map[uuid.UUID]policy.UserState{}, roles: map[[2]uuid.UUID]string{}, sessions: map[uuid.UUID]*policy.Impersonation{}}
	auth, err := policy.NewAuthenticator(verifier, store, logger)
	if err != nil {
		t.Fatal(err)
	}
	auth.DelegatedRows(contractDelegatedRows(t))
	fake := &fakeStripeAPI{}
	stripeServer := httptest.NewServer(fake)
	t.Cleanup(stripeServer.Close)
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Guard: policy.NewGuard(auth, logger), Logger: logger,
		Stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit", BaseURL: stripeServer.URL, HTTPClient: stripeServer.Client()})}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	scope := policy.NewScope(auth, logger)
	server := httptest.NewServer(scope.Principal(scope.OrgScope(scope.Impersonation(mux))))
	t.Cleanup(server.Close)
	return &routeRig{store: store, stripe: fake, signer: signer, server: server}
}

// contractDelegatedRows is the go-api rows' delegated_bearer lookup, read from the contract as go-api reads it.
func contractDelegatedRows(t *testing.T) func(string) bool {
	t.Helper()
	var document struct {
		Rows []struct {
			Service, Method, Route string
			DelegatedBearer        *string `json:"delegated_bearer"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{}
	for _, row := range document.Rows {
		if row.Service == "dev-health-ops-api" && row.DelegatedBearer != nil && *row.DelegatedBearer == "allowed" {
			allowed[row.Method+" "+row.Route] = true
		}
	}
	if len(allowed) == 0 {
		t.Fatal("no go-api row admits a delegated bearer: the contract parse measured nothing")
	}
	return func(profile string) bool { return allowed[profile] }
}

type billingCall struct {
	name, method, path, body string
}

// billingWrites is every billing route that writes for a user bearer. pull-stripe is not here: its platform wrap
// already refuses an impersonation session (policy.Guard, Superuser level). The Stripe webhook takes no user.
func billingWrites() []billingCall {
	plan, invoice, audit := uuid.NewString(), uuid.NewString(), uuid.NewString()
	return []billingCall{
		{"change-plan", http.MethodPost, prefix + "/subscriptions/change-plan", `{"price_id":"price_new"}`},
		{"cancel", http.MethodPost, prefix + "/subscriptions/cancel", `{}`},
		{"reactivate", http.MethodPost, prefix + "/subscriptions/reactivate", ""},
		{"void", http.MethodPost, prefix + "/invoices/" + invoice + "/void", ""},
		{"refunds", http.MethodPost, prefix + "/refunds", `{}`},
		{"reconcile", http.MethodPost, prefix + "/reconcile", ""},
		{"audit resolve", http.MethodPost, prefix + "/audit/" + audit + "/resolve", `{}`},
		{"checkout", http.MethodPost, prefix + "/checkout", `{}`},
		{"portal", http.MethodPost, prefix + "/portal", ""},
		{"create plan", http.MethodPost, prefix + "/plans", `{}`},
		{"update plan", http.MethodPut, prefix + "/plans/" + plan, `{}`},
		{"delete plan", http.MethodDelete, prefix + "/plans/" + plan, ""},
		{"sync plan", http.MethodPost, prefix + "/plans/" + plan + "/sync-stripe", ""},
	}
}

func billingReads(org uuid.UUID) []billingCall {
	id := uuid.NewString()
	return []billingCall{
		{"subscription", http.MethodGet, prefix + "/subscriptions", ""},
		{"subscription list", http.MethodGet, prefix + "/subscriptions/list", ""},
		{"subscription history", http.MethodGet, prefix + "/subscriptions/history", ""},
		{"invoices", http.MethodGet, prefix + "/invoices", ""},
		{"invoice", http.MethodGet, prefix + "/invoices/" + id, ""},
		{"refunds", http.MethodGet, prefix + "/refunds", ""},
		{"refund", http.MethodGet, prefix + "/refunds/" + id, ""},
		{"audit", http.MethodGet, prefix + "/audit", ""},
		{"audit entry", http.MethodGet, prefix + "/audit/" + id, ""},
		{"plans", http.MethodGet, prefix + "/plans", ""},
		{"plan", http.MethodGet, prefix + "/plans/" + id, ""},
		{"entitlements", http.MethodGet, prefix + "/entitlements/" + org.String(), ""},
	}
}

// send serves call with bearer, adding org_id when org is set; it answers the status, the body and the Stripe
// calls the request made.
func (rig *routeRig) send(t *testing.T, call billingCall, bearer string, org *uuid.UUID) (int, string, []string) {
	t.Helper()
	target := rig.server.URL + call.path
	if org != nil {
		target += "?" + url.Values{"org_id": {org.String()}}.Encode()
	}
	var body io.Reader
	if call.body != "" {
		body = strings.NewReader(call.body)
	}
	req, err := http.NewRequest(call.method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	if call.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	before := rig.stripe.count()
	resp, err := rig.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(raw)), rig.stripe.since(before)
}

// impersonator plants an impersonating admin: own org admin row, the given platform set (nil = the superuser flag's
// every action), a membership in the target with targetRole, and a live session on the target when live is set.
type impersonator struct {
	id, own, target uuid.UUID
	bearer          string
}

func (rig *routeRig) impersonator(t *testing.T, platform *rule.Platform, targetRole string, live bool, delegated bool) impersonator {
	t.Helper()
	p := impersonator{id: uuid.New(), own: uuid.New(), target: uuid.New()}
	rig.store.states[p.id] = policy.UserState{IsActive: true, IsSuperuser: true, Platform: platform}
	rig.store.roles[[2]uuid.UUID{p.id, p.own}] = "admin"
	rig.store.roles[[2]uuid.UUID{p.id, p.target}] = targetRole
	if live {
		rig.store.sessions[p.id] = &policy.Impersonation{ID: uuid.New(), AdminUserID: p.id, TargetUserID: uuid.New(),
			TargetOrgID: p.target, TargetRole: targetRole}
	}
	claims := edgetoken.AccessClaims{UserID: p.id.String(), Email: "admin@example.test", OrgID: p.own.String(), Role: "admin", IsSuperuser: true}
	if delegated {
		by := uuid.NewString()
		claims.ImpersonatingUserID = &by
	}
	bearer, err := rig.signer.Access(claims, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	p.bearer = bearer
	return p
}

type impersonatorKind struct {
	name     string
	platform func() *rule.Platform
}

var impersonatorKinds = []impersonatorKind{
	{"platform admin (every action, admin.billing too)", func() *rule.Platform { return nil }},
	{"admin.impersonate only", func() *rule.Platform { p := rule.PlatformOf(rule.ActionAdminImpersonate); return &p }},
}

// CHAOS-13143 (D6714): impersonation is reads only. An impersonation session (go-api's legacy impersonation row)
// gets 403 "Not allowed in an impersonation session" on every billing write, whatever the target's role or the
// impersonator's platform actions, with or without org_id, and no Stripe call is made.
func TestBillingWritesAreRefusedInAnImpersonationSession(t *testing.T) {
	for _, write := range billingWrites() {
		for _, kind := range impersonatorKinds {
			for _, targetRole := range []string{"admin", "viewer"} {
				for _, orgCase := range []string{"no org_id", "org_id = the target", "org_id = another org"} {
					t.Run(write.name+"/"+kind.name+"/"+targetRole+" target/"+orgCase, func(t *testing.T) {
						rig := newRouteRig(t)
						p := rig.impersonator(t, kind.platform(), targetRole, true, false)
						var org *uuid.UUID
						switch orgCase {
						case "org_id = the target":
							org = &p.target
						case "org_id = another org":
							other := uuid.New()
							rig.store.roles[[2]uuid.UUID{p.id, other}] = "admin"
							org = &other
						}
						status, body, calls := rig.send(t, write, p.bearer, org)
						if status != http.StatusForbidden || body != impersonationRefusalBody {
							t.Errorf("answer %d %s, want 403 %s", status, body, impersonationRefusalBody)
						}
						if len(calls) != 0 {
							t.Errorf("%d Stripe call(s) %v, want none", len(calls), calls)
						}
					})
				}
			}
		}
	}
}

// The same caller without a live session is not refused as impersonating: the write reaches its handler (here
// the rig's missing pool or the handler's own checks answer).
func TestBillingWritesWithoutASessionReachTheirHandler(t *testing.T) {
	for _, write := range billingWrites() {
		for _, kind := range impersonatorKinds {
			t.Run(write.name+"/"+kind.name, func(t *testing.T) {
				rig := newRouteRig(t)
				p := rig.impersonator(t, kind.platform(), "admin", false, false)
				status, body, _ := rig.send(t, write, p.bearer, nil)
				if body == impersonationRefusalBody {
					t.Errorf("answer %d %s: refused as impersonating without a session", status, body)
				}
			})
		}
	}
}

// A delegated session's bearer (impersonating_user_id) is refused on every billing write by the Guard's
// delegated_bearer rows, with the same body; this pins that the rows still deny it.
func TestBillingWritesRefuseADelegatedBearer(t *testing.T) {
	for _, write := range billingWrites() {
		t.Run(write.name, func(t *testing.T) {
			rig := newRouteRig(t)
			p := rig.impersonator(t, nil, "admin", false, true)
			status, body, calls := rig.send(t, write, p.bearer, nil)
			if status != http.StatusForbidden || body != impersonationRefusalBody || len(calls) != 0 {
				t.Errorf("answer %d %s, %d Stripe call(s); want 403 %s, none", status, body, len(calls), impersonationRefusalBody)
			}
		})
	}
}

// Reads while impersonating keep today's answer: the same status and body as the same caller without a session.
func TestBillingReadsInAnImpersonationSessionKeepTodaysAnswer(t *testing.T) {
	for _, kind := range impersonatorKinds {
		for _, orgCase := range []string{"no org_id", "org_id = the target"} {
			probe := newRouteRig(t)
			for _, read := range billingReads(uuid.New()) {
				t.Run(read.name+"/"+kind.name+"/"+orgCase, func(t *testing.T) {
					answer := func(live bool) (int, string) {
						p := probe.impersonator(t, kind.platform(), "viewer", live, false)
						var org *uuid.UUID
						if orgCase == "org_id = the target" {
							org = &p.target
						}
						status, body, _ := probe.send(t, read, p.bearer, org)
						return status, body
					}
					status, body := answer(true)
					wantStatus, wantBody := answer(false)
					if body == impersonationRefusalBody || status != wantStatus || body != wantBody {
						t.Errorf("impersonating: %d %s; without a session: %d %s", status, body, wantStatus, wantBody)
					}
				})
			}
		}
	}
}
