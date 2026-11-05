package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// legacySessionStore is the legacy impersonation caller: an active superuser
// with a live go-api session row whose target is a plain member. It counts
// the session reads.
type legacySessionStore struct {
	mu                  sync.Mutex
	admin, target, tOrg uuid.UUID
	sessionReads        int
}

func (s *legacySessionStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	if id == s.admin {
		return policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 1}, true, nil
	}
	return policy.UserState{IsActive: true, TokenVersion: 1}, true, nil
}

func (s *legacySessionStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	if user == s.target && org == s.tOrg {
		return "member", true, nil
	}
	return "", false, nil
}

func (s *legacySessionStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (s *legacySessionStore) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionReads++
	if admin != s.admin {
		return nil, nil
	}
	return &policy.Impersonation{ID: uuid.New(), AdminUserID: s.admin, TargetUserID: s.target, TargetOrgID: s.tOrg,
		TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (s *legacySessionStore) ReadsPlatformRoles() bool { return false }

// legacyAdmitted are the only routes where the switch lets the legacy caller
// through the scope in its session.
var legacyAdmitted = []string{"GET /api/v1/admin/impersonate/status", "POST /api/v1/admin/impersonate/stop"}

// TestLegacyImpersonationCallerCensusUnderTheProxySwitch is the census of
// every route the go-api listener mounts, on the real chain (ReadOnce,
// Principal, OrgScope, Impersonation, with the declared rows) and the scope
// newProtection builds with AUTH_PROXY_IMPERSONATION on: the legacy caller is
// let through in its session on status and stop only. Every other route
// either refuses it in the scope (403, no impersonation headers, its handler
// never runs) or never applies the session (a route that skips the scope or
// whose row takes no access token: the caller is anonymous there).
func TestLegacyImpersonationCallerCensusUnderTheProxySwitch(t *testing.T) {
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	store := &legacySessionStore{admin: uuid.New(), target: uuid.New(), tOrg: uuid.New()}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	upstream, _ := url.Parse("http://auth-service.invalid")
	// The switch needs AUTH_PROXY_SESSIONS; its proxied routes skip the scope, as on the listener.
	proxy := AuthProxyConfig{Upstream: upstream, Impersonation: true, On: map[AuthAggregate]bool{AuthSessions: true}}
	protected, err := newProtection(apiTestConfig(), nil, false, proxy, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	scope.DeclaredRows(rows)
	scope.RefuseLegacyImpersonation(protected.scope.RefusesLegacyImpersonation())
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	clickHouse, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer}
	routes := Routes(deps, quietLogger())
	server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	access, err := signer.Access(edgetoken.AccessClaims{UserID: store.admin.String(), Email: "admin@example.com", OrgID: uuid.NewString(),
		Role: "member", IsSuperuser: true, TokenVersion: 1}, time.Now().UTC(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}

	var admitted, refused, sessionless []string
	seen := map[string]bool{}
	for _, route := range routes {
		profile := route.Method + " " + route.Pattern
		if seen[profile] {
			continue
		}
		seen[profile] = true
		store.mu.Lock()
		store.sessionReads = 0
		store.mu.Unlock()
		path := tokenOrgRouteParameter.ReplaceAllStringFunc(route.Pattern, func(string) string { return uuid.NewString() })
		request := httptest.NewRequest(route.Method, path, strings.NewReader("{}"))
		request.Header.Set("Authorization", "Bearer "+access)
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		store.mu.Lock()
		reads := store.sessionReads
		store.mu.Unlock()
		impersonating := recorder.Header().Get("X-Impersonating") == "true"
		switch {
		case impersonating:
			admitted = append(admitted, profile)
		case reads > 0 && recorder.Code == http.StatusForbidden && recorder.Body.String() == `{"detail":"Not allowed in an impersonation session"}`:
			refused = append(refused, profile)
		case reads == 0:
			sessionless = append(sessionless, profile)
		default:
			t.Errorf("%s: the session was read and neither applied nor refused: %d %s", profile, recorder.Code, recorder.Body.String())
		}
	}
	slices.Sort(admitted)
	if !slices.Equal(admitted, legacyAdmitted) {
		t.Errorf("routes that admit the legacy caller in its session: %v, want exactly %v", admitted, legacyAdmitted)
	}
	t.Logf("census: %d mounted routes; %d admit the legacy caller %v; %d refuse it in the scope; %d never apply the session %v",
		len(seen), len(admitted), admitted, len(refused), len(sessionless), sessionless)
	if len(seen) < 200 || len(refused) < len(seen)-len(sessionless)-len(legacyAdmitted) {
		t.Fatalf("the census reached %d routes, refused %d; the mounted routes no longer reach it", len(seen), len(refused))
	}
}

// newProtection turns the refusal on from the switch the auth proxy reads
// (AuthProxyConfig.ImpersonationOn), and from no other.
func TestNewProtectionRefusesTheLegacyCallerOnlyWithTheImpersonationProxy(t *testing.T) {
	upstream, _ := url.Parse("http://auth-service.invalid")
	everyAggregate := map[AuthAggregate]bool{}
	for _, aggregate := range AuthAggregates {
		everyAggregate[aggregate] = true
	}
	cases := []struct {
		name  string
		proxy AuthProxyConfig
		want  bool
	}{
		{"zero config", AuthProxyConfig{}, false},
		{"impersonation on", AuthProxyConfig{Upstream: upstream, Impersonation: true}, true},
		{"impersonation on with every aggregate", AuthProxyConfig{Upstream: upstream, Impersonation: true, On: everyAggregate}, true},
		{"impersonation flag without a URL", AuthProxyConfig{Impersonation: true}, false},
		{"every other switch on", AuthProxyConfig{Upstream: upstream, On: everyAggregate, OrgProvisioning: true}, false},
	}
	for _, test := range cases {
		protected, err := newProtection(apiTestConfig(), nil, false, test.proxy, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		if got := protected.scope.RefusesLegacyImpersonation(); got != test.want {
			t.Errorf("%s: the scope refuses the legacy caller = %v, want %v", test.name, got, test.want)
		}
	}
}
