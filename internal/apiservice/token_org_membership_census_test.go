package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// removedMemberStore is a live, active user with no membership anywhere: a
// member removed after their access token was minted. It records each
// membership read.
type removedMemberStore struct {
	mu    sync.Mutex
	reads map[[2]uuid.UUID]int
}

func (s *removedMemberStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, TokenVersion: 1}, true, nil
}

func (s *removedMemberStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads[[2]uuid.UUID{user, org}]++
	return "", false, nil
}

func (s *removedMemberStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (s *removedMemberStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

func (s *removedMemberStore) ReadsPlatformRoles() bool { return false }

var tokenOrgRouteParameter = regexp.MustCompile(`\{[^}]+\}`)

// TestEveryOrgScopedRouteReadsTheTokenOrgMembership is the census of the
// go-api routes declared org_scoped: on the real listener chain (ReadOnce,
// Principal, OrgScope, Impersonation, with the declared rows), a removed
// member's still-valid access token is refused with the X-Org-Id refusal on
// every org-scoped route that accepts a user access token, after exactly one
// membership read of the token's own org. A route declared none or
// cross_org_superuser_only is not part of this census.
func TestEveryOrgScopedRouteReadsTheTokenOrgMembership(t *testing.T) {
	census := newTokenOrgCensus(t)
	store, server, rows, mounted := census.store, census.server, census.rows, census.mounted
	user, org, access := census.user, census.org, census.access

	var document struct {
		Rows []struct {
			Service           string `json:"service"`
			Method            string `json:"method"`
			Route             string `json:"route"`
			TenantRequirement string `json:"tenant_requirement"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, row := range document.Rows {
		profile := row.Method + " " + row.Route
		if row.Service != goAPIAccessService || row.TenantRequirement != "org_scoped" || !mounted[profile] {
			continue
		}
		if declared, _ := rows(profile); !declared.AccessToken {
			continue
		}
		// The org-less routes serve the caller, not an org: the scope reads
		// no token-org membership there (TestOrglessRoutesReadNoTokenOrgMembership).
		if policy.OrglessMayReach(row.Method, row.Route) {
			continue
		}
		checked++
		t.Run(profile, func(t *testing.T) {
			store.mu.Lock()
			store.reads = map[[2]uuid.UUID]int{}
			store.mu.Unlock()
			path := tokenOrgRouteParameter.ReplaceAllStringFunc(row.Route, func(string) string { return uuid.NewString() })
			request := httptest.NewRequest(row.Method, path, strings.NewReader("{}"))
			request.Header.Set("Authorization", "Bearer "+access)
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusForbidden || recorder.Body.String() != `{"detail": "X-Org-Id not permitted for this user"}` {
				t.Errorf("a removed member's token was answered %d %q, want the X-Org-Id refusal", recorder.Code, recorder.Body.String())
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if got := store.reads[[2]uuid.UUID{user, org}]; got != 1 {
				t.Errorf("membership reads of the token's own org: %d, want 1", got)
			}
		})
	}
	t.Logf("the census checked %d org-scoped routes", checked)
	if checked < 150 {
		t.Fatalf("the census checked %d org-scoped routes; the profile rows or the mounted routes no longer reach it", checked)
	}
}

// A removed member's still-valid legacy HS256 access token (the org exists, the row is
// gone) on every org-less route (policy.OrglessRouteKeys) is never the
// X-Org-Id refusal, and the request reads no membership of the token's org:
// those routes serve the caller, as the Python api does.
func TestOrglessRoutesReadNoTokenOrgMembership(t *testing.T) {
	census := newTokenOrgCensus(t)
	keys := policy.OrglessRouteKeys()
	if len(keys) == 0 {
		t.Fatal("no org-less routes")
	}
	for _, key := range keys {
		method, path, _ := strings.Cut(key, " ")
		t.Run(key, func(t *testing.T) {
			census.store.mu.Lock()
			census.store.reads = map[[2]uuid.UUID]int{}
			census.store.mu.Unlock()
			request := httptest.NewRequest(method, path, strings.NewReader("{}"))
			request.Header.Set("Authorization", "Bearer "+census.access)
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			census.server.Handler().ServeHTTP(recorder, request)
			if strings.Contains(recorder.Body.String(), "X-Org-Id not permitted for this user") {
				t.Errorf("a removed member's token on an org-less route was answered %d %q, the X-Org-Id refusal", recorder.Code, recorder.Body.String())
			}
			census.store.mu.Lock()
			defer census.store.mu.Unlock()
			if got := census.store.reads[[2]uuid.UUID{census.user, census.org}]; got != 0 {
				t.Errorf("membership reads of the token's own org: %d, want 0", got)
			}
		})
	}
}

// tokenOrgCensus is the real go-api listener chain (ReadOnce, Principal,
// OrgScope, Impersonation, with the declared rows) over removedMemberStore,
// and a removed member's still-valid access token for an existing org.
type tokenOrgCensus struct {
	store     *removedMemberStore
	server    *httpapi.Server
	rows      func(profile string) (policy.BearerRow, bool)
	mounted   map[string]bool
	user, org uuid.UUID
	access    string
}

func newTokenOrgCensus(t *testing.T) tokenOrgCensus {
	t.Helper()
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	store := &removedMemberStore{reads: map[[2]uuid.UUID]int{}}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	scope.DeclaredRows(rows)
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
	server, err := NewServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	mounted := map[string]bool{}
	for _, route := range Routes(deps, quietLogger()) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	user, org := uuid.New(), uuid.New()
	access, err := signer.Access(edgetoken.AccessClaims{UserID: user.String(), Email: "removed@example.com", OrgID: org.String(), Role: "admin", TokenVersion: 1}, time.Now().UTC(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return tokenOrgCensus{store: store, server: server, rows: rows, mounted: mounted, user: user, org: org, access: access}
}
