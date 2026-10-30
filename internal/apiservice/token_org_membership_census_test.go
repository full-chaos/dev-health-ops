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

func (s *removedMemberStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

func (s *removedMemberStore) ReadsPlatformRoles() bool { return false }

var routeParameter = regexp.MustCompile(`\{[^}]+\}`)

// TestEveryOrgScopedRouteReadsTheTokenOrgMembership is the census of the
// go-api routes declared org_scoped: on the real listener chain (ReadOnce,
// Principal, OrgScope, Impersonation, with the declared rows), a removed
// member's still-valid access token is refused with the X-Org-Id refusal on
// every org-scoped route that accepts a user access token, after exactly one
// membership read of the token's own org. A route declared none or
// cross_org_superuser_only is not part of this census.
func TestEveryOrgScopedRouteReadsTheTokenOrgMembership(t *testing.T) {
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
	user, org := uuid.New(), uuid.New()
	access, err := signer.Access(edgetoken.AccessClaims{UserID: user.String(), Email: "removed@example.com", OrgID: org.String(), Role: "admin", TokenVersion: 1}, time.Now().UTC(), uuid.NewString())
	if err != nil {
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
		checked++
		t.Run(profile, func(t *testing.T) {
			store.mu.Lock()
			store.reads = map[[2]uuid.UUID]int{}
			store.mu.Unlock()
			path := routeParameter.ReplaceAllStringFunc(row.Route, func(string) string { return uuid.NewString() })
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
	if checked < 150 {
		t.Fatalf("the census checked %d org-scoped routes; the profile rows or the mounted routes no longer reach it", checked)
	}
}
