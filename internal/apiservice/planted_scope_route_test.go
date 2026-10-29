package apiservice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// plantedStore records every users-row read by id and every other read.
type plantedStore struct {
	mu        sync.Mutex
	superuser uuid.UUID
	target    uuid.UUID
	userReads []uuid.UUID
	other     int
}

func (s *plantedStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userReads = append(s.userReads, id)
	return policy.UserState{IsActive: true, IsSuperuser: id == s.superuser}, true, nil
}

func (s *plantedStore) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.other++
	return "", false, nil
}

func (s *plantedStore) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.other++
	if admin != s.superuser {
		return nil, nil
	}
	return &policy.Impersonation{AdminUserID: s.superuser, TargetUserID: s.target, TargetOrgID: s.target}, nil
}

// countingRoutedValidator is the routed validator over the legacy path; it
// records every bearer it is asked to validate.
type countingRoutedValidator struct {
	mu      sync.Mutex
	legacy  func(context.Context, string) (*policy.User, error)
	bearers []string
}

func (v *countingRoutedValidator) Validate(ctx context.Context, _, _, bearer string) (*policy.RoutedResult, error) {
	v.mu.Lock()
	v.bearers = append(v.bearers, bearer)
	v.mu.Unlock()
	user, err := v.legacy(ctx, bearer)
	if err != nil {
		return nil, err
	}
	return &policy.RoutedResult{Legacy: user}, nil
}

const plantedRoute = "/api/v1/planted"

// TestGlobalScopeNeverReadsAuthorizationOnAPlantedRoute runs the api
// listener's global chain on a planted route with a trap between Principal
// and OrgScope: the trap replaces the Authorization header with another value (a
// valid superuser token with a live impersonation session). OrgScope and
// Impersonation must decide with the caller Principal read, never the replaced value.
func TestGlobalScopeNeverReadsAuthorizationOnAPlantedRoute(t *testing.T) {
	key := strings.Repeat("fixture-key-", 3)
	for _, mode := range []string{"legacy", "routed"} {
		t.Run(mode, func(t *testing.T) {
			verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
			if err != nil {
				t.Fatal(err)
			}
			caller, swapUser := uuid.New(), uuid.New()
			store := &plantedStore{superuser: swapUser, target: uuid.New()}
			auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
			if err != nil {
				t.Fatal(err)
			}
			validator := &countingRoutedValidator{legacy: auth.AuthenticateLegacy}
			if mode == "routed" {
				if err := auth.RouteAccess(validator, goAPIAccessService); err != nil {
					t.Fatal(err)
				}
			}
			scope := policy.NewScope(auth, quietLogger())
			scope.DeclaredRows(func(profile string) (policy.BearerRow, bool) {
				return policy.BearerRow{AccessToken: true}, profile == http.MethodGet+" "+plantedRoute
			})
			sign := func(sub uuid.UUID, superuser bool) string {
				token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
					"sub": sub.String(), "type": "access", "org_id": uuid.NewString(), "is_superuser": superuser,
					"iss": "dev-health-ops", "aud": "dev-health-api", "exp": time.Now().Add(time.Hour).Unix(),
				}).SignedString([]byte(key))
				if err != nil {
					t.Fatal(err)
				}
				return token
			}
			original, swapped := sign(caller, false), sign(swapUser, true)
			trapped := 0
			trap := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					trapped++
					r = r.Clone(r.Context())
					r.Header.Set("Authorization", "Bearer "+swapped)
					next.ServeHTTP(w, r)
				})
			}
			routes := []httpapi.Route{{Method: http.MethodGet, Pattern: plantedRoute, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "org="+policy.OrgIDFrom(r.Context()))
			})}}
			server, err := NewServerWithGate(apiTestConfig(), quietLogger(), routes, nil, scope.Principal, trap, scope.OrgScope, scope.Impersonation)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, plantedRoute, nil)
			request.Header.Set("Authorization", "Bearer "+original)
			request.Header.Set("X-Org-Id", uuid.NewString())
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)

			if trapped != 1 {
				t.Fatalf("trap ran %d times", trapped)
			}
			if recorder.Code != http.StatusForbidden || recorder.Header().Get("X-Impersonating") != "" {
				t.Fatalf("%d %s %v: the scope did not decide with the caller", recorder.Code, recorder.Body.String(), recorder.Header())
			}
			if slices.Contains(store.userReads, swapUser) || !slices.Equal(store.userReads, []uuid.UUID{caller}) {
				t.Fatalf("users reads %v: want exactly one, for the caller %s (swapped %s)", store.userReads, caller, swapUser)
			}
			if slices.Contains(validator.bearers, swapped) || (mode == "routed" && !slices.Equal(validator.bearers, []string{original})) {
				t.Fatalf("validated %d bearers, swapped included=%v", len(validator.bearers), slices.Contains(validator.bearers, swapped))
			}
		})
	}
}

// TestGoAPIScopeDeclaresTheEndpointProfileRows: the api listener's scope
// decides with the go-api endpoint-profile rows.
func TestGoAPIScopeDeclaresTheEndpointProfileRows(t *testing.T) {
	protected, err := newProtection(apiTestConfig(), nil, false, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if !protected.scope.DeclaresRows() {
		t.Fatal("the api listener's scope has no endpoint-profile rows")
	}
}

func (*plantedStore) ReadsPlatformRoles() bool { return false }
