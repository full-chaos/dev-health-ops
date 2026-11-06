package apiservice

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
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

func (s *plantedStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
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

// countingRoutedValidator is a routed validator that refuses every bearer
// (the planted tokens are HS256, a retired class); it records every bearer it
// is asked to validate.
type countingRoutedValidator struct {
	mu      sync.Mutex
	bearers []string
}

func (v *countingRoutedValidator) Validate(_ context.Context, _, _, bearer string) (*policy.RoutedResult, error) {
	v.mu.Lock()
	v.bearers = append(v.bearers, bearer)
	v.mu.Unlock()
	return nil, errors.New("hs256 refused")
}

const plantedRoute = "/api/v1/planted"

// TestGlobalScopeNeverReadsAuthorizationOnAPlantedRoute runs the api
// listener's global chain on a planted route with a trap between Principal
// and OrgScope: the trap replaces the Authorization header with another value (a
// valid superuser token with a live impersonation session). OrgScope and
// Impersonation must decide with the caller Principal read, never the replaced value.
func TestGlobalScopeNeverReadsAuthorizationOnAPlantedRoute(t *testing.T) {
	tokens := edtokens.New(t)
	for _, mode := range []string{"routed"} {
		t.Run(mode, func(t *testing.T) {
			caller, swapUser := uuid.New(), uuid.New()
			store := &plantedStore{superuser: swapUser, target: uuid.New()}
			auth, err := policy.NewAuthenticator(store, quietLogger())
			if err != nil {
				t.Fatal(err)
			}
			validator := &countingRoutedValidator{}
			if mode == "routed" {
				if err := auth.RouteAccess(validator, goAPIAccessService); err != nil {
					t.Fatal(err)
				}
			}
			scope := policy.NewScope(auth, quietLogger())
			scope.DeclaredRows(func(profile string) (policy.BearerRow, bool) {
				return policy.BearerRow{AccessToken: true}, profile == http.MethodGet+" "+plantedRoute
			})
			// swapUser's users row says superuser and it holds a live impersonation session.
			original, swapped := tokens.MustMint(t, edtokens.Claims{UserID: caller}), tokens.MustMint(t, edtokens.Claims{UserID: swapUser})
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
			// The validator refuses the caller's token, the planted route (no
			// guard) answers with no org, and the validator saw only the caller's
			// bearer: the scope decides with the caller Principal read, never the
			// swapped header.
			wantCode, wantReads := http.StatusForbidden, []uuid.UUID{caller}
			if mode == "routed" {
				wantCode, wantReads = http.StatusOK, nil
				if recorder.Body.String() != "org=" {
					t.Fatalf("routed: body %q, want no org", recorder.Body.String())
				}
			}
			if recorder.Code != wantCode || recorder.Header().Get("X-Impersonating") != "" {
				t.Fatalf("%d %s %v: the scope did not decide with the caller", recorder.Code, recorder.Body.String(), recorder.Header())
			}
			if slices.Contains(store.userReads, swapUser) || !slices.Equal(store.userReads, wantReads) {
				t.Fatalf("users reads %v: want %v, for the caller %s (swapped %s)", store.userReads, wantReads, caller, swapUser)
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
