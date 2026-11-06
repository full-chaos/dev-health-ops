package policy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// spoofStore is one active user, a member of org, with an active impersonation
// session; IsSuperuser is the users row's.
type spoofStore struct {
	user, org, target uuid.UUID
	superuser         bool
	sessionReads      int
}

func (s *spoofStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, IsSuperuser: s.superuser}, id == s.user, nil
}

func (s *spoofStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	if user == s.user && org == s.org {
		return "member", true, nil
	}
	return "", false, nil
}

func (s *spoofStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (s *spoofStore) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.sessionReads++
	if admin != s.user {
		return nil, nil
	}
	return &policy.Impersonation{ID: uuid.New(), AdminUserID: s.user, TargetUserID: s.target, TargetOrgID: s.org,
		TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (s *spoofStore) ReadsPlatformRoles() bool { return false }

func userAccessRow(t *testing.T) string {
	t.Helper()
	rows, err := accessrouting.Rows()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.AcceptsUser {
			return row.Profile
		}
	}
	t.Fatal("no go-api row accepts the user access token")
	return ""
}

// TestASuperuserClaimOnAUserAccessTokenGivesNoSuperuserPower: go-api reads
// superuser from the users row only. An EdDSA user access token that claims
// is_superuser is refused by the access-token contract (401) on a platform row
// and on the scope, with no impersonation session read. A token without the
// claim gets superuser power (platform row, impersonation) only when the users
// row says superuser.
func TestASuperuserClaimOnAUserAccessTokenGivesNoSuperuserPower(t *testing.T) {
	profile := userAccessRow(t)
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, tc := range []struct {
		name         string
		claim        bool
		row          bool
		status       int
		impersonated bool
	}{
		{name: "claim superuser, row not", claim: true, status: http.StatusUnauthorized},
		{name: "claim superuser, row superuser", claim: true, row: true, status: http.StatusUnauthorized},
		{name: "no claim, row not", status: http.StatusForbidden},
		{name: "no claim, row superuser", row: true, status: http.StatusOK, impersonated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issuer := edtokens.New(t)
			store := &spoofStore{user: uuid.New(), org: uuid.New(), target: uuid.New(), superuser: tc.row}
			minted := edtokens.Claims{UserID: store.user, OrgID: store.org.String()}
			if tc.claim {
				minted.Extra = map[string]any{"is_superuser": true}
			}
			token, err := issuer.Mint(minted)
			if err != nil {
				t.Fatal(err)
			}
			auth := issuer.AuthenticatorOver(t, store, nil)
			onRow := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					next.ServeHTTP(w, r.WithContext(policy.WithRouteProfile(r.Context(), profile)))
				})
			}
			serve := func(h http.Handler) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodGet, "/x", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				rec := httptest.NewRecorder()
				onRow(h).ServeHTTP(rec, req)
				return rec
			}

			platform := serve(policy.NewGuard(auth, nil).WrapPlatform(policy.ActionAdminOrgs, ok))
			if platform.Code != tc.status {
				t.Fatalf("platform row: %d %q, want %d", platform.Code, platform.Body.String(), tc.status)
			}

			scope := policy.NewScope(auth, nil)
			got := serve(scope.Principal(scope.Impersonation(ok)))
			impersonated := got.Header().Get("X-Impersonating") == "true"
			// The scope lets an unauthenticated request on; the route's guard refuses it.
			if got.Code != http.StatusOK || impersonated != tc.impersonated {
				t.Fatalf("impersonation: %d impersonated=%v, want 200 and %v", got.Code, impersonated, tc.impersonated)
			}
			if wantReads := map[bool]int{true: 1, false: 0}[tc.impersonated]; store.sessionReads != wantReads {
				t.Fatalf("impersonation session reads %d, want %d", store.sessionReads, wantReads)
			}
		})
	}
}
