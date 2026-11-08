package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/buildstamp"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// TestBuildInfoIsMountedGuardedAndStamped drives the real route set and
// server: an authenticated caller reads the build identity, an
// unauthenticated one is refused, and both answers name the plane and build.
func TestBuildInfoIsMountedGuardedAndStamped(t *testing.T) {
	previous := version.Commit
	version.Commit = "buildinfotest0123456789abcdef0123456789abcd"
	t.Cleanup(func() { version.Commit = previous })

	tokens := edtokens.New(t)
	user, org := uuid.New(), uuid.New()
	auth := tokens.AuthenticatorOver(t, memberScopeStore{scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, user, org}, quietLogger())
	server, err := NewServer(apiTestConfig(), quietLogger(), Routes(Deps{Auth: auth, Guard: policy.NewGuard(auth, quietLogger())}, quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	token := tokens.MustMint(t, edtokens.Claims{UserID: user, OrgID: org.String()})

	authed := httptest.NewRequest(http.MethodGet, "/buildinfo", nil)
	authed.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, authed)
	if recorder.Code != http.StatusOK {
		t.Fatalf("authenticated /buildinfo = %d %s", recorder.Code, recorder.Body.String())
	}
	var body struct{ Commit string }
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || body.Commit != version.Commit {
		t.Fatalf("body commit = %q (%v), want %q", body.Commit, err, version.Commit)
	}

	anonymous := httptest.NewRecorder()
	server.Handler().ServeHTTP(anonymous, httptest.NewRequest(http.MethodGet, "/buildinfo", nil))
	if anonymous.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /buildinfo = %d, want 401", anonymous.Code)
	}
	for name, response := range map[string]*httptest.ResponseRecorder{"authenticated": recorder, "unauthenticated": anonymous} {
		if response.Header().Get(buildstamp.PlaneHeader) != "go" || response.Header().Get(buildstamp.BuildHeader) != version.Commit {
			t.Errorf("%s answer lacks the plane/build stamp: %v", name, response.Header())
		}
	}
}

// memberScopeStore is scopeStore with one membership row: user is a member of org.
type memberScopeStore struct {
	scopeStore
	user, org uuid.UUID
}

func (s memberScopeStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	if user == s.user && org == s.org {
		return "member", true, nil
	}
	return "", false, nil
}

func (s memberScopeStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}
