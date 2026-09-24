package apiservice

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/buildstamp"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// TestBuildInfoIsMountedGuardedAndStamped drives the real route set and
// server: an authenticated caller reads the build identity, an
// unauthenticated one is refused, and both answers name the plane and build.
func TestBuildInfoIsMountedGuardedAndStamped(t *testing.T) {
	previous := version.Commit
	version.Commit = "buildinfotest0123456789abcdef0123456789abcd"
	t.Cleanup(func() { version.Commit = previous })

	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	user := uuid.New()
	auth, err := policy.NewAuthenticator(verifier, scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(apiTestConfig(), quietLogger(), Routes(Deps{Auth: auth, Guard: policy.NewGuard(auth, quietLogger())}, quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": user.String(), "type": "access", "org_id": uuid.NewString(), "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}

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
