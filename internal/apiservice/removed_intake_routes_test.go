package apiservice

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

func TestRemovedSharedSecretIntakeRoutesAnswerNotFound(t *testing.T) {
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	store := &countingStore{scopeStore: scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	routes := webhookintake.Routes(webhookintake.Deps{Secrets: webhookintake.Secrets{GitHub: "gh-" + uuid.NewString()}, Logger: quietLogger()})
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uuid.NewString(), "type": "access", "org_id": uuid.NewString(), "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	send := func(path string, headers map[string]string) int {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"x":1}`))
		for k, v := range headers {
			request.Header.Set(k, v)
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder.Code
	}
	bearer := map[string]string{"Authorization": "Bearer " + signed, "X-Org-Id": uuid.NewString()}
	control := send("/api/v1/webhooks/never-existed", bearer)
	for _, path := range []string{"/api/v1/webhooks/gitlab", "/api/v1/webhooks/jira"} {
		for name, headers := range map[string]map[string]string{
			"no credential":  {},
			"provider token": {"X-Gitlab-Event": "Push Hook", "X-Gitlab-Token": "x", "X-Hub-Signature": "sha256=" + strings.Repeat("0", 64)},
		} {
			if got := send(path, headers); got != http.StatusNotFound {
				t.Errorf("POST %s (%s): status %d; want 404", path, name, got)
			}
		}
		if got := send(path, bearer); got != control {
			t.Errorf("POST %s (user bearer): status %d; want %d, the answer for a path that never existed", path, got, control)
		}
	}
}
