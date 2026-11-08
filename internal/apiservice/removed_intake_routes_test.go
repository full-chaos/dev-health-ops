package apiservice

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

func TestRemovedSharedSecretIntakeRoutesAnswerNotFound(t *testing.T) {
	tokens := edtokens.New(t)
	store := &countingStore{scopeStore: scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}}
	auth := tokens.AuthenticatorOver(t, store, quietLogger())
	scope := policy.NewScope(auth, quietLogger())
	routes := webhookintake.Routes(webhookintake.Deps{Secrets: webhookintake.Secrets{GitHub: "gh-" + uuid.NewString()}, Logger: quietLogger()})
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	signed := tokens.MustMint(t, edtokens.Claims{UserID: uuid.New()})
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
