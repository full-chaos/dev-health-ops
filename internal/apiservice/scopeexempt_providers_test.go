package apiservice

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

func providerSign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestProviderIntakeRoutesRunTheirCheckBeforeAnyCredentialDecode(t *testing.T) {
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
	secrets := webhookintake.Secrets{GitHub: "gh-" + uuid.NewString(), GitLab: "gl-" + uuid.NewString(), Jira: "jr-" + uuid.NewString()}
	routes := webhookintake.Routes(webhookintake.Deps{Secrets: secrets, Logger: quietLogger()})
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	token := func(sub uuid.UUID, superuser bool) string {
		signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": sub.String(), "type": "access", "org_id": uuid.NewString(), "is_superuser": superuser,
			"exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	body := []byte(`{"x":1}`)
	binding := uuid.NewString()
	type provider struct {
		name, path string
		headers    map[string]string
		wantBad    int
	}
	providers := []provider{
		{"github", "/api/v1/webhooks/github", map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "d1", "X-Hub-Signature-256": "sha256=" + strings.Repeat("0", 64)}, http.StatusUnauthorized},
		{"gitlab", "/api/v1/webhooks/gitlab", map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Token": "wrong"}, http.StatusUnauthorized},
		{"jira", "/api/v1/webhooks/jira", map[string]string{"X-Hub-Signature": "sha256=" + strings.Repeat("0", 64)}, http.StatusUnauthorized},
		// no store is configured here, so the handler answers its store-unavailable status
		{"pagerduty", "/api/v1/webhooks/pagerduty/" + binding, map[string]string{}, http.StatusInternalServerError},
	}
	send := func(method, target string, headers map[string]string, bearer string) (int, int64) {
		before := store.calls.Load()
		request := httptest.NewRequest(method, target, strings.NewReader(string(body)))
		for name, value := range headers {
			request.Header.Set(name, value)
		}
		if bearer != "" {
			request.Header.Set("Authorization", "Bearer "+bearer)
			request.Header.Set("X-Org-Id", uuid.NewString())
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder.Code, store.calls.Load() - before
	}
	for _, p := range providers {
		for _, bearer := range []string{token(uuid.New(), false), token(store.admin, true)} {
			code, calls := send(http.MethodPost, p.path, p.headers, bearer)
			if code != p.wantBad || calls != 0 {
				t.Errorf("%s: bad credential gave status %d with %d auth calls; want %d and 0", p.name, code, calls, p.wantBad)
			}
		}
		for name, shape := range map[string][2]string{
			"other method":   {http.MethodGet, p.path},
			"trailing slash": {http.MethodPost, p.path + "/"},
			"encoded slash":  {http.MethodPost, strings.Replace(p.path, "/webhooks/", "/webhooks%2F", 1)},
		} {
			if _, calls := send(shape[0], shape[1], p.headers, token(uuid.New(), false)); calls == 0 {
				t.Errorf("%s %s: scope middlewares made no auth call", p.name, name)
			}
		}
	}
	for name, path := range map[string]string{
		"extra segment":   "/api/v1/webhooks/pagerduty/" + binding + "/x",
		"empty segment":   "/api/v1/webhooks/pagerduty/",
		"non-uuid":        "/api/v1/webhooks/pagerduty/not-a-uuid",
		"percent segment": "/api/v1/webhooks/pagerduty/" + binding[:35] + "%31",
	} {
		if _, calls := send(http.MethodPost, path, nil, token(uuid.New(), false)); calls == 0 {
			t.Errorf("pagerduty %s: scope middlewares made no auth call", name)
		}
	}
	good := []byte(`{"x":1}`)
	valid := map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "d2", "X-Hub-Signature-256": "sha256=" + providerSign(secrets.GitHub, good)}
	if code, calls := send(http.MethodPost, "/api/v1/webhooks/github", valid, token(uuid.New(), false)); code == http.StatusUnauthorized || calls != 0 {
		t.Errorf("github valid signature: status %d with %d auth calls; want past the check and 0 calls", code, calls)
	}
}
