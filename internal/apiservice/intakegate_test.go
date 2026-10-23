package apiservice

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

func TestSignatureOnlySetIsUnchangedByTheGate(t *testing.T) {
	want := map[string]string{
		"POST /api/v1/billing/webhooks/stripe":         "stripe_webhook_signature",
		"POST /api/v1/webhooks/github":                 "github_webhook_signature",
		"POST /api/v1/webhooks/gitlab":                 "gitlab_webhook_signature",
		"POST /api/v1/webhooks/jira":                   "jira_webhook_signature",
		"POST /api/v1/webhooks/pagerduty/{binding_id}": "pagerduty_oauth_credential",
	}
	if !reflect.DeepEqual(signatureOnlyRoutes, want) {
		t.Fatalf("the signature-only set changed: %v", signatureOnlyRoutes)
	}
}

func TestIntakeGateCountsEveryIdShapeOnceAheadOfScope(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	store := &countingStore{scopeStore: scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}}
	auth, err := policy.NewAuthenticator(verifier, store, quiet)
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quiet)
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(httpapi.NewMemoryCounters(nil), nil, quiet), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uuid.NewString(), "type": "access", "org_id": uuid.NewString(), "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "/api/v1/webhooks/pagerduty/"
	shapes := []struct {
		name   string
		id     func(int) string
		budget int
	}{
		{"canonical id", func(int) string { return uuid.NewString() }, 600},
		{"malformed id", func(i int) string { return "not-a-uuid-" + uuid.NewString()[:8] + string(rune('a'+i%26)) }, 600},
		{"too long id", func(int) string { return strings.Repeat("a", 200) + uuid.NewString() }, 600},
		{"percent segment", func(int) string { return "%31" + uuid.NewString() }, 600},
		{"dot segment, one fixed id", func(int) string { return "x.." }, 60},
	}
	peer := 0
	for _, shape := range shapes {
		for _, withBearer := range []bool{false, true} {
			peer++
			address := fmt.Sprintf("198.51.100.%d:4242", peer)
			send := func(i int) *httptest.ResponseRecorder {
				request := httptest.NewRequest(http.MethodPost, prefix+shape.id(i), nil)
				request.RemoteAddr = address
				if withBearer {
					request.Header.Set("Authorization", "Bearer "+bearer)
					request.Header.Set("X-Org-Id", uuid.NewString())
				}
				recorder := httptest.NewRecorder()
				server.Handler().ServeHTTP(recorder, request)
				return recorder
			}
			passed := 0
			var refused *httptest.ResponseRecorder
			for i := 0; i < shape.budget+5; i++ {
				got := send(i)
				if got.Code == http.StatusTooManyRequests {
					refused = got
					break
				}
				passed++
			}
			if passed != shape.budget || refused == nil {
				t.Errorf("%s bearer=%v: %d deliveries passed before the first 429, want %d", shape.name, withBearer, passed, shape.budget)
				continue
			}
			again := send(shape.budget + 100)
			if again.Code != http.StatusTooManyRequests || again.Body.String() != refused.Body.String() || refused.Body.String() != `{"detail":{"message":"Rate limit exceeded. Please try again later."}}` {
				t.Errorf("%s bearer=%v: throttled answers differ: %d %s vs %s", shape.name, withBearer, again.Code, again.Body.String(), refused.Body.String())
			}
			if refused.Header().Get("X-Frame-Options") != "DENY" {
				t.Errorf("%s bearer=%v: the refusal lacks the security headers: %v", shape.name, withBearer, refused.Header())
			}
		}
	}
}
