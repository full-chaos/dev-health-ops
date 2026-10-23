package apiservice

import (
	"context"
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
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
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
		{"dot segment", func(int) string { return "." }, 60},
		{"double dot segment", func(int) string { return ".." }, 60},
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

func TestIntakeGateCountsPercentEncodedRouteNamesInTheSameBucket(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes := webhookintake.Routes(webhookintake.Deps{Logger: quiet})
	server, err := NewServerWithGate(apiTestConfig(), quiet, routes, webhookintake.NewGate(httpapi.NewMemoryCounters(nil), nil, quiet))
	if err != nil {
		t.Fatal(err)
	}
	binding := uuid.NewString()
	cases := []struct {
		name, literal, alias string
		budget               int
	}{
		{"github", "/api/v1/webhooks/github", "/api/v1/webhooks/%67ithub", 3000},
		{"gitlab", "/api/v1/webhooks/gitlab", "/api/v1/webhooks/%67itlab", 3000},
		{"jira", "/api/v1/webhooks/jira", "/api/v1/webhooks/%6aira", 3000},
		{"pagerduty", "/api/v1/webhooks/pagerduty/" + binding, "/api/v1/webhooks/%70agerduty/" + binding, 60},
	}
	for index, c := range cases {
		address := fmt.Sprintf("203.0.113.%d:4242", index+1)
		send := func(target string) int {
			request := httptest.NewRequest(http.MethodPost, target, nil)
			request.RemoteAddr = address
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			return recorder.Code
		}
		for i := 0; i < c.budget; i++ {
			if send(c.literal) == http.StatusTooManyRequests {
				t.Fatalf("%s: delivery %d throttled early", c.name, i+1)
			}
		}
		if got := send(c.literal); got != http.StatusTooManyRequests {
			t.Fatalf("%s: literal path past its budget = %d, want 429", c.name, got)
		}
		if got := send(c.alias); got != http.StatusTooManyRequests {
			t.Errorf("%s: percent-encoded alias past the budget = %d, want 429", c.name, got)
		}
	}
}

func TestConfigureInstallsTheIntakeGate(t *testing.T) {
	components, err := configure(context.Background(), apiTestConfig(), health.NewRegistry(time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	}()
	var server *httpapi.Server
	for _, component := range components {
		if candidate, ok := component.(*httpapi.Server); ok {
			server = candidate
		}
	}
	if server == nil {
		t.Fatal("no api server was configured")
	}
	target := "/api/v1/webhooks/pagerduty/" + uuid.NewString()
	var last int
	for i := 0; i < 61; i++ {
		request := httptest.NewRequest(http.MethodPost, target, nil)
		request.RemoteAddr = "203.0.113.200:4242"
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		last = recorder.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("the 61st delivery to one PagerDuty path = %d, want 429 from the configured server's gate", last)
	}
}
