package authruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// goAPIWireBearerRoutes answer a bearer refusal in go-api's guard wire, which tells "no Authorization header" ("Not
// authenticated") from "a bearer that is not valid" ("Invalid or expired token") as go-api does, because go-api
// forwards them unread under AUTH_PROXY_CREDENTIALS. There the bootstrap credential must get exactly the answer of any
// invalid bearer: it is no credential, and it is told apart from nothing else.
var goAPIWireBearerRoutes = map[string]bool{"POST /api/v1/auth/change-password@api": true}

// perRequestFields are the error envelope members two requests never share
// (the request id and the time); the census compares answers without them.
var perRequestFields = regexp.MustCompile(`"(request_id|occurred_at)":"[^"]*"`)

// TestABootstrapCredentialOpensNoAuthRoute is G-36 on auth-service: the Compose
// bootstrap credential is accepted only as the exchange's subject_token. As an
// Authorization bearer it is not a credential anywhere: every route
// auth-service serves (the endpoint-profile probe's set, both exchange
// subjects on, on the API and the operator listener) answers a request
// carrying it exactly as the same request with no credential (on a route that
// speaks go-api's guard wire, exactly as an invalid bearer), so a route that
// needs a bearer answers it with its 401. /metrics changes between two
// scrapes, so there only the status is compared.
func TestABootstrapCredentialOpensNoAuthRoute(t *testing.T) {
	env := map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "census-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
		authconfig.EnvSocialLinkRoutes:   "true",
		authconfig.EnvUsersOwned:         "true",
		authconfig.EnvTokenIssuerURL:     wellKnownIssuer,
	}
	exchangeEnvironment(t, env)
	handlers := censusHandlers(t, ssoEnvironment(compatEnvironment(env)))
	value := workload.BootstrapPrefix + "CENSUSb00tstrapVALUE" + strings.Repeat("Q", 43-len("CENSUSb00tstrapVALUE"))
	if !workload.ValidBootstrapShape(value) {
		t.Fatal("the census value is not of the bootstrap shape")
	}
	answer := func(listener, method, path, authorization string) (int, string, string) {
		req := httptest.NewRequest(method, path, strings.NewReader("{}"))
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		rec := httptest.NewRecorder()
		handlers[listener].ServeHTTP(rec, req)
		body := perRequestFields.ReplaceAllString(rec.Body.String(), `"$1":""`)
		return rec.Code, body, rec.Body.String()
	}
	var keys []string
	for key := range servedAuthRoutes(t) {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	checked, operator, unauthorized := 0, 0, 0
	exchanged := false
	for _, key := range keys {
		method, rest, _ := strings.Cut(key, " ")
		path, listener, _ := strings.Cut(rest, "@")
		if handlers[listener] == nil {
			t.Fatalf("%s: no handler for listener %q", key, listener)
		}
		baseCode, baseBody, _ := answer(listener, method, path, "")
		if goAPIWireBearerRoutes[key] {
			baseCode, baseBody, _ = answer(listener, method, path, "Bearer census-not-a-credential")
		}
		for _, authorization := range []string{"Bearer " + value, "bearer " + value} {
			code, body, raw := answer(listener, method, path, authorization)
			if strings.Contains(raw, value) {
				t.Errorf("%s echoes the bootstrap credential", key)
			}
			if path == "/metrics" {
				body, baseBody = "", ""
			}
			if code != baseCode || body != baseBody {
				t.Errorf("%s with the bootstrap credential as a bearer: %d %q, want the no-credential answer %d %q",
					key, code, body, baseCode, baseBody)
			}
		}
		if baseCode == http.StatusUnauthorized {
			unauthorized++
		}
		if path == workloadexchange.Pattern {
			exchanged = true
		}
		if listener == "operator" {
			operator++
		}
		checked++
	}
	if checked != servedAuthRouteCount || operator == 0 || unauthorized == 0 || !exchanged {
		t.Fatalf("checked %d routes (%d operator), %d answered 401, exchange probed %v: the census measured too little", checked, operator, unauthorized, exchanged)
	}
	t.Logf("BOOTSTRAP_BEARER_CENSUS routes=%d operator=%d 401=%d", checked, operator, unauthorized)
}

// censusHandlers configures auth-service from env and returns its API and
// operator listeners' handlers, keyed as servedAuthRoutes names them.
func censusHandlers(t *testing.T, env map[string]string) map[string]http.Handler {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	components, err := configure(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	handlers := map[string]http.Handler{}
	for _, component := range components {
		announced, ok := component.(announcedListener)
		if !ok {
			continue
		}
		switch server := announced.listener.(type) {
		case *httpapi.Server:
			handlers["api"] = server.Handler()
		case *health.Server:
			handlers["operator"] = server.Handler()
		}
	}
	if handlers["api"] == nil || handlers["operator"] == nil {
		t.Fatalf("configure built listeners %v; want api and operator", handlers)
	}
	return handlers
}
