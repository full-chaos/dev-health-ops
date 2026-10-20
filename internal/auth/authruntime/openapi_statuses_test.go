package authruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// TestEveryStatusTheStackAnswersIsAnOpenAPIResponse drives each OpenAPI
// operation through the API handler configure builds (database unreachable)
// with: no or a malformed credential, a body over the limit, a well-formed
// credential the store cannot check, and enough requests to exhaust the
// route's rate bucket. Every status those answers carry must be a response the
// operation declares.
func TestEveryStatusTheStackAnswersIsAnOpenAPIResponse(t *testing.T) {
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(sessionEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:            "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvDatabaseConnectTimeout: "200ms",
		authconfig.EnvSigningKeyFile:         "/nonexistent",
		authconfig.EnvSigningKeyID:           "openapi-statuses",
		authconfig.EnvMaxBodyBytes:           "1024",
		authconfig.EnvRateLimitPerSecond:     "0.01",
		authconfig.EnvRateLimitBurst:         "3",
		authconfig.EnvAPIAddress:             "127.0.0.1:0",
		authconfig.EnvOperatorAddress:        "127.0.0.1:1",
	}))})
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
	var api http.Handler
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				api = server.Handler()
			}
		}
	}
	if api == nil {
		t.Fatal("configure built no API listener")
	}

	issuer, err := authapi.NewIssuer(testAccessSecret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := issuer.Access(authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New(), Role: "member"})
	if err != nil {
		t.Fatal(err)
	}
	credential := "dhr1_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	wellFormed := map[string]string{
		"/v1/token/refresh": `{"schema_version":"refresh-request.v1","refresh_credential":"` + credential + `"}`,
	}

	raw, err := os.ReadFile("../../../contracts/auth/v1/openapi/auth-v1.openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Paths map[string]map[string]struct {
			Responses map[string]any `json:"responses"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Paths) == 0 {
		t.Fatal("the OpenAPI document has no paths")
	}

	do := func(method, path, body, token string) int {
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, request)
		return recorder.Code
	}
	for path, operations := range document.Paths {
		for lower, operation := range operations {
			method := strings.ToUpper(lower)
			observed := map[int]bool{}
			observed[do(method, path, "{}", "")] = true
			observed[do(method, path, strings.Repeat("x", 4096), "")] = true
			if body, ok := wellFormed[path]; ok {
				observed[do(method, path, body, "")] = true
			} else {
				observed[do(method, path, "", bearer)] = true
			}
			for i := 0; i < 5; i++ {
				observed[do(method, path, "{}", "")] = true
			}
			var statuses []string
			for status := range observed {
				statuses = append(statuses, strconv.Itoa(status))
				if _, declared := operation.Responses[strconv.Itoa(status)]; !declared {
					t.Errorf("%s %s: the stack answers %d, which the OpenAPI operation does not declare", method, path, status)
				}
			}
			sort.Strings(statuses)
			if !observed[http.StatusTooManyRequests] || len(observed) < 3 {
				t.Errorf("%s %s: the probes reached only %v: they did not exercise the stack", method, path, statuses)
			}
			t.Logf("%s %s answered %v", method, path, statuses)
		}
	}
}
