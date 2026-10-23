package authruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

// failingBody is a request body whose read fails within the size limit: a
// transport failure the stack answers before the route.
type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

// undeclaredStatuses names every observed status the declared responses lack.
func undeclaredStatuses(operation string, observed map[int]bool, declared map[string]any) []string {
	var problems []string
	for status := range observed {
		if _, ok := declared[strconv.Itoa(status)]; !ok {
			problems = append(problems, fmt.Sprintf("%s: the stack answers %d, which the OpenAPI operation does not declare", operation, status))
		}
	}
	sort.Strings(problems)
	return problems
}

// TestEveryStatusTheStackAnswersIsAnOpenAPIResponse drives each OpenAPI
// operation through the API handler configure builds (database unreachable)
// with: no or a malformed credential, a body over the limit, a body whose read
// fails, a well-formed
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
		authconfig.EnvRateLimitBurst:         "4",
		authconfig.EnvAPIAddress:             "127.0.0.1:0",
		authconfig.EnvOperatorAddress:        "127.0.0.1:1",
		authconfig.EnvSocialLinkRoutes:       "true",
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

	do := func(method, path string, body io.Reader, token string) int {
		request := httptest.NewRequest(method, path, body)
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, request)
		if recorder.Code == http.StatusTooManyRequests {
			if seconds, err := strconv.Atoi(recorder.Header().Get("Retry-After")); err != nil || seconds < 1 {
				t.Errorf("%s %s: a 429 without Retry-After in whole seconds >= 1 (got %q)", method, path, recorder.Header().Get("Retry-After"))
			}
		}
		return recorder.Code
	}
	for path, operations := range document.Paths {
		for lower, operation := range operations {
			method := strings.ToUpper(lower)
			observed := map[int]bool{}
			observed[do(method, path, strings.NewReader("{}"), "")] = true
			observed[do(method, path, strings.NewReader(strings.Repeat("x", 4096)), "")] = true
			observed[do(method, path, failingBody{}, "")] = true
			if body, ok := wellFormed[path]; ok {
				observed[do(method, path, strings.NewReader(body), "")] = true
			} else {
				observed[do(method, path, strings.NewReader(""), bearer)] = true
			}
			for i := 0; i < 5; i++ {
				observed[do(method, path, strings.NewReader("{}"), "")] = true
			}
			var statuses []string
			for status := range observed {
				statuses = append(statuses, strconv.Itoa(status))
			}
			sort.Strings(statuses)
			for _, problem := range undeclaredStatuses(method+" "+path, observed, operation.Responses) {
				t.Error(problem)
			}
			// Planted control: the same probe result against this operation's
			// responses with 429 removed must be refused.
			planted := map[string]any{}
			for status, response := range operation.Responses {
				if status != "429" {
					planted[status] = response
				}
			}
			if len(undeclaredStatuses(method+" "+path, observed, planted)) == 0 {
				t.Errorf("%s %s: with 429 removed from the declared responses the gate reported nothing", method, path)
			}
			if !observed[http.StatusTooManyRequests] || len(observed) < 3 {
				t.Errorf("%s %s: the probes reached only %v: they did not exercise the stack", method, path, statuses)
			}
			t.Logf("%s %s answered %v", method, path, statuses)
		}
	}
}
