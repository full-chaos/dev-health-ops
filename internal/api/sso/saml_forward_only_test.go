package sso

import (
	"bytes"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// With sign-in configured and no SAML forward, the 3 SAML routes answer their 422, else 503 config_missing naming AUTH_PROXY_SAML.
func TestSAMLForwardOffValidatesThenAnswersConfigMissing(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, logger), Logger: logger,
		StateSecret: "saml-forward-only-state-secret", Issuer: okIssuer{},
		HTTPClient: &http.Client{Transport: samlNoUpstream{t: t}},
	})
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	want := httptest.NewRecorder()
	policy.WriteConfigMissing(want, "AUTH_PROXY_SAML")
	for _, tc := range []struct {
		name, method, action, body string
		status                     int
	}{
		{"metadata", http.MethodGet, "metadata", "", 503},
		{"initiate", http.MethodPost, "initiate", `{}`, 503},
		{"initiate bad body", http.MethodPost, "initiate", `{"relay_state":1}`, 422},
		{"acs missing response", http.MethodPost, "acs", `{}`, 422},
		{"acs", http.MethodPost, "acs", `{"SAMLResponse":"x"}`, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "/api/v1/auth/saml/00000000-0000-4000-8000-000000000001/"+tc.action,
				strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, request)
			if recorder.Code != tc.status {
				t.Fatalf("status=%d body=%s, want %d", recorder.Code, recorder.Body, tc.status)
			}
			if tc.status == http.StatusServiceUnavailable && !bytes.Equal(recorder.Body.Bytes(), want.Body.Bytes()) {
				t.Errorf("body=%q, want %q", recorder.Body.Bytes(), want.Body.Bytes())
			}
		})
	}
}

type samlNoUpstream struct{ t *testing.T }

func (s samlNoUpstream) RoundTrip(*http.Request) (*http.Response, error) {
	s.t.Fatal("SAML forward off reached an upstream")
	return nil, nil
}

// go-api's SSO package imports no SAML or XML signature library: SAML sign-in is auth-service's.
func TestSAMLForwardOnlyImportCensus(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(path, "github.com/crewjam/") || strings.HasPrefix(path, "github.com/russellhaering/goxmldsig") {
				t.Errorf("%s imports %s", name, path)
			}
		}
	}
}
