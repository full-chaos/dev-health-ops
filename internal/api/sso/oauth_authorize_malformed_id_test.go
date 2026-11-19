package sso

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// oauthPairHandler is the mounted /oauth/{first}/{second} POST handler with sign-in configured and no forward. The pool is
// zero: a request that reaches the database panics.
func oauthPairHandler(t *testing.T) http.Handler {
	t.Helper()
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		Logger: slog.New(slog.DiscardHandler), Issuer: okIssuer{}, StateSecret: "state-secret-for-tests",
	})
	for _, route := range routes {
		if route.Method == http.MethodPost && route.Pattern == "/api/v1/auth/oauth/{first}/{second}" {
			return route.Handler
		}
	}
	t.Fatal("route not mounted")
	return nil
}

func postOAuthAuthorize(t *testing.T, first, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oauth/"+first+"/authorize", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.SetPathValue("first", first)
	request.SetPathValue("second", "authorize")
	recorder := httptest.NewRecorder()
	oauthPairHandler(t).ServeHTTP(recorder, request)
	return recorder
}

type validationDetail struct {
	Detail []struct {
		Type string `json:"type"`
		Loc  []any  `json:"loc"`
	} `json:"detail"`
}

func decodeValidation(t *testing.T, recorder *httptest.ResponseRecorder) validationDetail {
	t.Helper()
	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d, want 422: %s", recorder.Code, recorder.Body.String())
	}
	var body validationDetail
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", recorder.Body.String(), err)
	}
	return body
}

// With the OIDC forward off, any provider id (malformed, or one the CPython parser takes) answers 503 config_missing naming
// AUTH_PROXY_OIDC and is never looked up: go-api has no own OAuth sign-in (CHAOS-14131).
func TestOAuthAuthorizeWithTheForwardOffAnswersConfigMissingForAnyProviderID(t *testing.T) {
	want := httptest.NewRecorder()
	policy.WriteConfigMissing(want, "AUTH_PROXY_OIDC")
	for _, first := range []string{"providers", "not-a-uuid", "0000000000000000000000000000001", "00000000-0000-4000-8000-00000000000g",
		"00000000-0000-4000-8000-000000000001", "0x" + strings.Repeat("1", 30)} {
		t.Run(first, func(t *testing.T) {
			recorder := postOAuthAuthorize(t, first, "{}")
			if recorder.Code != want.Code || recorder.Body.String() != want.Body.String() {
				t.Fatalf("got %d %s, want %d %s", recorder.Code, recorder.Body.String(), want.Code, want.Body.String())
			}
		})
	}
}

// The body is validated before the provider id: a malformed id adds no error of its own to the 422.
func TestOAuthAuthorizeValidatesTheBodyBeforeTheProviderID(t *testing.T) {
	for _, tc := range []struct{ name, body, wantType string }{
		{"field error", `{"redirect_uri": 5}`, "string_type"},
		{"not an object", `[]`, "model_attributes_type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := decodeValidation(t, postOAuthAuthorize(t, "providers", tc.body))
			if len(body.Detail) != 1 || body.Detail[0].Type != tc.wantType || len(body.Detail[0].Loc) == 0 ||
				body.Detail[0].Loc[0] != "body" {
				t.Fatalf("detail %+v, want only the body error %s", body.Detail, tc.wantType)
			}
		})
	}
}

// An undecodable body answers only the decode error, as FastAPI 0.136.3 does (a probe of the ops .venv, not a frozen pin).
func TestOAuthAuthorizeAnswersOnlyTheDecodeErrorForAnUndecodableBody(t *testing.T) {
	body := decodeValidation(t, postOAuthAuthorize(t, "providers", `{"redirect_uri": `))
	if len(body.Detail) != 1 || body.Detail[0].Type != "json_invalid" || len(body.Detail[0].Loc) == 0 ||
		body.Detail[0].Loc[0] != "body" {
		t.Fatalf("detail %+v, want only json_invalid", body.Detail)
	}
}
