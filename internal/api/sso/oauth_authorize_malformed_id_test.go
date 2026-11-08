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
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// oauthPairHandler is the mounted /oauth/{first}/{second} POST handler with the real OAuth handlers on. The pool is
// zero: a request that reaches the database panics.
func oauthPairHandler(t *testing.T) http.Handler {
	t.Helper()
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		Logger: slog.New(slog.DiscardHandler), Signer: &edgetoken.Signer{}, StateSecret: "state-secret-for-tests",
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
		Msg  string `json:"msg"`
	} `json:"detail"`
}

// A provider id that is not a UUID is FastAPI's path validation error (422 uuid_parsing at ["path", "provider_id"]),
// never a 500, and the database is never reached.
func TestOAuthAuthorizeRefusesAMalformedProviderIDAs422(t *testing.T) {
	for _, first := range []string{"providers", "not-a-uuid", "00000000-0000-4000-8000-00000000000g"} {
		t.Run(first, func(t *testing.T) {
			recorder := postOAuthAuthorize(t, first, "{}")
			if recorder.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status %d, want 422: %s", recorder.Code, recorder.Body.String())
			}
			var body validationDetail
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("body %q: %v", recorder.Body.String(), err)
			}
			if len(body.Detail) != 1 || body.Detail[0].Type != "uuid_parsing" || len(body.Detail[0].Loc) != 2 ||
				body.Detail[0].Loc[0] != "path" || body.Detail[0].Loc[1] != "provider_id" ||
				!strings.HasPrefix(body.Detail[0].Msg, "Input should be a valid UUID, ") {
				t.Fatalf("body %q, want one uuid_parsing error at [path provider_id]", recorder.Body.String())
			}
		})
	}
}

// The whole answer for the step the SSO venue drives.
func TestOAuthAuthorizeProvidersAnswer(t *testing.T) {
	recorder := postOAuthAuthorize(t, "providers", "{}")
	const want = `{"detail":[{"type":"uuid_parsing","loc":["path","provider_id"],"msg":"Input should be a valid UUID, invalid character: found ` + "`p`" + ` at 1"}]}`
	if recorder.Code != http.StatusUnprocessableEntity || recorder.Body.String() != want {
		t.Fatalf("got %d %s, want 422 %s", recorder.Code, recorder.Body.String(), want)
	}
}

// FastAPI reports the path error and the body errors of one request in one list, the path error first.
func TestOAuthAuthorizeReportsThePathErrorWithTheBodyErrors(t *testing.T) {
	recorder := postOAuthAuthorize(t, "providers", `{"redirect_uri": 5}`)
	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d, want 422: %s", recorder.Code, recorder.Body.String())
	}
	var body validationDetail
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", recorder.Body.String(), err)
	}
	if len(body.Detail) != 2 || body.Detail[0].Type != "uuid_parsing" || len(body.Detail[1].Loc) != 2 ||
		body.Detail[1].Loc[0] != "body" || body.Detail[1].Loc[1] != "redirect_uri" {
		t.Fatalf("body %q, want the path error then the redirect_uri error", recorder.Body.String())
	}
}

// A valid provider id passes the path check and goes on to the database (control).
func TestOAuthAuthorizeGoesOnWithAValidProviderID(t *testing.T) {
	reached := false
	func() {
		defer func() { reached = recover() != nil }()
		postOAuthAuthorize(t, "00000000-0000-4000-8000-000000000001", "{}")
	}()
	if !reached {
		t.Fatal("refused a valid provider id before the database")
	}
}
