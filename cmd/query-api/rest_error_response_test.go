package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
)

// TestWriteRESTErrorBodyShapes pins the exact status/Content-Type/body
// every writeRESTError-family call produces, against a live FastAPI
// capture of the equivalent Python response (see this route set's
// TEST-EVIDENCE for the capture commands and raw output).
func TestWriteRESTErrorBodyShapes(t *testing.T) {
	tests := []struct {
		name        string
		write       func(w http.ResponseWriter, r *http.Request)
		wantStatus  int
		wantBody    string
		wantWWWAuth string
	}{
		{
			name: "plain string detail (Data unavailable)",
			write: func(w http.ResponseWriter, r *http.Request) {
				writeRESTDataUnavailable(w, r, "test", "org-1")
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"detail":"Data unavailable"}` + "\n",
		},
		{
			name: "plain string detail (typed request error)",
			write: func(w http.ResponseWriter, r *http.Request) {
				writeRESTError(w, r, "test", "org-1", http.StatusBadRequest, "Comparative parameters are not supported.")
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   `{"detail":"Comparative parameters are not supported."}` + "\n",
		},
		{
			name: "not authenticated (missing header)",
			write: func(w http.ResponseWriter, r *http.Request) {
				writeRESTUnauthorized(w, r, "test", "Not authenticated")
			},
			wantStatus:  http.StatusUnauthorized,
			wantBody:    `{"detail":{"message":"Not authenticated"}}` + "\n",
			wantWWWAuth: "Bearer",
		},
		{
			name: "invalid authorization header",
			write: func(w http.ResponseWriter, r *http.Request) {
				writeRESTUnauthorized(w, r, "test", "Invalid authorization header")
			},
			wantStatus:  http.StatusUnauthorized,
			wantBody:    `{"detail":{"message":"Invalid authorization header"}}` + "\n",
			wantWWWAuth: "Bearer",
		},
		{
			name: "invalid or expired token",
			write: func(w http.ResponseWriter, r *http.Request) {
				writeRESTUnauthorized(w, r, "test", "Invalid or expired token")
			},
			wantStatus:  http.StatusUnauthorized,
			wantBody:    `{"detail":{"message":"Invalid or expired token"}}` + "\n",
			wantWWWAuth: "Bearer",
		},
		{
			name: "method not allowed",
			write: func(w http.ResponseWriter, r *http.Request) {
				writeRESTMethodNotAllowed(w, r, "test")
			},
			wantStatus: http.StatusMethodNotAllowed,
			wantBody:   `{"detail":"Method Not Allowed"}` + "\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
			rec := httptest.NewRecorder()
			tc.write(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}
			if got := rec.Body.String(); got != tc.wantBody {
				t.Fatalf("body = %q, want %q", got, tc.wantBody)
			}
			if got := rec.Header().Get("WWW-Authenticate"); got != tc.wantWWWAuth {
				t.Fatalf("WWW-Authenticate = %q, want %q", got, tc.wantWWWAuth)
			}
		})
	}
}

// TestAuthenticateRESTRequestMissingHeader pins get_current_user's first
// branch (auth/routers/dependencies.py): no Authorization header at all
// -> 401 {"detail":{"message":"Not authenticated"}}, WWW-Authenticate:
// Bearer -- confirmed live (see TEST-EVIDENCE). No verifier is
// constructed: this branch returns before Verify would ever be called.
func TestAuthenticateRESTRequestMissingHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	rec := httptest.NewRecorder()

	_, ok := authenticateRESTRequest(rec, req, nil, "test")
	if ok {
		t.Fatal("authenticateRESTRequest: ok = true, want false for a missing Authorization header")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestAuthenticateRESTRequestMalformedHeader pins get_current_user's
// second branch: a present Authorization header that extract_token_from_
// header (services/auth.py) cannot parse into a scheme+token pair (e.g.
// a non-Bearer scheme, or a Bearer scheme with no token) -> 401
// {"detail":{"message":"Invalid authorization header"}} -- confirmed
// live. Also returns before Verify would ever be called.
func TestAuthenticateRESTRequestMalformedHeader(t *testing.T) {
	for _, header := range []string{"Basic abc", "Bearer", "Bearer  "} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
		req.Header.Set("Authorization", header)
		rec := httptest.NewRecorder()

		_, ok := authenticateRESTRequest(rec, req, nil, "test")
		if ok {
			t.Fatalf("authenticateRESTRequest(%q): ok = true, want false", header)
		}
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("authenticateRESTRequest(%q): status = %d, want %d", header, rec.Code, http.StatusUnauthorized)
		}
		if got, want := rec.Body.String(), `{"detail":{"message":"Invalid authorization header"}}`+"\n"; got != want {
			t.Fatalf("authenticateRESTRequest(%q): body = %q, want %q", header, got, want)
		}
	}
}

// TestAuthenticateRESTRequestVerifyFailure pins get_current_user's third
// branch: a well-formed "Bearer <token>" value whose token fails
// verification -> 401 {"detail":{"message":"Invalid or expired token"}}
// -- confirmed live against a stubbed auth service (a real Postgres-backed
// AuthService is out of reach in this environment; see TEST-EVIDENCE).
// principal.NewVerifier never touches its JWKS path at construction time
// (that file is read lazily, per Verify -- see that function's own doc
// comment), so a nonexistent path here is enough to force Verify to fail
// on any token without any real key material.
func TestAuthenticateRESTRequestVerifyFailure(t *testing.T) {
	verifier, err := principal.NewVerifier(t.TempDir()+"/missing-jwks.json", "test-issuer", "test-audience")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	req.Header.Set("Authorization", "Bearer not-a-real-token")
	rec := httptest.NewRecorder()

	_, ok := authenticateRESTRequest(rec, req, verifier, "test")
	if ok {
		t.Fatal("authenticateRESTRequest: ok = true, want false for a token that fails verification")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Invalid or expired token"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
	}
}

// TestNoAPIV1RoutePlainTextErrors is a repo-wide guard, in two parts:
//
//  1. no /api/v1/* route file under cmd/query-api may answer an error
//     with http.Error or http.NotFound (both write a plain-text body,
//     never FastAPI's JSON {"detail": ...} envelope);
//  2. no /api/v1/* route file other than this one may declare its OWN
//     {"detail": ...} envelope type (a struct field tagged
//     `json:"detail"`) -- there is exactly ONE such type in this binary,
//     restErrorBody, and exactly one place any route writes it,
//     writeRESTError and its writeRESTUnauthorized/
//     writeRESTMethodNotAllowed/writeRESTDataUnavailable/
//     authenticateRESTRequest callers. A route-local re-implementation
//     of the same envelope (explain_route.go used to carry one,
//     explainErrorBody/writeExplainJSONError) is exactly what part 2
//     catches even though it never calls http.Error at all -- it still
//     answers the right JSON, just via a second, drifting copy of the
//     same logic part 1 alone cannot see.
//
// Every error path on these routes must go through writeRESTError's
// family (or the already-established writePydanticValidationError for
// 422) instead. The file set is discovered by glob + content match (any
// "*_route.go" that mentions an "/api/v1/" path), not a hand-maintained
// list, so a future route file is covered automatically.
func TestNoAPIV1RoutePlainTextErrors(t *testing.T) {
	matches, err := filepath.Glob("./*_route.go")
	if err != nil {
		t.Fatalf("glob *_route.go: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("glob *_route.go matched nothing -- guard is not exercising anything")
	}

	checked := 0
	for _, path := range matches {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if !strings.Contains(string(src), "/api/v1/") {
			continue // not a REST /api/v1/* route file (e.g. query_route.go, registry_route.go)
		}
		checked++
		for _, banned := range []string{"http.Error(", "http.NotFound("} {
			if strings.Contains(string(src), banned) {
				t.Errorf("%s calls %s -- REST /api/v1/* routes must answer errors via the shared writeRESTError family, never a plain-text response", path, banned)
			}
		}
		// The glob is "*_route.go", so rest_error_response.go (this
		// file's own implementation, the ONE legitimate owner of a
		// `json:"detail"` tag) can never appear in matches -- no
		// self-exclusion needed here.
		if strings.Contains(string(src), `json:"detail"`) {
			t.Errorf(`%s declares its own json:"detail"-tagged envelope type -- REST /api/v1/* routes must use rest_error_response.go's shared restErrorBody, never a route-local copy`, path)
		}
	}
	if checked == 0 {
		t.Fatal("no *_route.go file matched an /api/v1/ path -- guard is not exercising anything")
	}
}
