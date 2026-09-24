package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
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
				writeRESTDataUnavailable(w, r, "test", "org-1", errors.New("simulated downstream failure"))
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

	_, ok := authenticateRESTRequest(rec, req, nil, nil, "test")
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

		_, ok := authenticateRESTRequest(rec, req, nil, nil, "test")
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

	_, ok := authenticateRESTRequest(rec, req, verifier, nil, "test")
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

// edgeTestSecret is a fixture key built at runtime, never a literal --
// a short, obviously-fake token repeated past NewEdgeVerifier's
// 32-character floor. Never a real value, never read from any
// environment or file.
var edgeTestSecret = strings.Repeat("not-a-real-secret-", 3)

// signTestEdgeToken signs claims with HS256 using edgeTestSecret,
// mirroring AuthService.create_access_token's own jwt.encode call shape.
func signTestEdgeToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(edgeTestSecret))
	if err != nil {
		t.Fatalf("sign edge token: %v", err)
	}
	return signed
}

func validTestEdgeClaims(orgID string) jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"sub":    "11111111-1111-1111-1111-111111111111",
		"org_id": orgID,
		"type":   "access",
		"iss":    "dev-health-ops",
		"aud":    "dev-health-api",
		"exp":    now.Add(time.Hour).Unix(),
	}
}

// TestAuthenticateRESTRequestEdgeTokenAccepted pins the core edge-token
// contract: when edgeVerifier is configured, a well-formed HS256 edge
// access token is accepted on a REST route, and its org_id claim is what
// ends up in authctx.Claims -- get_current_user derives org_id the same
// way, from the validated claims alone, never a second lookup.
func TestAuthenticateRESTRequestEdgeTokenAccepted(t *testing.T) {
	edgeVerifier, err := principal.NewEdgeVerifier(edgeTestSecret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatalf("NewEdgeVerifier: %v", err)
	}
	envelopeVerifier, err := principal.NewVerifier(t.TempDir()+"/missing-jwks.json", "test-issuer", "test-audience")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}

	token := signTestEdgeToken(t, validTestEdgeClaims("org-edge-1"))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	claims, ok := authenticateRESTRequest(rec, req, envelopeVerifier, edgeVerifier, "test")
	if !ok {
		t.Fatalf("authenticateRESTRequest: ok = false, want true; body=%s", rec.Body.String())
	}
	if claims.OrgID != "org-edge-1" {
		t.Fatalf("OrgID = %q, want %q", claims.OrgID, "org-edge-1")
	}
}

// TestAuthenticateRESTRequestEdgeTokenRejected pins the SAME 401 body an
// envelope rejection produces -- get_current_user never distinguishes
// WHY a token was rejected in its response, only THAT it was.
func TestAuthenticateRESTRequestEdgeTokenRejected(t *testing.T) {
	edgeVerifier, err := principal.NewEdgeVerifier(edgeTestSecret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatalf("NewEdgeVerifier: %v", err)
	}
	envelopeVerifier, err := principal.NewVerifier(t.TempDir()+"/missing-jwks.json", "test-issuer", "test-audience")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}

	claims := validTestEdgeClaims("org-edge-1")
	claims["exp"] = time.Now().Add(-time.Hour).Unix()
	token := signTestEdgeToken(t, claims)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	_, ok := authenticateRESTRequest(rec, req, envelopeVerifier, edgeVerifier, "test")
	if ok {
		t.Fatal("authenticateRESTRequest: ok = true, want false for an expired edge token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Invalid or expired token"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestAuthenticateRESTRequestNilEdgeVerifierUnchanged pins the
// backward-compatibility contract: when edgeVerifier is nil (the pod has
// not been given GO_API_EDGE_JWT_SECRET), an HS256 token that WOULD have
// been accepted by an edge verifier is instead routed to the envelope
// verifier -- exactly this function's pre-existing behaviour -- and is
// refused, because it is not a valid EdDSA envelope.
func TestAuthenticateRESTRequestNilEdgeVerifierUnchanged(t *testing.T) {
	envelopeVerifier, err := principal.NewVerifier(t.TempDir()+"/missing-jwks.json", "test-issuer", "test-audience")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}

	token := signTestEdgeToken(t, validTestEdgeClaims("org-edge-1"))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	_, ok := authenticateRESTRequest(rec, req, envelopeVerifier, nil, "test")
	if ok {
		t.Fatal("authenticateRESTRequest: ok = true, want false -- edgeVerifier is nil, so an HS256 token must fall through to (and be refused by) the envelope verifier")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Invalid or expired token"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestJwtHeaderAlg pins jwtHeaderAlg's own contract: a legible alg header
// is read back exactly, and anything else -- too few segments, non-base64
// header, non-JSON header, an empty alg -- reports ok=false rather than
// panicking or guessing.
func TestJwtHeaderAlg(t *testing.T) {
	tests := []struct {
		name    string
		token   string
		wantAlg string
		wantOK  bool
	}{
		{"HS256 token", signTestEdgeToken(t, validTestEdgeClaims("org-1")), "HS256", true},
		{"too few segments", "abc.def", "", false},
		{"empty header segment", ".def.ghi", "", false},
		{"non-base64 header", "!!!.def.ghi", "", false},
		{"not-a-jwt", "not-a-jwt-at-all", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			alg, ok := jwtHeaderAlg(tc.token)
			if ok != tc.wantOK {
				t.Fatalf("jwtHeaderAlg(%q): ok = %v, want %v", tc.token, ok, tc.wantOK)
			}
			if ok && alg != tc.wantAlg {
				t.Fatalf("jwtHeaderAlg(%q): alg = %q, want %q", tc.token, alg, tc.wantAlg)
			}
		})
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

// TestDataUnavailableCallSitesLogTheCause is a totality guard, same
// method as unrestricted_read_options_test.go's
// TestEveryClickHouseReadClientUsesTheSharedUnrestrictedOptions: it
// derives its answer from the actual source on every run, walking every
// non-test .go file directly under cmd/query-api for the two violation
// shapes this guard exists to close, rather than a hand-maintained list
// of known degradation sites --
//
//  1. a call to writeRESTDataUnavailable whose final (err) argument is
//     the literal `nil`. writeRESTDataUnavailable now logs that argument
//     before writing the response, so a nil there is a real regression
//     back to a swallowed cause, not just a missing log call.
//  2. a call to writeRESTError (the lower-level, non-logging primitive)
//     whose status argument is the literal http.StatusServiceUnavailable
//     -- a 503 writer that bypasses the logging wrappers entirely, the
//     exact shape every route's degradation site had before this guard
//     existed (nothing logged, ever, for any of them).
//
// The BODIES of the logging wrappers themselves are excluded from
// violation class 2: writing the 503 is what they are for, and each one
// logs the cause first. loggingDegradationWrappers names them, and every
// CALL to one of them is checked for a literal-nil cause the same way.
// A route needs its own wrapper only when its Python counterpart's 503
// carries a different detail literal than "Data unavailable" -- which is
// why there is a set here rather than a single name.
func TestDataUnavailableCallSitesLogTheCause(t *testing.T) {
	const dir = "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	// loggingDegradationWrappers are the functions allowed to write a 503,
	// mapped to their own argument count. Each logs the cause before
	// writing the response, and each takes the cause as its LAST argument.
	// Pinning the count per wrapper keeps a signature change from silently
	// moving which argument the nil check below inspects.
	loggingDegradationWrappers := map[string]int{
		// (w, r, component, orgID, err)
		"writeRESTDataUnavailable": 5,
		// (w, r, orgID, err) -- route-local, so it names its own component.
		"writeWorkUnitExplainUnavailable": 4,
	}

	var violations []string
	sawDataUnavailableCall := false

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read %s: %v", path, readErr)
		}

		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, src, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", path, parseErr)
		}

		// enclosing is the name of the FuncDecl the walk is currently
		// inside, so a 503 written inside a logging wrapper's own body can
		// be told apart from one written in a handler.
		enclosing := ""
		ast.Inspect(file, func(n ast.Node) bool {
			if decl, isDecl := n.(*ast.FuncDecl); isDecl {
				enclosing = decl.Name.Name
				return true
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fnIdent, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}

			if wantArgs, isWrapper := loggingDegradationWrappers[fnIdent.Name]; isWrapper {
				sawDataUnavailableCall = true
				if len(call.Args) != wantArgs {
					violations = append(violations, fmt.Sprintf(
						"%s:%d: %s called with %d argument(s), want %d with the cause last",
						path, fset.Position(call.Pos()).Line, fnIdent.Name, len(call.Args), wantArgs))
					return true
				}
				if errIdent, ok := call.Args[wantArgs-1].(*ast.Ident); ok && errIdent.Name == "nil" {
					violations = append(violations, fmt.Sprintf(
						"%s:%d: %s's err argument is a literal nil -- the wrapped cause must be logged, not swallowed",
						path, fset.Position(call.Pos()).Line, fnIdent.Name))
				}
				return true
			}

			switch fnIdent.Name {
			case "writeRESTError":
				if _, insideWrapper := loggingDegradationWrappers[enclosing]; insideWrapper {
					return true
				}
				for _, arg := range call.Args {
					sel, ok := arg.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					pkgIdent, ok := sel.X.(*ast.Ident)
					if ok && pkgIdent.Name == "http" && sel.Sel.Name == "StatusServiceUnavailable" {
						violations = append(violations, fmt.Sprintf(
							"%s:%d: writeRESTError called directly with http.StatusServiceUnavailable -- a 503 must go through writeRESTDataUnavailable so its cause is logged",
							path, fset.Position(call.Pos()).Line))
					}
				}
			}
			return true
		})
	}

	if !sawDataUnavailableCall {
		t.Fatal("found zero logging-wrapper call sites under cmd/query-api's package-main files -- this guard's premise (there are 503 degradation sites to check) no longer holds; investigate before trusting a green result")
	}

	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("every REST 503 degradation site must log its cause through one of the logging wrappers (..., err); found %d violation(s):\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
}
