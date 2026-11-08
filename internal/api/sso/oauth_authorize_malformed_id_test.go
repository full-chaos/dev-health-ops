package sso

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// oauthPairHandler is the mounted /oauth/{first}/{second} POST handler with the real OAuth handlers on. The pool is
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

const malformedIDNotFound = `{"detail":"OAuth provider not found"}`

// A provider id that is not a UUID answers what an unknown provider id answers, and is never looked up.
func TestOAuthAuthorizeAnswersAMalformedProviderIDAsAnUnknownID(t *testing.T) {
	unknown := httptest.NewRecorder()
	writeOAuthProviderNotFound(unknown)
	for _, first := range []string{"providers", "not-a-uuid", "0000000000000000000000000000001", "00000000-0000-4000-8000-00000000000g"} {
		t.Run(first, func(t *testing.T) {
			recorder := postOAuthAuthorize(t, first, "{}")
			if recorder.Code != http.StatusNotFound || recorder.Body.String() != malformedIDNotFound {
				t.Fatalf("got %d %s, want 404 %s", recorder.Code, recorder.Body.String(), malformedIDNotFound)
			}
			if recorder.Code != unknown.Code || !reflect.DeepEqual(recorder.Header(), unknown.Header()) ||
				recorder.Body.String() != unknown.Body.String() {
				t.Fatalf("got %d %v %s, want the unknown-id answer %d %v %s", recorder.Code, recorder.Header(),
					recorder.Body.String(), unknown.Code, unknown.Header(), unknown.Body.String())
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

// A provider id the CPython parser takes goes on to the database (control), including a form pydantic refuses.
func TestOAuthAuthorizeGoesOnWithAValidProviderID(t *testing.T) {
	for _, first := range []string{"00000000-0000-4000-8000-000000000001", "0x" + strings.Repeat("1", 30)} {
		t.Run(first, func(t *testing.T) {
			reached := false
			func() {
				defer func() { reached = recover() != nil }()
				postOAuthAuthorize(t, first, "{}")
			}()
			if !reached {
				t.Fatal("refused before the database")
			}
		})
	}
}

// Both not-found branches of initiateOAuthAuth (the malformed id and the no-rows lookup) write the one shared answer.
func TestOAuthAuthorizeNotFoundBranchesShareOneWriter(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "oauth.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var handler *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "initiateOAuthAuth" {
			handler = fn
		}
	}
	if handler == nil {
		t.Fatal("initiateOAuthAuth not found")
	}
	branches := map[string]bool{}
	ast.Inspect(handler.Body, func(node ast.Node) bool {
		stmt, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		cond := "err != nil"
		if call, ok := stmt.Cond.(*ast.CallExpr); ok {
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "errorsIsNoRows" {
				cond = "errorsIsNoRows"
			}
		}
		if len(stmt.Body.List) != 2 {
			return true
		}
		expr, ok := stmt.Body.List[0].(*ast.ExprStmt)
		if !ok {
			return true
		}
		if call, ok := expr.X.(*ast.CallExpr); ok {
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "writeOAuthProviderNotFound" {
				branches[cond] = true
			}
		}
		return true
	})
	if !branches["errorsIsNoRows"] || !branches["err != nil"] {
		t.Fatalf("branches calling writeOAuthProviderNotFound: %v, want the no-rows and the malformed-id branch", branches)
	}
}
