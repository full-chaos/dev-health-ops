package server

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// fakeEdgeUsers is the users-row source of an edge verifier under test.
type fakeEdgeUsers struct {
	state policy.UserState
	found bool
	err   error
}

func (f *fakeEdgeUsers) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return f.state, f.found, f.err
}

func (f *fakeEdgeUsers) IsMember(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return false, errors.New("IsMember must not be read")
}

func (f *fakeEdgeUsers) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, errors.New("ActiveImpersonation must not be read")
}

// authWithEdgeUsers runs authenticateRESTRequest with a valid HS256 edge token
// for the fixture subject against a verifier reading users.
func authWithEdgeUsers(t *testing.T, users policy.Store) (bool, *httptest.ResponseRecorder) {
	t.Helper()
	edgeVerifier, err := principal.NewEdgeVerifier(edgeTestSecret, "dev-health-ops", "dev-health-api", users)
	if err != nil {
		t.Fatalf("NewEdgeVerifier: %v", err)
	}
	envelopeVerifier, err := principal.NewVerifier(t.TempDir()+"/missing-jwks.json", "test-issuer", "test-audience")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	req.Header.Set("Authorization", "Bearer "+signTestEdgeToken(t, validTestEdgeClaims("org-edge-1")))
	rec := httptest.NewRecorder()
	_, ok := authenticateRESTRequest(rec, req, envelopeVerifier, edgeVerifier, "test")
	return ok, rec
}

// CHAOS-6290: the REST plane refuses the access token of a user the users row
// says is inactive, missing or stale, and serves the active one.
func TestAuthenticateRESTRequestEdgeUsersRow(t *testing.T) {
	const refused = `{"detail":{"message":"Invalid or expired token"}}` + "\n"
	cases := []struct {
		name     string
		users    *fakeEdgeUsers
		wantOK   bool
		wantCode int
		wantBody string
	}{
		{"active is served", &fakeEdgeUsers{state: policy.UserState{IsActive: true}, found: true}, true, http.StatusOK, ""},
		{"inactive is refused", &fakeEdgeUsers{state: policy.UserState{IsActive: false}, found: true}, false, http.StatusUnauthorized, refused},
		{"missing is refused", &fakeEdgeUsers{found: false}, false, http.StatusUnauthorized, refused},
		{"stale token_version is refused", &fakeEdgeUsers{state: policy.UserState{IsActive: true, TokenVersion: 2}, found: true}, false, http.StatusUnauthorized, refused},
		// Same answer as the Go api guard for the same store failure
		// (policy.WriteAuthFailure: guard.go currentUser): 503 when the store
		// is unavailable, 500 otherwise -- never a pass.
		{"store unavailable answers 503", &fakeEdgeUsers{err: errors.Join(policy.ErrUnavailable, errors.New("down"))}, false, http.StatusServiceUnavailable, ""},
		{"store error answers 500", &fakeEdgeUsers{err: errors.New("permission denied for table users")}, false, http.StatusInternalServerError, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, rec := authWithEdgeUsers(t, tc.users)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v; body=%s", ok, tc.wantOK, rec.Body.String())
			}
			if !tc.wantOK && rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantBody != "" && rec.Body.String() != tc.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tc.wantBody)
			}
		})
	}
}

func edgeSettings(kv map[string]string) getenvFunc {
	return func(key string) string { return kv[key] }
}

// The edge secret without a users store is a build error, never a JWT-only
// verifier (CHAOS-6290 fail closed).
func TestNewEdgeUserStoreFailsClosedWithoutPostgres(t *testing.T) {
	store, closeStore, err := newEdgeUserStore(edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret}))
	if err == nil {
		closeStore()
		t.Fatalf("newEdgeUserStore = (%v, nil), want an error when GO_API_REGISTRY_POSTGRES_URI is empty", store)
	}
	if _, err := buildEdgeVerifierFromEnv(edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret}), nil); err == nil {
		t.Fatal("buildEdgeVerifierFromEnv built a verifier with a nil users store")
	}
}

func TestNewEdgeUserStoreOffWithoutEdgeSecret(t *testing.T) {
	store, closeStore, err := newEdgeUserStore(edgeSettings(nil))
	if err != nil || store != nil {
		t.Fatalf("newEdgeUserStore = (%v, %v), want (nil, nil) when the edge secret is unset", store, err)
	}
	closeStore()
	if v, err := buildEdgeVerifierFromEnv(edgeSettings(nil), nil); v != nil || err != nil {
		t.Fatalf("buildEdgeVerifierFromEnv = (%v, %v), want (nil, nil) without the edge secret", v, err)
	}
}

func TestNewEdgeUserStoreBuildsOneLazyPool(t *testing.T) {
	// pgxpool.New does not dial; the DSN points nowhere on purpose.
	store, closeStore, err := newEdgeUserStore(edgeSettings(map[string]string{
		edgeJWTSecretEnvVar:            edgeTestSecret,
		"GO_API_REGISTRY_POSTGRES_URI": "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1",
	}))
	if err != nil {
		t.Fatalf("newEdgeUserStore: %v", err)
	}
	defer closeStore()
	if _, ok := store.(policy.PGStore); !ok {
		t.Fatalf("store = %T, want policy.PGStore (the Go api's own users-row reader)", store)
	}
}

// ONE pool for every edge-verified route: newEdgeUserStore is called exactly
// once in the package (Build) and every route builder receives its result;
// none builds a pool for the edge users check. Reads the package source and
// FAILS when it finds no call, so a moved or renamed call cannot pass silently.
func TestEdgeUserStoreIsBuiltOnceAndPassedToEveryEdgeRoute(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob package sources: %v (%d files)", err, len(files))
	}
	fset := token.NewFileSet()
	storeCalls, verifierCalls, verifierCallsWithStore := 0, 0, 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			ident, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			switch ident.Name {
			case "newEdgeUserStore":
				storeCalls++
			case "buildEdgeVerifierFromEnv":
				verifierCalls++
				if len(call.Args) == 2 {
					if arg, ok := call.Args[1].(*ast.Ident); ok && arg.Name == "edgeUsers" {
						verifierCallsWithStore++
					}
				}
			}
			return true
		})
	}
	if storeCalls != 1 {
		t.Fatalf("newEdgeUserStore is called %d times in non-test sources, want exactly 1 (Build)", storeCalls)
	}
	if verifierCalls == 0 {
		t.Fatal("found no buildEdgeVerifierFromEnv call; the scan measured nothing")
	}
	if verifierCallsWithStore != verifierCalls {
		t.Fatalf("%d of %d buildEdgeVerifierFromEnv calls pass the shared edgeUsers store", verifierCallsWithStore, verifierCalls)
	}
}
