package server

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// fakeEdgeUsers is the users-row source of an edge verifier under test.
type fakeEdgeUsers struct {
	state policy.UserState
	found bool
	err   error

	notMember bool
	memberErr error
}

func (f *fakeEdgeUsers) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return f.state, f.found, f.err
}

func (f *fakeEdgeUsers) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	if f.memberErr != nil {
		return "", false, f.memberErr
	}
	return "admin", !f.notMember, nil
}

func (f *fakeEdgeUsers) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := f.Membership(ctx, user, org)
	return role, member, true, err
}

func (f *fakeEdgeUsers) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, errors.New("ActiveImpersonation must not be read")
}

func edgeSettings(kv map[string]string) getenvFunc {
	return func(key string) string { return kv[key] }
}

// The issuer without a users store is a build error, never a JWT-only
// verifier (CHAOS-6290 fail closed): there is no registry pool when the /query
// route is not configured.
func TestNewEdgeUserStoreFailsClosedWithoutPool(t *testing.T) {
	store, err := newEdgeUserStore(edgeSettings(map[string]string{accessTokenIssuerEnvVar: accessTestIssuer}), nil)
	if err == nil {
		t.Fatalf("newEdgeUserStore = (%v, nil), want an error when there is no registry pool", store)
	}
	if _, err := buildEdgeVerifierFromEnv(edgeSettings(map[string]string{accessTokenIssuerEnvVar: accessTestIssuer}), nil); err == nil {
		t.Fatal("buildEdgeVerifierFromEnv built a verifier with a nil users store")
	}
}

func TestNewEdgeUserStoreOffWithoutTheIssuer(t *testing.T) {
	store, err := newEdgeUserStore(edgeSettings(nil), nil)
	if err != nil || store != nil {
		t.Fatalf("newEdgeUserStore = (%v, %v), want (nil, nil) when AUTH_ACCESS_TOKEN_ISSUER is unset", store, err)
	}
	if v, err := buildEdgeVerifierFromEnv(edgeSettings(nil), nil); v != nil || err != nil {
		t.Fatalf("buildEdgeVerifierFromEnv = (%v, %v), want (nil, nil) without AUTH_ACCESS_TOKEN_ISSUER", v, err)
	}
}

// The store reads through the SAME pool it was given (the /query route's), not
// a pool of its own.
func TestNewEdgeUserStoreReusesTheGivenPool(t *testing.T) {
	// pgxpool.New does not dial; the DSN points nowhere on purpose.
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := newEdgeUserStore(edgeSettings(map[string]string{accessTokenIssuerEnvVar: accessTestIssuer}), pool)
	if err != nil {
		t.Fatalf("newEdgeUserStore: %v", err)
	}
	pg, ok := pgStoreOf(store)
	if !ok || pg.Pool != pool {
		t.Fatalf("store = %#v, want query-api's users store over policy.PGStore over the given pool", store)
	}
}

// ONE pool for every edge-verified route: newEdgeUserStore is called exactly
// once in the package (Build), over the /query route's pool (registryPool),
// and every route builder receives its result. Reads the package source and
// FAILS when it finds no call, so a moved or renamed call cannot pass silently.
func TestEdgeUserStoreIsBuiltOnceAndPassedToEveryEdgeRoute(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob package sources: %v (%d files)", err, len(files))
	}
	fset := token.NewFileSet()
	storeCalls, storeCallsOverQueryPool, verifierCalls, verifierCallsWithStore := 0, 0, 0, 0
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
				if len(call.Args) == 2 {
					if arg, ok := call.Args[1].(*ast.Ident); ok && arg.Name == "registryPool" {
						storeCallsOverQueryPool++
					}
				}
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
	if storeCallsOverQueryPool != 1 {
		t.Fatalf("newEdgeUserStore is called over registryPool (the /query route's pool) %d times, want 1", storeCallsOverQueryPool)
	}
	if verifierCalls == 0 {
		t.Fatal("found no buildEdgeVerifierFromEnv call; the scan measured nothing")
	}
	if verifierCallsWithStore != verifierCalls {
		t.Fatalf("%d of %d buildEdgeVerifierFromEnv calls pass the shared edgeUsers store", verifierCallsWithStore, verifierCalls)
	}
}

func (fakeEdgeUsers) ReadsPlatformRoles() bool { return false }
