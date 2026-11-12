package authapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// CHAOS-11207 (C3a): every human access token carries a session.v1 sid, so a
// web approver reference (whose sid web takes from that token) exists for every
// session class. The proof has three parts: every sign-in, refresh, org switch,
// delegated and compat path mints through Issuer with a Grant that names its
// session; Issuer refuses a Grant without one; and both minted tokens carry
// the wire form of that session.

// grantSites are the files whose production code builds a Grant: login,
// social, enterprise SSO (SAML and OIDC share sso_session.go), refresh, org
// switch, go-api's session issue, the credential flows, delegated sessions and
// the compat routes.
var grantSites = []string{"compat.go", "credentials.go", "delegated.go", "issue.go", "login.go", "refresh.go", "social.go", "sso_session.go", "switch_org.go"}

func TestEveryProductionGrantNamesItsSession(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var sites []string
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
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if ident, ok := lit.Type.(*ast.Ident); !ok || ident.Name != "Grant" {
				return true
			}
			named := false
			for _, elt := range lit.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "SessionID" {
						named = true
					}
				}
			}
			if !named {
				t.Errorf("%s: a Grant names no SessionID", fset.Position(lit.Pos()))
			}
			if !slices.Contains(sites, name) {
				sites = append(sites, name)
			}
			return true
		})
	}
	slices.Sort(sites)
	if !slices.Equal(sites, grantSites) {
		t.Fatalf("files that build a Grant %v; want %v (a new mint path must be added here and name its session)", sites, grantSites)
	}
}

func TestIssuerRefusesAGrantWithoutASession(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	if _, err := issuer.Tokens(context.Background(), Grant{PrincipalID: uuid.New()}); err == nil {
		t.Fatal("Tokens minted for a grant without a session")
	}
	if _, err := issuer.AccessContext(context.Background(), Grant{PrincipalID: uuid.New()}); err == nil {
		t.Fatal("AccessContext minted for a grant without a session")
	}
}

func TestBothMintedTokensCarryTheWireSid(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	sid := uuid.New()
	tokens, err := issuer.Tokens(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: sid})
	if err != nil {
		t.Fatal(err)
	}
	for name, compact := range map[string]string{"access": tokens.Access, "session": tokens.Session} {
		parts := strings.Split(compact, ".")
		if len(parts) != 3 {
			t.Fatalf("%s token is not compact", name)
		}
		raw, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		var claims struct {
			Sid string `json:"sid"`
		}
		if err := json.Unmarshal(raw, &claims); err != nil || claims.Sid != session.WireSessionID(sid) {
			t.Fatalf("%s token sid %q; want %q", name, claims.Sid, session.WireSessionID(sid))
		}
	}
}
