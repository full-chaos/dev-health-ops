package categorize

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// CHAOS-2558: an org BYO provider with a stored base_url dials through the
// SSRF-guarded transport; a loopback (or any internal) address is refused
// before a request byte -- the org's key included -- is sent. The
// env-configured provider is not customer-controlled and is unchanged.
func TestOrgBYOProvidersRefuseAnInternalBaseURLAtDial(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{}"}}],"usage":{}}`))
	}))
	defer server.Close()
	for _, kind := range []ProviderKind{ProviderKindOpenAI, ProviderKindLocal, ProviderKindOllama} {
		t.Run(string(kind), func(t *testing.T) {
			hits.Store(0)
			provider, err := NewProviderFromCredentials(kind, "org-secret-key", server.URL+"/v1", "m")
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Complete(context.Background(), CompletionRequest{Prompt: "p", SystemMessage: "s"})
			if err == nil {
				t.Fatal("the internal base_url was reached")
			}
			if hits.Load() != 0 {
				t.Fatalf("the server received %d requests", hits.Load())
			}
			if strings.Contains(err.Error(), "org-secret-key") || strings.Contains(err.Error(), server.URL) {
				t.Fatalf("error %q carries the key or the URL", err.Error())
			}
		})
	}
}

func TestEnvConfiguredLocalProviderIsNotGuarded(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{}"}}],"usage":{}}`))
	}))
	defer server.Close()
	provider := NewLocalProvider(LocalProviderConfig{BaseURL: server.URL + "/v1", Model: "m"})
	if _, err := provider.Complete(context.Background(), CompletionRequest{Prompt: "p", SystemMessage: "s"}); err != nil {
		t.Fatalf("the env-configured local provider must reach its own server: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}
}

func TestOrgHTTPClientIsGuardedOnlyForASuppliedBaseURL(t *testing.T) {
	if orgHTTPClient("", nil) != nil || orgHTTPClient("  ", nil) != nil {
		t.Fatal("no base_url must keep the provider default client")
	}
	if client := orgHTTPClient("https://org.example/v1", nil); client == nil || client.Transport == nil {
		t.Fatal("a supplied base_url must get the guarded client")
	}
}

// The transport-injecting constructor is a guard bypass: no production code
// may reference it except NewProviderFromCredentials, which calls it with a
// nil transport (the guarded one). Checked on the syntax tree, so an alias
// (`var f = newProviderFromCredentialsWithTransport`), a method value or a
// call with a transport argument fails too. Tests in this package may use it.
func TestOnlyNewProviderFromCredentialsReferencesTheTransportInjectingConstructor(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	const target = "newProviderFromCredentialsWithTransport"
	references := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				ast.Inspect(declaration, func(node ast.Node) bool {
					if ident, ok := node.(*ast.Ident); ok && ident.Name == target {
						t.Errorf("%s: %s is referenced outside a function body", name, target)
					}
					return true
				})
				continue
			}
			if function.Name.Name == target {
				continue
			}
			allowed := name == "providerkind.go" && function.Name.Name == "NewProviderFromCredentials"
			ast.Inspect(function, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.CallExpr:
					if ident, ok := value.Fun.(*ast.Ident); ok && ident.Name == target {
						references++
						last, isIdent := value.Args[len(value.Args)-1].(*ast.Ident)
						if !allowed || !isIdent || last.Name != "nil" {
							t.Errorf("%s: %s calls %s; only NewProviderFromCredentials may, with a nil transport", name, function.Name.Name, target)
						}
					}
				case *ast.Ident:
					if value.Name == target && !isCallee(function, value) {
						t.Errorf("%s: %s uses %s other than as a direct call (an alias or a function value)", name, function.Name.Name, target)
					}
				}
				return true
			})
		}
	}
	if references != 1 {
		t.Errorf("found %d production calls of %s, want exactly 1 (in NewProviderFromCredentials)", references, target)
	}
}

// isCallee reports whether ident is the function position of a call inside function.
func isCallee(function *ast.FuncDecl, ident *ast.Ident) bool {
	callee := false
	ast.Inspect(function, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && call.Fun == ast.Expr(ident) {
			callee = true
		}
		return true
	})
	return callee
}
