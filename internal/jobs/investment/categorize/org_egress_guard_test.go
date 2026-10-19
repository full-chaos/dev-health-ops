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
	t.Setenv("LLM_BASE_URL", "")
	t.Setenv("LOCAL_LLM_BASE_URL", server.URL+"/v1")
	provider, err := NewProviderFromEnvWithModel(ProviderKindLocal, "m")
	if err != nil {
		t.Fatal(err)
	}
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
// may reference it except NewProviderFromCredentials, which calls it with the
// predeclared nil (the guarded transport). Checked on the syntax tree, so an
// alias (`var f = newProviderFromCredentialsWithTransport`), a method value, a
// reference inside the constructor's own body, a call with a transport
// argument or a shadowed `nil` fails too. Tests in this package may use it.
const injectingConstructor = "newProviderFromCredentialsWithTransport"

func TestOnlyNewProviderFromCredentialsReferencesTheTransportInjectingConstructor(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		fileCalls, problems := guardBypassFindings(name, file)
		calls += fileCalls
		for _, problem := range problems {
			t.Error(problem)
		}
	}
	if calls != 1 {
		t.Errorf("found %d production calls of %s, want exactly 1 (in NewProviderFromCredentials)", calls, injectingConstructor)
	}
}

// guardBypassFindings returns the number of calls of the injecting
// constructor in file and every way file reaches it other than the one
// allowed call.
func guardBypassFindings(name string, file *ast.File) (calls int, problems []string) {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			ast.Inspect(declaration, func(node ast.Node) bool {
				if ident, ok := node.(*ast.Ident); ok && ident.Name == injectingConstructor {
					problems = append(problems, name+": "+injectingConstructor+" is referenced outside a function body")
				}
				return true
			})
			continue
		}
		allowed := name == "providerkind.go" && function.Name.Name == "NewProviderFromCredentials"
		ast.Inspect(function, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.CallExpr:
				if ident, ok := value.Fun.(*ast.Ident); ok && ident.Name == injectingConstructor {
					calls++
					last, isIdent := value.Args[len(value.Args)-1].(*ast.Ident)
					if !allowed || !isIdent || last.Name != "nil" {
						problems = append(problems, name+": "+function.Name.Name+" calls "+injectingConstructor+"; only NewProviderFromCredentials may, with a nil transport")
					}
				}
			case *ast.Ident:
				if value.Name == injectingConstructor && value != function.Name && !isCallee(function, value) {
					problems = append(problems, name+": "+function.Name.Name+" uses "+injectingConstructor+" other than as a direct call (an alias or a function value)")
				}
			}
			return true
		})
		if function.Name.Name == injectingConstructor {
			// Its own body may not call or reference itself.
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if ident, ok := node.(*ast.Ident); ok && ident.Name == injectingConstructor {
					problems = append(problems, name+": "+injectingConstructor+" references itself")
				}
				return true
			})
		}
		if allowed && declaresNil(function) {
			problems = append(problems, name+": "+function.Name.Name+" declares its own `nil`, so the nil transport argument is not the predeclared nil")
		}
	}
	return calls, problems
}

// declaresNil reports whether function declares an identifier named nil (a
// short variable declaration, an assignment, a var, a parameter or result, a
// range variable).
func declaresNil(function *ast.FuncDecl) bool {
	found := false
	isNil := func(expr ast.Expr) bool {
		ident, ok := expr.(*ast.Ident)
		return ok && ident.Name == "nil"
	}
	ast.Inspect(function, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.AssignStmt:
			for _, left := range value.Lhs {
				if isNil(left) {
					found = true
				}
			}
		case *ast.ValueSpec:
			for _, ident := range value.Names {
				if ident.Name == "nil" {
					found = true
				}
			}
		case *ast.Field:
			for _, ident := range value.Names {
				if ident.Name == "nil" {
					found = true
				}
			}
		case *ast.RangeStmt:
			if (value.Key != nil && isNil(value.Key)) || (value.Value != nil && isNil(value.Value)) {
				found = true
			}
		}
		return true
	})
	return found
}

// The census is only worth running if it can fail: each source below is a way
// to reach the guard bypass, and must be reported; the real shape must not be.
func TestGuardBypassCensusFindsEveryWayToReachTheConstructor(t *testing.T) {
	const header = "package categorize\nimport \"net/http\"\nvar _ http.RoundTripper\n"
	for name, tc := range map[string]struct {
		file, source string
		wantProblem  bool
	}{
		"the real shape":                    {"providerkind.go", header + `func NewProviderFromCredentials() { newProviderFromCredentialsWithTransport(1, nil) }`, false},
		"top-level alias":                   {"providerkind.go", header + `var f = newProviderFromCredentialsWithTransport`, true},
		"local alias":                       {"providerkind.go", header + `func NewProviderFromCredentials() { f := newProviderFromCredentialsWithTransport; f(1, nil) }`, true},
		"a second function calls it":        {"other.go", header + `func other() { newProviderFromCredentialsWithTransport(1, nil) }`, true},
		"a transport argument":              {"providerkind.go", header + `func NewProviderFromCredentials() { newProviderFromCredentialsWithTransport(1, http.DefaultTransport) }`, true},
		"a shadowed nil":                    {"providerkind.go", header + `func NewProviderFromCredentials() { nil := http.DefaultTransport; newProviderFromCredentialsWithTransport(1, nil) }`, true},
		"a nil parameter":                   {"providerkind.go", header + `func NewProviderFromCredentials(nil http.RoundTripper) { newProviderFromCredentialsWithTransport(1, nil) }`, true},
		"the constructor references itself": {"providerkind.go", header + `func newProviderFromCredentialsWithTransport() { _ = newProviderFromCredentialsWithTransport }`, true},
	} {
		t.Run(name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), tc.file, tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, problems := guardBypassFindings(tc.file, file)
			if (len(problems) > 0) != tc.wantProblem {
				t.Fatalf("problems = %v, want a problem: %v", problems, tc.wantProblem)
			}
		})
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
