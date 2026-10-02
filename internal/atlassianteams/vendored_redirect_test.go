package atlassianteams

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"
	"atlassian/atlassian/rest"
)

// CHAOS-7921: the vendored Atlassian module's DEFAULT http clients (the ones it builds when its caller supplied none) follow no
// redirect (patch 0005). Production supplies its own guarded client, so these only matter when none is supplied.

// redirectProbe is a server that answers every request with a 302 to a second host, and that second host's request counter.
func redirectProbe(t *testing.T) (first string, landed *atomic.Int32, firstHits *atomic.Int32) {
	t.Helper()
	landed, firstHits = new(atomic.Int32), new(atomic.Int32)
	second := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { landed.Add(1) }))
	t.Cleanup(second.Close)
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		http.Redirect(w, r, second.URL+"/landed", http.StatusFound)
	}))
	t.Cleanup(redirecting.Close)
	return redirecting.URL, landed, firstHits
}

// defaultClientSites maps the enclosing function of every NewDefaultHTTPClient call in the vendored module (derived from the source
// below) to the call that reaches it with no client supplied. The test fails when the derived set and this table differ, so a new
// default-client site cannot be added without a behaviour case.
var defaultClientSites = map[string]func(t *testing.T, base string){
	"Execute": func(t *testing.T, base string) {
		_, _ = (&graph.Client{BaseURL: base}).Execute(context.Background(), "query Q { a }", nil, "Q", nil, 1)
	},
	"ExecuteWithExtraHeaders": func(t *testing.T, base string) {
		_, _ = (&graph.Client{BaseURL: base}).ExecuteWithExtraHeaders(context.Background(), "query Q { a }", nil, "Q", nil, 1, map[string]string{"X-Probe": "1"})
	},
	"FetchSchemaIntrospection": func(t *testing.T, base string) {
		_, _ = graph.FetchSchemaIntrospection(context.Background(), base, atlassian.BasicAPITokenAuth{Email: "e@example.test", Token: "t"}, graph.SchemaFetchOptions{OutputDir: t.TempDir()})
	},
	"postOAuthToken": func(t *testing.T, base string) {
		_, _ = atlassian.RefreshAccessToken(context.Background(), "id", "secret", "refresh", atlassian.OAuthTokenRequestOptions{TokenURL: base + "/token"})
	},
	"FetchAccessibleResources": func(t *testing.T, base string) {
		_, _ = atlassian.FetchAccessibleResources(context.Background(), "token", atlassian.AccessibleResourcesOptions{URL: base + "/resources"})
	},
	"GetJSON": func(t *testing.T, base string) {
		_, _ = (&rest.JiraRESTClient{BaseURL: base}).GetJSON(context.Background(), "/rest/api/3/myself", nil)
	},
	"requestJSON": func(t *testing.T, base string) {
		_, _ = (&rest.JiraRESTClient{BaseURL: base}).PostJSON(context.Background(), "/rest/api/3/x", map[string]any{"a": 1})
	},
	"Delete": func(t *testing.T, base string) {
		_ = (&rest.JiraRESTClient{BaseURL: base}).Delete(context.Background(), "/rest/api/3/x")
	},
}

// vendoredGoFiles parses every non-test Go file of the vendored atlassian module, at any depth.
func vendoredGoFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..", "third_party", "vendor", "atlassian", "atlassian")
	files := map[string]*ast.File{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		files[path] = parsed
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no vendored Go file found")
	}
	return files
}

// netHTTPName is the local name a file gives the net/http import ("" when it does not import it); a dot or blank import is refused,
// because it would hide every http.X use from the scan.
func netHTTPName(t *testing.T, path string, file *ast.File) string {
	t.Helper()
	for _, spec := range file.Imports {
		if spec.Path.Value != `"net/http"` {
			continue
		}
		if spec.Name == nil {
			return "http"
		}
		if spec.Name.Name == "." || spec.Name.Name == "_" {
			t.Errorf("%s: net/http imported as %q: the redirect scan cannot see its uses", path, spec.Name.Name)
			return ""
		}
		return spec.Name.Name
	}
	return ""
}

// TestEveryVendoredHTTPClientRefusesRedirects derives, from the vendored source, every http.Client the module builds and every
// default-client site, and requires: no http.Client literal anywhere but the one default constructor, which sets CheckRedirect; no
// http.DefaultClient or package-level http.Get/Post/Head; and the set of default-client sites equals the behaviour table.
func TestEveryVendoredHTTPClientRefusesRedirects(t *testing.T) {
	sites := map[string]int{}
	literals := 0
	for path, file := range vendoredGoFiles(t) {
		name := netHTTPName(t, path, file)
		isHTTP := func(expr ast.Expr, member string) bool {
			selector, ok := expr.(*ast.SelectorExpr)
			if !ok || name == "" {
				return false
			}
			pkg, ok := selector.X.(*ast.Ident)
			return ok && pkg.Name == name && selector.Sel.Name == member
		}
		var enclosing string
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				enclosing = n.Name.Name
			case *ast.CompositeLit:
				if isHTTP(n.Type, "Client") {
					literals++
					if enclosing != "NewDefaultHTTPClient" {
						t.Errorf("%s: an http.Client literal in %s: build it with NewDefaultHTTPClient (no redirect followed)", path, enclosing)
					}
					hasCheck := false
					for _, element := range n.Elts {
						if kv, ok := element.(*ast.KeyValueExpr); ok {
							if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "CheckRedirect" {
								hasCheck = true
							}
						}
					}
					if !hasCheck {
						t.Errorf("%s: an http.Client literal in %s with no CheckRedirect", path, enclosing)
					}
				}
			case *ast.ValueSpec: // var client http.Client
				if isHTTP(n.Type, "Client") {
					t.Errorf("%s: a non-pointer http.Client value in %s: build it with NewDefaultHTTPClient", path, enclosing)
				}
			case *ast.SelectorExpr:
				for _, member := range []string{"DefaultClient", "Get", "Post", "Head", "PostForm"} {
					if isHTTP(n, member) {
						t.Errorf("%s: http.%s in %s uses the redirect-following default client", path, member, enclosing)
					}
				}
			case *ast.CallExpr:
				if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == "new" && len(n.Args) == 1 && isHTTP(n.Args[0], "Client") {
					t.Errorf("%s: new(http.Client) in %s: build it with NewDefaultHTTPClient", path, enclosing)
				}
				callee := ""
				switch fun := n.Fun.(type) {
				case *ast.Ident:
					callee = fun.Name
				case *ast.SelectorExpr:
					callee = fun.Sel.Name
				}
				if callee == "NewDefaultHTTPClient" && enclosing != "NewDefaultHTTPClient" {
					sites[enclosing]++
				}
			}
			return true
		})
	}
	if literals == 0 {
		t.Fatal("the derived set of http.Client literals is empty: the scan found nothing to check")
	}
	if len(sites) == 0 {
		t.Fatal("the derived set of default-client sites is empty: the scan found nothing to check")
	}
	var derived, covered []string
	for name := range sites {
		derived = append(derived, name)
	}
	for name := range defaultClientSites {
		covered = append(covered, name)
	}
	sort.Strings(derived)
	sort.Strings(covered)
	if strings.Join(derived, ",") != strings.Join(covered, ",") {
		t.Errorf("default-client sites in the vendored source = %v, behaviour cases = %v", derived, covered)
	}
}

// TestVendoredDefaultClientsFollowNoRedirect runs every default-client site with no client supplied against a server that redirects
// to a second host: the first host must be reached, the second must see 0 requests.
func TestVendoredDefaultClientsFollowNoRedirect(t *testing.T) {
	names := make([]string, 0, len(defaultClientSites))
	for name := range defaultClientSites {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("no default-client site to run")
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			base, landed, firstHits := redirectProbe(t)
			defaultClientSites[name](t, base)
			if firstHits.Load() == 0 {
				t.Fatal("the redirecting host was never reached: the case exercised nothing")
			}
			if n := landed.Load(); n != 0 {
				t.Fatalf("the default client followed the redirect: the second host saw %d request(s)", n)
			}
		})
	}
}
