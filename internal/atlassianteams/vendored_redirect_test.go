package atlassianteams

import (
	"context"
	"go/ast"
	"go/types"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"
	"atlassian/atlassian/rest"
	"golang.org/x/tools/go/packages"
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

// loadVendoredPackages type-checks EVERY package of the vendored atlassian module (pattern atlassian/..., any depth, production files
// only). Nothing loaded, or any load or type error, fails the test: a scan that cannot see the code must not pass.
func loadVendoredPackages(t *testing.T, pattern string) []*packages.Package {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir:  "../..",
	}, pattern)
	if err != nil {
		t.Fatalf("loading %s: %v", pattern, err)
	}
	if len(loaded) == 0 {
		t.Fatalf("loading %s found no package: the scan has nothing to check", pattern)
	}
	failed := false
	for _, pkg := range loaded {
		for _, loadErr := range pkg.Errors {
			t.Errorf("%s: %v", pkg.PkgPath, loadErr)
			failed = true
		}
	}
	if failed {
		t.FailNow()
	}
	return loaded
}

func isNetHTTP(object types.Object, name string) bool {
	return object != nil && object.Pkg() != nil && object.Pkg().Path() == "net/http" && object.Name() == name
}

// clientTypes answers, from the types alone, whether a type holds a net/http.Client VALUE (so creating a value of it creates a client
// that follows redirects): net/http.Client itself, any alias of it, a defined type whose underlying type is net/http.Client's, a struct
// with such a field (embedded or not), or an array, slice, map or channel of such values. A pointer does not: it holds no client.
type clientTypes struct {
	clientUnderlying types.Type
	visiting         map[types.Type]bool
}

func (c *clientTypes) holdsValue(t types.Type) bool {
	t = types.Unalias(t)
	if t == nil {
		return false
	}
	if c.visiting[t] {
		return false
	}
	c.visiting[t] = true
	defer delete(c.visiting, t)
	if named, ok := t.(*types.Named); ok {
		if isNetHTTP(named.Obj(), "Client") {
			return true
		}
		if c.clientUnderlying != nil && types.Identical(named.Underlying(), c.clientUnderlying) {
			return true // a defined type over http.Client
		}
		return c.holdsValue(named.Underlying())
	}
	switch u := t.(type) {
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			if c.holdsValue(u.Field(i).Type()) {
				return true
			}
		}
	case *types.Array:
		return c.holdsValue(u.Elem())
	case *types.Slice:
		return c.holdsValue(u.Elem())
	case *types.Map:
		return c.holdsValue(u.Key()) || c.holdsValue(u.Elem())
	case *types.Chan:
		return c.holdsValue(u.Elem())
	}
	return false
}

// vendoredRedirectScan is what the types say about every package: the places a value that holds an http.Client is CREATED (a composite
// literal, new() or make(), a var, a struct field or a named result of such a type) or the redirect-following package-level client is
// used (http.DefaultClient, http.Get/Post/Head/PostForm), each with the function that holds it; and the calls to the default-client
// helper, by enclosing function. It goes through types.Info, so an import alias, a dot import, a type alias, a defined type or the
// depth of the file does not matter.
type vendoredRedirectScan struct {
	creations []string // "<pkg>.<func>: <what>" for every creation outside the helper
	helperLit int      // composite literals of http.Client inside NewDefaultHTTPClient that set CheckRedirect
	sites     map[string]int
	packages  int
}

func scanVendored(t *testing.T, pattern string) vendoredRedirectScan {
	t.Helper()
	scan := vendoredRedirectScan{sites: map[string]int{}}
	loaded := loadVendoredPackages(t, pattern)
	holder := &clientTypes{visiting: map[types.Type]bool{}}
	packages.Visit(loaded, nil, func(pkg *packages.Package) {
		if pkg.PkgPath == "net/http" && pkg.Types != nil {
			if object := pkg.Types.Scope().Lookup("Client"); object != nil {
				holder.clientUnderlying = object.Type().Underlying()
			}
		}
	})
	if holder.clientUnderlying == nil {
		t.Fatal("net/http.Client was not found among the loaded packages: the scan cannot recognise a client")
	}
	for _, pkg := range loaded {
		scan.packages++
		info := pkg.TypesInfo
		for _, file := range pkg.Syntax {
			var enclosing string
			report := func(what string) {
				scan.creations = append(scan.creations, pkg.PkgPath+"."+enclosing+": "+what)
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.FuncDecl:
					enclosing = n.Name.Name
					if n.Type.Results != nil {
						for _, result := range n.Type.Results.List {
							for _, name := range result.Names {
								if object := info.Defs[name]; object != nil && holder.holdsValue(object.Type()) {
									report("a named result that holds an http.Client value")
								}
							}
						}
					}
				case *ast.CompositeLit:
					if holder.holdsValue(info.TypeOf(n)) {
						hasCheck := false
						for _, element := range n.Elts {
							if kv, ok := element.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "CheckRedirect" {
									hasCheck = true
								}
							}
						}
						if enclosing == "NewDefaultHTTPClient" && hasCheck {
							scan.helperLit++
						} else {
							report("a composite literal that holds an http.Client value (the helper's own literal must set CheckRedirect)")
						}
					}
				case *ast.CallExpr:
					if ident, ok := n.Fun.(*ast.Ident); ok {
						if builtin, ok := info.Uses[ident].(*types.Builtin); ok && len(n.Args) >= 1 {
							switch builtin.Name() {
							case "new":
								if holder.holdsValue(info.TypeOf(n.Args[0])) {
									report("new() of a type that holds an http.Client value")
								}
							case "make":
								if holder.holdsValue(info.TypeOf(n)) {
									report("make() of a type that holds http.Client values")
								}
							}
						}
					}
					var callee types.Object
					switch fun := n.Fun.(type) {
					case *ast.Ident:
						callee = info.Uses[fun]
					case *ast.SelectorExpr:
						callee = info.Uses[fun.Sel]
					}
					if callee != nil && callee.Name() == "NewDefaultHTTPClient" && enclosing != "NewDefaultHTTPClient" {
						scan.sites[enclosing]++
					}
				case *ast.ValueSpec:
					for _, name := range n.Names {
						if object := info.Defs[name]; object != nil && holder.holdsValue(object.Type()) {
							report("a var that holds an http.Client value (a zero value)")
						}
					}
				case *ast.StructType:
					for _, field := range n.Fields.List {
						if holder.holdsValue(info.TypeOf(field.Type)) {
							report("a struct field that holds an http.Client value")
						}
					}
				case *ast.Ident:
					object := info.Uses[n]
					for _, member := range []string{"DefaultClient", "Get", "Post", "Head", "PostForm"} {
						if isNetHTTP(object, member) {
							if function, isFunc := object.(*types.Func); isFunc && function.Type().(*types.Signature).Recv() != nil {
								continue // (*http.Client).Get: a method of a client that already exists
							}
							report("net/http." + member + " (the redirect-following default client)")
						}
					}
				}
				return true
			})
		}
	}
	return scan
}

// TestEveryVendoredHTTPClientRefusesRedirects derives, from the TYPES of every vendored package, each place an http.Client value is
// created or the redirect-following default client is used, and requires none but the helper's own literal (which sets CheckRedirect);
// and the default-client sites (by enclosing function) must equal the behaviour table below. An empty derived set, a load error or a
// type error fails.
func TestEveryVendoredHTTPClientRefusesRedirects(t *testing.T) {
	scan := scanVendored(t, "atlassian/...")
	if scan.packages == 0 || scan.helperLit != 1 {
		t.Fatalf("scanned %d package(s) and found %d helper literal(s): the scan did not see the default-client constructor", scan.packages, scan.helperLit)
	}
	if len(scan.sites) == 0 {
		t.Fatal("the derived set of default-client sites is empty: the scan found nothing to check")
	}
	for _, creation := range scan.creations {
		t.Errorf("%s: build every client with atlassian.NewDefaultHTTPClient (no redirect followed)", creation)
	}
	var derived, covered []string
	for name := range scan.sites {
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
