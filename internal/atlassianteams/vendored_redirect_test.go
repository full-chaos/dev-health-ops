package atlassianteams

import (
	"context"
	"fmt"
	"go/ast"
	"go/types"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	return loadPackagesIn(t, "../..", pattern)
}

// loadPackagesIn is loadVendoredPackages for any module directory (the planted-form fixtures use it).
func loadPackagesIn(t *testing.T, dir, pattern string) []*packages.Package {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir:  dir,
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
	return scanPackages(t, "../..", pattern)
}

func scanPackages(t *testing.T, dir, pattern string) vendoredRedirectScan {
	t.Helper()
	scan := vendoredRedirectScan{sites: map[string]int{}}
	loaded := loadPackagesIn(t, dir, pattern)
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
					if n.Recv != nil {
						for _, field := range n.Recv.List {
							if holder.holdsValue(info.TypeOf(field.Type)) {
								report("a receiver that holds an http.Client value")
							}
						}
					}
				case *ast.FuncType:
					// every function declaration, literal and function TYPE: a parameter passes a client value in by value, a result (named or
					// not) hands one out; either way a value of the type exists that this scan did not see created.
					if n.Params != nil {
						for _, field := range n.Params.List {
							if holder.holdsValue(info.TypeOf(field.Type)) {
								report("a parameter that holds an http.Client value")
							}
						}
					}
					if n.Results != nil {
						for _, field := range n.Results.List {
							if holder.holdsValue(info.TypeOf(field.Type)) {
								report("a result that holds an http.Client value (named or not)")
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
					if instance, ok := info.Instances[n]; ok && instance.TypeArgs != nil {
						for i := 0; i < instance.TypeArgs.Len(); i++ {
							if holder.holdsValue(instance.TypeArgs.At(i)) {
								report("a generic instantiation whose type argument holds an http.Client value")
							}
						}
					}
					// Reflection builds or rewrites values of a type chosen at run time (reflect.New, Value.SetZero, ...): the scan cannot say
					// which, so ANY use of an object of package reflect (function, method, type) fails the scan. The vendored source imports none.
					if object != nil && object.Pkg() != nil && object.Pkg().Path() == "reflect" {
						report("reflect." + object.Name() + " (reflection is not covered by this scan)")
					}
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

// A SUPPLIED client with Timeout 0 is copied by three vendored functions (the copy gets the module's default timeout). The copy must keep
// the supplied client's own redirect policy: a caller that passed a guarded client must not get a following one back (CHAOS-7921
// follow-up; the default-client cases above never reach this code).
var suppliedClientCopySites = map[string]func(t *testing.T, base string, client *http.Client){
	"postOAuthToken": func(t *testing.T, base string, client *http.Client) {
		_, _ = atlassian.RefreshAccessToken(context.Background(), "id", "secret", "refresh", atlassian.OAuthTokenRequestOptions{TokenURL: base + "/token", HTTPClient: client})
	},
	"FetchAccessibleResources": func(t *testing.T, base string, client *http.Client) {
		_, _ = atlassian.FetchAccessibleResources(context.Background(), "token", atlassian.AccessibleResourcesOptions{URL: base + "/resources", HTTPClient: client})
	},
	"FetchSchemaIntrospection": func(t *testing.T, base string, client *http.Client) {
		_, _ = graph.FetchSchemaIntrospection(context.Background(), base, atlassian.BasicAPITokenAuth{Email: "e@example.test", Token: "t"}, graph.SchemaFetchOptions{OutputDir: t.TempDir(), HTTPClient: client})
	},
}

// TestEveryVendoredCopyOfASuppliedClientIsCovered derives, from the types of every vendored package, each function that copies an
// http.Client VALUE (a dereference of a *http.Client): those are the supplied-client copy sites, and they must equal the table above.
func TestEveryVendoredCopyOfASuppliedClientIsCovered(t *testing.T) {
	derived := map[string]bool{}
	for _, pkg := range loadVendoredPackages(t, "atlassian/...") {
		for _, file := range pkg.Syntax {
			var enclosing string
			ast.Inspect(file, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.FuncDecl:
					enclosing = n.Name.Name
				case *ast.StarExpr:
					if named, ok := types.Unalias(pkg.TypesInfo.TypeOf(n)).(*types.Named); ok && isNetHTTP(named.Obj(), "Client") {
						derived[enclosing] = true
					}
				}
				return true
			})
		}
	}
	if len(derived) == 0 {
		t.Fatal("the derived set of client-copy sites is empty: the scan found nothing to check")
	}
	var got, want []string
	for name := range derived {
		got = append(got, name)
	}
	for name := range suppliedClientCopySites {
		want = append(want, name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("client-copy sites in the vendored source = %v, behaviour cases = %v", got, want)
	}
}

// TestSuppliedGuardedClientKeepsItsRedirectPolicy runs every copy site with a supplied client that refuses redirects, once with Timeout 0
// (the client is copied) and once with a timeout (used as it is): a redirect to a second host is never followed.
func TestSuppliedGuardedClientKeepsItsRedirectPolicy(t *testing.T) {
	names := make([]string, 0, len(suppliedClientCopySites))
	for name := range suppliedClientCopySites {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, timeout := range []time.Duration{0, time.Second} {
			t.Run(fmt.Sprintf("%s/timeout=%s", name, timeout), func(t *testing.T) {
				base, landed, firstHits := redirectProbe(t)
				guarded := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
				suppliedClientCopySites[name](t, base, guarded)
				if firstHits.Load() == 0 {
					t.Fatal("the redirecting host was never reached: the case exercised nothing")
				}
				if n := landed.Load(); n != 0 {
					t.Fatalf("the supplied guarded client followed the redirect: the second host saw %d request(s)", n)
				}
			})
		}
	}
}

// The scan's planted-form fixtures (CHAOS-7997): each package of a scratch module holds ONE way to get a redirect-following client that the
// scan used to miss (or a clean control). Every defect package must be reported by its own name, the control must not.
func TestRedirectScanSeesTheUnusualWaysToBuildAClient(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"go.mod":             "module planted\n\ngo 1.22\n",
		"control/c.go":       "package control\n\nimport \"net/http\"\n\nfunc Use(c *http.Client) *http.Client { return c }\n",
		"genericzero/g.go":   "package genericzero\n\nimport \"net/http\"\n\nfunc zero[T any]() T { var v T; return v }\n\nfunc Build() *http.Client { client := zero[http.Client](); return &client }\n",
		"valueparam/v.go":    "package valueparam\n\nimport \"net/http\"\n\ntype sink func(http.Client)\n",
		"unnamedresult/u.go": "package unnamedresult\n\nimport \"net/http\"\n\ntype maker func() http.Client\n",
		"receiver/r.go":      "package receiver\n\nimport \"net/http\"\n\ntype mine http.Client\n\nfunc (m mine) Do() {}\n",
		"secondarg/s.go":     "package secondarg\n\nimport \"net/http\"\n\nfunc second[A, B any]() *B { return new(B) }\n\nfunc Build() *http.Client { c := second[int, http.Client](); return c }\n",
		"reflecttype/t.go":   "package reflecttype\n\nimport \"reflect\"\n\ntype holder struct{ v reflect.Value }\n",
		"reflectmethod/m.go": "package reflectmethod\n\nimport \"reflect\"\n\nfunc Reset(v reflect.Value) { v.SetZero() }\n",
		"reflectnew/r.go":    "package reflectnew\n\nimport \"reflect\"\n\nfunc Build(t reflect.Type) reflect.Value { return reflect.New(t) }\n",
	}
	for name, text := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	scan := scanPackages(t, dir, "./...")
	found := map[string]bool{}
	for _, creation := range scan.creations {
		found[strings.SplitN(creation, ".", 2)[0]] = true
	}
	for _, defect := range []string{"planted/genericzero", "planted/valueparam", "planted/unnamedresult", "planted/reflectnew", "planted/receiver", "planted/secondarg", "planted/reflectmethod", "planted/reflecttype"} {
		if !found[defect] {
			t.Errorf("the scan did not report %s (reported: %v)", defect, scan.creations)
		}
	}
	if found["planted/control"] {
		t.Errorf("the scan reported the clean control package: %v", scan.creations)
	}
}
