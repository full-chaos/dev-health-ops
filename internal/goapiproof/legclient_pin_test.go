package goapiproof

import (
	"fmt"
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// proverPackages are the packages whose reads must all go through the
// LegClient: both provers, this package, and the routing tool that reads
// the registry and /buildinfo through it.
var proverPackages = []string{
	"./cmd/go-api-rest-prove",
	"./cmd/go-api-prove",
	"./cmd/go-api-routing",
	"./internal/goapiproof",
}

// netHTTPSendNames are the net/http functions and methods that put a
// request on the wire.
var netHTTPSendNames = map[string]bool{
	"Get": true, "Post": true, "PostForm": true, "Head": true, "Do": true, "RoundTrip": true,
}

// httpSite is one reference to net/http that can send a request or build
// something that can: a send call, a Client/Transport value, or the
// package's default client or transport.
type httpSite struct {
	pos, enclosing, what string
}

// scanHTTPSites type-checks the prover packages (no _test.go files) and
// lists every net/http send call, every http.Client/http.Transport
// composite literal or new(), and every use of http.DefaultClient or
// http.DefaultTransport, each with its enclosing function. Resolved by the
// type checker, not by text: an alias, a dot-import or a method value
// still names the net/http object.
func scanHTTPSites(t *testing.T) []httpSite {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  filepath.Join("..", ".."),
	}
	pkgs, err := packages.Load(cfg, proverPackages...)
	if err != nil {
		t.Fatalf("load prover packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 || len(pkgs) != len(proverPackages) {
		t.Fatalf("loading %v reported errors or %d packages (see above)", proverPackages, len(pkgs))
	}
	isNetHTTPType := func(typ types.Type, names ...string) bool {
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		named, ok := typ.(*types.Named)
		if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "net/http" {
			return false
		}
		for _, name := range names {
			if named.Obj().Name() == name {
				return true
			}
		}
		return false
	}
	var sites []httpSite
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				enclosing := "<package scope>"
				if function, ok := decl.(*ast.FuncDecl); ok {
					enclosing = function.Name.Name
					if function.Recv != nil && len(function.Recv.List) == 1 {
						enclosing = "(" + types.ExprString(function.Recv.List[0].Type) + ")." + enclosing
					}
				}
				ast.Inspect(decl, func(node ast.Node) bool {
					pos := func(n ast.Node) string {
						p := pkg.Fset.Position(n.Pos())
						return fmt.Sprintf("%s/%s:%d", pkg.PkgPath[strings.LastIndex(pkg.PkgPath, "/")+1:], filepath.Base(p.Filename), p.Line)
					}
					switch n := node.(type) {
					case *ast.SelectorExpr:
						object := pkg.TypesInfo.Uses[n.Sel]
						if object == nil || object.Pkg() == nil || object.Pkg().Path() != "net/http" {
							break
						}
						if _, isVar := object.(*types.Var); isVar && (object.Name() == "DefaultClient" || object.Name() == "DefaultTransport") {
							sites = append(sites, httpSite{pos(n), enclosing, "http." + object.Name()})
						}
						function, isFunc := object.(*types.Func)
						if !isFunc || !netHTTPSendNames[function.Name()] {
							break
						}
						if receiver := function.Type().(*types.Signature).Recv(); receiver != nil && !isNetHTTPType(receiver.Type(), "Client", "Transport", "RoundTripper") {
							break
						}
						sites = append(sites, httpSite{pos(n), enclosing, "send " + types.ObjectString(function, nil)})
					case *ast.CompositeLit:
						if typ := pkg.TypesInfo.TypeOf(n); typ != nil && isNetHTTPType(typ, "Client", "Transport") {
							sites = append(sites, httpSite{pos(n), enclosing, "build " + typ.String()})
						}
					case *ast.CallExpr:
						if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == "new" && len(n.Args) == 1 {
							if typ := pkg.TypesInfo.TypeOf(n.Args[0]); typ != nil && isNetHTTPType(typ, "Client", "Transport") {
								sites = append(sites, httpSite{pos(n), enclosing, "build new " + typ.String()})
							}
						}
					}
					return true
				})
			}
		}
	}
	return sites
}

// TestProverHTTPSendsGoOnlyThroughTheLegClient pins the one-client rule
// at the compiler's level: in the prover packages, a request reaches the
// wire only through (*LegClient).Do, and a client or transport is built
// only by NewLegClient. Any other send, client, transport, or use of the
// default client fails here by position.
func TestProverHTTPSendsGoOnlyThroughTheLegClient(t *testing.T) {
	sites := scanHTTPSites(t)
	allowed := map[string]map[string]bool{
		"(*LegClient).Do": {"send func (*net/http.Client).Do(req *net/http.Request) (*net/http.Response, error)": true},
		"NewLegClient": {
			"http.DefaultTransport":    true,
			"build net/http.Transport": true,
			"build net/http.Client":    true,
		},
	}
	var outside []string
	found := map[string]int{}
	for _, site := range sites {
		if allowed[site.enclosing][site.what] {
			found[site.enclosing+" "+site.what]++
			continue
		}
		outside = append(outside, fmt.Sprintf("%s in %s: %s", site.pos, site.enclosing, site.what))
	}
	sort.Strings(outside)
	if len(outside) > 0 {
		t.Fatalf("net/http sends or clients outside the LegClient:\n  %s", strings.Join(outside, "\n  "))
	}
	for enclosing, whats := range allowed {
		for what := range whats {
			if found[enclosing+" "+what] == 0 {
				t.Errorf("the scan found no %q in %s: the scanner does not see the one allowed site, so it would pass while blind", what, enclosing)
			}
		}
	}
}
