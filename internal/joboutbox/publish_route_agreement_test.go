package joboutbox

import (
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"golang.org/x/tools/go/packages"
)

// producerPackagePath is the package that owns the four publish seams. Call
// sites inside it are the implementation of those seams (PublishStandalone
// calls Publish), not consumers choosing a route, so they are excluded.
const producerPackagePath = "github.com/full-chaos/dev-health-ops/internal/joboutbox"

// publishKindArgument maps each producer publish method to the index of its
// job-kind argument and to the route the call selects.
var publishKindArgument = map[string]struct {
	kindArg  int
	deferred bool
}{
	"Publish":              {kindArg: 2, deferred: false},
	"PublishAfter":         {kindArg: 2, deferred: false},
	"PublishDeferred":      {kindArg: 2, deferred: true},
	"PublishDeferredAfter": {kindArg: 2, deferred: true},
	// PublishStandalone owns its transaction and delegates to Publish, so it
	// selects the executable route with the kind in its first argument.
	"PublishStandalone": {kindArg: 1, deferred: false},
}

type publishSite struct {
	pkg      string
	function string
	position string
	method   string
	deferred bool
	// kind is the published job kind when the argument is a compile-time
	// constant, and "" when it is a variable the caller resolves at run time.
	kind string
}

// TestEveryOutboxPublishSiteAgreesWithTheCheckedInRoute is the class-level
// guard for CHAOS-3946.
//
// descriptorAllowsPublish permits exactly one of the two routes for any given
// descriptor: the deferred route only while a kind is still pinned to Celery
// on both its route and its rollback route, and the executable route only once
// the kind is River-capable. A call site that hard-codes the wrong one can
// therefore never publish -- and because native post-sync fanout stages every
// handoff in ONE transaction, a single permanently-rejected publish discards
// the whole generation.
//
// teamAutoimportPostSyncWriter.PublishTx drifted exactly this way: it called
// PublishDeferred unconditionally for sync.team_autoimport long after the kind
// was cut over to route=river, and nothing in the suite compared the set of
// call sites against the checked-in contract. So this test enumerates EVERY
// producer publish call in the module -- resolved through go/types, not a
// hand-maintained list -- and holds each enclosing function to one rule:
//
//   - a function whose published kind is a compile-time constant must use the
//     route the checked-in descriptor actually permits for that kind (it may
//     also carry the other branch, which is how a rollback stays possible);
//   - a function that publishes a kind chosen at run time cannot be checked
//     per kind, so it must carry BOTH routes and pick between them from the
//     descriptor, exactly as the daily, remaining and workgraph publishers do.
//
// The rule is evaluated with the production predicate (descriptorAllowsPublish)
// against the production contract (contracts/jobs/v1), so it cannot drift from
// what the producer will actually accept at run time.
func TestEveryOutboxPublishSiteAgreesWithTheCheckedInRoute(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := jobruntime.Load(filepath.Join(root, "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatalf("load checked-in contracts: %v", err)
	}
	sites := outboxPublishSites(t, root)
	if len(sites) == 0 {
		t.Fatal("no joboutbox.Producer publish call sites were found: the walk is broken, " +
			"and a guard that inspects nothing would pass vacuously")
	}

	type group struct {
		modes map[bool][]publishSite
		kinds map[string][]publishSite
		// unresolved holds sites whose kind is decided at run time.
		unresolved []publishSite
	}
	groups := map[string]*group{}
	order := []string{}
	for _, site := range sites {
		key := site.pkg + "." + site.function
		current, ok := groups[key]
		if !ok {
			current = &group{modes: map[bool][]publishSite{}, kinds: map[string][]publishSite{}}
			groups[key] = current
			order = append(order, key)
		}
		current.modes[site.deferred] = append(current.modes[site.deferred], site)
		if site.kind == "" {
			current.unresolved = append(current.unresolved, site)
			continue
		}
		current.kinds[site.kind] = append(current.kinds[site.kind], site)
	}
	sort.Strings(order)

	for _, key := range order {
		current := groups[key]
		bothRoutes := len(current.modes) == 2
		for _, kind := range sortedKeys(current.kinds) {
			descriptor, ok := registry.Descriptor(kind)
			if !ok {
				t.Errorf("%s publishes kind %q, which contracts/jobs/v1 does not register (%s)",
					key, kind, current.kinds[kind][0].position)
				continue
			}
			permitted, ok := permittedRoute(descriptor)
			if !ok {
				t.Errorf("kind %q permits neither route under descriptorAllowsPublish "+
					"(state=%s route=%s rollback=%s): no call site can ever publish it",
					kind, descriptor.MigrationState, descriptor.Route, descriptor.RollbackRoute)
				continue
			}
			if _, used := current.modes[permitted]; !used {
				t.Errorf(
					"%s publishes %q only via the %s route, but the checked-in contract "+
						"(state=%s route=%s rollback=%s) permits only the %s route, so every "+
						"publish from this call site is rejected with "+
						"publish_not_permitted_for_route.\n  sites: %s",
					key, kind, routeName(!permitted), descriptor.MigrationState, descriptor.Route,
					descriptor.RollbackRoute, routeName(permitted), positions(current.kinds[kind]),
				)
			}
		}
		if len(current.unresolved) > 0 && !bothRoutes {
			t.Errorf(
				"%s publishes a kind that is only known at run time, but carries only the %s "+
					"route. Such a call site cannot be checked per kind, so it must branch on "+
					"descriptor.Executable() and carry both routes.\n  sites: %s",
				key, routeName(current.unresolved[0].deferred), positions(current.unresolved),
			)
		}
	}
}

// permittedRoute returns the single route descriptorAllowsPublish accepts for
// descriptor. It reports false when the descriptor permits both or neither,
// which the producer's own predicate makes impossible today but which a future
// edit to that predicate would surface here rather than in production.
func permittedRoute(descriptor jobruntime.Descriptor) (bool, bool) {
	executable := descriptorAllowsPublish(descriptor, false)
	deferred := descriptorAllowsPublish(descriptor, true)
	if executable == deferred {
		return false, false
	}
	return deferred, true
}

func routeName(deferred bool) string {
	if deferred {
		return "deferred"
	}
	return "executable"
}

func positions(sites []publishSite) string {
	rendered := make([]string, 0, len(sites))
	for _, site := range sites {
		rendered = append(rendered, site.position+" ("+site.method+")")
	}
	sort.Strings(rendered)
	return strings.Join(rendered, ", ")
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// outboxPublishSites type-checks the module and returns every call to a
// *joboutbox.Producer publish method outside the joboutbox package itself.
// Resolution goes through go/types, so a call reached through any receiver
// name, embedded field or alias is still found -- a textual grep for
// "producer.Publish" would not have been a class-level guard.
func outboxPublishSites(t *testing.T, root string) []publishSite {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports |
			packages.NeedDeps,
		Dir:   root,
		Tests: false,
	}, "./...")
	if err != nil {
		t.Fatalf("load module packages: %v", err)
	}
	var loadErrors []string
	packages.Visit(loaded, nil, func(pkg *packages.Package) {
		for _, packageErr := range pkg.Errors {
			loadErrors = append(loadErrors, pkg.PkgPath+": "+packageErr.Error())
		}
	})
	if len(loadErrors) > 0 {
		t.Fatalf("module did not type-check, so the walk would be incomplete:\n%s",
			strings.Join(loadErrors, "\n"))
	}

	var sites []publishSite
	for _, pkg := range loaded {
		if pkg.PkgPath == producerPackagePath || pkg.TypesInfo == nil {
			continue
		}
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok || function.Body == nil {
					continue
				}
				name := functionName(function)
				ast.Inspect(function.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					shape, ok := publishKindArgument[selector.Sel.Name]
					if !ok || !isProducerMethod(pkg.TypesInfo, selector) {
						return true
					}
					sites = append(sites, publishSite{
						pkg:      pkg.PkgPath,
						function: name,
						position: relativePosition(root, pkg, call.Pos()),
						method:   selector.Sel.Name,
						deferred: shape.deferred,
						kind:     constantKind(pkg.TypesInfo, call, shape.kindArg),
					})
					return true
				})
			}
		}
	}
	return sites
}

func isProducerMethod(info *types.Info, selector *ast.SelectorExpr) bool {
	function, ok := info.Uses[selector.Sel].(*types.Func)
	if !ok {
		return false
	}
	receiver := function.Signature().Recv()
	if receiver == nil {
		return false
	}
	receiverType := receiver.Type()
	if pointer, ok := receiverType.(*types.Pointer); ok {
		receiverType = pointer.Elem()
	}
	named, ok := receiverType.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == producerPackagePath && named.Obj().Name() == "Producer"
}

func constantKind(info *types.Info, call *ast.CallExpr, index int) string {
	if index >= len(call.Args) {
		return ""
	}
	value := info.Types[call.Args[index]].Value
	if value == nil || value.Kind() != constant.String {
		return ""
	}
	return constant.StringVal(value)
}

func functionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	return receiverTypeName(function.Recv.List[0].Type) + "." + function.Name.Name
}

func receiverTypeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(typed.X)
	case *ast.IndexExpr:
		return receiverTypeName(typed.X)
	case *ast.IndexListExpr:
		return receiverTypeName(typed.X)
	case *ast.Ident:
		return typed.Name
	default:
		return "?"
	}
}

func relativePosition(root string, pkg *packages.Package, pos token.Pos) string {
	position := pkg.Fset.Position(pos)
	if relative, err := filepath.Rel(root, position.Filename); err == nil {
		position.Filename = relative
	}
	return position.String()
}
