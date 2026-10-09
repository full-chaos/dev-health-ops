package clickhouse

import (
	"fmt"
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	driverPackage      = "github.com/ClickHouse/clickhouse-go/v2"
	driverTypesPackage = driverPackage + "/lib/driver"
	namedValueType     = "github.com/ClickHouse/clickhouse-go/v2/lib/driver.NamedValue"
	queryClientPackage = "github.com/full-chaos/dev-health-go/clickhouse"
	queryClientBinding = queryClientPackage + ".Binding"
)

// The driver writes each Go type of a server-side {name:Type} parameter in
// its own way, and a driver release can change that way for one type only
// (v2.48.0 changed time.Time, bool, float, map and nil). These are the types
// param_encoding_integration_test.go proves on a real server; a parameter of
// any other type is a new encoding nobody has executed.
var (
	provenNamedTypes = map[string]bool{
		"string": true, "[]string": true, "[]github.com/google/uuid.UUID": true,
		"[]uint32": true, "uint32": true, "uint64": true,
	}
	// The dev-health-go query client encodes these itself and refuses others.
	provenBindingTypes = map[string]bool{
		"string": true, "[]string": true, "int": true, "int64": true,
		"uint32": true, "uint64": true, "time.Time": true,
	}
	// Helpers that bind each value of a map[string]any their callers build.
	// The census does not type those values: a caller can hand them any type,
	// and the same packages build log and JSON maps of the same type.
	anyValueHelpers = map[string]bool{
		"internal/jobs/metrics/remaining.namedArguments":        true,
		"internal/syncdispatchruntime.namedClickHouseArguments": true,
		"internal/queryapi/aianalytics.loadWorkflowEdges":       true,
	}
)

type parameterSite struct {
	kind, typ, where string
}

func TestEveryServerSideParameterHasAProvenType(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	// Type-check only the packages that import a parameter API: a whole-repo
	// typed load does not fit the memory cap of a test run.
	list, err := packages.Load(&packages.Config{Mode: packages.NeedName | packages.NeedImports, Dir: root}, "./internal/...", "./cmd/...")
	if err != nil {
		t.Fatal(err)
	}
	var importers []string
	for _, p := range list {
		if p.Imports[driverPackage] != nil || p.Imports[driverTypesPackage] != nil || p.Imports[queryClientPackage] != nil {
			importers = append(importers, p.PkgPath)
		}
	}
	if len(importers) == 0 {
		t.Fatal("no package imports the driver, its driver types or the query client: the scan did not run over production code")
	}
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedModule | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  root,
	}
	pkgs, err := packages.Load(cfg, importers...)
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("packages.Load reported errors (see above)")
	}
	sites, refusals := censusParameterSites(root, pkgs)
	counts := map[string]int{}
	for _, site := range sites {
		counts[site.kind+" "+site.typ]++
	}
	// A census that found nothing measured nothing.
	for _, must := range []string{"Named []string", "Named string", "Binding []string", "Binding string"} {
		if counts[must] == 0 {
			t.Errorf("census found no %s parameter: the scan did not run over production code", must)
		}
	}
	for _, refusal := range refusals {
		t.Error(refusal)
	}
}

func censusParameterSites(root string, pkgs []*packages.Package) (sites []parameterSite, refusals []string) {
	matchedHelpers := map[string]bool{}
	module := ""
	for _, p := range pkgs {
		if p.Module != nil {
			module = p.Module.Path
			break
		}
	}
	for _, p := range pkgs {
		for _, file := range p.Syntax {
			name := p.Fset.Position(file.Pos()).Filename
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			rel, _ := filepath.Rel(root, name)
			for _, decl := range file.Decls {
				fn, _ := decl.(*ast.FuncDecl)
				enclosing := ""
				if fn != nil {
					enclosing = strings.TrimPrefix(strings.TrimPrefix(p.PkgPath, module), "/") + "." + fn.Name.Name
				}
				ast.Inspect(decl, func(n ast.Node) bool {
					record := func(kind string, value ast.Expr, proven map[string]bool) {
						where := fmt.Sprintf("%s:%d", rel, p.Fset.Position(value.Pos()).Line)
						typ := "<untyped>"
						if tv := p.TypesInfo.TypeOf(value); tv != nil {
							typ = types.TypeString(tv, nil)
						}
						sites = append(sites, parameterSite{kind: kind, typ: typ, where: where})
						if proven[typ] {
							return
						}
						if typ == "any" && anyValueHelpers[enclosing] {
							matchedHelpers[enclosing] = true
							return
						}
						refusals = append(refusals, fmt.Sprintf("%s: a %s parameter of type %s has no executed encoding proof; add the type to param_encoding_integration_test.go and to this census, or bind a proven type", where, kind, typ))
					}
					switch x := n.(type) {
					case *ast.SelectorExpr:
						obj := p.TypesInfo.Uses[x.Sel]
						if obj == nil || obj.Pkg() == nil || obj.Pkg().Path() != driverPackage {
							return true
						}
						if obj.Name() == "WithParameters" || obj.Name() == "Parameters" {
							refusals = append(refusals, fmt.Sprintf("%s:%d: clickhouse.%s writes parameter text past the driver's encoder; bind values with clickhouse.Named", rel, p.Fset.Position(x.Pos()).Line, obj.Name()))
						}
					case *ast.CallExpr:
						sel, ok := x.Fun.(*ast.SelectorExpr)
						if !ok {
							return true
						}
						obj := p.TypesInfo.Uses[sel.Sel]
						if obj == nil || obj.Pkg() == nil || obj.Pkg().Path() != driverPackage || len(x.Args) < 2 {
							return true
						}
						switch obj.Name() {
						case "Named":
							record("Named", x.Args[1], provenNamedTypes)
						case "DateNamed":
							record("DateNamed", x.Args[1], nil)
						}
					case *ast.CompositeLit:
						tv := p.TypesInfo.TypeOf(x)
						if tv == nil {
							return true
						}
						kind, proven := "", map[string]bool(nil)
						switch types.TypeString(tv, nil) {
						case namedValueType:
							kind, proven = "NamedValue", provenNamedTypes
						case queryClientBinding:
							kind, proven = "Binding", provenBindingTypes
						default:
							return true
						}
						for _, el := range x.Elts {
							if kv, ok := el.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Value" {
									record(kind, kv.Value, proven)
								}
							}
						}
					}
					return true
				})
			}
		}
	}
	for helper := range anyValueHelpers {
		if !matchedHelpers[helper] {
			refusals = append(refusals, fmt.Sprintf("%s binds no any-typed parameter any more: remove it from anyValueHelpers", helper))
		}
	}
	sort.Strings(refusals)
	return sites, refusals
}
