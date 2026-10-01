package operationalordering

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Every non-test file that names OPERATIONAL_ORDERING_CONTRACT as a string literal
// resolves it through this package (ResolveValue / Resolve): there is ONE parser
// and one default (unset means contract 2, any other value than 2 is refused), so
// two readers of the same variable can never disagree again (CHAOS-7421; the
// defect class of CHAOS-7371).
// declaresOnly lists the files that name the variable without reading it.
var declaresOnly = map[string]string{
	"internal/platform/config/queryapi_options.go": "declares the name in the query API's documented direct reads; the read is internal/queryapi/workgraph/displaynames.go, which resolves it here",
}

// notYetMigrated is the ONE reader still on its own parser, and it must stay one:
// the test fails on a second entry. The dho backfill verb keeps the Python parser
// (unset = contract 1) because its frozen Python goldens and live-oracle scenarios
// (backfill_integration_test.go) are recorded under it; contract 1 is unsupported
// only after the Python is deleted (D3635), so the exception dies with CHAOS-7308
// (its deletion list: remove the operationalbackfill legacy parser, this entry and
// the 5 legacy frozen scenarios). CHAOS-7421, D3703.
var notYetMigrated = map[string]string{
	"internal/operationalbackfill/write.go": "Python-parity parser (unset = 1) until CHAOS-7308 deletes the Python it mirrors",
}

func TestTheOrderingContractResolverHasExactlyOneNamedException(t *testing.T) {
	if len(notYetMigrated) != 1 {
		t.Fatalf("%d readers are exempt from the one resolver, want exactly 1 (the backfill verb, until CHAOS-7308): a new reader must use operationalordering.ResolveValue", len(notYetMigrated))
	}
	for file, reason := range notYetMigrated {
		if !strings.Contains(reason, "CHAOS-7308") {
			t.Fatalf("the exception for %s must name the ticket that removes it (CHAOS-7308): %q", file, reason)
		}
	}
}

func TestEveryReaderOfTheOrderingContractVariableUsesTheOneResolver(t *testing.T) {
	root := filepath.Join("..", "..")
	var offenders []string
	checked := 0
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		slash := filepath.ToSlash(path)
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") ||
			strings.Contains(slash, "/testsupport/") || strings.Contains(slash, "/.git/") ||
			strings.HasPrefix(slash, "../../internal/operationalordering/") || strings.Contains(slash, "/tests/") {
			return nil
		}
		file, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if perr != nil {
			return nil
		}
		named := false
		ast.Inspect(file, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if v, _ := strconv.Unquote(lit.Value); v == Env {
					named = true
				}
			}
			return true
		})
		if !named {
			return nil
		}
		checked++
		// The file must CALL the resolver (ResolveValue, Resolve or CheckForRead through
		// the package's import name), not merely import the package for something else.
		uses := false
		importName := ""
		for _, spec := range file.Imports {
			if imported, _ := strconv.Unquote(spec.Path.Value); imported == "github.com/full-chaos/dev-health-ops/internal/operationalordering" {
				importName = "operationalordering"
				if spec.Name != nil {
					importName = spec.Name.Name
				}
			}
		}
		if importName != "" {
			// A call whose result is thrown away as a bare statement governs nothing.
			discarded := map[ast.Node]bool{}
			ast.Inspect(file, func(n ast.Node) bool {
				if statement, ok := n.(*ast.ExprStmt); ok {
					discarded[statement.X] = true
				}
				return true
			})
			ast.Inspect(file, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok && !discarded[call] {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						if owner, ok := sel.X.(*ast.Ident); ok && owner.Name == importName &&
							(sel.Sel.Name == "ResolveValue" || sel.Sel.Name == "Resolve" || sel.Sel.Name == "CheckForRead") {
							uses = true
						}
					}
				}
				return true
			})
		}
		rel := strings.TrimPrefix(slash, "../../")
		if _, exception := notYetMigrated[rel]; exception {
			// The exception covers ONE parser: a second literal of the variable in the same
			// file would be a second parser the file-level exception would hide.
			literals := 0
			ast.Inspect(file, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if v, _ := strconv.Unquote(lit.Value); v == Env {
						literals++
					}
				}
				return true
			})
			if literals != 1 {
				offenders = append(offenders, rel+" (the exception file names the variable "+strconv.Itoa(literals)+" times, want exactly 1)")
			}
		}
		_, declares := declaresOnly[rel]
		_, unmigrated := notYetMigrated[rel]
		if !uses && !declares && !unmigrated {
			offenders = append(offenders, strings.TrimPrefix(slash, "../../"))
		}
		return nil
	})
	if checked < 5 {
		t.Fatalf("the scan found %d files naming %s: it measured nothing", checked, Env)
	}
	for _, file := range offenders {
		t.Errorf("%s names %s but does not use the operationalordering resolver: parse it with operationalordering.ResolveValue", file, Env)
	}
}
