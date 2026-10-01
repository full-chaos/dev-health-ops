package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// knownDirectEnvironmentReads is every place under internal/queryapi that
// reads the process environment directly instead of through the lookup Run
// is given, keyed "<package>/<file>: <key or key expression>". Run's doc
// comment names them. The server package itself has none: its settings all
// come from lookup.
var knownDirectEnvironmentReads = []string{
	"heatmap/identity.go: IDENTITY_MAPPING_PATH",
	"investmentexplain/provider.go: LLM_MODEL",
	"investmentexplain/provider.go: envName",
	"investmentexplain/provider_org.go: LLM_MODEL",
	"investmentexplain/provider_org.go: envName",
	"people/identity.go: IDENTITY_MAPPING_PATH",
	"quadrant/identity.go: IDENTITY_MAPPING_PATH",
	"workgraph/displaynames.go: operationalOrderingContractEnv",
	"workgraph/issuepr.go: investmentMaterializeNativeEnabledEnv",
}

// TestDirectEnvironmentReads parses every non-test file under
// internal/queryapi and lists its os.Getenv/os.LookupEnv/os.Environ calls. A
// new one fails here, so a setting cannot silently bypass the injected lookup.
func TestDirectEnvironmentReads(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	files := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files++
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(root, path)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok || pkg.Name != "os" {
				return true
			}
			switch selector.Sel.Name {
			case "Getenv", "LookupEnv", "Environ":
			default:
				return true
			}
			key := selector.Sel.Name
			if len(call.Args) == 1 {
				switch argument := call.Args[0].(type) {
				case *ast.BasicLit:
					key, _ = strconv.Unquote(argument.Value)
				case *ast.Ident:
					key = argument.Name
				}
			}
			found = append(found, filepath.ToSlash(relative)+": "+key)
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 100 {
		t.Fatalf("walked %d files under %s; the query-api tree has several hundred -- the walk is broken", files, root)
	}
	sort.Strings(found)
	if !reflect.DeepEqual(found, knownDirectEnvironmentReads) {
		t.Fatalf("direct process-environment reads under internal/queryapi changed:\n  found %q\n  known %q\n"+
			"read a new setting through Run's lookup instead, or add it here and to Run's doc comment", found, knownDirectEnvironmentReads)
	}
}
