package investment

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The census of every reader of the LLM provider selection (CHAOS-8874).
//
// LLM_PROVIDER=typesafe selects the decision backend, which writes no text. One
// value now reaches every reader of the provider selection, so each reader is
// listed here with what it does with the decision kind, and a new reader fails
// this test until it is listed on purpose:
//
//   - the raw resolution (categorize.ResolveProviderKind / ResolveProviderKindForOrg)
//     gives the decision kind. Its only reader outside package categorize is the
//     investment worker, which serves from the decision adapter on purpose.
//   - every text reader (the two explanation routes, through investmentexplain)
//     uses categorize.ResolveTextProviderKind / ResolveTextProviderKindForOrg,
//     which never gives the decision kind for the platform default.
//   - the literal "LLM_PROVIDER" is read in one place (providerkind.go); the
//     explain route names it only in an error hint.
//   - the TypeSafe client is built only by the investment worker.
//
// Non-Go files: no Python source, script or CI file reads LLM_PROVIDER; the
// deploy and compose files only pass it to the processes (the root compose
// file and the bigboy overlay to query-api; a values.yaml comment; the bigboy
// cut script's comment). The worker groups get it from ops/.env or the
// platform Secret, outside this repository.
func TestEveryReaderOfTheProviderSelectionIsKnown(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	// "<package qualifier or ->.<name>" -> file -> number of call sites.
	want := map[string]map[string]int{
		"categorize.ResolveProviderKind": {
			"internal/jobs/investment/nativeexecutor.go": 1,
		},
		"-.ResolveProviderKind": {}, // only its definition, in categorize
		"-.ResolveProviderKindForOrg": {
			"internal/jobs/investment/categorize/providerkind.go": 2, // ResolveProviderKind and ResolveTextProviderKindForOrg
		},
		"-.ResolveTextProviderKindForOrg": {
			"internal/jobs/investment/categorize/providerkind.go": 1, // ResolveTextProviderKind
		},
		"categorize.ResolveTextProviderKind": {
			"internal/queryapi/investmentexplain/provider.go": 3,
		},
		"categorize.ResolveTextProviderKindForOrg": {
			"internal/queryapi/investmentexplain/provider_org.go": 5,
		},
		"investmentexplain.ResolveProviderKindForOrg": {
			"internal/queryapi/server/workunit_explain_route.go": 1,
		},
		"categorize.NewTypeSafeClientFromEnvWithHTTPClient": {
			"internal/jobs/investment/nativeexecutor.go": 2, // the served backend and the shadow phase
		},
		"categorize.NewTypeSafeClientFromEnv": {},
		"categorize.NewTypeSafeClient":        {},
		`literal "LLM_PROVIDER"`: {
			"internal/jobs/investment/categorize/providerkind.go": 1,
			"internal/queryapi/server/workunit_explain_route.go":  1,
		},
	}
	got := map[string]map[string]int{}
	for name := range want {
		got[name] = map[string]int{}
	}
	files := 0
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return walkErr
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			files++
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			ast.Inspect(parsed, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.CallExpr:
					key := ""
					switch fn := n.Fun.(type) {
					case *ast.SelectorExpr:
						if pkg, ok := fn.X.(*ast.Ident); ok {
							key = pkg.Name + "." + fn.Sel.Name
						}
					case *ast.Ident:
						key = "-." + fn.Name
					}
					if _, tracked := got[key]; tracked {
						got[key][rel]++
					}
				case *ast.BasicLit:
					if n.Kind == token.STRING {
						if value, err := strconv.Unquote(n.Value); err == nil && strings.Contains(value, "LLM_PROVIDER") && !strings.Contains(value, " ") {
							got[`literal "LLM_PROVIDER"`][rel]++
						}
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if files < 1000 {
		t.Fatalf("the walk read %d non-test Go files: it did not cover the module", files)
	}
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !reflect.DeepEqual(got[name], want[name]) {
			t.Errorf("%s is called at %v, the census lists %v: a reader of the provider selection changed; list it with what it does with the decision kind", name, got[name], want[name])
		}
	}

	// Non-Go sources that could read the variable at run time.
	readers := map[string]bool{}
	for _, top := range []string{"src", "scripts", "ci", "deploy"} {
		_ = filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				return nil
			}
			switch filepath.Ext(path) {
			case ".py", ".sh", ".yml", ".yaml", ".toml":
			default:
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil || !strings.Contains(string(data), "LLM_PROVIDER") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			readers[filepath.ToSlash(rel)] = true
			return nil
		})
	}
	if data, err := os.ReadFile(filepath.Join(root, "compose.yml")); err != nil {
		t.Fatal(err)
	} else if strings.Contains(string(data), "LLM_PROVIDER") {
		readers["compose.yml"] = true
	}
	wantReaders := map[string]bool{
		"ci/bigboy/bigboy-cut.sh":             true, // a comment that names the Secret keys
		"ci/bigboy/compose.bigboy.images.yml": true, // passes LLM_PROVIDER to query-api
		"deploy/helm/dev-health/values.yaml":  true, // a comment on query-api envFrom
		"compose.yml":                         true, // passes LLM_PROVIDER to query-api
	}
	if !reflect.DeepEqual(readers, wantReaders) {
		t.Errorf("non-Go files that name LLM_PROVIDER: %v, the census lists %v", readers, wantReaders)
	}
}
