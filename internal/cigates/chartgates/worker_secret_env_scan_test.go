package chartgates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/dsnclass"
)

// A name is read by a Go source when a non-test file holds it as a string constant or literal in code (a comment is not
// code) and the same file calls an environment lookup: os.Getenv, os.LookupEnv, the secrets Getenv* family,
// ResolveSecret, or a lookup function passed in. A name that appears only in a comment, or in a file that
// never looks anything up, is not read.
var envLookupCallees = map[string]bool{
	"Getenv": true, "LookupEnv": true, "GetenvSecret": true, "GetenvNamed": true, "GetenvLogin": true, "GetenvDSN": true,
	"ResolveSecret": true, "lookup": true,
}

func scanEnvReaders(sources map[string]string) map[string][]string {
	readers := map[string][]string{}
	fset := token.NewFileSet()
	for path, src := range sources {
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			continue
		}
		lookups := false
		names := map[string]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				switch fun := typed.Fun.(type) {
				case *ast.Ident:
					lookups = lookups || envLookupCallees[fun.Name]
				case *ast.SelectorExpr:
					lookups = lookups || envLookupCallees[fun.Sel.Name]
				}
			case *ast.BasicLit:
				if typed.Kind == token.STRING {
					if value, err := strconv.Unquote(typed.Value); err == nil {
						names[value] = true
					}
				}
			}
			return true
		})
		if !lookups {
			continue
		}
		for name := range names {
			readers[name] = append(readers[name], path)
		}
	}
	for name := range readers {
		sort.Strings(readers[name])
	}
	return readers
}

// realEnvReaders scans the non-test Go sources of the tree under g.root.
func realEnvReaders(g *guard) map[string][]string {
	sources := map[string]string{}
	for _, dir := range []string{"internal", "cmd"} {
		_ = filepath.WalkDir(g.path(dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if entry.IsDir() && entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if data, rerr := os.ReadFile(path); rerr == nil {
				rel, _ := filepath.Rel(g.root, path)
				sources[filepath.ToSlash(rel)] = string(data)
			}
			return nil
		})
	}
	g.expect(len(sources) > 0, "no Go source was found under %s, so the reader scan measured nothing", g.root)
	return scanEnvReaders(sources)
}

func TestWorkerSecretKeyReaderScan(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want bool
	}{
		{"E01-name-only-in-a-comment", "package x\nimport \"os\"\n// reads \"SOME_KEY\"\nfunc f() string { return os.Getenv(\"OTHER\") }\n", false},
		{"E02-name-in-a-file-with-no-lookup", "package x\nvar table = map[string]string{\"SOME_KEY\": \"v\"}\n", false},
		{"E03-os-lookupenv-literal", "package x\nimport \"os\"\nfunc f() { _, _ = os.LookupEnv(\"SOME_KEY\") }\n", true},
		{"E04-constant-name-and-getenv", "package x\nimport \"os\"\nconst envKey = \"SOME_KEY\"\nfunc f() string { return os.Getenv(envKey) }\n", true},
		{"E05-secrets-getenv-family", "package x\nfunc f() string { return envsecrets.GetenvSecret(\"SOME_KEY\") }\n", true},
		{"E06-lookup-function-passed-in", "package x\nfunc f(lookup func(string) (string, bool)) { lookup(\"SOME_KEY\") }\n", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := len(scanEnvReaders(map[string]string{"internal/x/x.go": c.src})["SOME_KEY"]) > 0
			if got != c.want {
				t.Errorf("%s: SOME_KEY read = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// Two limits of the DSN checks are accepted and pinned here, so a change to either is seen and decided:
//   - A DSN held under a name the chart does not know, in a ConfigMap or an operator-created Secret, is not seen by the
//     chart. The process start guard still refuses a worker process whose environment carries a PostgreSQL URL under any name.
//   - A libpq key=value connection string (not a URL) under an unknown name is not recognised by the start guard. Closing
//     this needs a change to dsnclass (production code), which is out of scope of this test-only leaf.
func dsnAliasLimits(g *guard, check func(class string, environ []string) error) {
	g.expect(check(dsnclass.River, []string{"UNKNOWN_ALIAS=postgresql://u:p@h:5432/db"}) != nil,
		"the start guard no longer refuses a DSN URL under an unknown name: the accepted limit text is stale")
	g.expect(check(dsnclass.River, []string{"UNKNOWN_ALIAS=host=h port=5432 dbname=d user=u"}) == nil,
		"the start guard now refuses a libpq key=value string under an unknown name: close the accepted limit and update its text")
}
