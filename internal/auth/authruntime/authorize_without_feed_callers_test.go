package authruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// withoutFeedCallerDirs are the only non-test package directories that may name AuthorizeRoutesWithoutFeed: the test
// decision point. auth-service builds its routes with no wrap (runtime.go:601, authorizeRoutes(..., feed.decisions)).
var withoutFeedCallerDirs = []string{"internal/testsupport/authplane"}

type withoutFeedSite struct {
	file string
	line int
}

// withoutFeedSkipped are the directories the walk does not enter: vendored third-party code, fixtures, dot dirs.
var withoutFeedSkipped = []string{"third_party", "vendor", "testdata", "node_modules"}

// withoutFeedSites is every non-test use of the name AuthorizeRoutesWithoutFeed in the module's Go files (every
// directory but withoutFeedSkipped) but its declaration: a call, a qualified or unqualified reference, a method value.
func withoutFeedSites(t *testing.T, root string) []withoutFeedSite {
	t.Helper()
	var sites []withoutFeedSite
	fset := token.NewFileSet()
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (strings.HasPrefix(d.Name(), ".") || slices.Contains(withoutFeedSkipped, d.Name())) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		var declared *ast.Ident
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "AuthorizeRoutesWithoutFeed" {
				declared = fn.Name
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == "AuthorizeRoutesWithoutFeed" && id != declared {
				rel, _ := filepath.Rel(root, path)
				sites = append(sites, withoutFeedSite{file: filepath.ToSlash(rel), line: fset.Position(id.Pos()).Line})
			}
			return true
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return sites
}

// AuthorizeRoutesWithoutFeed takes a wrap of the decision state: a seam for a planted defect. No production package
// may reach it; the test decision point is its one caller.
func TestAuthorizeRoutesWithoutFeedHasNoProductionCaller(t *testing.T) {
	sites := withoutFeedSites(t, "../../..")
	allowed := 0
	for _, s := range sites {
		if slices.Contains(withoutFeedCallerDirs, filepath.ToSlash(filepath.Dir(s.file))) {
			allowed++
			continue
		}
		t.Errorf("%s:%d names AuthorizeRoutesWithoutFeed: only %v may (a test seam; auth-service wraps nothing, runtime.go:601)", s.file, s.line, withoutFeedCallerDirs)
	}
	if allowed == 0 {
		t.Fatal("no use in internal/testsupport/authplane: the walk found nothing, so it proves nothing")
	}
}
