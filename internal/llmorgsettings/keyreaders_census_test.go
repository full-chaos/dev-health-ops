package llmorgsettings

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The census of BYO api_key readers (CHAOS-13096): every read of a stored key
// goes through the one accessor (ReadScopeKey, and the loader's binding), which
// withholds a key older than its provider or base_url row. A new reader that
// reads the key row another way fails here.

// keyReaders are the readers, by package directory and function, and the
// accessor call each one must make.
var keyReaders = []struct{ reader, dir, function, via string }{
	{"runtime: investigate, explain, categorize worker, acr llm-config", "internal/llmorgsettings", "loadRawSettingsFrom", "markUnboundKeys"},
	{"readiness probe, live model list, status fingerprint", "internal/apiservice/admin", "loadReadinessBYOConfig", "scopeKey"},
	{"spend: own model active", "internal/apiservice/admin", "orgLLMActive", "scopeKey"},
	{"status", "internal/apiservice/admin", "evaluateOrgLLMStatus", "scopeKey"},
	{"GET default + purpose (masked)", "internal/apiservice/admin", "llmSettingValue", "scopeKey"},
	{"write: base_url leak check of a purpose key", "internal/apiservice/admin", "storedPurposeAPIKey", "scopeKey"},
	{"write: provider/base_url rule and purpose key need", "internal/apiservice/admin", "keyStepFor", "scopeKey"},
	{"write: provider/base_url rule and purpose key need", "internal/apiservice/admin", "purposeNeedsKey", "scopeKey"},
	{"GET default (masked)", "internal/apiservice/admin", "llmSettingsResponse", "llmSettingValue"},
	{"GET purpose (masked)", "internal/apiservice/admin", "llmPurposeObject", "llmSettingValue"},
}

// forChecksUsers are the only functions that take the stored key whatever its
// binding (ScopeKey.ForChecks): the masked display, the base_url leak checks.
// Every other reader takes ScopeKey.Served.
var forChecksUsers = map[string]bool{
	"internal/apiservice/admin.llmSettingValue":     true,
	"internal/apiservice/admin.storedPurposeAPIKey": true,
	"internal/apiservice/admin.bindDefaultKey":      true,
}

// settingReads are the functions that read one settings row by name.
var settingReads = map[string]bool{"settingByKeyOn": true, "settingText": true, "settingRowForWrite": true}

// rowForWriteUsers are the only callers of the unguarded row read: the write.
var rowForWriteUsers = map[string]bool{
	"internal/apiservice/admin.upsertSetting":   true,
	"internal/apiservice/admin.settingByKeyOn": true,
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test")
		}
		dir = parent
	}
}

// namesKeyRow is whether expr is a literal naming a BYO api_key row, or a
// PurposeKey call whose field is "api_key".
func namesKeyRow(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return false
		}
		text, err := strconv.Unquote(e.Value)
		return err == nil && (text == "api_key" || strings.HasSuffix(text, ":api_key"))
	case *ast.Ident:
		return e.Name == "keyAPIKey"
	case *ast.CallExpr:
		if callName(e) == "PurposeKey" && len(e.Args) == 2 {
			return namesKeyRow(e.Args[1])
		}
	}
	return false
}

func callName(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

type parsedFile struct {
	path string
	file *ast.File
}

func goSources(t *testing.T, root string) []parsedFile {
	t.Helper()
	var out []parsedFile
	fset := token.NewFileSet()
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "testdata" || d.Name() == "vendor") {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			out = append(out, parsedFile{filepath.ToSlash(rel), file})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(out) < 100 {
		t.Fatalf("the census parsed %d Go files: it measures nothing", len(out))
	}
	return out
}

func TestEveryBYOKeyReadGoesThroughTheAccessor(t *testing.T) {
	files := goSources(t, repoRoot(t))
	found := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f.file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if settingReads[callName(node)] {
					for _, arg := range node.Args {
						if namesKeyRow(arg) {
							t.Errorf("%s: %s reads a BYO api_key row directly; read it through the accessor", f.path, callName(node))
						}
					}
				}
			case *ast.BasicLit:
				if node.Kind == token.STRING {
					text, _ := strconv.Unquote(node.Value)
					lower := strings.ToLower(text)
					if strings.Contains(lower, "api_key") && (strings.Contains(lower, "from settings") || strings.Contains(lower, "join settings") ||
						strings.Contains(lower, "public.settings") || strings.Contains(lower, "categorize_byo_settings")) {
						t.Errorf("%s: SQL names a BYO api_key row: read it through the accessor", f.path)
					}
				}
			}
			return true
		})
		for _, decl := range f.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			owner := filepath.ToSlash(filepath.Dir(f.path)) + "." + fn.Name.Name
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok && callName(call) == "ForChecks" && !forChecksUsers[owner] {
					t.Errorf("%s takes the stored key whatever its binding (ForChecks); a reader takes Served", owner)
				}
				if call, ok := n.(*ast.CallExpr); ok && callName(call) == "settingRowForWrite" && !rowForWriteUsers[owner] {
					t.Errorf("%s reads a row through the write's unguarded read", owner)
				}
				return true
			})
			for _, r := range keyReaders {
				if fn.Name.Name != r.function || filepath.ToSlash(filepath.Dir(f.path)) != r.dir {
					continue
				}
				calls := false
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok && callName(call) == r.via {
						calls = true
					}
					return true
				})
				found[r.dir+"."+r.function] = true
				if !calls {
					t.Errorf("%s %s (%s) does not read the key through %s", r.dir, r.function, r.reader, r.via)
				}
			}
		}
	}
	for _, r := range keyReaders {
		if !found[r.dir+"."+r.function] {
			t.Errorf("reader %s %s (%s) is gone: update the census", r.dir, r.function, r.reader)
		}
	}
}
