package aclguard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// A GRANT or REVOKE statement literal: the privilege list, then ON, TO or FROM.
var privilegeStatement = regexp.MustCompile(`(?is)\b(GRANT|REVOKE)\b\s+[A-Za-z_ ,()]*?\s*\b(ON|TO|FROM)\b`)

// A SQL line comment may quote a statement without running it.
var sqlComment = regexp.MustCompile(`--[^\n]*`)

var executingCalls = map[string]bool{"Exec": true, "ExecContext": true}

// Trees whose privilege statements are not the shared Postgres database's: ClickHouse grants, and the test fixtures
// that own a throwaway database.
var skippedDirs = []string{"internal/storage/clickhouse", "internal/cigates", "internal/testsupport"}

type census struct {
	scanned   int
	lockers   []string
	unguarded []string
}

// scan reports the non-test Go files under root that hold a GRANT or REVOKE literal and execute SQL without calling
// aclguard.Lock in the same file.
func scan(root string) (census, error) {
	var c census
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == "aclguard" && strings.HasSuffix(rel, "storage/postgres/aclguard") {
				return filepath.SkipDir
			}
			for _, skip := range skippedDirs {
				if rel == skip {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		c.scanned++
		var statement, executes, locks bool
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					if s, err := strconv.Unquote(x.Value); err == nil && privilegeStatement.MatchString(sqlComment.ReplaceAllString(s, "")) {
						statement = true
					}
				}
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					if executingCalls[sel.Sel.Name] {
						executes = true
					}
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == "aclguard" && sel.Sel.Name == "Lock" {
						locks = true
					}
				}
			}
			return true
		})
		if locks {
			c.lockers = append(c.lockers, rel)
		}
		if statement && executes && !locks {
			c.unguarded = append(c.unguarded, rel)
		}
		return nil
	})
	sort.Strings(c.lockers)
	sort.Strings(c.unguarded)
	return c, err
}

func TestEveryPrivilegeWriterOfTheSharedDatabaseTakesTheLock(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	c, err := scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if c.scanned < 500 {
		t.Fatalf("the census scanned %d Go files; it must see the whole module", c.scanned)
	}
	want := []string{
		"internal/storage/postgres/authschema/grants.go",
		"internal/storage/river/migrate.go",
		"internal/storage/roleprovision/roleprovision.go",
	}
	if strings.Join(c.lockers, ",") != strings.Join(want, ",") {
		t.Fatalf("files that take aclguard.Lock = %q, want %q", c.lockers, want)
	}
	if len(c.unguarded) > 0 {
		t.Fatalf("these files run GRANT or REVOKE without aclguard.Lock in the same file (two writers at once fail with SQLSTATE XX000): %q", c.unguarded)
	}
}

func TestCensusFlagsAPlantedWriterAndNotAGuardedOne(t *testing.T) {
	const writer = "package p\n\nimport \"context\"\n\ntype tx interface{ Exec(context.Context, string, ...any) (int, error) }\n\nfunc grant(ctx context.Context, t tx) error {\n%s\t_, err := t.Exec(ctx, `GRANT SELECT ON TABLE public.users TO someone`)\n\treturn err\n}\n"
	for _, c := range []struct {
		name, lock string
		flagged    bool
	}{
		{"unguarded", "", true},
		{"guarded", "\tif err := aclguard.Lock(ctx, nil); err != nil {\n\t\treturn err\n\t}\n", false},
	} {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "internal", "x"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "internal", "x", "w.go"), []byte(fmt.Sprintf(writer, c.lock)), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := scan(dir)
		if err != nil {
			t.Fatal(err)
		}
		if flagged := len(got.unguarded) == 1 && got.unguarded[0] == "internal/x/w.go"; flagged != c.flagged {
			t.Fatalf("%s writer: census %+v, flagged want %v", c.name, got, c.flagged)
		}
	}
}

func TestCensusOfAnEmptyTreeSeesNothing(t *testing.T) {
	got, err := scan(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if got.scanned != 0 || len(got.lockers) != 0 {
		t.Fatalf("empty tree: %+v", got)
	}
}
