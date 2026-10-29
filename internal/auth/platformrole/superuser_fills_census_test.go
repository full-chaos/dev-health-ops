package platformrole

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// wideUsersRead is a statement shape that carries every column of users,
// is_superuser included, into Go without naming it.
var wideUsersRead = regexp.MustCompile(`(?i)\b(?:users|u)\.\*|select\s+\*\s+from\s+(?:public\.)?users\b|returning\s+\*|returning[^;]*\bis_superuser\b`)

// TestNoWideReadOfUsers: no non-test string literal reads users wide
// (users.*, u.*, SELECT * FROM users, RETURNING * or RETURNING is_superuser).
// A wide read would carry is_superuser into Go outside the switched reads.
// SQL assembled from pieces names its columns in some literal, which
// TestEveryPublicSuperuserLiteralIsListed sees; a read into a superuser field,
// however its SQL is built, is TestEverySuperuserFillIsListed's.
func TestNoWideReadOfUsers(t *testing.T) {
	walkGo(t, func(rel string, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && wideUsersRead.MatchString(lit.Value) {
				t.Errorf("%s: a wide read of users (%s); name the columns, and read is_superuser through policy.PlatformRoleRead", rel, firstLine(lit.Value))
			}
			return true
		})
	})
}

// TestEverySuperuserFillIsListed is the census of the Go values filled from
// the column: every non-test call that takes the address of an IsSuperuser
// field (any field named *superuser*) or of a variable named *superuser* (a Scan destination, or a helper
// that scans), by file and function, with its kind (testdata/superuser_fills.tsv).
// A fill of is_superuser outside the switched reads fails here until it is
// classified.
func TestEverySuperuserFillIsListed(t *testing.T) {
	listed := readLiteralRowsFrom(t, "superuser_fills.tsv")
	found := map[string]int{}
	walkGo(t, func(rel string, file *ast.File) {
		for function, n := range superuserFills(file) {
			found[rel+"\t"+function] += n
		}
	})
	for key, row := range listed {
		if got := found[key]; got != row.count {
			t.Errorf("%s: %d superuser fills, listed %d (%s): update the census", strings.ReplaceAll(key, "\t", " "), got, row.count, row.kind)
		}
	}
	var unlisted []string
	for key := range found {
		if _, ok := listed[key]; !ok {
			unlisted = append(unlisted, key)
		}
	}
	sort.Strings(unlisted)
	for _, key := range unlisted {
		t.Errorf("unlisted fill of a superuser value (classify it in testdata/superuser_fills.tsv; a read of the column goes through policy.PlatformRoleRead): %s\t%d", key, found[key])
	}
}

func superuserFills(file *ast.File) map[string]int {
	counts := map[string]int{}
	superuserName := regexp.MustCompile(`(?i)superuser`)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			for _, arg := range call.Args {
				unary, ok := arg.(*ast.UnaryExpr)
				if !ok || unary.Op != token.AND {
					continue
				}
				switch x := unary.X.(type) {
				case *ast.SelectorExpr:
					if superuserName.MatchString(x.Sel.Name) {
						counts[funcName(fn)]++
					}
				case *ast.Ident:
					if superuserName.MatchString(x.Name) {
						counts[funcName(fn)]++
					}
				}
			}
			return true
		})
	}
	return counts
}

func walkGo(t *testing.T, visit func(rel string, file *ast.File)) {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			visit(filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator))), file)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// The census probes see the shapes they name, and only those.
func TestTheSuperuserCensusProbesSeeTheirShapes(t *testing.T) {
	for _, wide := range []string{"SELECT * FROM users WHERE id = $1", "select u.* from users u", "SELECT users.* FROM users",
		"INSERT INTO users (id) VALUES ($1) RETURNING *", "UPDATE users SET x = 1 RETURNING id, is_superuser"} {
		if !wideUsersRead.MatchString(wide) {
			t.Errorf("a wide read is not seen: %q", wide)
		}
	}
	for _, narrow := range []string{"SELECT id, email FROM users WHERE id = $1", "SELECT * FROM organizations", "RETURNING id"} {
		if wideUsersRead.MatchString(narrow) {
			t.Errorf("a narrow read is taken for wide: %q", narrow)
		}
	}
	src := `package p
func f(row Row, u *User) {
	var isSuperuser, active bool
	row.Scan(&active, &isSuperuser)
	row.Scan(&u.IsSuperuser, &u.Superuser, &u.Email)
}`
	file, err := parser.ParseFile(token.NewFileSet(), "p.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	if got := superuserFills(file); got["f"] != 3 || len(got) != 1 {
		t.Errorf("superuserFills = %v, want f: 3", got)
	}
}
