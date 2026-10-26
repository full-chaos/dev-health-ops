package local

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The reset-link binding holds only while every write of public.users.password_hash
// also raises token_version. This census reads every SQL string literal of the
// module's non-test Go files: each UPDATE of the go-api users table that sets
// password_hash must set token_version in the same statement, and the set of
// such writers, and of the INSERTs that create a row with a password, is pinned
// so a new writer is reviewed here first.
var (
	passwordUpdate = regexp.MustCompile(`(?is)^\s*UPDATE\s+(public\.)?users\s+SET\b.*\bpassword_hash\s*=`)
	passwordInsert = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(public\.)?users\s*\(.*\bpassword_hash\b`)
	setsToken      = regexp.MustCompile(`(?i)\btoken_version\s*=`)
)

// passwordUpdaters are the writers that change an existing user's password; each
// raises token_version in the same statement.
var passwordUpdaters = []string{
	"internal/api/session/changepassword.go",
	"internal/api/session/verify.go",
	"internal/apiservice/admin/userstore.go",
	"internal/auth/identity/publicusers.go",
}

// passwordInserters create a new row: no link can be bound to a principal that
// did not exist when the link was issued.
var passwordInserters = []string{
	"internal/api/session/register.go",
	"internal/api/session/social.go",
	"internal/apiservice/admin/userstore.go",
	"internal/auth/identity/publicusers.go",
	"internal/testsupport/sessionscenario/sessionscenario.go",
}

func TestEveryPasswordWriteOfPublicUsersRaisesTokenVersion(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	updaters, inserters := map[string]bool{}, map[string]bool{}
	scanned := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			sql, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			switch {
			case passwordUpdate.MatchString(sql):
				updaters[rel] = true
				if !setsToken.MatchString(sql) {
					t.Errorf("%s: a password_hash UPDATE of users does not set token_version in the same statement: %q", rel, sql)
				}
			case passwordInsert.MatchString(sql):
				inserters[rel] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Fatalf("scanned %d Go files; the census walked too little of the module", scanned)
	}
	if got := keys(updaters); strings.Join(got, ",") != strings.Join(passwordUpdaters, ",") {
		t.Errorf("password_hash UPDATE writers of users = %v, want %v (review the binding before adding one)", got, passwordUpdaters)
	}
	if got := keys(inserters); strings.Join(got, ",") != strings.Join(passwordInserters, ",") {
		t.Errorf("password_hash INSERT writers of users = %v, want %v (review the binding before adding one)", got, passwordInserters)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
