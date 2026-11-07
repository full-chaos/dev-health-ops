package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// stringLiteralsOf returns every string literal in the named function of file.
func stringLiteralsOf(t *testing.T, file, fn string) []string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	found := false
	for _, decl := range parsed.Decls {
		f, ok := decl.(*ast.FuncDecl)
		if !ok || f.Name.Name != fn {
			continue
		}
		found = true
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if text, err := strconv.Unquote(lit.Value); err == nil {
					out = append(out, text)
				}
			}
			return true
		})
	}
	if !found {
		t.Fatalf("%s has no func %s", file, fn)
	}
	return out
}

// authCopyStatus matches a read of an auth.principals or auth.organizations
// copy: the quoted schema is spliced in at run time, so the literal holds
// ".principals" or ".organizations" after it.
var authCopyStatus = regexp.MustCompile(`\.(principals|organizations)\b|\bdisabled_at\b`)

// D6651: the public rows decide, and an auth copy is never a decision input.
// The session issue statement and the refresh lock read public.users only.
func TestSessionIssueAndRefreshLockReadPublicStatusOnly(t *testing.T) {
	for _, site := range []struct{ file, fn string }{
		{"store.go", "lockSession"},
		{"signin.go", "insertSessionAs"},
		{"signin.go", "projectCopies"},
	} {
		joined := strings.Join(stringLiteralsOf(t, site.file, site.fn), "\n")
		if m := authCopyStatus.FindString(joined); m != "" {
			t.Errorf("%s %s reads the auth copy (%q): decide on public.users.is_active", site.file, site.fn, m)
		}
	}
	issue := strings.Join(stringLiteralsOf(t, "signin.go", "insertSessionAs"), "\n")
	if !strings.Contains(issue, "public.users u") || !strings.Contains(issue, "u.is_active") {
		t.Error("the session issue statement does not gate on public.users.is_active")
	}
	lock := strings.Join(stringLiteralsOf(t, "store.go", "lockSession"), "\n")
	if !strings.Contains(lock, "public.users pu") || !strings.Contains(lock, "pu.is_active") {
		t.Error("lockSession does not read public.users.is_active")
	}
}
