// Package membershipreads refuses a go-api or query-api read of the auth schema's memberships or organizations tables
// (CHAOS-10985, acp OPTION P, D6358).
//
// users, organizations and memberships stay in the public tables. auth-service writes the auth rows and the public rows in
// one transaction, so go-api and query-api read public.memberships and public.organizations only: a read of the auth
// copies would make them a second read authority and would need an auth-schema GRANT that nobody holds. The query-api
// login holds no privilege on those two auth tables; this gate is the code side of the same rule.
//
// A SITE is a string literal (a raw string, an interpreted string, or a concatenation of literals folded into one) of
// production code under one of Scopes that names `<qualifier>.memberships` or `<qualifier>.organizations` where the
// qualifier is anything but `public`: auth, a quoted "auth", a Sprintf verb (%s), or no visible qualifier at all (a
// literal that starts with ".memberships" because the schema is concatenated in front). Case and spaces around the dot do
// not matter. There is no allow list: a read of those tables has no legitimate place in these packages.
//
// NOT covered, stated: code outside Scopes (the shared storage packages, internal/auth/**, which is auth-service and holds
// the writes), an unqualified `memberships` that a search_path resolves to the auth schema (the query-api and go-api
// logins pin their search_path and the posture test pins their grants), SQL assembled from pieces no literal fold
// reaches (a name built by a function call or a variable), and _test.go files.
package membershipreads

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Scope is one source root and the service whose code lives there.
type Scope struct {
	Root    string
	Service string
}

// Scopes are the production packages of go-api and query-api. Every root is a directory below the module root.
var Scopes = []Scope{
	{"internal/api", "go-api"},
	{"internal/apiservice", "go-api"},
	{"internal/queryapi", "query-api"},
	{"internal/queryapiservice", "query-api"},
}

var (
	// tableDot finds `.memberships` / `.organizations`: the dot, optional spaces and an optional quote, then the table name.
	tableDot = regexp.MustCompile(`(?i)\.\s*"?(memberships|organizations)\b`)
	// qualifierEnd is the token that ends a string: a quoted identifier or a bare word (letters, digits, underscore, a % verb).
	qualifierEnd = regexp.MustCompile(`("[^"]*"|[A-Za-z0-9_%$]+)\s*$`)
)

// Check scans the non-test .go files below each scope root of root and returns one named finding per site, sorted. A file
// that does not parse is an error: a gate that skipped it would scan nothing.
func Check(root string, scopes []Scope) ([]string, error) {
	var findings []string
	for _, scope := range scopes {
		err := walk(root, scope, func(rel string, fset *token.FileSet, file *ast.File) {
			ast.Inspect(file, func(n ast.Node) bool {
				text, ok := foldString(n)
				if !ok {
					return true
				}
				for _, ref := range refs(text) {
					findings = append(findings, fmt.Sprintf("%s:%d: membership-read: %s code names %s: go-api and query-api read public.memberships and public.organizations only; auth-service writes both copies in one transaction (D6358, CHAOS-10985)",
						rel, fset.Position(n.Pos()).Line, scope.Service, ref))
				}
				return false
			})
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(findings)
	return findings, nil
}

// Packages is the sorted list of module-relative package directories Check scans (one per directory that holds a
// non-test .go file), so a reviewer can cite every package the gate reads.
func Packages(root string, scopes []Scope) ([]string, error) {
	seen := map[string]bool{}
	for _, scope := range scopes {
		err := walk(root, scope, func(rel string, _ *token.FileSet, _ *ast.File) { seen[path.Dir(rel)] = true })
		if err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}

func walk(root string, scope Scope, visit func(rel string, fset *token.FileSet, file *ast.File)) error {
	base := filepath.Join(root, filepath.FromSlash(scope.Root))
	return filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == base {
				return nil // a scope root that does not exist scans nothing; the real-tree test requires every root to hold code
			}
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if p != base && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("membershipreads: cannot parse %s: %w", rel, err)
		}
		visit(rel, fset, file)
		return nil
	})
}

// foldString is the text of a string literal, or of a `+` chain of literals, which is folded into one string. A chain
// with any non-literal operand is not folded as a whole: its literal operands are visited one by one.
func foldString(n ast.Node) (string, bool) {
	switch e := n.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(e.Value)
		return s, err == nil
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		left, ok := foldOperand(e.X)
		if !ok {
			return "", false
		}
		right, ok := foldOperand(e.Y)
		if !ok {
			return "", false
		}
		return left + right, true
	}
	return "", false
}

func foldOperand(e ast.Expr) (string, bool) {
	if p, ok := e.(*ast.ParenExpr); ok {
		return foldOperand(p.X)
	}
	return foldString(e)
}

// refs lists the references in text to a table that is not in the public schema, as they are written.
func refs(text string) []string {
	var out []string
	for _, m := range tableDot.FindAllStringSubmatchIndex(text, -1) {
		table := strings.ToLower(text[m[2]:m[3]])
		qualifier := ""
		if q := qualifierEnd.FindString(text[:m[0]]); q != "" {
			qualifier = strings.TrimSpace(q)
		}
		if strings.Trim(strings.ToLower(qualifier), `"`) == "public" {
			continue
		}
		if qualifier == "" {
			out = append(out, `".`+table+`" with no public qualifier (the schema is concatenated in front of it)`)
			continue
		}
		out = append(out, fmt.Sprintf("%s.%s", strings.Trim(qualifier, `"`), table))
	}
	return out
}
