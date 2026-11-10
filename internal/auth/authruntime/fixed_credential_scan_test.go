package authruntime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
)

// A test credential is built at run time. No fixed credential-shaped literal (32 or more base64url characters mixing
// upper case, lower case and digits) and no fixed role password (a password name bound to a string literal, or an
// inline SQL role password literal) stands in the package's sources, where a secret scanner would read it as a key.
func TestNoFixedCredentialLiteralInThePackage(t *testing.T) {
	shaped := regexp.MustCompile("[\"`][A-Za-z0-9_+=-]{32,}[\"`]")
	mixed := func(text string) bool {
		return strings.ContainsAny(text, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && strings.ContainsAny(text, "abcdefghijklmnopqrstuvwxyz") && strings.ContainsAny(text, "0123456789")
	}
	bound := regexp.MustCompile("(?i)\\b[a-z0-9_]*passw(or)?d[a-z0-9_]*\\s*:?=\\s*[\"`]")
	inline := regexp.MustCompile(`(?i)\bpassword\s+'[^'%]`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	sources := map[string][]byte{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		raw, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		sources[entry.Name()] = raw
		for number, line := range strings.Split(string(raw), "\n") {
			for _, found := range shaped.FindAllString(line, -1) {
				if mixed(found) {
					t.Errorf("%s:%d holds a fixed credential-shaped literal of %d characters", entry.Name(), number+1, len(found)-2)
				}
			}
			if bound.MatchString(line) || inline.MatchString(line) {
				t.Errorf("%s:%d holds a fixed password literal", entry.Name(), number+1)
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no Go source scanned: the scan measured nothing")
	}
	aliased, parsed, err := aliasedPasswordFindings(sources)
	if err != nil {
		t.Fatal(err)
	}
	if parsed == 0 || parsed != scanned {
		t.Fatalf("the alias pass parsed %d of %d Go sources: it measured nothing", parsed, scanned)
	}
	for _, finding := range aliased {
		t.Errorf("%s holds a fixed password bound to a literal and formatted into a role password", finding)
	}
}

// aliasedPasswordFindings parses every source and reports, as "<line> <file>", each call whose format holds
// PASSWORD '%s' and takes a string literal, or an identifier bound to a string literal anywhere in the package.
// Residual: a password built by string concatenation is not seen.
func aliasedPasswordFindings(sources map[string][]byte) (findings []string, parsed int, err error) {
	format := regexp.MustCompile(`(?i)password\s+'%s'`)
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for name, src := range sources {
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			return nil, 0, err
		}
		files[name] = file
	}
	bound := map[string]bool{}
	isString := func(expr ast.Expr) bool {
		lit, ok := expr.(*ast.BasicLit)
		return ok && lit.Kind == token.STRING
	}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if i < len(n.Values) && isString(n.Values[i]) {
						bound[name.Name] = true
					}
				}
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if ident, ok := lhs.(*ast.Ident); ok && i < len(n.Rhs) && isString(n.Rhs[i]) {
						bound[ident.Name] = true
					}
				}
			}
			return true
		})
	}
	for name, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			for i, arg := range call.Args {
				lit, ok := arg.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				at := format.FindStringIndex(lit.Value)
				if at == nil {
					continue
				}
				verbs := 0
				for j := 0; j < at[0]; j++ {
					if lit.Value[j] != '%' {
						continue
					}
					if lit.Value[j+1] == '%' {
						j++
						continue
					}
					verbs++
				}
				if i+1+verbs >= len(call.Args) {
					continue
				}
				target := call.Args[i+1+verbs]
				ident, isIdent := target.(*ast.Ident)
				if (isIdent && bound[ident.Name]) || isString(target) {
					findings = append(findings, fmt.Sprintf("%d %s", fset.Position(target.Pos()).Line, name))
				}
			}
			return true
		})
	}
	return findings, len(files), nil
}

func TestAliasedPasswordConstIsFound(t *testing.T) {
	src, err := os.ReadFile("testdata/pw_alias_fixture.txt")
	if err != nil {
		t.Fatal(err)
	}
	found, parsed, err := aliasedPasswordFindings(map[string][]byte{"pw_alias_fixture.go": src})
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 1 {
		t.Fatalf("parsed %d files, want 1", parsed)
	}
	want := map[int]bool{8: true, 13: true, 17: true, 30: true}
	for _, line := range found {
		var number int
		if _, err := fmt.Sscanf(line, "%d", &number); err != nil {
			t.Fatalf("finding %q has no leading line number", line)
		}
		if !want[number] {
			t.Errorf("unexpected finding %q", line)
		}
		delete(want, number)
	}
	for number := range want {
		t.Errorf("fixture line %d is not reported", number)
	}
}

func TestAliasedPasswordConstInAnotherFileIsFound(t *testing.T) {
	found, parsed, err := aliasedPasswordFindings(map[string][]byte{
		"a.go": []byte("package p\n\nconst pw = \"x1\"\n"),
		"b.go": []byte("package p\n\nimport \"fmt\"\n\nfunc f() string {\n\treturn fmt.Sprintf(`CREATE ROLE r LOGIN PASSWORD '%s'`, pw)\n}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 2 || len(found) != 1 || found[0] != "6 b.go" {
		t.Fatalf("parsed %d, findings %q", parsed, found)
	}
}

const aliasCaseHead = `package p

import "fmt"

func build() string { return "" }

`

func aliasCaseFindings(t *testing.T, body string) []string {
	t.Helper()
	found, parsed, err := aliasedPasswordFindings(map[string][]byte{"case.go": []byte(aliasCaseHead + body)})
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 1 {
		t.Fatalf("parsed %d files, want 1", parsed)
	}
	return found
}

func TestNamedFormatBindingsAreFound(t *testing.T) {
	cases := map[string]string{
		"const": `const f = "CREATE ROLE r LOGIN PASSWORD '%s'"
const pw = "x1"

func a() string { return fmt.Sprintf(f, pw) }
`,
		"var": `var f = "CREATE ROLE r LOGIN PASSWORD '%s'"
const pw = "x1"

func a() string { return fmt.Sprintf(f, pw) }
`,
		"short-declaration": `const pw = "x1"

func a() string {
	f := "CREATE ROLE r LOGIN PASSWORD '%s'"
	return fmt.Sprintf(f, pw)
}
`,
		"assignment": `const pw = "x1"

func a() string {
	var f string
	f = "CREATE ROLE r LOGIN PASSWORD '%s'"
	return fmt.Sprintf(f, pw)
}
`,
	}
	for name, body := range cases {
		if found := aliasCaseFindings(t, body); len(found) != 1 {
			t.Errorf("%s: findings %q, want one", name, found)
		}
	}
	if found := aliasCaseFindings(t, `func a() string {
	return fmt.Sprintf("%s%s", "x PASSWORD '%s'", "y")
}
`); len(found) != 0 {
		t.Errorf("a marker in a data argument is flagged: %q", found)
	}
}

func TestIdentifierBindingsFollowObjects(t *testing.T) {
	use := `func b(role string) string {
	value := build()
	return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
}
`
	cases := map[string]string{
		"const-other-scope":         "func a() { const value = \"x\"; _ = value }\n\n" + use,
		"var-other-scope":           "func a() { var value = \"x\"; _ = value }\n\n" + use,
		"short-declaration-other":   "func a() { value := \"x\"; _ = value }\n\n" + use,
		"assignment-other-function": "func a() { var value string; value = \"x\"; _ = value }\n\n" + use,
		"overwritten": `func b(role string) string {
	value := "x"
	value = build()
	return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
}
`,
		"nested-shadow": `func b(role string) string {
	value := "x"
	_ = value
	{
		value := build()
		return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
	}
}
`,
	}
	for name, body := range cases {
		if found := aliasCaseFindings(t, body); len(found) != 0 {
			t.Errorf("%s: findings %q, want none", name, found)
		}
	}
	if found := aliasCaseFindings(t, `func b(role string) string {
	value := "x"
	return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
}
`); len(found) != 1 {
		t.Errorf("a literal-bound value in the same function: findings %q, want one", found)
	}
}
